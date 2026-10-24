//go:build windows

package host

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unsafe"

	"golang.org/x/sys/windows"
)

// The Windows binding of aclAPI, tested on the machine it runs on without an
// elevated prompt: a process may always rewrite the DACL and the owner of a file
// it owns, so a temporary project directory exercises the real
// ACLFromEntries/SetNamedSecurityInfo path. What the test deliberately does not
// cover is the elevated half — naming BUILTIN\Administrators as the owner and
// resolving NT SERVICE\<service> — and hardening a directory a second time,
// which needs the WRITE_DAC only the elevated install has; U8-R covers both on a
// real machine.

// currentUserSID is the SID of the account running the test, which is the only
// owner a non-elevated process may assign.
func currentUserSID(t *testing.T) *windows.SID {
	t.Helper()
	tu, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		t.Fatalf("GetTokenUser: %v", err)
	}
	return tu.User.Sid
}

// testACL is the production binding with the two SIDs a non-elevated test can
// name: the service's account and the owner are both the current user.
func testACL(t *testing.T) windowsACL {
	t.Helper()
	self := currentUserSID(t)
	return windowsACL{
		lookup: func(string) (*windows.SID, error) { return self, nil },
		owner:  func() (*windows.SID, error) { return self, nil },
	}
}

func TestWindowsACLHarden(t *testing.T) {
	self := currentUserSID(t)
	dir := t.TempDir()
	writable := filepath.Join(dir, LogDir)
	readOnly := filepath.Join(dir, BinDir)
	for _, d := range []string{writable, readOnly} {
		if err := os.Mkdir(d, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	acl := testACL(t)
	targets := []aclTarget{{Path: readOnly}, {Path: writable, Write: true}}
	if err := acl.Harden(`NT SERVICE\piperoom-connect`, targets); err != nil {
		t.Fatalf("Harden: %v", err)
	}
	admins, _ := windows.CreateWellKnownSid(windows.WinBuiltinAdministratorsSid)
	system, _ := windows.CreateWellKnownSid(windows.WinLocalSystemSid)
	users, _ := windows.CreateWellKnownSid(windows.WinBuiltinUsersSid)
	for _, target := range targets {
		wantService := windows.ACCESS_MASK(aclReadExecute)
		if target.Write {
			wantService = aclModify
		}
		aces := readACEs(t, target.Path)
		want := []aceWant{
			{sid: admins.String(), mask: aclFullControl},
			{sid: system.String(), mask: aclFullControl},
			{sid: self.String(), mask: wantService},
			{sid: users.String(), mask: aclReadExecute},
		}
		if len(aces) != len(want) {
			t.Fatalf("%s: %d ACEs %v, want %d (%v)", target.Path, len(aces), aces, len(want), want)
		}
		for j := range want {
			if aces[j].sid != want[j].sid || aces[j].mask != want[j].mask {
				t.Errorf("%s ACE %d = %v, want %v", target.Path, j, aces[j], want[j])
			}
		}
	}
}

// TestWindowsACLProtectsTheDACL: the DACL is protected, so the ACEs the person's
// account inherits from the project directory do not apply on top of the ones
// install sets.
func TestWindowsACLProtectsTheDACL(t *testing.T) {
	dir := t.TempDir()
	if err := testACL(t).Harden("ignored", []aclTarget{{Path: dir}}); err != nil {
		t.Fatalf("Harden: %v", err)
	}
	sd, err := windows.GetNamedSecurityInfo(dir, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		t.Fatalf("GetNamedSecurityInfo: %v", err)
	}
	control, _, err := sd.Control()
	if err != nil {
		t.Fatalf("Control: %v", err)
	}
	if control&windows.SE_DACL_PROTECTED == 0 {
		t.Error("the DACL is not protected: the directory's inherited ACEs still apply")
	}
	if control&windows.SE_SELF_RELATIVE == 0 {
		t.Error("the descriptor is absolute; the ACE read-back below assumes the self-relative layout")
	}
}

// TestWindowsACLRefusals: a target that is gone, and an account that does not
// resolve, are errors the install reports — never a service reported as
// installed with nothing hardened.
func TestWindowsACLRefusals(t *testing.T) {
	dir := t.TempDir()
	if err := testACL(t).Harden("ignored", []aclTarget{{Path: filepath.Join(dir, "gone")}}); err == nil {
		t.Error("Harden of a missing directory returned no error")
	}
	acl := windowsACL{
		lookup: func(string) (*windows.SID, error) { return nil, windows.ERROR_NONE_MAPPED },
		owner:  func() (*windows.SID, error) { return currentUserSID(t), nil },
	}
	err := acl.Harden(`NT SERVICE\piperoom-not-installed`, []aclTarget{{Path: dir}})
	if err == nil || !strings.Contains(err.Error(), "resolving the service's account") {
		t.Fatalf("Harden: %v, want the account resolution refusal", err)
	}
	if !errors.Is(err, ErrServiceManager) {
		t.Errorf("err %v, want ErrServiceManager", err)
	}
}

// aceWant is one expected ACE: the trustee and the access mask.
type aceWant struct {
	sid  string
	mask windows.ACCESS_MASK
}

// readACEs reads the DACL of path and returns its ACEs in order. The descriptor
// GetNamedSecurityInfo returns is self-relative (x/sys documents that), so the
// DACL is at the offset the SD header holds rather than at a pointer the SDK
// helpers would dereference; GetAce then walks the real ACL the same way the
// access check does.
func readACEs(t *testing.T, path string) []aceWant {
	t.Helper()
	sd, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		t.Fatalf("GetNamedSecurityInfo(%s): %v", path, err)
	}
	// The self-relative header: revision, sbz1, control, then four 32-bit
	// offsets (owner, group, sacl, dacl) into the same buffer.
	offset := *(*uint32)(unsafe.Add(unsafe.Pointer(sd), 16))
	if offset == 0 {
		t.Fatalf("%s has no DACL", path)
	}
	acl := (*windows.ACL)(unsafe.Add(unsafe.Pointer(sd), offset))
	out := make([]aceWant, 0, acl.AceCount)
	for i := uint32(0); i < uint32(acl.AceCount); i++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(acl, i, &ace); err != nil {
			t.Fatalf("GetAce(%s, %d): %v", path, i, err)
		}
		sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
		out = append(out, aceWant{sid: sid.String(), mask: ace.Mask})
	}
	return out
}

// TestWindowsACLHardensTheFilesInside: a file the person's account put in a
// target before install (a jar, an older exe) gets the same protected DACL as
// the directory, not only the directory's inherited ACEs: SetNamedSecurityInfo
// never passes the owner down, and an owner can always rewrite a file's DACL.
func TestWindowsACLHardensTheFilesInside(t *testing.T) {
	self := currentUserSID(t)
	dir := t.TempDir()
	libs := filepath.Join(dir, DistDir, "libs")
	jar := filepath.Join(libs, "kafka-clients.jar")
	if err := os.MkdirAll(libs, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(jar, []byte("jar"), 0o644); err != nil {
		t.Fatal(err)
	}
	// The hardened entries grant this (non-elevated) test read and execute
	// only, so give it back full control before t.TempDir removes them.
	t.Cleanup(func() { restoreForCleanup(t, filepath.Join(dir, DistDir), self) })

	if err := testACL(t).Harden(`NT SERVICE\piperoom-connect`, []aclTarget{{Path: filepath.Join(dir, DistDir)}}); err != nil {
		t.Fatalf("Harden: %v", err)
	}
	for _, p := range []string{libs, jar} {
		sd, err := windows.GetNamedSecurityInfo(p, windows.SE_FILE_OBJECT, windows.DACL_SECURITY_INFORMATION)
		if err != nil {
			t.Fatalf("GetNamedSecurityInfo(%s): %v", p, err)
		}
		control, _, err := sd.Control()
		if err != nil {
			t.Fatalf("Control: %v", err)
		}
		if control&windows.SE_DACL_PROTECTED == 0 {
			t.Errorf("%s: the DACL is not protected", p)
		}
		aces := readACEs(t, p)
		if len(aces) != 4 || aces[2].sid != self.String() || aces[2].mask != aclReadExecute {
			t.Errorf("%s: ACEs %v, want the four of the target with the service read and execute", p, aces)
		}
	}
}

// restoreForCleanup gives the test's own account full control of root and
// everything under it, parents first so each directory can be listed again.
func restoreForCleanup(t *testing.T, root string, self *windows.SID) {
	t.Helper()
	acl, err := windows.ACLFromEntries([]windows.EXPLICIT_ACCESS{
		accessEntry(aclFullControl, self, windows.TRUSTEE_IS_USER),
	}, nil)
	if err != nil {
		t.Errorf("cleanup ACL: %v", err)
		return
	}
	_ = filepath.WalkDir(root, func(p string, _ os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if err := windows.SetNamedSecurityInfo(p, windows.SE_FILE_OBJECT,
			windows.DACL_SECURITY_INFORMATION|windows.PROTECTED_DACL_SECURITY_INFORMATION, nil, nil, acl, nil); err != nil {
			t.Errorf("cleanup %s: %v", p, err)
		}
		return nil
	})
}
