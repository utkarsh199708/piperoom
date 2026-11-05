//go:build windows

package host

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"unsafe"

	"golang.org/x/sys/windows"
)

// defaultProgramFiles is %ProgramFiles% from the known-folder API. The
// environment variable is not used: an account can set its own ProgramFiles
// in its environment, and the elevated install inherits it.
func defaultProgramFiles() (string, error) {
	dir, err := windows.KnownFolderPath(windows.FOLDERID_ProgramFiles, windows.KF_FLAG_DEFAULT)
	if err != nil {
		return "", fmt.Errorf("%w: locating Program Files: %w", ErrServiceManager, err)
	}
	return dir, nil
}

// defaultTrust is the production trustAPI.
func defaultTrust() trustAPI { return windowsTrust{} }

// windowsTrust reads owners and DACLs (trustAPI).
type windowsTrust struct{}

// trustedOwnerSIDs are the accounts allowed to own, and to write, what the
// service runs: SYSTEM, Administrators and TrustedInstaller (which owns the
// Windows and Program Files folders themselves).
var trustedOwnerSIDs = []string{
	"S-1-5-18",     // SYSTEM
	"S-1-5-32-544", // BUILTIN\Administrators
	"S-1-5-80-956008885-3418522649-1831038044-1853292631-2271478464", // NT SERVICE\TrustedInstaller
}

// placeholderSIDs name no account on an object's own ACE: CREATOR OWNER and
// CREATOR GROUP only shape what a child inherits, and OWNER RIGHTS is the
// owner, which is checked as the owner.
var placeholderSIDs = []string{"S-1-3-0", "S-1-3-1", "S-1-3-4"}

// The rights an untrusted account must not hold. On a file or directory the
// service loads, any right that changes it; on a directory above one, the
// rights that let a child be renamed or deleted, or the directory itself be
// re-permissioned (adding a file next to the path changes nothing it runs).
const (
	rightsChangeObject = windows.DELETE | windows.WRITE_DAC | windows.WRITE_OWNER |
		windows.GENERIC_ALL | windows.GENERIC_WRITE |
		fileWriteData | fileAppendData | fileWriteEA | fileDeleteChild
	rightsChangeAncestor = windows.DELETE | windows.WRITE_DAC | windows.WRITE_OWNER |
		windows.GENERIC_ALL | fileDeleteChild
)

// fileDeleteChild is FILE_DELETE_CHILD: delete or rename any entry of a
// directory, whatever the entry's own DACL says.
const fileDeleteChild = 0x0040

func (windowsTrust) CheckTrusted(path string, tree bool) error {
	trusted, err := sidsFromStrings(trustedOwnerSIDs)
	if err != nil {
		return err
	}
	placeholders, err := sidsFromStrings(placeholderSIDs)
	if err != nil {
		return err
	}
	clean := filepath.Clean(path)
	if !filepath.IsAbs(clean) {
		return fmt.Errorf("%w: %s is not an absolute path", ErrUntrusted, q(path))
	}
	// The path itself, then every directory above it up to the volume root.
	for p, first := clean, true; ; first = false {
		mask := windows.ACCESS_MASK(rightsChangeAncestor)
		if first {
			mask = rightsChangeObject
		}
		if err := checkObject(p, mask, trusted, placeholders); err != nil {
			return err
		}
		parent := filepath.Dir(p)
		if parent == p {
			break
		}
		p = parent
	}
	if !tree {
		return nil
	}
	return filepath.WalkDir(clean, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("%w: reading %s: %w", ErrUntrusted, q(p), err)
		}
		if p == clean {
			return nil
		}
		return checkObject(p, rightsChangeObject, trusted, placeholders)
	})
}

// checkObject refuses p when it is a reparse point, when its owner is not
// trusted, when it has no DACL (everyone has full control) or when an ACE that
// applies to it grants an untrusted account any of mask.
func checkObject(p string, mask windows.ACCESS_MASK, trusted, placeholders []*windows.SID) error {
	fi, err := os.Lstat(p)
	if err != nil {
		return fmt.Errorf("%w: %s: %w", ErrUntrusted, q(p), err)
	}
	if !fi.IsDir() && !fi.Mode().IsRegular() {
		return fmt.Errorf("%w: %s is a link or another reparse point", ErrUntrusted, q(p))
	}
	sd, err := windows.GetNamedSecurityInfo(p, windows.SE_FILE_OBJECT, windows.OWNER_SECURITY_INFORMATION|windows.DACL_SECURITY_INFORMATION)
	if err != nil {
		return fmt.Errorf("%w: reading the security of %s: %w", ErrUntrusted, q(p), err)
	}
	owner, _, err := sd.Owner()
	if err != nil {
		return fmt.Errorf("%w: reading the owner of %s: %w", ErrUntrusted, q(p), err)
	}
	if !sidIn(owner, trusted) {
		return fmt.Errorf("%w: %s is owned by %s, not SYSTEM, Administrators or TrustedInstaller", ErrUntrusted, q(p), owner.String())
	}
	dacl, _, err := sd.DACL()
	if err != nil {
		return fmt.Errorf("%w: %s has no DACL: %w", ErrUntrusted, q(p), err)
	}
	if dacl == nil {
		return fmt.Errorf("%w: %s has a NULL DACL (full control for everyone)", ErrUntrusted, q(p))
	}
	for i := uint32(0); i < uint32(dacl.AceCount); i++ {
		var ace *windows.ACCESS_ALLOWED_ACE
		if err := windows.GetAce(dacl, i, &ace); err != nil {
			return fmt.Errorf("%w: reading ACE %d of %s: %w", ErrUntrusted, i, q(p), err)
		}
		// Only an allow ACE grants anything; an inherit-only one applies to
		// the children, which the tree walk checks on their own.
		if ace.Header.AceType != windows.ACCESS_ALLOWED_ACE_TYPE || ace.Header.AceFlags&windows.INHERIT_ONLY_ACE != 0 {
			continue
		}
		if ace.Mask&mask == 0 {
			continue
		}
		sid := (*windows.SID)(unsafe.Pointer(&ace.SidStart))
		if sidIn(sid, trusted) || sidIn(sid, placeholders) {
			continue
		}
		return fmt.Errorf("%w: %s grants %s the right to change it (mask %#x)", ErrUntrusted, q(p), sid.String(), uint32(ace.Mask&mask))
	}
	return nil
}

func sidsFromStrings(ss []string) ([]*windows.SID, error) {
	out := make([]*windows.SID, 0, len(ss))
	for _, s := range ss {
		sid, err := windows.StringToSid(s)
		if err != nil {
			return nil, fmt.Errorf("host: building the SID %s: %w", s, err)
		}
		out = append(out, sid)
	}
	return out, nil
}

func sidIn(sid *windows.SID, set []*windows.SID) bool {
	if sid == nil {
		return false
	}
	for _, s := range set {
		if sid.Equals(s) {
			return true
		}
	}
	return false
}
