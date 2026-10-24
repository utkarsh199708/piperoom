//go:build windows

package host

import (
	"fmt"
	"runtime"

	"golang.org/x/sys/windows"
)

// The Windows binding of aclAPI: LookupSID resolves the service's virtual
// account, ACLFromEntries builds the DACL and SetNamedSecurityInfo writes it
// with an owner of its own. Every other decision (which paths, which rights)
// lives in acl.go and compiles on any OS.

// windowsACL is aclAPI over the Windows security APIs.
type windowsACL struct {
	// lookup resolves an account name to its SID (windows.LookupSID in
	// production). It is why install creates the service before it hardens
	// anything: the SCM registers NT SERVICE\<name> when CreateService runs with
	// the unrestricted SID type, so the SID exists only from then on.
	lookup func(account string) (*windows.SID, error)
	// owner is the SID the hardened paths are owned by. The owner of an object
	// holds an implicit WRITE_DAC whatever the DACL says, so a path the
	// person's account still owns could have its DACL rewritten and the exe
	// replaced after all; hardening therefore takes ownership too. Production
	// names BUILTIN\Administrators. A test names the current user, the only SID
	// a non-elevated process may assign: writing another owner needs
	// SeRestorePrivilege, which only an elevated process holds.
	owner func() (*windows.SID, error)
}

// defaultServiceACL is the production binding.
func defaultServiceACL() aclAPI {
	return windowsACL{
		lookup: func(account string) (*windows.SID, error) {
			sid, _, _, err := windows.LookupSID("", account)
			return sid, err
		},
		owner: func() (*windows.SID, error) {
			return windows.CreateWellKnownSid(windows.WinBuiltinAdministratorsSid)
		},
	}
}

// The access masks of a hardened target, in the specific rights an ACE carries.
// GENERIC_* must not be used here: Windows maps a generic bit in the *requested*
// access mask through the object type before the access check, so an ACE holding
// GENERIC_READ grants nothing (the mistake is silent: the DACL sets, and the
// account is then denied everything).
const (
	fileReadData   = 0x0001 // FILE_READ_DATA (a file) / FILE_LIST_DIRECTORY (a directory)
	fileWriteData  = 0x0002 // FILE_WRITE_DATA / FILE_ADD_FILE
	fileAppendData = 0x0004 // FILE_APPEND_DATA / FILE_ADD_SUBDIRECTORY
	fileReadEA     = 0x0008
	fileWriteEA    = 0x0010
	fileExecute    = 0x0020 // FILE_EXECUTE / FILE_TRAVERSE
	fileReadAttrs  = 0x0080
	fileWriteAttrs = 0x0100
)

// aclReadExecute is FILE_GENERIC_READ|FILE_GENERIC_EXECUTE, "Read & execute":
// what a user or the service account needs on the exe, the jars, the plugins
// and the record.
const aclReadExecute = windows.READ_CONTROL | windows.SYNCHRONIZE |
	fileReadData | fileReadEA | fileExecute | fileReadAttrs

// aclModify is "Modify": read and execute plus writing, appending, deleting and
// renaming inside — what the service needs in logs\ (log4j creates, rolls and
// deletes its files) and on a standalone worker's offset file (Connect truncates
// and rewrites it).
const aclModify = aclReadExecute | fileWriteData | fileAppendData | fileWriteEA | fileWriteAttrs | windows.DELETE

// aclFullControl is FILE_ALL_ACCESS, for Administrators and SYSTEM.
const aclFullControl = windows.STANDARD_RIGHTS_ALL | 0x01FF

func (a windowsACL) Harden(account string, targets []aclTarget) error {
	if len(targets) == 0 {
		return nil
	}
	service, err := a.lookup(account)
	if err != nil {
		return fmt.Errorf("%w: resolving the service's account %s: %v", ErrServiceManager, q(account), err)
	}
	owner, err := a.owner()
	if err != nil {
		return fmt.Errorf("host: resolving the owner of the service's directories: %w", err)
	}
	admins, err := windows.CreateWellKnownSid(windows.WinBuiltinAdministratorsSid)
	if err != nil {
		return fmt.Errorf("host: building the Administrators SID: %w", err)
	}
	system, err := windows.CreateWellKnownSid(windows.WinLocalSystemSid)
	if err != nil {
		return fmt.Errorf("host: building the SYSTEM SID: %w", err)
	}
	users, err := windows.CreateWellKnownSid(windows.WinBuiltinUsersSid)
	if err != nil {
		return fmt.Errorf("host: building the Users SID: %w", err)
	}
	// TrusteeValueFromSID keeps a pointer to the SID, so the SIDs must not move
	// while an ACL is built from an entry that names one (x/sys documents the
	// same requirement for TrusteeValueFromSID's caller).
	var pin runtime.Pinner
	defer pin.Unpin()
	for _, sid := range []*windows.SID{service, owner, admins, system, users} {
		pin.Pin(sid)
	}
	for _, t := range targets {
		if err := hardenPath(t, owner, admins, system, users, service); err != nil {
			return err
		}
	}
	return nil
}

// hardenPath writes the protected DACL and the owner of one target.
func hardenPath(t aclTarget, owner, admins, system, users, service *windows.SID) error {
	mask := windows.ACCESS_MASK(aclReadExecute)
	if t.Write {
		mask = aclModify
	}
	entries := []windows.EXPLICIT_ACCESS{
		accessEntry(aclFullControl, admins, windows.TRUSTEE_IS_GROUP),
		accessEntry(aclFullControl, system, windows.TRUSTEE_IS_GROUP),
		accessEntry(mask, service, windows.TRUSTEE_IS_USER),
		accessEntry(aclReadExecute, users, windows.TRUSTEE_IS_GROUP),
	}
	acl, err := windows.ACLFromEntries(entries, nil)
	if err != nil {
		return fmt.Errorf("host: building the DACL of %s: %w", q(t.Path), err)
	}
	// PROTECTED drops the inherited ACEs rather than merging them: the project
	// directory's ACEs are the person's account (full control) and, on a project
	// under C:\, Authenticated Users with Modify on every subfolder, which are
	// exactly the grants this closes.
	const info = windows.DACL_SECURITY_INFORMATION | windows.PROTECTED_DACL_SECURITY_INFORMATION | windows.OWNER_SECURITY_INFORMATION
	if err := windows.SetNamedSecurityInfo(t.Path, windows.SE_FILE_OBJECT, info, owner, nil, acl, nil); err != nil {
		return fmt.Errorf("host: hardening %s: %w", q(t.Path), err)
	}
	return nil
}

// accessEntry is one granted ACE, inherited by what the directory holds.
func accessEntry(mask windows.ACCESS_MASK, sid *windows.SID, kind windows.TRUSTEE_TYPE) windows.EXPLICIT_ACCESS {
	return windows.EXPLICIT_ACCESS{
		AccessPermissions: mask,
		AccessMode:        windows.GRANT_ACCESS,
		Inheritance:       windows.SUB_CONTAINERS_AND_OBJECTS_INHERIT,
		Trustee: windows.TRUSTEE{
			TrusteeForm:  windows.TRUSTEE_IS_SID,
			TrusteeType:  kind,
			TrusteeValue: windows.TrusteeValueFromSID(sid),
		},
	}
}
