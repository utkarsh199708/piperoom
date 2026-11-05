//go:build windows

package osroot

import (
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

// What os.Root does with each Windows form of design §2.3, on a real NTFS
// volume (run on the owner's Windows 11 machine and the windows CI leg):
//
//	go test -v ./cmd/setup-spike/osroot/
//
// root is "refuses" (os.Root must fail the open), "opens" (os.Root succeeds:
// the explicit check is the only guard) or "varies" (depends on the Windows
// version or the volume's 8.3 setting: logged, not asserted). Every row but
// the control rows must be refused by explicitCheck.
func TestRootWindowsForms(t *testing.T) {
	dir := t.TempDir()
	proj, out := filepath.Join(dir, "proj"), filepath.Join(dir, "outside")
	for _, d := range []string{out, filepath.Join(proj, "sub"), filepath.Join(proj, ".git"), filepath.Join(proj, ".piperoom")} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range []string{filepath.Join(out, "secret.txt"), filepath.Join(proj, "a.yaml"), filepath.Join(proj, ".git", "config"), filepath.Join(proj, ".piperoom", "secrets.json")} {
		if err := os.WriteFile(f, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	// A junction needs no privilege; a symlink needs Developer Mode or admin.
	if b, err := exec.Command("cmd", "/c", "mklink", "/J", filepath.Join(proj, "jn"), out).CombinedOutput(); err != nil {
		t.Fatalf("mklink /J: %v: %s", err, b)
	}
	haveSymlink := os.Symlink(".piperoom", filepath.Join(proj, "ln")) == nil

	r, err := os.OpenRoot(proj)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	cases := []struct {
		name, root string
		control    bool // an allowed path: explicitCheck must accept it
	}{
		{name: "a.yaml", root: "opens", control: true},
		{name: `sub\..\a.yaml`, root: "opens"}, // design refuses ".." anywhere; os.Root only refuses an escape
		{name: `..\outside\secret.txt`, root: "refuses"},
		{name: `C:foo`, root: "refuses"},                          // drive-relative: IsLocal rejects ':'
		{name: `C:\Windows\win.ini`, root: "refuses"},             // absolute
		{name: `\Windows\win.ini`, root: "refuses"},               // rooted on the current drive
		{name: `\\localhost\C$\Windows\win.ini`, root: "refuses"}, // UNC: leading separator
		{name: `//localhost/C$/Windows/win.ini`, root: "refuses"},
		{name: `\\?\C:\Windows\win.ini`, root: "refuses"}, // \\?\: leading separator, and '?' is refused
		{name: `\\.\NUL`, root: "refuses"},
		{name: `a.yaml:x`, root: "refuses"}, // ADS: ':'
		{name: `a.yaml::$DATA`, root: "refuses"},
		{name: "CON", root: "refuses"}, // reserved names: the os.Root doc's Windows rule
		{name: "nul", root: "refuses"},
		{name: "COM1", root: "refuses"},
		{name: "LPT1", root: "refuses"},
		{name: "CONIN$", root: "refuses"},
		{name: `sub\AUX`, root: "refuses"},
		{name: "NUL ", root: "refuses"},
		{name: "CON.txt", root: "varies"}, // reserved on Windows 10, a plain name on Windows 11
		{name: `sub\nul.yaml`, root: "varies"},
		{name: `jn\secret.txt`, root: "refuses"}, // a junction's target is absolute, and os.Root refuses absolute links
		{name: "jn", root: "refuses"},
		{name: `.GIT\config`, root: "opens"},            // NTFS is case-insensitive: the deny list must fold case
		{name: `.git.\config`, root: "varies"},          // NT paths keep the trailing dot; Win32 paths strip it
		{name: `PIPERO~1\secrets.json`, root: "varies"}, // opens when the volume generates 8.3 names
		// Does NTFS's upcase table fold the dotless ı to I? If these open,
		// the name rule is the only thing between a path and .git/.piperoom.
		{name: ".g\u0131t\\config", root: "varies"},
		{name: ".p\u0131peroom\\secrets.json", root: "varies"},
		{name: "\uff0egit\\config", root: "varies"},  // fullwidth full stop: NT does not normalise, expect not found
		{name: "a\u2215b", root: "varies"},           // division slash: a plain character to NT
		{name: "a\x01b", root: "varies"},             // control character: NTFS refuses most of them
		{name: ".g\u200cit\\config", root: "varies"}, // zero-width non-joiner: NTFS does not ignore it (HFS+ does), expect not found
	}
	if haveSymlink {
		// os.Root follows a relative link that stays inside the root.
		cases = append(cases, struct {
			name, root string
			control    bool
		}{name: `ln\secrets.json`, root: "opens"})
	} else {
		t.Log("no symlink privilege: the relative-symlink row is skipped (enable Developer Mode to run it)")
	}

	for _, c := range cases {
		f, openErr := r.Open(c.name)
		if f != nil {
			f.Close()
		}
		switch {
		case c.root == "refuses" && openErr == nil:
			t.Errorf("os.Root opened %q", c.name)
		case c.root == "opens" && openErr != nil:
			t.Errorf("os.Root refused %q: %v (the table is out of date)", c.name, openErr)
		}
		t.Logf("%-34q os.Root: %v", c.name, openErr)
		checkErr := explicitCheck(r, c.name)
		if c.control != (checkErr == "") {
			t.Errorf("explicitCheck(%q) = %q, control %v", c.name, checkErr, c.control)
		}
	}
}

// explicitCheck is explicitRefusal plus the per-component walk U1-2 needs
// (walkRefusal): any component that is a symlink or another reparse point (a
// junction is ModeIrregular since Go 1.23), or that os.SameFile matches with
// the root's .git or .piperoom, is refused.
func explicitCheck(r *os.Root, name string) string {
	if why := explicitRefusal(name); why != "" {
		return why
	}
	var denied []fs.FileInfo
	for _, d := range []string{".git", ".piperoom"} {
		if fi, err := r.Lstat(d); err == nil {
			denied = append(denied, fi)
		}
	}
	return walkRefusal(name, r.Lstat, denied, os.SameFile)
}
