//go:build windows

package osroot

import (
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
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

// explicitCheck is explicitRefusal plus the per-component walk U1-2 needs:
// any component that is a symlink or another reparse point (a junction is
// ModeIrregular since Go 1.23) is refused, existing or not at its end.
func explicitCheck(r *os.Root, name string) string {
	if why := explicitRefusal(name); why != "" {
		return why
	}
	parts := strings.FieldsFunc(name, func(c rune) bool { return c == '/' || c == '\\' })
	for i := range parts {
		fi, err := r.Lstat(strings.Join(parts[:i+1], `\`))
		if err != nil {
			if os.IsNotExist(err) {
				return ""
			}
			return "lstat: " + err.Error()
		}
		if fi.Mode()&(fs.ModeSymlink|fs.ModeIrregular) != 0 {
			return "symlink, junction or reparse point"
		}
	}
	return ""
}
