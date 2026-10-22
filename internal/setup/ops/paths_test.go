package ops

import (
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// The design §2.3 rows, every OS. "" means allowed.
func TestLexicalRefusal(t *testing.T) {
	cases := []struct{ name, want string }{
		{"a.yaml", ""},
		{"connect/worker.properties", ""},
		{`connect\worker.properties`, ""},
		{"./a.yaml", ""},
		{"plugins/debezium-2.7/x.jar", ""},
		{"", "empty"},
		{"..", "dot-dot"},
		{"a/../../x", "dot-dot"},
		{`a\..\a.yaml`, "dot-dot"},
		{"/etc/passwd", "absolute, rooted or UNC"},
		{`\Windows\win.ini`, "absolute, rooted or UNC"},
		{`C:foo`, "drive letter or alternate data stream"},
		{`C:\Windows\win.ini`, "drive letter or alternate data stream"},
		{`\\host\share\x`, "absolute, rooted or UNC"},
		{`//host/share/x`, "absolute, rooted or UNC"},
		{`\\?\C:\Windows\win.ini`, "wildcard"},
		{`\\.\NUL`, "absolute, rooted or UNC"},
		{"a.yaml:x", "drive letter or alternate data stream"},
		{"a.yaml::$DATA", "drive letter or alternate data stream"},
		{"CON", "reserved device name"},
		{"nul", "reserved device name"},
		{"COM1", "reserved device name"},
		{"LPT9", "reserved device name"},
		{"COM\u00b9", "not NFKC-normal"},
		{"COM\u00b9.txt", "not NFKC-normal"},
		{"CONIN$", "reserved device name"},
		{"NUL ", "trailing dot or space"},
		{"CON.txt", "reserved device name"},
		{"sub/aux.yaml", "reserved device name"},
		{"console.txt", ""},
		{"COM10", ""},
		{".git/config", "denied directory"},
		{".GIT/config", "denied directory"},
		{`.git.\config`, "trailing dot or space"},
		{".git /config", "trailing dot or space"},
		{".piperoom/secrets.json", "denied directory"},
		{".Piperoom/ops.jsonl", "denied directory"},
		{`PIPERO~1\secrets.json`, "8.3 short-name alias"},
		{"GIT~1/config", "8.3 short-name alias"},
		{"a~b.txt", ""},
		{"a\x00b", "control character"},
		{"a\x01b", "control character"},
		{"a\tb.yaml", "control character"},
		{"a\nb", "control character"},
		{"a\x1fb", "control character"},
		{"a\x7fb", "control character"},
		{"*.yaml", "wildcard"},
		// Format characters inside ".git": HFS+ ignores them (CVE-2014-9390).
		{".g\u200bit/config", "format character"},
		{".g\u200cit/config", "format character"},
		{".g\u200dit/config", "format character"},
		{".g\u202eit/config", "format character"},
		{"\ufeff.git/config", "format character"},
		{".g\u00adit/config", "format character"},
		{".g\u2060it/config", "format character"},
		{".g\u034fit/config", "format character"},
		{".pipe\u200croom/secrets.json", "format character"},
		// The dotless ı upper-cases to I (EqualFold misses it).
		{".g\u0131t/config", "denied directory"},
		{".G\u0131T/config", "denied directory"},
		{".p\u0131peroom/secrets.json", "denied directory"},
		// NFKC folds these to ".", "/", "\" or ":".
		{"\uff0egit/config", "not NFKC-normal"},
		{"a\uff0fb", "not NFKC-normal"},
		{"a\uff3cb", "not NFKC-normal"},
		{"a.yaml\uff1ax", "not NFKC-normal"},
		{"\ufe52git/config", "not NFKC-normal"},
		{"\u212aafka.properties", "not NFKC-normal"},
		{"co\ufb01g.yaml", "not NFKC-normal"},
		// NFKC-normal look-alikes and ANSI best-fit targets (WorstFit).
		{"a\u2215b", "slash, colon or dot look-alike"},
		{"a\u2044b", "slash, colon or dot look-alike"},
		{"a\u00a5b", "slash, colon or dot look-alike"},
		{"a\u20a9b", "slash, colon or dot look-alike"},
		{"a\u2236b", "slash, colon or dot look-alike"},
		{"caf\u00e9.yaml", ""},
		// The remaining nested forms of the row.
		{".piperoom", "denied directory"},
		{"sub/.piperoom/ops.jsonl", "denied directory"},
		{".Git/config", "denied directory"},
		{"foo/../bar", "dot-dot"},
		{"a.yaml.", "trailing dot or space"},
		{"a.yaml ", "trailing dot or space"},
	}
	for _, c := range cases {
		if got := lexicalRefusal(c.name); got != c.want {
			t.Errorf("lexicalRefusal(%q) = %q, want %q", c.name, got, c.want)
		}
	}
}

// fakeInfo is a FileInfo whose identity is id, compared by sameID: an alias
// of a directory cannot be built portably without privileges, so the nested
// alias test fakes the file system.
type fakeInfo struct {
	id   int
	mode fs.FileMode
}

func (f fakeInfo) Name() string       { return "" }
func (f fakeInfo) Size() int64        { return 0 }
func (f fakeInfo) Mode() fs.FileMode  { return f.mode }
func (f fakeInfo) ModTime() time.Time { return time.Time{} }
func (f fakeInfo) IsDir() bool        { return f.mode.IsDir() }
func (f fakeInfo) Sys() any           { return f.id }

func sameID(a, b fs.FileInfo) bool { return a.Sys() == b.Sys() }

// A nested alias of .git or .piperoom is refused at whichever component it
// appears, not only as the leaf or its parent.
func TestWalkRefusesNestedAlias(t *testing.T) {
	git, pr := fakeInfo{id: 1, mode: fs.ModeDir}, fakeInfo{id: 2, mode: fs.ModeDir}
	files := map[string]fakeInfo{
		".git": git, ".piperoom": pr,
		"a":                           {id: 10, mode: fs.ModeDir},
		"a/gitalias":                  git,
		"a/gitalias/hooks":            {id: 11, mode: fs.ModeDir},
		"a/gitalias/hooks/pre-commit": {id: 12},
		"a/b":                         {id: 13, mode: fs.ModeDir},
		"a/b/c.yaml":                  {id: 14},
		"a/b/sec":                     pr,
		"a/link":                      {id: 15, mode: fs.ModeSymlink},
		"a/junction":                  {id: 16, mode: fs.ModeIrregular},
	}
	lstat := func(p string) (fs.FileInfo, error) {
		if fi, ok := files[p]; ok {
			return fi, nil
		}
		return nil, fs.ErrNotExist
	}
	denied := []fs.FileInfo{git, pr}
	cases := []struct{ name, want string }{
		{"a/b/c.yaml", ""},
		{"a/b/new.yaml", ""},
		{`a\b\c.yaml`, ""},
		{"a/gitalias/hooks/pre-commit", "alias of a denied directory"},
		{"a/gitalias/hooks/new-hook", "alias of a denied directory"},
		{"a/gitalias", "alias of a denied directory"},
		{"a/b/sec/secrets.json", "alias of a denied directory"},
		{"a/link/x", "symlink, junction or reparse point"},
		{"a/junction/x", "symlink, junction or reparse point"},
	}
	for _, c := range cases {
		if got := walkRefusal(c.name, lstat, denied, sameID); got != c.want {
			t.Errorf("walkRefusal(%q) = %q, want %q", c.name, got, c.want)
		}
	}
}

// The same walk on a real directory through os.Root and os.SameFile: a
// directory registered as denied is refused as a middle component.
func TestWalkRealFS(t *testing.T) {
	dir := t.TempDir()
	for _, d := range []string{".git", "sub/deep"} {
		if err := os.MkdirAll(filepath.Join(dir, d), 0o700); err != nil {
			t.Fatal(err)
		}
	}
	c, err := OpenConfiner(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	git, err := c.root.Lstat(".git")
	if err != nil {
		t.Fatal(err)
	}
	if got := walkRefusal("sub/deep/x.yaml", c.root.Lstat, []fs.FileInfo{git}, os.SameFile); got != "" {
		t.Errorf("clean path refused: %q", got)
	}
	sub, err := c.root.Lstat("sub")
	if err != nil {
		t.Fatal(err)
	}
	if got := walkRefusal("sub/deep/x.yaml", c.root.Lstat, []fs.FileInfo{git, sub}, os.SameFile); got != "alias of a denied directory" {
		t.Errorf("denied middle component: %q", got)
	}
}

// A symlink inside the project that points outside it is refused before the
// open, and the target's content never reaches the result.
func TestConfinerRefusesSymlinkEscape(t *testing.T) {
	proj, outside := t.TempDir(), t.TempDir()
	secret := filepath.Join(outside, "secret.txt")
	if err := os.WriteFile(secret, []byte("outside-secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(proj, "escape")); err != nil {
		t.Skipf("symlinks are not available: %v", err)
	}
	c, err := OpenConfiner(proj)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, _, err := c.prepare("escape/secret.txt"); !errors.Is(err, ErrPath) {
		t.Fatalf("prepare = %v, want ErrPath", err)
	}
}

// A relative symlink that stays inside the root is followed by os.Root; the
// §2.3 rule refuses it (there is no ambiguity), which the walk enforces.
func TestConfinerRefusesRelativeSymlink(t *testing.T) {
	proj := t.TempDir()
	if err := os.MkdirAll(filepath.Join(proj, "real"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(proj, "real", "a.yaml"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("real", filepath.Join(proj, "link")); err != nil {
		t.Skipf("symlinks are not available: %v", err)
	}
	c, err := OpenConfiner(proj)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, _, err := c.prepare("link/a.yaml"); !errors.Is(err, ErrPath) {
		t.Fatalf("prepare = %v, want ErrPath", err)
	}
}

// A junction is a reparse point and is refused by the walk (ModeIrregular
// since Go 1.23). Windows only; skipped when mklink /J is unavailable.
func TestConfinerRefusesJunction(t *testing.T) {
	if runtime.GOOS != "windows" {
		t.Skip("junctions are a Windows form")
	}
	proj, outside := t.TempDir(), t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	junction := filepath.Join(proj, "jn")
	if out, err := exec.Command("cmd", "/c", "mklink", "/J", junction, outside).CombinedOutput(); err != nil {
		t.Skipf("mklink /J: %v: %s", err, out)
	}
	c, err := OpenConfiner(proj)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if _, _, err := c.prepare("jn/secret.txt"); !errors.Is(err, ErrPath) {
		t.Fatalf("prepare = %v, want ErrPath", err)
	}
}

// The opened handle is re-checked with os.SameFile: a handle that is the same
// file as a denied identity is refused, so a swap between the walk and the
// open is caught.
func TestVerifyHandleRefusesDeniedIdentity(t *testing.T) {
	proj := t.TempDir()
	if err := os.WriteFile(filepath.Join(proj, "a.txt"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	c, err := OpenConfiner(proj)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	fi, err := c.root.Lstat("a.txt")
	if err != nil {
		t.Fatal(err)
	}
	f, err := c.root.Open("a.txt")
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if err := c.verifyHandle(f, []fs.FileInfo{fi}); !errors.Is(err, ErrPath) {
		t.Fatalf("verifyHandle = %v, want ErrPath", err)
	}
	if err := c.verifyHandle(f, nil); err != nil {
		t.Fatalf("verifyHandle with no denied identities = %v", err)
	}
}

// The .piperoom identity is re-resolved on every op, so a .piperoom the
// runner created after start is still refused.
func TestPiperoomResolvedAfterStart(t *testing.T) {
	proj := t.TempDir()
	c, err := OpenConfiner(proj)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if got := c.denied(); len(got) != 0 {
		t.Fatalf("denied before .piperoom exists = %d entries", len(got))
	}
	if err := os.MkdirAll(filepath.Join(proj, ".piperoom"), 0o700); err != nil {
		t.Fatal(err)
	}
	if got := c.denied(); len(got) != 1 {
		t.Fatalf("denied after .piperoom exists = %d entries, want 1", len(got))
	}
	if _, _, err := c.prepare(".piperoom/ops.jsonl"); !errors.Is(err, ErrPath) {
		t.Fatalf("prepare = %v, want ErrPath", err)
	}
}
