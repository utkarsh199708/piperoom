package secrets

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"piperoom/internal/integrations/spec"
)

// refPathOf turns an absolute path into a files reference path.
func refPathOf(p string) string { return strings.TrimPrefix(filepath.ToSlash(p), "/") }

func newFilesTest(t *testing.T, poll time.Duration) (*FilesProvider, string, string) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("reference paths in this test are POSIX")
	}
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(base, "secrets")
	outside := filepath.Join(base, "outside")
	for _, d := range []string{root, outside} {
		if err := os.MkdirAll(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	p, err := NewFilesProvider(FilesOptions{Roots: []string{root}, Poll: poll})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Close)
	return p, root, outside
}

func write(t *testing.T, path, data string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(data), 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestFilesResolve(t *testing.T) {
	ctx := context.Background()
	p, root, outside := newFilesTest(t, time.Hour)
	write(t, filepath.Join(root, "kafka", "password"), "s3cret\n")
	write(t, filepath.Join(root, "db.json"), `{"password":"p"}`)
	write(t, filepath.Join(outside, "shadow"), "nope")

	v, err := p.Resolve(ctx, Ref{Path: refPathOf(filepath.Join(root, "kafka", "password"))})
	if err != nil || v.Data.Reveal() != "s3cret" || v.Version == "" {
		t.Fatalf("got %q %q %v", v.Data.Reveal(), v.Version, err)
	}
	keys, err := p.Keys(ctx, Ref{Path: refPathOf(filepath.Join(root, "db.json"))})
	if err != nil || !reflect.DeepEqual(keys, []string{"password"}) {
		t.Fatalf("keys %v %v", keys, err)
	}

	// A symlink inside the root that points outside is refused after
	// EvalSymlinks.
	link := filepath.Join(root, "escape")
	if err := os.Symlink(filepath.Join(outside, "shadow"), link); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Resolve(ctx, Ref{Path: refPathOf(link)}); !errors.Is(err, ErrOutsideRoots) {
		t.Fatalf("symlink escape err = %v", err)
	}
	// A directory symlink too.
	dirLink := filepath.Join(root, "dir")
	if err := os.Symlink(outside, dirLink); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Resolve(ctx, Ref{Path: refPathOf(filepath.Join(dirLink, "shadow"))}); !errors.Is(err, ErrOutsideRoots) {
		t.Fatalf("dir symlink escape err = %v", err)
	}
	// Outside the roots directly.
	if _, err := p.Resolve(ctx, Ref{Path: refPathOf(filepath.Join(outside, "shadow"))}); !errors.Is(err, ErrOutsideRoots) {
		t.Fatalf("outside err = %v", err)
	}
	// A sibling directory sharing the root's name as prefix.
	write(t, root+"-evil/x", "nope")
	if _, err := p.Resolve(ctx, Ref{Path: refPathOf(root + "-evil/x")}); !errors.Is(err, ErrOutsideRoots) {
		t.Fatalf("prefix sibling err = %v", err)
	}
	// The root itself is a directory, not a secret.
	if _, err := p.Resolve(ctx, Ref{Path: refPathOf(filepath.Join(root, "kafka"))}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("dir err = %v", err)
	}
	if _, err := p.Resolve(ctx, Ref{Path: refPathOf(filepath.Join(root, "missing"))}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing err = %v", err)
	}
	if _, err := p.Resolve(ctx, Ref{Path: refPathOf(filepath.Join(root, "kafka", "password")), Version: "2"}); !errors.Is(err, ErrInvalidRef) {
		t.Fatalf("version err = %v", err)
	}
	// Through the parser: ".." never reaches the provider.
	if _, err := ParseRef("secret://files/" + refPathOf(root) + "/../outside/shadow"); !errors.Is(err, ErrInvalidRef) {
		t.Fatalf("parser err = %v", err)
	}
}

func TestFilesTooLarge(t *testing.T) {
	p, root, _ := newFilesTest(t, time.Hour)
	big := filepath.Join(root, "big")
	write(t, big, strings.Repeat("x", maxSecretBytes+1))
	if _, err := p.Resolve(context.Background(), Ref{Path: refPathOf(big)}); err == nil {
		t.Fatal("want size error")
	}
}

func TestFilesRootValidation(t *testing.T) {
	for _, roots := range [][]string{nil, {"relative/dir"}, {"/"}} {
		if _, err := NewFilesProvider(FilesOptions{Roots: roots}); !errors.Is(err, ErrBadProviderConfig) {
			t.Errorf("roots %v: err = %v", roots, err)
		}
	}
	if got := ParseRoots(" /run/secrets, ,/mnt/x "); !reflect.DeepEqual(got, []string{"/run/secrets", "/mnt/x"}) {
		t.Errorf("ParseRoots = %v", got)
	}
}

func TestFilesChangeDetection(t *testing.T) {
	ctx := context.Background()
	p, root, _ := newFilesTest(t, 20*time.Millisecond)
	changed := make(chan string, 4)
	p.SetOnChange(func(path string) { changed <- path })

	// Kubernetes atomic writer layout: name -> ..data/name, ..data -> ..v1.
	v1 := filepath.Join(root, "..v1")
	write(t, filepath.Join(v1, "password"), "one")
	if err := os.Symlink("..v1", filepath.Join(root, "..data")); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join("..data", "password"), filepath.Join(root, "password")); err != nil {
		t.Fatal(err)
	}
	ref := Ref{Path: refPathOf(filepath.Join(root, "password"))}
	if v, err := p.Resolve(ctx, ref); err != nil || v.Data.Reveal() != "one" {
		t.Fatalf("%q %v", v.Data.Reveal(), err)
	}
	// Swap ..data to a new version directory.
	v2 := filepath.Join(root, "..v2")
	write(t, filepath.Join(v2, "password"), "two")
	tmp := filepath.Join(root, "..data_tmp")
	if err := os.Symlink("..v2", tmp); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(tmp, filepath.Join(root, "..data")); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-changed:
		if got != ref.Path {
			t.Fatalf("changed %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no change reported")
	}
	if v, _ := p.Resolve(ctx, ref); v.Data.Reveal() != "two" {
		t.Fatalf("after swap %q", v.Data.Reveal())
	}

	// In-place rewrite with a different size.
	plain := filepath.Join(root, "plain")
	write(t, plain, "a")
	if _, err := p.Resolve(ctx, Ref{Path: refPathOf(plain)}); err != nil {
		t.Fatal(err)
	}
	write(t, plain, "bbbb")
	select {
	case got := <-changed:
		if got != refPathOf(plain) {
			t.Fatalf("changed %q", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no change reported for rewrite")
	}
}

func TestFilesRegistryRotation(t *testing.T) {
	_, root, _ := newFilesTest(t, time.Hour)
	fp, err := NewFilesProvider(FilesOptions{Roots: []string{root}, Poll: 20 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	changed := make(chan Ref, 1)
	r := newTestRegistry(t, Options{OnChange: func(_ string, ref Ref) { changed <- ref }})
	mustRegister(t, r, spec.DefaultWorkspace, spec.ProviderFiles, fp)
	path := filepath.Join(root, "kafka-password")
	write(t, path, "one")
	ref := "secret://files/" + refPathOf(path)
	if v, err := r.Resolve(context.Background(), spec.DefaultWorkspace, ref); err != nil || v.Reveal() != "one" {
		t.Fatalf("%q %v", v.Reveal(), err)
	}
	write(t, path, "two!")
	select {
	case <-changed:
	case <-time.After(2 * time.Second):
		t.Fatal("no OnChange")
	}
	if v, _ := r.Resolve(context.Background(), spec.DefaultWorkspace, ref); v.Reveal() != "two!" {
		t.Fatalf("after change %q", v.Reveal())
	}
}

func TestFilesList(t *testing.T) {
	ctx := context.Background()
	p, root, outside := newFilesTest(t, time.Hour)
	write(t, filepath.Join(root, "kafka", "password"), "x")
	write(t, filepath.Join(root, "kafka", "user"), "x")
	write(t, filepath.Join(root, "db"), "x")
	if err := os.Symlink("x", filepath.Join(root, "..data")); err != nil {
		t.Fatal(err)
	}
	got, err := p.List(ctx, refPathOf(root)+"/", 10)
	want := []string{refPathOf(root) + "/db", refPathOf(root) + "/kafka/"}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("List = %v %v, want %v", got, err, want)
	}
	got, _ = p.List(ctx, refPathOf(root)+"/kafka/p", 10)
	if !reflect.DeepEqual(got, []string{refPathOf(root) + "/kafka/password"}) {
		t.Fatalf("List prefix = %v", got)
	}
	if got, _ := p.List(ctx, refPathOf(root)+"/kafka/", 1); len(got) != 1 {
		t.Fatalf("limit: %v", got)
	}
	if _, err := p.List(ctx, refPathOf(outside)+"/", 10); !errors.Is(err, ErrOutsideRoots) {
		t.Fatalf("outside list err = %v", err)
	}
	if got, _ := p.List(ctx, "", 10); !reflect.DeepEqual(got, []string{refPathOf(root) + "/"}) {
		t.Fatalf("empty prefix = %v", got)
	}
}

func TestFilesCheck(t *testing.T) {
	p, root, _ := newFilesTest(t, time.Hour)
	write(t, filepath.Join(root, "x"), `{"password":"p"}`)
	checks := p.Check(context.Background(), &Ref{Path: refPathOf(filepath.Join(root, "x")), Key: "password"})
	if spec.Failed(checks) || checks[0].Status != spec.CheckPass || checks[1].Status != spec.CheckPass {
		t.Fatalf("checks = %+v", checks)
	}
	checks = p.Check(context.Background(), &Ref{Path: refPathOf(filepath.Join(root, "x")), Key: "nope"})
	if !spec.Failed(checks) {
		t.Fatalf("missing key should fail: %+v", checks)
	}
}
