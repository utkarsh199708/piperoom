package wiring

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/actions/fixpr"
	"piperoom/internal/clients/forge"
	"piperoom/internal/config"
	"piperoom/internal/tools/forgetools"
)

// stubForgeReader reads nothing; it has the reader's methods only.
type stubForgeReader struct{}

var errStubForge = errors.New("stub forge")

func (stubForgeReader) RepoInfo(context.Context, forge.RepoRef) (forge.RepoInfo, error) {
	return forge.RepoInfo{}, errStubForge
}
func (stubForgeReader) BaseHead(context.Context, forge.RepoRef) (string, error) {
	return "", errStubForge
}
func (stubForgeReader) File(context.Context, forge.RepoRef, string, string) (forge.RepoFile, error) {
	return forge.RepoFile{}, errStubForge
}
func (stubForgeReader) ListFiles(context.Context, forge.RepoRef, string) ([]forge.RepoEntry, bool, error) {
	return nil, false, errStubForge
}
func (stubForgeReader) PullRequest(context.Context, forge.RepoRef, int) (forge.PullRequest, error) {
	return forge.PullRequest{}, errStubForge
}

// stubForgeWriter is a writer that records nothing.
type stubForgeWriter struct{}

func (stubForgeWriter) OpenPullRequest(context.Context, forge.RepoRef, forge.PullRequestRequest) (forge.PullRequest, error) {
	return forge.PullRequest{}, errStubForge
}

// stubForgeOpener is openForge over the stubs: the writer only with write on.
func stubForgeOpener(calls *[]bool) forgeOpener {
	return func(_ config.Config, write bool) (forgeSet, error) {
		*calls = append(*calls, write)
		fs := forgeSet{reader: stubForgeReader{}, webHost: "github.com"}
		if write {
			fs.writer = stubForgeWriter{}
		}
		return fs, nil
	}
}

var forgeToolList = []string{forgetools.NameListFiles, forgetools.NameReadFile, forgetools.NamePRStatus}

// With FORGE_KIND set the repository tools and repo_open_pull_request are in
// every CDC set; the writer reaches the action only with write mode on, and
// with it off no ForgeWriter is reachable (P1a-3, extended for Q7f).
func TestForgeWiringWriteModes(t *testing.T) {
	for name, tc := range map[string]struct {
		mode     string
		readOnly bool
		writer   bool
	}{
		"off":              {mode: "off"},
		"on, ReadOnly set": {mode: "on", readOnly: true},
		"on":               {mode: "on", writer: true},
	} {
		t.Run(name, func(t *testing.T) {
			f := newFakeOpener()
			d := f.deps(env(twoTenantEnv()))
			var calls []bool
			d.forge = stubForgeOpener(&calls)
			cfg := config.Config{SourceDBURL: readerDSN, SourceDBWriteURL: writerDSN, WriteMode: tc.mode}
			st, err := openStacks(context.Background(), cfg, nil, loadOwners(t, threeSetYAML()), StackOptions{ReadOnly: tc.readOnly}, d)
			if err != nil {
				t.Fatalf("openStacks: %v", err)
			}
			defer st.Close()
			if len(calls) != 1 || calls[0] != tc.writer {
				t.Fatalf("forge opened %v, want once with write=%v", calls, tc.writer)
			}
			for _, s := range st.All() {
				for _, n := range forgeToolList {
					if _, ok := s.Tools.Get(n); !ok {
						t.Errorf("set %q: %s not registered", s.Name, n)
					}
				}
				if _, ok := s.Actions.Get(fixpr.Name); !ok {
					t.Errorf("set %q: %s not registered", s.Name, fixpr.Name)
				}
				reached := reachableTypes(s.Actions)[reflect.TypeOf(stubForgeWriter{})]
				if reached != tc.writer {
					t.Errorf("set %q: forge writer reachable %v, want %v", s.Name, reached, tc.writer)
				}
				if !tc.writer {
					assertNoWriterReachable(t, s.Name, s.Actions)
					assertNoForgeWriter(t, s.Name, s.Actions)
				}
				if !reachableTypes(s.Actions)[reflect.TypeOf(stubForgeReader{})] {
					t.Errorf("set %q: the action does not hold the forge reader", s.Name)
				}
			}
			for _, n := range forgeToolList {
				if _, ok := st.For("nope").Tools.Get(n); ok {
					t.Errorf("the empty set offers %s", n)
				}
			}
		})
	}
}

// assertNoForgeWriter fails when an action can reach a ForgeWriter or a type
// with its write method.
func assertNoForgeWriter(t *testing.T, name string, areg *actions.Registry) {
	t.Helper()
	w := ifaceOf[forge.ForgeWriter]()
	for ty := range reachableTypes(areg) {
		if _, ok := ty.MethodByName("OpenPullRequest"); ok || ty.Implements(w) {
			t.Errorf("%s: an action can reach %s, which opens pull requests", name, ty)
		}
	}
}

// FORGE_KIND empty: nothing of the forge, and the lists are those of a set
// built without a forge opener at all.
func TestForgeOffRegistersNothing(t *testing.T) {
	names := func(d stackDeps) (tl, al []string) {
		st, err := openStacks(context.Background(), config.Config{SourceDBURL: readerDSN}, nil, loadOwners(t, threeSetYAML()), StackOptions{}, d)
		if err != nil {
			t.Fatalf("openStacks: %v", err)
		}
		defer st.Close()
		for _, s := range st.All() {
			for _, x := range s.Tools.All() {
				tl = append(tl, s.Name+"/"+x.Name())
			}
			for _, a := range s.Actions.All() {
				al = append(al, s.Name+"/"+a.Name())
			}
		}
		return tl, al
	}
	base := newFakeOpener().deps(env(twoTenantEnv()))
	withOff := newFakeOpener().deps(env(twoTenantEnv()))
	withOff.forge = openForge // config.Config{} has FORGE_KIND ""
	t1, a1 := names(base)
	t2, a2 := names(withOff)
	if strings.Join(t1, ",") != strings.Join(t2, ",") || strings.Join(a1, ",") != strings.Join(a2, ",") {
		t.Errorf("FORGE_KIND empty changed the lists:\n%v\n%v", t1, t2)
	}
	for _, n := range append(t2, a2...) {
		if strings.Contains(n, "repo_") || strings.Contains(n, "pr_status") {
			t.Errorf("forge off registered %s", n)
		}
	}
}

// openForge: off without FORGE_KIND, an error for a half-set forge or a bad
// key, a reader (and a writer only with write mode on) otherwise.
func TestOpenForge(t *testing.T) {
	if fs, err := openForge(config.Config{}, true); err != nil || fs.on() {
		t.Fatalf("off: %+v, %v", fs, err)
	}
	if _, err := openForge(config.Config{GitHubAppID: "1"}, false); err == nil {
		t.Error("GITHUB_APP_ID without FORGE_KIND accepted")
	}
	bad := filepath.Join(t.TempDir(), "key.pem")
	if err := os.WriteFile(bad, []byte("not a key"), 0o600); err != nil {
		t.Fatal(err)
	}
	gh := config.Config{ForgeKind: "github", GitHubAppID: "1", GitHubAppInstallationID: "2", GitHubAppPrivateKeyFile: bad}
	if _, err := openForge(gh, false); !errors.Is(err, forge.ErrBadKey) {
		t.Errorf("bad key: err = %v, want ErrBadKey", err)
	}
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	good := filepath.Join(t.TempDir(), "app.pem")
	if err := os.WriteFile(good, pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)}), 0o600); err != nil {
		t.Fatal(err)
	}
	gh.GitHubAppPrivateKeyFile = good
	for _, write := range []bool{false, true} {
		fs, err := openForge(gh, write)
		if err != nil {
			t.Fatalf("write=%v: %v", write, err)
		}
		if !fs.on() || fs.webHost != "github.com" || (fs.writer != nil) != write {
			t.Errorf("write=%v: forge %+v", write, fs)
		}
	}
}
