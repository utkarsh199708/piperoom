package forge

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

const dockerfile = "FROM quay.io/debezium/connect:2.5\nARG DEBEZIUM_VERSION=2.5.0.Final\n"

func baseFiles() map[string]string {
	return map[string]string{
		"docker/connect/Dockerfile": dockerfile,
		"connectors/orders-pg.json": `{"name":"orders-pg"}`,
		"README.md":                 "readme\n",
		".github/workflows/ci.yml":  "on: push\n",
	}
}

func ctx10(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestAppJWT(t *testing.T) {
	g, _ := newGH(t, nil)
	jwt, err := appJWT(AppKey{k: rsaKey(t)}, tAppID, g.now)
	if err != nil {
		t.Fatal(err)
	}
	if err := g.verifyJWT(jwt.Reveal()); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(jwt) != "[redacted]" {
		t.Fatalf("jwt prints as %q", fmt.Sprint(jwt))
	}
	if _, err := appJWT(AppKey{}, 1, g.now); !errors.Is(err, ErrBadKey) {
		t.Fatalf("no key: %v", err)
	}
}

func TestTokensNarrowedAndCached(t *testing.T) {
	g, base := newGH(t, baseFiles())
	c := g.client(t)
	ctx := ctx10(t)
	if _, err := c.RepoInfo(ctx, tRepo); err != nil {
		t.Fatal(err)
	}
	if _, err := c.File(ctx, tRepo, base, "README.md"); err != nil {
		t.Fatal(err)
	}
	if n := g.count("POST", `access_tokens$`); n != 1 {
		t.Fatalf("minted %d tokens, want 1 (cached)", n)
	}
	var body struct {
		Repositories []string          `json:"repositories"`
		Permissions  map[string]string `json:"permissions"`
	}
	_ = json.Unmarshal(g.requests()[0].Body, &body)
	want := map[string]string{"contents": "read", "pull_requests": "read", "checks": "read", "statuses": "read"}
	if len(body.Repositories) != 1 || body.Repositories[0] != tName || fmt.Sprint(body.Permissions) != fmt.Sprint(want) {
		t.Fatalf("token body = %+v", body)
	}
	// 56 minutes later the token expires within 5 minutes: a new one.
	g.now = g.now.Add(56 * time.Minute)
	if _, err := c.RepoInfo(ctx, tRepo); err != nil {
		t.Fatal(err)
	}
	if n := g.count("POST", `access_tokens$`); n != 2 {
		t.Fatalf("minted %d tokens, want 2 after the refresh window", n)
	}
	// The writer's token is a separate, narrowed write token.
	w, _ := NewWriter(c)
	if _, err := w.OpenPullRequest(ctx, tRepo, fixReq(base, "README.md", "readme 2\n")); err != nil {
		t.Fatal(err)
	}
	reqs := g.requests()
	var last struct {
		Permissions map[string]string `json:"permissions"`
	}
	for _, r := range reqs {
		if strings.HasSuffix(r.Path, "access_tokens") {
			last.Permissions = nil
			_ = json.Unmarshal(r.Body, &last)
		}
	}
	if fmt.Sprint(last.Permissions) != fmt.Sprint(map[string]string{"contents": "write", "pull_requests": "write"}) {
		t.Fatalf("writer token permissions = %v", last.Permissions)
	}
}

func TestTokenMintRefused(t *testing.T) {
	g, _ := newGH(t, nil)
	g.setHook(func(w http.ResponseWriter, r *http.Request) bool {
		if strings.HasSuffix(r.URL.Path, "access_tokens") {
			ghErr(w, 422, "There is at least one repository that does not exist or is not accessible to the parent installation.")
			return true
		}
		return false
	})
	_, err := g.client(t).RepoInfo(ctx10(t), tRepo)
	if err == nil || !strings.Contains(err.Error(), "is the App installed") {
		t.Fatalf("err = %v", err)
	}
	// A wrong key: GitHub answers 401.
	other, _ := rsaKeyOther()
	g.setHook(nil)
	c := g.client(t, func(c *Config) { c.Key = other })
	if _, err := c.RepoInfo(ctx10(t), tRepo); !errors.Is(err, ErrAuth) {
		t.Fatalf("wrong key: %v", err)
	}
}

func rsaKeyOther() (AppKey, error) {
	k, err := rsa.GenerateKey(rand.Reader, 2048)
	return AppKey{k: k}, err
}

func TestRepoInfoAndBaseHead(t *testing.T) {
	g, base := newGH(t, baseFiles())
	c := g.client(t)
	ctx := ctx10(t)
	info, err := c.RepoInfo(ctx, tRepo)
	if err != nil || info != (RepoInfo{Visibility: "private", DefaultBranch: "main"}) {
		t.Fatalf("info = %+v, %v", info, err)
	}
	g.repoJSON = map[string]any{"private": false, "archived": true, "default_branch": "main"}
	if info, _ := c.RepoInfo(ctx, tRepo); info.Visibility != "public" || !info.Archived {
		t.Fatalf("old GHE answer: %+v", info)
	}
	head, err := c.BaseHead(ctx, tRepo)
	if err != nil || head != base {
		t.Fatalf("head = %s, %v", head, err)
	}
	if _, err := c.BaseHead(ctx, RepoRef{ID: "x", Owner: tOwner, Name: tName, Base: "nope"}); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing base: %v", err)
	}
}

func TestBadArgumentsRefusedBeforeIO(t *testing.T) {
	g, base := newGH(t, baseFiles())
	c := g.client(t)
	ctx := ctx10(t)
	cases := []func() error{
		func() error { _, err := c.RepoInfo(ctx, RepoRef{Owner: "..", Name: "x"}); return err },
		func() error { _, err := c.RepoInfo(ctx, RepoRef{Owner: "a/b", Name: "x"}); return err },
		func() error { _, err := c.BaseHead(ctx, RepoRef{Owner: "a", Name: "b", Base: "../x"}); return err },
		func() error { _, err := c.File(ctx, tRepo, "main", "README.md"); return err },
		func() error { _, err := c.File(ctx, tRepo, base, "../etc/passwd"); return err },
		func() error { _, err := c.File(ctx, tRepo, base, ".github/workflows/ci.yml"); return err },
		func() error { _, err := c.File(ctx, tRepo, base, "a//b"); return err },
		func() error { _, _, err := c.ListFiles(ctx, tRepo, "HEAD"); return err },
		func() error { _, err := c.PullRequest(ctx, tRepo, 0); return err },
	}
	for i, f := range cases {
		if err := f(); err == nil {
			t.Errorf("case %d: no error", i)
		}
	}
	if n := len(g.requests()); n != 0 {
		t.Fatalf("%d requests sent for refused arguments", n)
	}
}

func TestFile(t *testing.T) {
	big := strings.Repeat("x", MaxRepoFileBytes+1)
	files := baseFiles()
	files["big.txt"] = big
	files["docker/link"] = "target"
	files["docker/sub"] = ""
	files["docker/followed"] = ""
	g, base := newGH(t, files)
	g.special["docker/link"] = "symlink"
	g.special["docker/sub"] = "submodule"
	g.special["docker/followed"] = "symlink-to:README.md"
	c := g.client(t)
	ctx := ctx10(t)

	f, err := c.File(ctx, tRepo, base, "docker/connect/Dockerfile")
	if err != nil {
		t.Fatal(err)
	}
	if string(f.Content) != dockerfile || f.SHA != base || f.BlobSHA != BlobSHA([]byte(dockerfile)) || f.Size != len(dockerfile) || f.Path != "docker/connect/Dockerfile" {
		t.Fatalf("file = %+v", f)
	}
	for _, p := range []string{"docker", "docker/link", "docker/sub", "docker/followed"} {
		if _, err := c.File(ctx, tRepo, base, p); !errors.Is(err, ErrNotRegularFile) {
			t.Errorf("%s: %v, want ErrNotRegularFile", p, err)
		}
	}
	f, err = c.File(ctx, tRepo, base, "big.txt")
	if !errors.Is(err, ErrRepoFileTooLarge) || f.Size != len(big) || f.Content != nil {
		t.Fatalf("big: %+v %v", f.Size, err)
	}
	if _, err := c.File(ctx, tRepo, base, "missing.txt"); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
	// A tampered answer (content that is not the blob) is refused.
	g.setHook(func(w http.ResponseWriter, r *http.Request) bool {
		writeJSON(w, 200, map[string]any{"type": "file", "path": "README.md", "sha": BlobSHA([]byte("other")), "size": 7, "encoding": "base64", "content": "cmVhZG1lCg=="})
		return true
	})
	if _, err := c.File(ctx, tRepo, base, "README.md"); err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("tampered: %v", err)
	}
	g.setHook(func(w http.ResponseWriter, r *http.Request) bool {
		writeJSON(w, 200, map[string]any{"type": "file", "path": "README.md", "sha": BlobSHA([]byte("readme\n")), "size": 7, "encoding": "none", "content": ""})
		return true
	})
	if _, err := c.File(ctx, tRepo, base, "README.md"); err == nil || !strings.Contains(err.Error(), "encoding") {
		t.Fatalf("encoding none: %v", err)
	}
}

func TestListFiles(t *testing.T) {
	files := baseFiles()
	files["docker/link"] = "x"
	g, base := newGH(t, files)
	g.special["docker/link"] = "symlink"
	c := g.client(t)
	got, trunc, err := c.ListFiles(ctx10(t), tRepo, base)
	if err != nil || trunc {
		t.Fatal(err, trunc)
	}
	var paths []string
	for _, e := range got {
		paths = append(paths, e.Path)
	}
	// The symlink and the denied .github file are not listed.
	if fmt.Sprint(paths) != "[README.md connectors/orders-pg.json docker/connect/Dockerfile]" {
		t.Fatalf("paths = %v", paths)
	}
	if !strings.Contains(g.requests()[1].Query, "recursive=1") {
		t.Fatalf("tree query = %q", g.requests()[1].Query)
	}

	many := map[string]string{}
	for i := 0; i < MaxRepoListEntries+5; i++ {
		many[fmt.Sprintf("f/%04d.txt", i)] = "x"
	}
	g2, base2 := newGH(t, many)
	got, trunc, err = g2.client(t).ListFiles(ctx10(t), tRepo, base2)
	if err != nil || !trunc || len(got) != MaxRepoListEntries {
		t.Fatalf("cap: %d %v %v", len(got), trunc, err)
	}
}

func TestPullRequestStatesAndChecks(t *testing.T) {
	g, base := newGH(t, baseFiles())
	c := g.client(t)
	ctx := ctx10(t)
	g.pulls = []*ghPull{
		{number: 1, state: "open", headRef: "piperoom/r_1-p_1-0", headSHA: base},
		{number: 2, state: "closed", headRef: "piperoom/r_1-p_2-0", headSHA: base},
		{number: 3, state: "closed", merged: true, mergedAt: "2026-10-23T14:02:00Z", mergeSHA: strings.Repeat("a", 40), headRef: "piperoom/r_1-p_3-0", headSHA: base},
	}
	pr, err := c.PullRequest(ctx, tRepo, 1)
	if err != nil {
		t.Fatal(err)
	}
	want := PullRequest{Repo: tRepo.ID, Number: 1, URL: "https://" + c.WebHost() + "/acme/connect-config/pull/1", State: PRStateOpen,
		HeadRef: "piperoom/r_1-p_1-0", HeadSHA: base, HeadRepo: "acme/connect-config", Checks: PullRequestChecks{State: ChecksNone}}
	if fmt.Sprint(pr) != fmt.Sprint(want) {
		t.Fatalf("pr =\n%+v\nwant\n%+v", pr, want)
	}
	if pr, _ := c.PullRequest(ctx, tRepo, 2); pr.State != PRStateClosed {
		t.Fatalf("closed: %+v", pr)
	}
	pr, _ = c.PullRequest(ctx, tRepo, 3)
	if pr.State != PRStateMerged || pr.MergeSHA != strings.Repeat("a", 40) || !pr.MergedAt.Equal(time.Date(2026, 10, 23, 14, 2, 0, 0, time.UTC)) {
		t.Fatalf("merged: %+v", pr)
	}
	if _, err := c.PullRequest(ctx, tRepo, 9); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}

	g.runs = []map[string]any{{"name": "build", "status": "in_progress"}}
	if pr, _ := c.PullRequest(ctx, tRepo, 1); pr.Checks.State != ChecksPending || pr.Checks.Total != 1 {
		t.Fatalf("pending: %+v", pr.Checks)
	}
	g.runs = []map[string]any{{"name": "build", "status": "completed", "conclusion": "success"}}
	g.statuses = []map[string]any{{"context": "ci/legacy", "state": "success"}}
	if pr, _ := c.PullRequest(ctx, tRepo, 1); pr.Checks.State != ChecksSuccess || pr.Checks.Total != 2 {
		t.Fatalf("success: %+v", pr.Checks)
	}
	g.runs = nil
	for i := 0; i < 12; i++ {
		g.runs = append(g.runs, map[string]any{"name": fmt.Sprintf("job %d password=hunter2secret %s", i, strings.Repeat("n", 200)), "status": "completed", "conclusion": "failure"})
	}
	g.statuses = []map[string]any{{"context": "ci/legacy", "state": "error"}}
	pr, _ = c.PullRequest(ctx, tRepo, 1)
	if pr.Checks.State != ChecksFailure || len(pr.Checks.Failed) != maxFailedChecks {
		t.Fatalf("failure: %+v", pr.Checks)
	}
	for _, n := range pr.Checks.Failed {
		if len(n) > maxCheckName || strings.Contains(n, "hunter2") {
			t.Fatalf("failed check name not cut or redacted: %q", n)
		}
	}
}

func TestStatusErrors(t *testing.T) {
	g, _ := newGH(t, baseFiles())
	// Only the "timeout" case runs on the 300 ms client: the others send up
	// to 4 MiB, which under -race can take longer than that to read.
	c := g.client(t, func(c *Config) { c.Timeout = 10 * time.Second })
	slow := g.client(t, func(c *Config) { c.Timeout = 300 * time.Millisecond })
	ctx := ctx10(t)
	for _, cl := range []*Client{c, slow} {
		if _, err := cl.RepoInfo(ctx, tRepo); err != nil { // mint a token first
			t.Fatal(err)
		}
	}
	reset := g.now.Add(10 * time.Minute).Unix()
	cases := []struct {
		name  string
		serve func(w http.ResponseWriter)
		check func(error) bool
	}{
		{"401", func(w http.ResponseWriter) { ghErr(w, 401, "Bad credentials") }, func(e error) bool { return errors.Is(e, ErrAuth) }},
		{"403", func(w http.ResponseWriter) { ghErr(w, 403, "Resource not accessible by integration") }, func(e error) bool { return errors.Is(e, ErrAuth) && !errors.Is(e, ErrRateLimited) }},
		{"403 rate", func(w http.ResponseWriter) {
			w.Header().Set("X-RateLimit-Remaining", "0")
			w.Header().Set("X-RateLimit-Reset", fmt.Sprint(reset))
			ghErr(w, 403, "API rate limit exceeded")
		}, func(e error) bool {
			var se *StatusError
			return errors.Is(e, ErrRateLimited) && !errors.Is(e, ErrAuth) && errors.As(e, &se) && se.Reset.Unix() == reset
		}},
		{"429", func(w http.ResponseWriter) {
			w.Header().Set("Retry-After", "30")
			ghErr(w, 429, "secondary rate limit")
		}, func(e error) bool {
			var se *StatusError
			return errors.Is(e, ErrRateLimited) && errors.As(e, &se) && se.Reset.Equal(g.now.Add(30*time.Second))
		}},
		{"404", func(w http.ResponseWriter) { ghErr(w, 404, "Not Found") }, func(e error) bool { return errors.Is(e, clients.ErrNotFound) }},
		{"409", func(w http.ResponseWriter) { ghErr(w, 409, "Git Repository is empty.") }, func(e error) bool {
			var se *StatusError
			return errors.As(e, &se) && se.Code == 409 && strings.Contains(e.Error(), "empty")
		}},
		{"422", func(w http.ResponseWriter) { ghErr(w, 422, "Validation Failed\n\x1b[31mevil") }, func(e error) bool {
			return strings.Contains(e.Error(), "status 422") && !strings.ContainsAny(e.Error(), "\n\x1b")
		}},
		{"500 text", func(w http.ResponseWriter) { w.WriteHeader(500); fmt.Fprint(w, strings.Repeat("e", 5000)) }, func(e error) bool {
			return strings.Contains(e.Error(), "status 500") && len(e.Error()) < 400
		}},
		{"timeout", func(w http.ResponseWriter) { time.Sleep(time.Second) }, func(e error) bool {
			return e != nil && strings.Contains(e.Error(), "GET /repos/acme/connect-config")
		}},
		{"oversized", func(w http.ResponseWriter) {
			w.WriteHeader(200)
			fmt.Fprint(w, `{"x":"`+strings.Repeat("a", MaxBodyBytes)+`"}`)
		}, func(e error) bool { return errors.Is(e, ErrTooLarge) }},
		{"bad json", func(w http.ResponseWriter) { w.WriteHeader(200); fmt.Fprint(w, `{"visibility":`) }, func(e error) bool {
			return e != nil && strings.Contains(e.Error(), "not the expected JSON")
		}},
		{"redirect", func(w http.ResponseWriter) {
			w.Header().Set("Location", "/repos/acme/connect-config/pulls/1/merge")
			w.WriteHeader(http.StatusFound)
		}, func(e error) bool { return errors.Is(e, errRedirect) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g.setHook(func(w http.ResponseWriter, r *http.Request) bool {
				if strings.HasSuffix(r.URL.Path, "access_tokens") {
					return false
				}
				tc.serve(w)
				return true
			})
			cl := c
			if tc.name == "timeout" {
				cl = slow
			}
			_, err := cl.RepoInfo(ctx, tRepo)
			if !tc.check(err) {
				t.Fatalf("err = %v", err)
			}
			if err != nil && strings.Contains(err.Error(), "ghs_") {
				t.Fatalf("token in error: %v", err)
			}
		})
	}
	g.setHook(nil)
	for _, r := range g.requests() {
		if strings.Contains(r.Path, "merge") {
			t.Fatalf("redirect was followed: %s", r.Path)
		}
	}
}

func TestAllowListRefusesInCode(t *testing.T) {
	g, _ := newGH(t, nil)
	c := g.client(t)
	ctx := ctx10(t)
	bad := []request{
		{ep: epRef, method: http.MethodPatch, path: "/repos/acme/connect-config/git/ref/heads/main"},
		{ep: epDeleteOwnRef, method: http.MethodDelete, path: "/repos/acme/connect-config/git/refs/heads/main"},
		{ep: epNewPull, method: http.MethodPut, path: "/repos/acme/connect-config/pulls/1/merge"},
		{ep: epRepo, method: http.MethodGet, path: "/repos/acme/connect-config/actions/secrets"},
		{ep: endpoint(999), method: http.MethodGet, path: "/repos/acme/connect-config"},
	}
	for _, r := range bad {
		if err := c.do(ctx, "t", r, nil); !errors.Is(err, ErrRefused) {
			t.Errorf("%s %s: %v", r.method, r.path, err)
		}
	}
	if len(g.requests()) != 0 {
		t.Fatal("a refused request was sent")
	}
}

func TestProvenance(t *testing.T) {
	g, base := newGH(t, baseFiles())
	c := g.client(t)
	rec := tools.NewRecorder()
	ctx := tools.WithRecorder(ctx10(t), rec)
	if _, err := c.File(ctx, tRepo, base, "docker/connect/Dockerfile"); err != nil {
		t.Fatal(err)
	}
	calls := rec.Calls()
	if len(calls) != 1 {
		t.Fatalf("calls = %+v (the token mint is not a receipt)", calls)
	}
	want := "GET /repos/acme/connect-config/contents/docker/connect/Dockerfile?ref=" + base
	if calls[0].Kind != callForge || calls[0].Text != want || strings.Contains(calls[0].Target, "@") {
		t.Fatalf("call = %+v", calls[0])
	}
}

func TestNewConfig(t *testing.T) {
	k := AppKey{k: rsaKey(t)}
	ok := Config{AppID: 1, InstallationID: 2, Key: k}
	c, err := New(ok)
	if err != nil || c.WebHost() != "github.com" || c.base.String() != DefaultAPIURL {
		t.Fatalf("default: %v %v", c, err)
	}
	ghe := ok
	ghe.APIURL = "https://ghe.acme.test/api/v3/"
	c, err = New(ghe)
	if err != nil || c.WebHost() != "ghe.acme.test" || c.base.Path != "/api/v3" {
		t.Fatalf("ghe: %v %v", c, err)
	}
	for _, u := range []string{"http://ghe.acme.test/api/v3", "https://u:p@api.github.com", "https://api.github.com?x=1", "https://api.github.com#f", "ftp://x", "://"} {
		cfg := ok
		cfg.APIURL = u
		if _, err := New(cfg); err == nil {
			t.Errorf("%q accepted", u)
		}
	}
	for _, cfg := range []Config{{AppID: 0, InstallationID: 2, Key: k}, {AppID: 1, InstallationID: -1, Key: k}, {AppID: 1, InstallationID: 2}} {
		if _, err := New(cfg); err == nil {
			t.Errorf("%+v accepted", cfg)
		}
	}
	cfg := ok
	cfg.APIURL = "http://127.0.0.1:9"
	if _, err := New(cfg); err != nil {
		t.Errorf("loopback http refused: %v", err)
	}
}

func TestParsePrivateKey(t *testing.T) {
	k := rsaKey(t)
	pk1 := pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(k)})
	der8, _ := x509.MarshalPKCS8PrivateKey(k)
	pk8 := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der8})
	for _, b := range [][]byte{pk1, pk8} {
		got, err := ParsePrivateKey(b)
		if err != nil || got.k.N.Cmp(k.N) != 0 {
			t.Fatalf("parse: %v", err)
		}
		if s := fmt.Sprintf("%v %+v %#v %s", got, got, got, got); strings.Contains(s, k.D.String()[:20]) || strings.Count(s, "[redacted]") != 4 {
			t.Fatalf("key prints as %q", s)
		}
	}
	ec, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	derEC, _ := x509.MarshalPKCS8PrivateKey(ec)
	for _, b := range [][]byte{nil, []byte("not pem"), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: derEC}),
		pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: []byte("junk")}), pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: []byte("x")})} {
		if _, err := ParsePrivateKey(b); !errors.Is(err, ErrBadKey) {
			t.Errorf("%q: %v", b, err)
		}
	}

	dir := t.TempDir()
	p := filepath.Join(dir, "app.pem")
	if err := os.WriteFile(p, pk1, 0o644); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	log := slog.New(slog.NewTextHandler(&logs, nil))
	if _, err := LoadPrivateKeyFile(p, log); err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && !strings.Contains(logs.String(), "readable by group or others") {
		t.Fatalf("no permission warning: %q", logs.String())
	}
	if strings.Contains(logs.String(), "PRIVATE KEY") {
		t.Fatal("key content logged")
	}
	if _, err := LoadPrivateKeyFile(filepath.Join(dir, "missing.pem"), log); err == nil {
		t.Fatal("missing file accepted")
	}
}

// TestSecretsNeverLogged runs every operation with a capturing default
// logger and checks that no token, JWT or key reaches a log line, an error
// or a printed client.
func TestSecretsNeverLogged(t *testing.T) {
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})))
	t.Cleanup(func() { slog.SetDefault(old) })

	g, base := newGH(t, baseFiles())
	c := g.client(t)
	w, _ := NewWriter(c)
	ctx := ctx10(t)
	var errs []string
	collect := func(err error) {
		if err != nil {
			errs = append(errs, err.Error())
		}
	}
	_, err := c.RepoInfo(ctx, tRepo)
	collect(err)
	_, err = c.File(ctx, tRepo, base, "missing")
	collect(err)
	_, err = w.OpenPullRequest(ctx, tRepo, fixReq(base, "README.md", "x\n"))
	collect(err)
	_, err = c.PullRequest(ctx, tRepo, 1)
	collect(err)
	g.setHook(func(w http.ResponseWriter, r *http.Request) bool { ghErr(w, 401, "Bad credentials"); return true })
	g.now = g.now.Add(2 * time.Hour)
	_, err = c.RepoInfo(ctx, tRepo)
	collect(err)

	printed := fmt.Sprintf("%v %+v %#v %v %+v", c, c, c, w, *w)
	var secrets []string
	for _, r := range g.requests() {
		if tok, ok := strings.CutPrefix(r.Auth, "Bearer "); ok && tok != "" {
			secrets = append(secrets, tok)
		}
	}
	if len(secrets) == 0 {
		t.Fatal("no secrets seen")
	}
	hay := logs.String() + strings.Join(errs, "\n") + printed
	for _, s := range secrets {
		if strings.Contains(hay, s) {
			t.Fatalf("secret %.12s... leaked into logs, errors or prints", s)
		}
	}
	if strings.Contains(hay, rsaKey(t).D.String()[:20]) {
		t.Fatal("key leaked")
	}
}
