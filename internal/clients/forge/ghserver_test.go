package forge

import (
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// ghServer is a small in-memory GitHub REST API behind httptest: it checks
// the App JWT, mints narrowed tokens, serves the reads and the git database
// writes the client makes, and records every request for the allow-list
// walk (TestEveryRequestIsAllowed and checkAllowed).

var (
	testKeyOnce sync.Once
	testKey     *rsa.PrivateKey
)

func rsaKey(t testing.TB) *rsa.PrivateKey {
	testKeyOnce.Do(func() {
		k, err := rsa.GenerateKey(rand.Reader, 2048)
		if err != nil {
			panic(err)
		}
		testKey = k
	})
	return testKey
}

const (
	tOwner = "acme"
	tName  = "connect-config"
	tAppID = 4242
	tInst  = 777
)

var tRepo = RepoRef{ID: "connect-config", Owner: tOwner, Name: tName, Base: "main"}

// tSlug is the test App's slug; its bot user is tBot.
const (
	tSlug = "piperoom-test"
	tBot  = tSlug + "[bot]"
)

type ghReq struct {
	Method, Path, Query, Auth string
	Body                      []byte
}

type mintedToken struct {
	repos []string
	perms map[string]string
}

type ghCommit struct {
	parent string
	tree   string
	files  map[string][]byte
	// bot: written through the API with an installation token (author and
	// committer are the App's bot); otherwise a person's push.
	bot bool
}

// ghDir is a directory of a tree, for one-level tree listings.
type ghDir struct {
	files  map[string][]byte
	prefix string // "" for the root, else "a/b/"
}

type ghPull struct {
	number           int
	state            string
	merged           bool
	mergedAt         string
	mergeSHA         string
	headRef, headSHA string
	base             string
	// user and userType are the PR's author; headRepo the head's
	// owner/name ("" means the repository itself).
	user, userType, headRepo string
}

type ghServer struct {
	t   *testing.T
	srv *httptest.Server
	now time.Time

	mu       sync.Mutex
	reqs     []ghReq
	tokens   map[string]mintedToken
	minted   int
	ttl      time.Duration
	commits  map[string]*ghCommit
	trees    map[string]map[string][]byte // tree sha -> files
	blobs    map[string][]byte
	branches map[string]string
	special  map[string]string // path -> symlink | submodule | symlink-to:<target>
	modes    map[string]string // path -> git mode other than 100644 (100755)
	dirs     map[string]ghDir  // directory tree id -> directory
	pulls    []*ghPull
	runs     []map[string]any
	statuses []map[string]any
	repoJSON map[string]any
	// hook may answer a request first; it returns true when it did.
	hook func(w http.ResponseWriter, r *http.Request) bool
}

func newGH(t *testing.T, files map[string]string) (*ghServer, string) {
	t.Helper()
	g := &ghServer{t: t, now: time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC), tokens: map[string]mintedToken{}, ttl: time.Hour,
		commits: map[string]*ghCommit{}, trees: map[string]map[string][]byte{}, blobs: map[string][]byte{},
		branches: map[string]string{}, special: map[string]string{}, modes: map[string]string{}, dirs: map[string]ghDir{},
		repoJSON: map[string]any{"visibility": "private", "private": true, "archived": false, "default_branch": "main"}}
	fs := map[string][]byte{}
	for p, c := range files {
		fs[p] = []byte(c)
	}
	base := g.addCommit("", fs)
	g.branches["main"] = base
	g.srv = httptest.NewServer(http.HandlerFunc(g.serve))
	t.Cleanup(g.srv.Close)
	t.Cleanup(func() { checkAllowed(t, g.requests()) })
	return g, base
}

func fakeHash(parts ...string) string {
	h := sha1.New()
	for _, p := range parts {
		fmt.Fprintf(h, "%d:%s;", len(p), p)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func treeHash(files map[string][]byte) string {
	ps := make([]string, 0, len(files))
	for p := range files {
		ps = append(ps, p)
	}
	sort.Strings(ps)
	parts := []string{"tree"}
	for _, p := range ps {
		parts = append(parts, p, BlobSHA(files[p]))
	}
	return fakeHash(parts...)
}

func (g *ghServer) addCommit(parent string, files map[string][]byte) string {
	tr := treeHash(files)
	g.trees[tr] = files
	sha := fakeHash("commit", parent, tr, strconv.Itoa(len(g.commits)))
	g.commits[sha] = &ghCommit{parent: parent, tree: tr, files: files}
	return sha
}

// commitOn adds a commit on branch setting files.
func (g *ghServer) commitOn(branch string, set map[string]string) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	fs := map[string][]byte{}
	for p, c := range g.commits[g.branches[branch]].files {
		fs[p] = c
	}
	for p, c := range set {
		fs[p] = []byte(c)
	}
	sha := g.addCommit(g.branches[branch], fs)
	g.branches[branch] = sha
	return sha
}

func (g *ghServer) setHook(h func(w http.ResponseWriter, r *http.Request) bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.hook = h
}

func (g *ghServer) requests() []ghReq {
	g.mu.Lock()
	defer g.mu.Unlock()
	return append([]ghReq(nil), g.reqs...)
}

// count returns how many recorded requests match method and a path regexp.
func (g *ghServer) count(method, pathRE string) int {
	re := regexp.MustCompile(pathRE)
	n := 0
	for _, r := range g.requests() {
		if r.Method == method && re.MatchString(r.Path) {
			n++
		}
	}
	return n
}

func (g *ghServer) client(t *testing.T, opts ...func(*Config)) *Client {
	t.Helper()
	cfg := Config{APIURL: g.srv.URL, AppID: tAppID, InstallationID: tInst, Key: AppKey{k: rsaKey(t)}, Now: func() time.Time { return g.now }}
	for _, o := range opts {
		o(&cfg)
	}
	c, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func ghErr(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"message": msg, "documentation_url": "https://docs.github.com/rest"})
}

// verifyJWT checks an App JWT the way GitHub does: RS256 over the test key,
// iss the App id, a lifetime of at most 10 minutes around now.
func (g *ghServer) verifyJWT(tok string) error {
	parts := strings.Split(tok, ".")
	if len(parts) != 3 {
		return fmt.Errorf("jwt: %d parts", len(parts))
	}
	head, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil || string(head) != `{"alg":"RS256","typ":"JWT"}` {
		return fmt.Errorf("jwt header %q", head)
	}
	sig, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return err
	}
	sum := sha256.Sum256([]byte(parts[0] + "." + parts[1]))
	if err := rsa.VerifyPKCS1v15(&rsaKey(g.t).PublicKey, crypto.SHA256, sum[:], sig); err != nil {
		return fmt.Errorf("jwt signature: %w", err)
	}
	cb, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return err
	}
	var cl struct{ IAT, EXP, ISS int64 }
	if err := json.Unmarshal(cb, &cl); err != nil {
		return err
	}
	if cl.ISS != tAppID || cl.EXP-cl.IAT > 600 || cl.IAT > g.now.Unix() || cl.EXP <= g.now.Unix() {
		return fmt.Errorf("jwt claims %+v", cl)
	}
	return nil
}

func (g *ghServer) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	g.mu.Lock()
	g.reqs = append(g.reqs, ghReq{Method: r.Method, Path: r.URL.EscapedPath(), Query: r.URL.RawQuery, Auth: r.Header.Get("Authorization"), Body: body})
	hook := g.hook
	g.mu.Unlock()
	if hook != nil && hook(w, r) {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if r.Header.Get("User-Agent") == "" || r.Header.Get("X-GitHub-Api-Version") != apiVersion {
		ghErr(w, http.StatusBadRequest, "missing User-Agent or API version")
		return
	}
	auth, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	p := r.URL.Path
	if r.Method == http.MethodPost && p == fmt.Sprintf("/app/installations/%d/access_tokens", tInst) {
		if err := g.verifyJWT(auth); err != nil {
			ghErr(w, http.StatusUnauthorized, "A JSON web token could not be decoded")
			return
		}
		var in struct {
			Repositories []string          `json:"repositories"`
			Permissions  map[string]string `json:"permissions"`
		}
		if err := json.Unmarshal(body, &in); err != nil {
			ghErr(w, http.StatusUnprocessableEntity, "bad body")
			return
		}
		g.minted++
		tok := "ghs_TESTTOKEN" + strconv.Itoa(g.minted) + "xyz"
		g.tokens[tok] = mintedToken{repos: in.Repositories, perms: in.Permissions}
		writeJSON(w, http.StatusCreated, map[string]any{"token": tok, "expires_at": g.now.Add(g.ttl).Format(time.RFC3339), "permissions": in.Permissions, "repository_selection": "selected"})
		return
	}
	if r.Method == http.MethodGet && p == "/app" {
		if err := g.verifyJWT(auth); err != nil {
			ghErr(w, http.StatusUnauthorized, "A JSON web token could not be decoded")
			return
		}
		writeJSON(w, 200, map[string]any{"id": tAppID, "slug": tSlug, "name": "Piperoom test"})
		return
	}
	mt, ok := g.tokens[auth]
	if !ok {
		ghErr(w, http.StatusUnauthorized, "Bad credentials")
		return
	}
	prefix := "/repos/" + tOwner + "/" + tName
	if len(mt.repos) != 1 || mt.repos[0] != tName || !strings.HasPrefix(p, prefix) {
		ghErr(w, http.StatusNotFound, "Not Found")
		return
	}
	if r.Method != http.MethodGet && mt.perms["contents"] != "write" {
		ghErr(w, http.StatusForbidden, "Resource not accessible by integration")
		return
	}
	rest := strings.TrimPrefix(p, prefix)
	switch {
	case r.Method == http.MethodGet && rest == "":
		writeJSON(w, 200, g.repoJSON)
	case r.Method == http.MethodGet && strings.HasPrefix(rest, "/git/ref/heads/"):
		b := strings.TrimPrefix(rest, "/git/ref/heads/")
		sha, ok := g.branches[b]
		if !ok {
			ghErr(w, 404, "Not Found")
			return
		}
		writeJSON(w, 200, map[string]any{"ref": "refs/heads/" + b, "object": map[string]string{"sha": sha, "type": "commit"}})
	case r.Method == http.MethodGet && strings.HasPrefix(rest, "/contents/"):
		g.contents(w, strings.TrimPrefix(rest, "/contents/"), r.URL.Query().Get("ref"))
	case r.Method == http.MethodGet && strings.HasPrefix(rest, "/git/trees/") && r.URL.Query().Get("recursive") == "":
		id := strings.TrimPrefix(rest, "/git/trees/")
		d, ok := g.dirs[id]
		if c, isCommit := g.commits[id]; isCommit {
			d, ok = ghDir{files: c.files}, true
		} else if fs, isTree := g.trees[id]; isTree {
			d, ok = ghDir{files: fs}, true
		}
		if !ok {
			ghErr(w, 404, "Not Found")
			return
		}
		writeJSON(w, 200, map[string]any{"sha": id, "tree": g.level(d), "truncated": false})
	case r.Method == http.MethodGet && strings.HasPrefix(rest, "/git/trees/"):
		c, ok := g.commits[strings.TrimPrefix(rest, "/git/trees/")]
		if !ok {
			ghErr(w, 404, "Not Found")
			return
		}
		var tree []map[string]any
		ps := make([]string, 0, len(c.files))
		for p := range c.files {
			ps = append(ps, p)
		}
		sort.Strings(ps)
		for _, p := range ps {
			mode := "100644"
			if k := g.special[p]; k == "symlink" || strings.HasPrefix(k, "symlink-to:") {
				mode = "120000"
			} else if k == "submodule" {
				mode = "160000"
			} else if m := g.modes[p]; m != "" {
				mode = m
			}
			tree = append(tree, map[string]any{"path": p, "mode": mode, "type": "blob", "sha": BlobSHA(c.files[p]), "size": len(c.files[p])})
		}
		writeJSON(w, 200, map[string]any{"sha": c.tree, "tree": tree, "truncated": false})
	case r.Method == http.MethodGet && strings.HasPrefix(rest, "/git/commits/"):
		c, ok := g.commits[strings.TrimPrefix(rest, "/git/commits/")]
		if !ok {
			ghErr(w, 404, "Not Found")
			return
		}
		parents := []map[string]string{}
		if c.parent != "" {
			parents = append(parents, map[string]string{"sha": c.parent})
		}
		who := "Mallory"
		if c.bot {
			who = tBot
		}
		person := map[string]string{"name": who, "email": who + "@users.noreply.github.com"}
		writeJSON(w, 200, map[string]any{"sha": strings.TrimPrefix(rest, "/git/commits/"), "tree": map[string]string{"sha": c.tree}, "parents": parents,
			"author": person, "committer": person, "verification": map[string]any{"verified": true, "reason": "valid"}})
	case r.Method == http.MethodPost && rest == "/git/blobs":
		var in struct{ Content, Encoding string }
		_ = json.Unmarshal(body, &in)
		b, err := base64.StdEncoding.DecodeString(in.Content)
		if err != nil || in.Encoding != "base64" {
			ghErr(w, 422, "bad blob")
			return
		}
		g.blobs[BlobSHA(b)] = b
		writeJSON(w, 201, map[string]string{"sha": BlobSHA(b)})
	case r.Method == http.MethodPost && rest == "/git/trees":
		var in struct {
			BaseTree string `json:"base_tree"`
			Tree     []struct{ Path, Mode, Type, SHA string }
		}
		_ = json.Unmarshal(body, &in)
		baseFiles, ok := g.trees[in.BaseTree]
		if !ok {
			ghErr(w, 422, "base_tree not found")
			return
		}
		fs := map[string][]byte{}
		for p, c := range baseFiles {
			fs[p] = c
		}
		for _, e := range in.Tree {
			b, ok := g.blobs[e.SHA]
			if !ok || (e.Mode != "100644" && e.Mode != "100755") || e.Type != "blob" {
				ghErr(w, 422, "bad tree entry")
				return
			}
			fs[e.Path] = b
		}
		tr := treeHash(fs)
		g.trees[tr] = fs
		writeJSON(w, 201, map[string]string{"sha": tr})
	case r.Method == http.MethodPost && rest == "/git/commits":
		var in struct {
			Message string
			Tree    string
			Parents []string
		}
		_ = json.Unmarshal(body, &in)
		fs, ok := g.trees[in.Tree]
		if !ok || len(in.Parents) != 1 || g.commits[in.Parents[0]] == nil {
			ghErr(w, 422, "bad commit")
			return
		}
		sha := g.addCommit(in.Parents[0], fs)
		g.commits[sha].bot = true
		writeJSON(w, 201, map[string]string{"sha": sha})
	case r.Method == http.MethodPost && rest == "/git/refs":
		var in struct{ Ref, SHA string }
		_ = json.Unmarshal(body, &in)
		b, ok := strings.CutPrefix(in.Ref, "refs/heads/")
		if !ok || g.commits[in.SHA] == nil {
			ghErr(w, 422, "bad ref")
			return
		}
		if _, exists := g.branches[b]; exists {
			ghErr(w, 422, "Reference already exists")
			return
		}
		g.branches[b] = in.SHA
		writeJSON(w, 201, map[string]any{"ref": in.Ref, "object": map[string]string{"sha": in.SHA, "type": "commit"}})
	case r.Method == http.MethodDelete && strings.HasPrefix(rest, "/git/refs/heads/"):
		b := strings.TrimPrefix(rest, "/git/refs/heads/")
		if _, ok := g.branches[b]; !ok {
			ghErr(w, 422, "Reference does not exist")
			return
		}
		delete(g.branches, b)
		w.WriteHeader(http.StatusNoContent)
	case r.Method == http.MethodGet && rest == "/pulls":
		q := r.URL.Query()
		out := []map[string]any{}
		for _, pr := range g.pulls {
			if tOwner+":"+pr.headRef == q.Get("head") && pr.state == q.Get("state") {
				out = append(out, pullJSON(pr))
			}
		}
		writeJSON(w, 200, out)
	case r.Method == http.MethodPost && rest == "/pulls":
		var in struct {
			Title, Body, Head, Base string
			Draft                   bool
		}
		_ = json.Unmarshal(body, &in)
		sha, ok := g.branches[in.Head]
		if !ok || in.Title == "" {
			ghErr(w, 422, "Validation Failed")
			return
		}
		pr := &ghPull{number: len(g.pulls) + 1, state: "open", headRef: in.Head, headSHA: sha, base: in.Base, user: tBot, userType: "Bot"}
		g.pulls = append(g.pulls, pr)
		writeJSON(w, 201, pullJSON(pr))
	case r.Method == http.MethodGet && strings.HasPrefix(rest, "/pulls/"):
		n, _ := strconv.Atoi(strings.TrimPrefix(rest, "/pulls/"))
		for _, pr := range g.pulls {
			if pr.number == n {
				writeJSON(w, 200, pullJSON(pr))
				return
			}
		}
		ghErr(w, 404, "Not Found")
	case r.Method == http.MethodGet && strings.HasSuffix(rest, "/check-runs"):
		if mt.perms["checks"] != "read" {
			ghErr(w, 403, "Resource not accessible by integration")
			return
		}
		writeJSON(w, 200, map[string]any{"total_count": len(g.runs), "check_runs": page(g.runs, r)})
	case r.Method == http.MethodGet && strings.HasSuffix(rest, "/status"):
		state := "pending"
		if len(g.statuses) > 0 {
			state = "success"
		}
		writeJSON(w, 200, map[string]any{"state": state, "total_count": len(g.statuses), "statuses": page(g.statuses, r)})
	default:
		ghErr(w, 404, "Not Found")
	}
}

// page returns the page of all that r asks for (per_page default 30, at
// most 100; page from 1), as GitHub pages a list.
func page(all []map[string]any, r *http.Request) []map[string]any {
	per, _ := strconv.Atoi(r.URL.Query().Get("per_page"))
	if per <= 0 {
		per = 30
	}
	per = min(per, 100)
	n, _ := strconv.Atoi(r.URL.Query().Get("page"))
	n = max(n, 1)
	from := min((n-1)*per, len(all))
	out := all[from:min(from+per, len(all))]
	if out == nil {
		out = []map[string]any{}
	}
	return out
}

func pullJSON(pr *ghPull) map[string]any {
	headRepo, user, userType := pr.headRepo, pr.user, pr.userType
	if headRepo == "" {
		headRepo = tOwner + "/" + tName
	}
	if user == "" {
		user, userType = "mallory", "User"
	}
	m := map[string]any{"number": pr.number, "state": pr.state, "merged": pr.merged, "merged_at": nil,
		"merge_commit_sha": pr.mergeSHA, "html_url": "https://evil.example/phish",
		"head": map[string]any{"ref": pr.headRef, "sha": pr.headSHA, "repo": map[string]string{"full_name": headRepo}}, "base": map[string]string{"ref": pr.base},
		"user": map[string]string{"login": user, "type": userType}}
	if pr.mergedAt != "" {
		m["merged_at"] = pr.mergedAt
	}
	return m
}

// level lists one directory of d: its files with their modes, and its
// subdirectories as trees with ids registered in g.dirs.
func (g *ghServer) level(d ghDir) []map[string]any {
	seen := map[string]bool{}
	out := []map[string]any{}
	ps := make([]string, 0, len(d.files))
	for p := range d.files {
		ps = append(ps, p)
	}
	sort.Strings(ps)
	for _, p := range ps {
		rel, ok := strings.CutPrefix(p, d.prefix)
		if !ok {
			continue
		}
		name, _, isDir := strings.Cut(rel, "/")
		if seen[name] {
			continue
		}
		seen[name] = true
		if isDir {
			id := fakeHash("dir", treeHash(d.files), d.prefix+name)
			g.dirs[id] = ghDir{files: d.files, prefix: d.prefix + name + "/"}
			out = append(out, map[string]any{"path": name, "mode": "040000", "type": "tree", "sha": id})
			continue
		}
		mode := "100644"
		if k := g.special[p]; k == "symlink" || strings.HasPrefix(k, "symlink-to:") {
			mode = "120000"
		} else if k == "submodule" {
			mode = "160000"
		} else if m := g.modes[p]; m != "" {
			mode = m
		}
		out = append(out, map[string]any{"path": name, "mode": mode, "type": "blob", "sha": BlobSHA(d.files[p]), "size": len(d.files[p])})
	}
	return out
}

func (g *ghServer) contents(w http.ResponseWriter, p, ref string) {
	c, ok := g.commits[ref]
	if !ok {
		ghErr(w, 404, "No commit found for the ref "+ref)
		return
	}
	if k, ok := g.special[p]; ok {
		if target, isTo := strings.CutPrefix(k, "symlink-to:"); isTo {
			b := c.files[target]
			writeJSON(w, 200, map[string]any{"type": "file", "path": target, "name": target, "sha": BlobSHA(b), "size": len(b), "encoding": "base64", "content": base64.StdEncoding.EncodeToString(b)})
			return
		}
		writeJSON(w, 200, map[string]any{"type": k, "path": p, "sha": BlobSHA([]byte(p)), "size": 0})
		return
	}
	if b, ok := c.files[p]; ok {
		enc := base64.StdEncoding.EncodeToString(b)
		// GitHub wraps the base64 content at 60 characters.
		var wrapped strings.Builder
		for i := 0; i < len(enc); i += 60 {
			wrapped.WriteString(enc[i:min(i+60, len(enc))] + "\n")
		}
		content := wrapped.String()
		if len(b) > 1<<20 {
			content = ""
		}
		writeJSON(w, 200, map[string]any{"type": "file", "path": p, "name": p, "sha": BlobSHA(b), "size": len(b), "encoding": "base64", "content": content})
		return
	}
	var list []map[string]any
	for q := range c.files {
		if strings.HasPrefix(q, p+"/") {
			list = append(list, map[string]any{"type": "file", "path": q})
		}
	}
	if list != nil {
		writeJSON(w, 200, list)
		return
	}
	ghErr(w, 404, "Not Found")
}

// allowedRequests is the allow-list of design §4.3 written independently of
// the client's own table: any other method and path fails the test.
var allowedRequests = []struct {
	method string
	re     *regexp.Regexp
}{
	{"POST", regexp.MustCompile(`^/app/installations/[0-9]+/access_tokens$`)},
	{"GET", regexp.MustCompile(`^/app$`)},
	{"GET", regexp.MustCompile(`^/repos/[^/]+/[^/]+$`)},
	{"GET", regexp.MustCompile(`^/repos/[^/]+/[^/]+/git/ref/heads/.+$`)},
	{"GET", regexp.MustCompile(`^/repos/[^/]+/[^/]+/contents/.+$`)},
	{"GET", regexp.MustCompile(`^/repos/[^/]+/[^/]+/git/trees/[0-9a-f]{40}$`)},
	{"GET", regexp.MustCompile(`^/repos/[^/]+/[^/]+/git/commits/[0-9a-f]{40}$`)},
	{"GET", regexp.MustCompile(`^/repos/[^/]+/[^/]+/pulls(/[0-9]+)?$`)},
	{"GET", regexp.MustCompile(`^/repos/[^/]+/[^/]+/commits/[0-9a-f]{40}/(check-runs|status)$`)},
	{"POST", regexp.MustCompile(`^/repos/[^/]+/[^/]+/git/(blobs|trees|commits|refs)$`)},
	{"POST", regexp.MustCompile(`^/repos/[^/]+/[^/]+/pulls$`)},
	{"DELETE", regexp.MustCompile(`^/repos/[^/]+/[^/]+/git/refs/heads/piperoom/.+$`)},
}

func checkAllowed(t *testing.T, reqs []ghReq) {
	t.Helper()
	for _, r := range reqs {
		ok := false
		for _, a := range allowedRequests {
			ok = ok || (a.method == r.Method && a.re.MatchString(r.Path))
		}
		if !ok {
			t.Errorf("request outside the allow-list: %s %s", r.Method, r.Path)
		}
		if r.Method == http.MethodPost && strings.HasSuffix(r.Path, "/git/refs") {
			var in struct{ Ref string }
			_ = json.Unmarshal(r.Body, &in)
			if !strings.HasPrefix(in.Ref, "refs/heads/piperoom/") {
				t.Errorf("ref created outside piperoom/: %q", in.Ref)
			}
			if strings.Contains(string(r.Body), "force") {
				t.Errorf("ref request carries force: %s", r.Body)
			}
		}
	}
}
