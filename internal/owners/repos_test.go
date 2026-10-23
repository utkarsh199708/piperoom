package owners

import (
	"errors"
	"strings"
	"testing"
)

// reposYAML is one cdc pipeline with the given repos: block (indented as the
// pipeline's key) and the extra top-level YAML.
func reposYAML(repos, extra string) string {
	return `
pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [pg-orders]
    repos:
` + repos + extra
}

const goodRepos = `      - id: connect-config
        url: https://github.com/acme/connect-config
        base: main
        draft: true
        paths:
          - {glob: "connectors/orders-*.json", layer: kafka-connect}
          - {glob: "docker/connect/Dockerfile", layer: kafka-connect}
          - {glob: "consumers/**/application*.yml", layer: consumer}
      - id: orders-consumer
        url: https://ghe.example.com:8443/acme/orders_consumer.v2
        base: release/2026.10
        public_ok: true
        paths:
          - {glob: "src/**", layer: consumer}
          - {glob: "src/main/resources/schema/**", layer: schema-registry}
`

func TestReposLoadAndLayerOf(t *testing.T) {
	o, err := Load(writeTemp(t, reposYAML(goodRepos, "")))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, _ := o.Pipeline("orders")
	r, ok := p.Repo("connect-config")
	if !ok || !r.Draft || r.PublicOK || r.Base != "main" {
		t.Fatalf("Repo(connect-config) = %+v, %v", r, ok)
	}
	if got, want := r.Ref(), (RepoRef{ID: "connect-config", Owner: "acme", Name: "connect-config", Base: "main", Host: "github.com"}); got != want {
		t.Fatalf("Ref = %+v, want %+v", got, want)
	}
	r2, _ := p.Repo("orders-consumer")
	if got := r2.Ref(); got.Owner != "acme" || got.Name != "orders_consumer.v2" || got.Base != "release/2026.10" {
		t.Fatalf("Ref = %+v", got)
	}
	if _, ok := p.Repo("nope"); ok {
		t.Fatal("unknown repo id")
	}
	// Repo returns a copy.
	r.Paths[0].Layer = "kafka"
	if again, _ := p.Repo("connect-config"); again.Paths[0].Layer != "kafka-connect" {
		t.Fatal("Repo must return a copy of paths")
	}

	for _, tc := range []struct {
		repo, path, layer string
		err               error
	}{
		{"connect-config", "connectors/orders-main.json", "kafka-connect", nil},
		{"connect-config", "docker/connect/Dockerfile", "kafka-connect", nil},
		// '*' never crosses '/'.
		{"connect-config", "connectors/x/orders-main.json", "", ErrPathNotListed},
		{"connect-config", "connectors/orders-main.yaml", "", ErrPathNotListed},
		// '**' matches zero or more segments.
		{"connect-config", "consumers/application.yml", "consumer", nil},
		{"connect-config", "consumers/a/b/application-prod.yml", "consumer", nil},
		{"connect-config", "docker/connect/dockerfile", "", ErrPathNotListed}, // case-sensitive
		// Two layers match: refused, not first-match.
		{"orders-consumer", "src/main/resources/schema/orders.avsc", "", ErrPathAmbiguous},
		{"orders-consumer", "src/main/App.java", "consumer", nil},
		// Denied paths are refused before the globs ("src/**" matches them).
		{"orders-consumer", "src/.github/workflows/ci.yml", "", ErrRepoPathDenied},
		{"orders-consumer", "src/CODEOWNERS", "", ErrRepoPathDenied},
		{"orders-consumer", "src/.gitignore", "", ErrRepoPathDenied},
		{"orders-consumer", "src/../etc/passwd", "", ErrRepoPathDenied},
		{"orders-consumer", "src/a*.java", "", ErrRepoPathDenied},
		{"nope", "src/a.java", "", ErrRepoNotListed},
	} {
		layer, err := p.RepoLayerOf(tc.repo, tc.path)
		if tc.err != nil {
			if !errors.Is(err, tc.err) {
				t.Errorf("RepoLayerOf(%s, %s) = %q, %v; want %v", tc.repo, tc.path, layer, err, tc.err)
			}
			if layer != "" {
				t.Errorf("RepoLayerOf(%s, %s) = %q with an error", tc.repo, tc.path, layer)
			}
			continue
		}
		if err != nil || layer != tc.layer {
			t.Errorf("RepoLayerOf(%s, %s) = %q, %v; want %q", tc.repo, tc.path, layer, err, tc.layer)
		}
		rr, _ := p.Repo(tc.repo)
		if !rr.Allows(tc.path) {
			t.Errorf("Allows(%s) = false", tc.path)
		}
	}
	if r2.Allows("src/main/resources/schema/orders.avsc") {
		t.Fatal("an ambiguous path is not allowed")
	}
}

func TestReposAbsentAndBatch(t *testing.T) {
	// Existing files have no repos: block; nothing changes for them.
	o, err := Load(writeTemp(t, validYAML))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	p, _ := o.Pipeline("orders")
	if _, err := p.RepoLayerOf("x", "a"); !errors.Is(err, ErrRepoNotListed) {
		t.Fatalf("no repos: %v", err)
	}
	// Rule 6: a batch pipeline may list repos, with its own layers.
	_, err = Load(writeTemp(t, `
pipelines:
  - id: nightly
    kind: batch
    dags: [nightly]
    repos:
      - id: dbt
        url: https://github.com/acme/dbt
        base: main
        paths: [{glob: "models/**/*.sql", layer: transform}]
`))
	if err != nil {
		t.Fatalf("batch repos: %v", err)
	}
	_, err = Load(writeTemp(t, `
pipelines:
  - id: nightly
    kind: batch
    dags: [nightly]
    repos:
      - id: dbt
        url: https://github.com/acme/dbt
        base: main
        paths: [{glob: "models/**", layer: kafka}]
`))
	if err == nil || !strings.Contains(err.Error(), `layer "kafka" is not a layer of the pipeline`) {
		t.Fatalf("batch repo with a cdc layer: %v", err)
	}
}

// repo returns one repos: entry with the given fields replaced.
func repo(id, url, base, paths string) string {
	return "      - id: " + id + "\n        url: " + url + "\n        base: " + base + "\n        paths: " + paths + "\n"
}

const okPaths = `[{glob: "connectors/*.json", layer: kafka-connect}]`

func TestReposLoadErrors(t *testing.T) {
	gh := "https://github.com/acme/cfg"
	for _, tc := range []struct {
		name, repos, extra, want string
	}{
		{"bad id", repo("a/b", gh, "main", okPaths), "", "id (a value that is not a plain name; not shown) must match"},
		{"empty id", repo(`""`, gh, "main", okPaths), "", "repos[0]: id"},
		{"dup id", repo("cfg", gh, "main", okPaths) + repo("cfg", "https://github.com/acme/other", "main", okPaths), "", `id "cfg" is listed twice`},
		{"same url twice", repo("a", gh, "main", okPaths) + repo("b", "https://github.com/Acme/CFG", "main", okPaths), "", "names the same repository twice"},
		{"no url", repo("cfg", `""`, "main", okPaths), "", "url: is required"},
		{"http", repo("cfg", "http://github.com/acme/cfg", "main", okPaths), "", "want an https:// url"},
		{"ssh", repo("cfg", "git@github.com:acme/cfg", "main", okPaths), "", "url:"},
		{"user info", repo("cfg", "https://x-access-token:ghp_secret@github.com/acme/cfg", "main", okPaths), "", "must not carry user info"},
		{"query", repo("cfg", gh+"?ref=x", "main", okPaths), "", "no query"},
		{"fragment", repo("cfg", `"`+gh+`#readme"`, "main", okPaths), "", "no query, fragment"},
		{"escape", repo("cfg", "https://github.com/acme%2Fx/cfg", "main", okPaths), "", "escape"},
		{"dot git", repo("cfg", gh+".git", "main", okPaths), "", "drop the .git suffix"},
		{"extra path", repo("cfg", gh+"/tree/main", "main", okPaths), "", "no extra path"},
		{"trailing slash", repo("cfg", gh+"/", "main", okPaths), "", "no extra path"},
		{"owner only", repo("cfg", "https://github.com/acme", "main", okPaths), "", "no extra path"},
		{"no host", repo("cfg", "https:///acme/cfg", "main", okPaths), "", "url:"},
		{"dots name", repo("cfg", "https://github.com/acme/..", "main", okPaths), "", "not be only dots"},
		{"no base", repo("cfg", gh, `""`, okPaths), "", "base: want a branch name"},
		{"base dotdot", repo("cfg", gh, "a..b", okPaths), "", "base: want a branch name"},
		{"base lock", repo("cfg", gh, "main.lock", okPaths), "", "base: want a branch name"},
		{"base dash", repo("cfg", gh, "-main", okPaths), "", "base: want a branch name"},
		{"base space", repo("cfg", gh, `"ma in"`, okPaths), "", "base: want a branch name"},
		{"base fix branch", repo("cfg", gh, "piperoom/r1-p1-0", okPaths), "", "is a fix branch"},
		{"no paths", repo("cfg", gh, "main", "[]"), "", "paths: lists no glob"},
		{"no layer", repo("cfg", gh, "main", `[{glob: "a/*"}]`), "", "paths[0]: layer: is required"},
		{"unknown layer", repo("cfg", gh, "main", `[{glob: "a/*", layer: dba}]`), "", `layer "dba" is not a layer of the pipeline`},
		{"batch layer on cdc", repo("cfg", gh, "main", `[{glob: "a/*", layer: transform}]`), "", `layer "transform" is not a layer`},
		{"glob under .github", repo("cfg", gh, "main", `[{glob: ".github/workflows/*.yml", layer: kafka-connect}]`), "", "paths[0]: glob: clients: repository path is not allowed"},
		{"glob .GitHub nested", repo("cfg", gh, "main", `[{glob: "svc/.GitHub/**", layer: kafka-connect}]`), "", "repository path is not allowed"},
		{"glob CODEOWNERS", repo("cfg", gh, "main", `[{glob: "docs/CODEOWNERS", layer: kafka-connect}]`), "", "governance file"},
		{"glob Jenkinsfile", repo("cfg", gh, "main", `[{glob: "jenkinsfile", layer: kafka-connect}]`), "", "governance file"},
		{"glob gitlab-ci", repo("cfg", gh, "main", `[{glob: ".gitlab-ci.yml", layer: kafka-connect}]`), "", "not allowed"},
		{"glob .gitmodules", repo("cfg", gh, "main", `[{glob: ".gitmodules", layer: kafka-connect}]`), "", "not allowed"},
		{"glob absolute", repo("cfg", gh, "main", `[{glob: "/etc/*", layer: kafka-connect}]`), "", "absolute"},
		{"glob dotdot", repo("cfg", gh, "main", `[{glob: "a/../b", layer: kafka-connect}]`), "", "'..' segment"},
		{"glob empty segment", repo("cfg", gh, "main", `[{glob: "a//b", layer: kafka-connect}]`), "", "empty"},
		{"glob backslash", repo("cfg", gh, "main", `[{glob: 'a\b', layer: kafka-connect}]`), "", "backslash"},
		{"glob bad class", repo("cfg", gh, "main", `[{glob: "a/[", layer: kafka-connect}]`), "", "not a valid glob"},
		{"glob partial **", repo("cfg", gh, "main", `[{glob: "a/**.json", layer: kafka-connect}]`), "", "whole segment"},
		{"glob too long", repo("cfg", gh, "main", `[{glob: "`+strings.Repeat("a", 256)+`", layer: kafka-connect}]`), "", "longer than 255"},
		{"glob twice", repo("cfg", gh, "main", `[{glob: "a/*", layer: kafka-connect}, {glob: "a/*", layer: kafka-connect}]`), "", "listed twice"},
		{"literal overlap", repo("cfg", gh, "main", `[{glob: "conf/*", layer: kafka-connect}, {glob: "conf/app.yml", layer: consumer}]`), "", "match the same file"},
		{"literal overlap **", repo("cfg", gh, "main", `[{glob: "conf/app.yml", layer: consumer}, {glob: "**", layer: kafka-connect}]`), "", "match the same file"},
		{"unknown key", repo("cfg", gh, "main", okPaths) + "        token: x\n", "", "field token not found"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeTemp(t, reposYAML(tc.repos, tc.extra)))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to contain %q", err, tc.want)
			}
			// A value that is not a plain name is never echoed.
			if strings.Contains(err.Error(), "ghp_secret") {
				t.Fatalf("error leaks the url: %v", err)
			}
		})
	}
}

func TestReposLiteralSameLayerOverlapLoads(t *testing.T) {
	// Overlap within one layer is harmless: one approver either way.
	_, err := Load(writeTemp(t, reposYAML(repo("cfg", "https://github.com/acme/cfg", "main",
		`[{glob: "conf/*", layer: consumer}, {glob: "conf/app.yml", layer: consumer}]`), "")))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
}

func TestReposAcrossPipelines(t *testing.T) {
	two := func(urlB, baseB, tenancy string) string {
		return tenancy + `
pipelines:
  - id: a
    topic_prefix: a
    source_tables: [public.a]
    connectors: [ca]
    shared_connections: true
    repos:
      - {id: cfg, url: https://github.com/acme/cfg, base: main, paths: [{glob: "a/*", layer: kafka-connect}]}
  - id: b
    topic_prefix: b
    source_tables: [public.b]
    connectors: [cb]
    shared_connections: true
    tenant: TB
    repos:
      - {id: cfg, url: ` + urlB + `, base: ` + baseB + `, paths: [{glob: "b/*", layer: consumer}]}
`
	}
	noTenant := func(s string) string { return strings.Replace(s, "    tenant: TB\n", "", 1) }

	// Same id, same url (case aside) and base in two pipelines: one repository.
	o, err := Load(writeTemp(t, noTenant(two("https://github.com/ACME/cfg", "main", ""))))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	pb, _ := o.Pipeline("b")
	if l, err := pb.RepoLayerOf("cfg", "b/x.yml"); err != nil || l != "consumer" {
		t.Fatalf("pipeline b globs are its own: %q, %v", l, err)
	}
	if _, err := pb.RepoLayerOf("cfg", "a/x.yml"); !errors.Is(err, ErrPathNotListed) {
		t.Fatalf("pipeline a's globs do not apply to b: %v", err)
	}
	if r, ok := o.TenantRepo(DefaultTenant, "cfg"); !ok || r.Ref().Name != "cfg" {
		t.Fatalf("TenantRepo = %+v, %v", r, ok)
	}

	for _, tc := range []struct{ name, yaml, want string }{
		{"two urls", noTenant(two("https://github.com/acme/other", "main", "")), `repo "cfg": pipelines "a" and "b" give it two urls`},
		{"two bases", noTenant(two("https://github.com/acme/cfg", "dev", "")), "give it two bases"},
		{"two tenants", two("https://github.com/acme/cfg", "main", "tenants: {default: {members: []}, TB: {members: []}}\n"), "a repo serves one tenant"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := Load(writeTemp(t, tc.yaml))
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want %q", err, tc.want)
			}
		})
	}

	// A tenant's repos: may not name an id that only another tenant lists.
	_, err = Load(writeTemp(t, `
tenants: {TA: {members: []}, TB: {members: [], repos: [cfg]}}
pipelines:
  - id: a
    tenant: TA
    topic_prefix: a
    source_tables: [public.a]
    connectors: [ca]
    shared_connections: true
    repos:
      - {id: cfg, url: https://github.com/acme/cfg, base: main, paths: [{glob: "a/*", layer: kafka-connect}]}
`))
	if err == nil || !strings.Contains(err.Error(), `tenants.TB: repos: "cfg" is listed by pipeline "a" of tenant "TA"`) {
		t.Fatalf("cross-tenant repos: %v", err)
	}

	// Tenant scoping of the resolver.
	o, err = Load(writeTemp(t, `
tenants: {TA: {members: [], repos: [cfg, later]}, TB: {members: []}}
pipelines:
  - id: a
    tenant: TA
    topic_prefix: a
    source_tables: [public.a]
    connectors: [ca]
    shared_connections: true
    repos:
      - {id: cfg, url: https://github.com/acme/cfg, base: main, paths: [{glob: "a/*", layer: kafka-connect}]}
`))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if _, ok := o.TenantRepo("TA", "cfg"); !ok {
		t.Fatal("TA resolves cfg")
	}
	if _, ok := o.TenantRepo("TB", "cfg"); ok {
		t.Fatal("TB must not resolve TA's repo")
	}
	if _, ok := o.TenantRepo("TA", "later"); ok {
		t.Fatal("an id no pipeline lists does not resolve")
	}
	var nilO *Owners
	if _, ok := nilO.TenantRepo("TA", "cfg"); ok {
		t.Fatal("nil owners")
	}
}

func TestCheckRepoPath(t *testing.T) {
	for _, tc := range []struct {
		p    string
		glob bool
		ok   bool
	}{
		{"connectors/orders.json", false, true},
		{"Dockerfile", false, true},
		{"a/b/c.yml", false, true},
		{"a/*.yml", true, true},
		{"**/x.yml", true, true},
		{"a/[ab]?.yml", true, true},
		{"a/*.yml", false, false},
		{"", false, false},
		{"/a", false, false},
		{"a/", false, false},
		{"./a", false, false},
		{"a/./b", false, false},
		{"a/../b", false, false},
		{"a\\b", false, false},
		{"a\x00b", false, false},
		{"a\nb", false, false},
		{"a\x7fb", false, false},
		{"\xff", false, false},
		{".github/workflows/ci.yml", false, false},
		{"x/.GITHUB/y", false, false},
		{".circleci/config.yml", false, false},
		{".husky/pre-commit", false, false},
		{".devcontainer/devcontainer.json", false, false},
		{".git/config", false, false},
		{".gitkeep", false, false},
		{"CODEOWNERS", false, false},
		{"docs/codeowners", false, false},
		{"Jenkinsfile", false, false},
		{"ci/azure-pipelines.yml", false, false},
		{"bitbucket-pipelines.yml", false, false},
		{".pre-commit-config.yaml", false, false},
		{".travis.yml", false, false},
		{".drone.yml", false, false},
		// A denied file name as a directory segment is a directory, not the file.
		{"jenkinsfile/readme.md", false, true},
		{strings.Repeat("a", 255), false, true},
		{strings.Repeat("a", 256), false, false},
		{"a/**b", true, false},
		{"a/[", true, false},
	} {
		err := CheckRepoPath(tc.p, tc.glob)
		if (err == nil) != tc.ok {
			t.Errorf("CheckRepoPath(%q, %v) = %v, want ok=%v", tc.p, tc.glob, err, tc.ok)
		}
		if err != nil && !errors.Is(err, ErrRepoPathDenied) {
			t.Errorf("CheckRepoPath(%q) error does not wrap ErrRepoPathDenied: %v", tc.p, err)
		}
	}
}

func TestMatchGlob(t *testing.T) {
	for _, tc := range []struct {
		pat, name string
		want      bool
	}{
		{"a/*.json", "a/x.json", true},
		{"a/*.json", "a/b/x.json", false},
		{"**", "a/b/c", true},
		{"**", "a", true},
		{"a/**", "a", true}, // zero segments
		{"a/**", "a/b/c", true},
		{"a/**/c", "a/c", true},
		{"a/**/c", "a/b/b/c", true},
		{"a/**/c", "a/b/b/d", false},
		{"**/**/x", "x", true},
		{"a/?.yml", "a/b.yml", true},
		{"a/?.yml", "a/bb.yml", false},
		{"A/b", "a/b", false},
	} {
		if got := matchGlob(tc.pat, tc.name); got != tc.want {
			t.Errorf("matchGlob(%q, %q) = %v, want %v", tc.pat, tc.name, got, tc.want)
		}
	}
	// Many "**" against a long path that fails at the end: a backtracking
	// matcher takes exponential time here; the table takes microseconds.
	pat := strings.Repeat("**/a/", 25) + "b"
	name := strings.TrimSuffix(strings.Repeat("a/", 120), "/")
	if matchGlob(pat, name) {
		t.Fatal("no b segment, no match")
	}
	if !matchGlob(pat, name+"/b") {
		t.Fatal("want a match")
	}
}

func TestValidBranchName(t *testing.T) {
	for _, tc := range []struct {
		s  string
		ok bool
	}{
		{"main", true}, {"release/2026.10", true}, {"feature_x-1", true},
		{"", false}, {strings.Repeat("a", 201), false}, {strings.Repeat("a", 200), true},
		{"a..b", false}, {"a//b", false}, {"a@{b", false}, {"-a", false}, {"/a", false},
		{"a/", false}, {"a.", false}, {"a/.b", false}, {".a", false}, {"a.lock", false},
		{"a.lock/b", false}, {"ma in", false}, {"a~b", false}, {"a:b", false}, {"é", false},
	} {
		if got := ValidBranchName(tc.s); got != tc.ok {
			t.Errorf("ValidBranchName(%q) = %v, want %v", tc.s, got, tc.ok)
		}
	}
}

func TestRefWithoutLoad(t *testing.T) {
	r := Repo{ID: "x", URL: "https://github.com/o/n", Base: "main"}
	if got := r.Ref(); got.Owner != "o" || got.Name != "n" {
		t.Fatalf("Ref = %+v", got)
	}
	if got := (Repo{ID: "x", URL: "nope"}).Ref(); got.Owner != "" || got.Name != "" {
		t.Fatalf("bad url Ref = %+v", got)
	}
}

func TestAutoApproveRefusesDeferred(t *testing.T) {
	path := writeTemp(t, `
pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [pg-orders]
    auto_approve: [connect_restart_task, repo_open_pull_request]
`)
	names := []string{"connect_restart_task", "repo_open_pull_request"}
	if _, err := LoadWithActions(path, names); err != nil {
		t.Fatalf("LoadWithActions without deferred names: %v", err)
	}
	_, err := LoadWithDeferred(path, names, []string{"repo_open_pull_request"})
	if err == nil || !strings.Contains(err.Error(), `"repo_open_pull_request" is never approved by a policy`) {
		t.Fatalf("LoadWithDeferred: %v", err)
	}
	o, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := o.ValidateAutoApproveDeferred(names, []string{"repo_open_pull_request"}); err == nil {
		t.Fatal("ValidateAutoApproveDeferred must refuse a deferred action")
	}
	if err := o.ValidateAutoApproveDeferred(names, []string{"other"}); err != nil {
		t.Fatalf("no deferred entry: %v", err)
	}
	// Unknown names are still refused.
	if err := o.ValidateAutoApproveDeferred([]string{"connect_restart_task"}, nil); err == nil {
		t.Fatal("an unknown name is refused")
	}
	var nilO *Owners
	if err := nilO.ValidateAutoApproveDeferred(nil, nil); err != nil {
		t.Fatal(err)
	}
}

// Every repos[].url must be on the forge's web host: CheckRepoHosts refuses
// another host (here the GHES url of orders-consumer against github.com), and
// Ref records the host so the forge client can refuse it on every call too.
func TestCheckRepoHosts(t *testing.T) {
	o, err := Load(writeTemp(t, reposYAML(goodRepos, "")))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	err = o.CheckRepoHosts("github.com")
	if err == nil || !strings.Contains(err.Error(), "orders-consumer") || strings.Contains(err.Error(), "ghe.example.com") {
		t.Fatalf("CheckRepoHosts(github.com) = %v, want a refusal naming the repo id and not the url", err)
	}
	p, _ := o.Pipeline("orders")
	r, _ := p.Repo("orders-consumer")
	if h := r.Ref().Host; h != "ghe.example.com:8443" {
		t.Fatalf("Ref().Host = %q", h)
	}
	if err := (&Owners{}).CheckRepoHosts("github.com"); err != nil {
		t.Fatalf("no repos: %v", err)
	}
	one := &Owners{Pipelines: []Pipeline{{ID: "p", Repos: []Repo{{ID: "a", URL: "https://GitHub.com/acme/a"}}}}}
	if err := one.CheckRepoHosts("github.com"); err != nil {
		t.Fatalf("same host, other case: %v", err)
	}
	if err := one.CheckRepoHosts("ghe.example.com"); err == nil {
		t.Fatal("github.com url accepted for a GHES forge")
	}
}
