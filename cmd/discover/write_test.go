package main

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"piperoom/internal/config"
	"piperoom/internal/discover"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
)

// cdcLayers are the layers both generated files must name: discovery finds CDC
// pipelines only, and events.Layers also holds the batch-only ones.
func cdcLayers() []string { return events.LayersOf(events.PipelineKindCDC) }

// goodResult is two pipelines that pass owners.Load: one with a found sink
// table, one without.
func goodResult() discover.Result {
	return discover.Result{
		Pipelines: []discover.Pipeline{
			{ID: "pg-billing", Connector: "pg-billing-connector", TopicPrefix: "billing",
				SourceTables: []string{"public.invoices"}, Topics: []string{"billing.public.invoices"}},
			{ID: "pg-orders", Connector: "pg-orders-connector", TopicPrefix: "pg",
				SourceTables: []string{"public.orders"}, Topics: []string{"pg.public.orders"},
				ConsumerGroups: []string{"order-sync"}, SinkTables: []string{"analytics.orders"}},
		},
		Skipped:  []discover.Skipped{{Connector: "s3-sink", Reason: "not a Debezium Postgres source"}},
		Warnings: []string{"two connectors share prefix pg"},
	}
}

// resultRunner returns a runner whose discovery yields res. The owned and
// users renderers are the real ones (withDefaults).
func resultRunner(res discover.Result) runner {
	r := testRunner(new(bool))
	r.discover = func(context.Context, discover.Deps, discover.Options) (discover.Result, error) { return res, nil }
	return r
}

// names lists a directory, to prove no temporary file is left behind.
func names(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, e := range entries {
		out = append(out, e.Name())
	}
	return out
}

func TestWriteOwnersLoadsWithTheRealLoaders(t *testing.T) {
	dir := t.TempDir()
	ownersPath, usersPath := filepath.Join(dir, "owners.yaml"), filepath.Join(dir, "users.yaml")
	code, stdout, stderr := runT(t, resultRunner(goodResult()), "-write-owners", ownersPath, "-users", usersPath, "-owner", "alice")
	if code != exitOK {
		t.Fatalf("exit = %d; stderr:\n%s", code, stderr)
	}
	if got := names(t, dir); !slices.Equal(got, []string{"owners.yaml", "users.yaml"}) {
		t.Errorf("directory = %v, want only the two files", got)
	}

	own, err := owners.Load(ownersPath)
	if err != nil {
		t.Fatalf("owners.Load: %v", err)
	}
	if len(own.Pipelines) != 2 {
		t.Errorf("pipelines = %+v", own.Pipelines)
	}
	alice := owners.Principal{Kind: owners.KindUser, Name: "alice"}
	for _, layer := range cdcLayers() {
		if ps := own.Layers[layer]; len(ps) != 1 || !ps.Contains(alice) {
			t.Errorf("layer %s = %v, want user:alice", layer, ps)
		}
	}
	for _, p := range own.Pipelines {
		if len(p.SinkTables) != 0 {
			t.Errorf("%s: sink_tables = %v, want empty (the column is unknown)", p.ID, p.SinkTables)
		}
	}

	// room is the server's own loader for users.yaml and its agreement check.
	users, err := room.LoadUsers(usersPath)
	if err != nil {
		t.Fatalf("room.LoadUsers: %v", err)
	}
	if err := room.ValidateUsers(users, own); err != nil {
		t.Fatalf("room.ValidateUsers: %v", err)
	}
	u, ok := users.Get("alice")
	if !ok || u.Name != "alice" || !u.Verified {
		t.Errorf("alice = %+v, %v", u, ok)
	}

	for _, want := range []string{"PIPELINE", "SINK TABLES", "pg-billing", "pg-orders", "billing.public.invoices", "MISSING", "analytics.orders (TODO)", ownersPath + ", " + usersPath} {
		if !strings.Contains(stdout, want) {
			t.Errorf("stdout lacks %q:\n%s", want, stdout)
		}
	}
	if !strings.Contains(stderr, "discover: 2 pipelines, 1 skipped connectors, 1 warnings\n") {
		t.Errorf("stderr lacks the counts line:\n%s", stderr)
	}
}

func TestOutAloneStillPrintsNoTable(t *testing.T) {
	code, stdout, stderr := runT(t, resultRunner(goodResult()), "-out", filepath.Join(t.TempDir(), "o.yaml"))
	if code != exitOK || stdout != "" {
		t.Fatalf("exit = %d, stdout = %q; stderr:\n%s", code, stdout, stderr)
	}
}

func TestOutAndWriteOwnersTogether(t *testing.T) {
	dir := t.TempDir()
	out, ownersPath := filepath.Join(dir, "draft.yaml"), filepath.Join(dir, "owners.yaml")
	code, stdout, stderr := runT(t, resultRunner(goodResult()), "-out", out, "-write-owners", ownersPath, "-owner", "alice")
	if code != exitOK {
		t.Fatalf("exit = %d; stderr:\n%s", code, stderr)
	}
	assertFile(t, out, draft) // -out keeps its own renderer
	if _, err := owners.Load(ownersPath); err != nil {
		t.Errorf("owners.Load: %v", err)
	}
	if !strings.Contains(stdout, out+", "+ownersPath) {
		t.Errorf("table does not list both files:\n%s", stdout)
	}
}

func TestOutStdoutKeepsStdoutForTheDraft(t *testing.T) {
	ownersPath := filepath.Join(t.TempDir(), "owners.yaml")
	code, stdout, stderr := runT(t, resultRunner(goodResult()), "-out", "-", "-write-owners", ownersPath, "-owner", "alice")
	if code != exitOK {
		t.Fatalf("exit = %d; stderr:\n%s", code, stderr)
	}
	if stdout != draft {
		t.Errorf("stdout = %q, want only the draft", stdout)
	}
	if !strings.Contains(stderr, "PIPELINE") || !strings.Contains(stderr, ownersPath) {
		t.Errorf("the table should be on stderr and name the owners file:\n%s", stderr)
	}
}

func TestDraftOnValidationError(t *testing.T) {
	dup := goodResult()
	dup.Pipelines[1].ID = dup.Pipelines[0].ID
	noTables := goodResult()
	noTables.Pipelines[0].SourceTables = nil
	noPrefix := goodResult()
	noPrefix.Pipelines[1].TopicPrefix = ""
	tests := []struct {
		name string
		res  discover.Result
		want string
	}{
		{"duplicate pipeline id", dup, `duplicate pipeline id "pg-billing"`},
		{"no source tables", noTables, `pipeline "pg-billing": no source tables`},
		{"no topic prefix", noPrefix, `pipeline "pg-orders": topic_prefix is required`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			ownersPath, usersPath := filepath.Join(dir, "owners.yaml"), filepath.Join(dir, "users.yaml")
			code, stdout, stderr := runT(t, resultRunner(tt.res), "-write-owners", ownersPath, "-users", usersPath, "-owner", "alice")
			if code != exitError {
				t.Fatalf("exit = %d, want %d; stderr:\n%s", code, exitError, stderr)
			}
			draftPath := ownersPath + ".draft"
			if got := names(t, dir); !slices.Equal(got, []string{"owners.yaml.draft", "users.yaml"}) {
				t.Fatalf("directory = %v, want the .draft and the users file only", got)
			}
			// The message names the .draft and the reason.
			for _, want := range []string{"owners.Load", draftPath, tt.want} {
				if !strings.Contains(stderr, want) {
					t.Errorf("stderr lacks %q:\n%s", want, stderr)
				}
			}
			raw, err := os.ReadFile(draftPath)
			if err != nil {
				t.Fatal(err)
			}
			head, body, ok := strings.Cut(string(raw), "\n\n")
			if !ok || !strings.HasPrefix(head, "# DRAFT: this file did not pass owners.Load") || !strings.Contains(head, tt.want) {
				t.Errorf("draft header = %q", head)
			}
			for _, l := range strings.Split(head, "\n") {
				if !strings.HasPrefix(l, "#") {
					t.Errorf("header line %q is not a comment", l)
				}
			}
			if strings.Contains(head, ".owners-") {
				t.Errorf("header names the temporary file:\n%s", head)
			}
			// What follows is the same YAML a good run writes, so a human can fix it.
			var doc struct {
				Pipelines []map[string]any  `yaml:"pipelines"`
				Layers    map[string]string `yaml:"layers"`
			}
			if err := yaml.Unmarshal([]byte(body), &doc); err != nil || len(doc.Pipelines) != 2 || doc.Layers["kafka"] != "user:alice" {
				t.Errorf("draft body does not parse as the owners file (%v): %+v\n%s", err, doc, body)
			}
			// The table says where the pipelines went.
			if !strings.Contains(stdout, draftPath) {
				t.Errorf("table does not name the .draft:\n%s", stdout)
			}
			// The users file is still written: it matches the draft's layers.
			if _, err := room.LoadUsers(usersPath); err != nil {
				t.Errorf("users file: %v", err)
			}
		})
	}
}

func TestInvalidDraftLeavesAnExistingOwnersFileAlone(t *testing.T) {
	dir := t.TempDir()
	ownersPath := filepath.Join(dir, "owners.yaml")
	const old = "pipelines: []\nlayers: {}\n"
	if err := os.WriteFile(ownersPath, []byte(old), 0o644); err != nil {
		t.Fatal(err)
	}
	bad := goodResult()
	bad.Pipelines[1].ID = bad.Pipelines[0].ID
	code, _, stderr := runT(t, resultRunner(bad), "-force", "-write-owners", ownersPath, "-owner", "alice")
	if code != exitError {
		t.Fatalf("exit = %d; stderr:\n%s", code, stderr)
	}
	assertFile(t, ownersPath, old)
	if _, err := os.Stat(ownersPath + ".draft"); err != nil {
		t.Errorf("no .draft: %v", err)
	}
}

func TestExistingDraftIsNotOverwrittenWithoutForce(t *testing.T) {
	dir := t.TempDir()
	ownersPath := filepath.Join(dir, "owners.yaml")
	const mine = "# my edits\n"
	if err := os.WriteFile(ownersPath+".draft", []byte(mine), 0o644); err != nil {
		t.Fatal(err)
	}
	bad := goodResult()
	bad.Pipelines[1].ID = bad.Pipelines[0].ID
	code, _, stderr := runT(t, resultRunner(bad), "-write-owners", ownersPath, "-owner", "alice")
	if code != exitError {
		t.Fatalf("exit = %d; stderr:\n%s", code, stderr)
	}
	assertFile(t, ownersPath+".draft", mine)
	// The reason is not lost with the refused write.
	for _, want := range []string{"already exists", "-force", "duplicate pipeline id"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("stderr lacks %q:\n%s", want, stderr)
		}
	}
	if got := names(t, dir); !slices.Equal(got, []string{"owners.yaml.draft"}) {
		t.Errorf("directory = %v", got)
	}
}

func TestWriteOwnersRefusesToOverwrite(t *testing.T) {
	for _, existing := range []string{"owners.yaml", "users.yaml", "draft.yaml"} {
		t.Run(existing, func(t *testing.T) {
			dir := t.TempDir()
			const old = "hand edited\n"
			if err := os.WriteFile(filepath.Join(dir, existing), []byte(old), 0o644); err != nil {
				t.Fatal(err)
			}
			built := false
			r := resultRunner(goodResult())
			r.build = func(context.Context, config.Config, bool) (discover.Deps, func(), error) {
				built = true
				return discover.Deps{}, func() {}, nil
			}
			code, _, stderr := runT(t, r, "-out", filepath.Join(dir, "draft.yaml"),
				"-write-owners", filepath.Join(dir, "owners.yaml"), "-users", filepath.Join(dir, "users.yaml"))
			if code != exitError || !strings.Contains(stderr, existing+" already exists") {
				t.Fatalf("exit = %d; stderr:\n%s", code, stderr)
			}
			if built {
				t.Error("cluster was read although an output could not be written")
			}
			// All or nothing: no other file appears.
			if got := names(t, dir); !slices.Equal(got, []string{existing}) {
				t.Errorf("directory = %v, want only %s", got, existing)
			}
			assertFile(t, filepath.Join(dir, existing), old)
		})
	}
}

func TestOwnersFileAppearsDuringRun(t *testing.T) {
	// The pre-check passes, then another process creates the file: the
	// exclusive install must refuse to replace it.
	dir := t.TempDir()
	ownersPath := filepath.Join(dir, "owners.yaml")
	const old = "written meanwhile\n"
	r := resultRunner(goodResult())
	r.discover = func(context.Context, discover.Deps, discover.Options) (discover.Result, error) {
		if err := os.WriteFile(ownersPath, []byte(old), 0o644); err != nil {
			t.Fatal(err)
		}
		return goodResult(), nil
	}
	code, _, stderr := runT(t, r, "-write-owners", ownersPath, "-owner", "alice")
	if code != exitError || !strings.Contains(stderr, "already exists") {
		t.Fatalf("exit = %d; stderr:\n%s", code, stderr)
	}
	assertFile(t, ownersPath, old)
	if got := names(t, dir); !slices.Equal(got, []string{"owners.yaml"}) {
		t.Errorf("directory = %v", got)
	}
}

func TestForceReplacesOwnersAndUsers(t *testing.T) {
	dir := t.TempDir()
	ownersPath, usersPath := filepath.Join(dir, "owners.yaml"), filepath.Join(dir, "users.yaml")
	for _, p := range []string{ownersPath, usersPath} {
		if err := os.WriteFile(p, []byte(strings.Repeat("old and rather long\n", 100)), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	code, _, stderr := runT(t, resultRunner(goodResult()), "-force", "-write-owners", ownersPath, "-users", usersPath, "-owner", "bob")
	if code != exitOK {
		t.Fatalf("exit = %d; stderr:\n%s", code, stderr)
	}
	own, err := owners.Load(ownersPath)
	if err != nil {
		t.Fatalf("owners.Load: %v", err)
	}
	users, err := room.LoadUsers(usersPath)
	if err != nil {
		t.Fatalf("room.LoadUsers: %v", err)
	}
	if err := room.ValidateUsers(users, own); err != nil {
		t.Fatal(err)
	}
	if got := names(t, dir); !slices.Equal(got, []string{"owners.yaml", "users.yaml"}) {
		t.Errorf("directory = %v", got)
	}
	if fi, err := os.Stat(ownersPath); err != nil || fi.Mode().Perm() != 0o644 {
		t.Errorf("owners file mode = %v, %v; want 0644 like the other outputs", fi.Mode(), err)
	}
}

func TestMissingDirectoryFailsBeforeTheCluster(t *testing.T) {
	built := false
	r := resultRunner(goodResult())
	r.build = func(context.Context, config.Config, bool) (discover.Deps, func(), error) {
		built = true
		return discover.Deps{}, func() {}, nil
	}
	code, _, stderr := runT(t, r, "-write-owners", filepath.Join(t.TempDir(), "nope", "owners.yaml"))
	if code != exitError || built || !strings.Contains(stderr, "does not exist") {
		t.Fatalf("exit = %d, built = %v; stderr:\n%s", code, built, stderr)
	}
}

func TestFailedRenderOwnedWritesNothing(t *testing.T) {
	dir := t.TempDir()
	r := resultRunner(goodResult())
	r.renderOwned = func(w io.Writer, _ discover.Result, _ string) error {
		_, _ = io.WriteString(w, "partial")
		return errors.New("boom")
	}
	code, _, stderr := runT(t, r, "-write-owners", filepath.Join(dir, "owners.yaml"), "-users", filepath.Join(dir, "users.yaml"))
	if code != exitError {
		t.Fatalf("exit = %d; stderr:\n%s", code, stderr)
	}
	if got := names(t, dir); len(got) != 0 {
		t.Errorf("files written: %v", got)
	}
}

func TestNoPipelinesWritesNoOwnersFile(t *testing.T) {
	dir := t.TempDir()
	res := discover.Result{Skipped: []discover.Skipped{{Connector: "s3-sink", Reason: "not a Debezium Postgres source"}}}
	code, stdout, stderr := runT(t, resultRunner(res), "-write-owners", filepath.Join(dir, "owners.yaml"), "-users", filepath.Join(dir, "users.yaml"))
	if code != exitError {
		t.Fatalf("exit = %d; stderr:\n%s", code, stderr)
	}
	for _, want := range []string{"no Debezium Postgres source connector found", "nothing written", "skipped: s3-sink: not a Debezium Postgres source"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("stderr lacks %q:\n%s", want, stderr)
		}
	}
	if got := names(t, dir); len(got) != 0 || stdout != "" {
		t.Errorf("files = %v, stdout = %q", got, stdout)
	}
}

func TestFlagRules(t *testing.T) {
	dir := t.TempDir()
	p := func(name string) string { return filepath.Join(dir, name) }
	tests := []struct {
		name string
		args []string
		want string
	}{
		{"owner without write-owners", []string{"-out", p("a"), "-owner", "alice"}, "go with -write-owners"},
		{"users without write-owners", []string{"-out", p("a"), "-users", p("u")}, "go with -write-owners"},
		{"write-owners to stdout", []string{"-write-owners", "-"}, "need a file path"},
		{"users to stdout", []string{"-write-owners", p("o"), "-users", "-"}, "need a file path"},
		{"same file twice", []string{"-write-owners", p("o"), "-users", p("o")}, "name the same file"},
		{"same file, different spelling", []string{"-out", p("o"), "-write-owners", dir + string(filepath.Separator) + "." + string(filepath.Separator) + "o"}, "name the same file"},
		{"owner with a space", []string{"-write-owners", p("o"), "-owner", "a b"}, "whitespace"},
		{"owner with a colon", []string{"-write-owners", p("o"), "-owner", "user:a"}, "colon"},
		{"owner with the reserved prefix", []string{"-write-owners", p("o"), "-owner", "u_alice"}, "reserved"},
		{"empty owner", []string{"-write-owners", p("o"), "-owner", ""}, "empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			built := false
			r := resultRunner(goodResult())
			r.build = func(context.Context, config.Config, bool) (discover.Deps, func(), error) {
				built = true
				return discover.Deps{}, func() {}, nil
			}
			code, _, stderr := runT(t, r, tt.args...)
			if code != exitUsage || built || !strings.Contains(stderr, tt.want) {
				t.Fatalf("exit = %d, built = %v, want usage error containing %q; stderr:\n%s", code, built, tt.want, stderr)
			}
			if got := names(t, dir); len(got) != 0 {
				t.Errorf("files written: %v", got)
			}
		})
	}
}

func TestDefaultOwner(t *testing.T) {
	osUser := func(name string, err error) func() (string, error) {
		return func() (string, error) { return name, err }
	}
	env := func(user string) func(string) string {
		return func(k string) string {
			if k == "USER" {
				return user
			}
			return ""
		}
	}
	tests := []struct {
		name string
		os   func() (string, error)
		env  func(string) string
		want string
	}{
		{"OS user wins", osUser("alice", nil), env("bob"), "alice"},
		{"OS lookup fails, $USER", osUser("", errors.New("no passwd entry")), env("bob"), "bob"},
		{"OS name empty, $USER", osUser("", nil), env("bob"), "bob"},
		{"Windows domain is cut", osUser(`CORP\alice`, nil), env(""), "alice"},
		{"OS name with a space is skipped", osUser("John Smith", nil), env("jsmith"), "jsmith"},
		{"reserved prefix is skipped", osUser("u_build", nil), env("bob"), "bob"},
		{"both unusable", osUser("John Smith", nil), env("a:b"), fallbackOwner},
		{"nothing at all", osUser("", errors.New("x")), env(""), "piperoom"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := defaultOwner(tt.os, tt.env); got != tt.want {
				t.Errorf("defaultOwner = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestOwnerDefaultsWhenTheFlagIsAbsent(t *testing.T) {
	tests := []struct {
		name string
		os   func() (string, error)
		env  string
		args []string
		want string
	}{
		{"OS user", func() (string, error) { return "carol", nil }, "dave", nil, "carol"},
		{"$USER", func() (string, error) { return "", errors.New("no user") }, "dave", nil, "dave"},
		{"piperoom", func() (string, error) { return "", errors.New("no user") }, "", nil, "piperoom"},
		{"flag wins", func() (string, error) { return "carol", nil }, "dave", []string{"-owner", "erin"}, "erin"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("USER", tt.env)
			dir := t.TempDir()
			ownersPath, usersPath := filepath.Join(dir, "owners.yaml"), filepath.Join(dir, "users.yaml")
			r := resultRunner(goodResult())
			r.osUser = tt.os
			args := append([]string{"-write-owners", ownersPath, "-users", usersPath}, tt.args...)
			if code, _, stderr := runT(t, r, args...); code != exitOK {
				t.Fatalf("exit = %d; stderr:\n%s", code, stderr)
			}
			own, err := owners.Load(ownersPath)
			if err != nil {
				t.Fatal(err)
			}
			users, err := room.LoadUsers(usersPath)
			if err != nil {
				t.Fatal(err)
			}
			if err := room.ValidateUsers(users, own); err != nil {
				t.Fatal(err)
			}
			if got := own.Layers["kafka"].Users(); !slices.Equal(got, []string{tt.want}) {
				t.Errorf("kafka owner = %v, want %s", got, tt.want)
			}
			if _, ok := users.Get(tt.want); !ok {
				t.Errorf("users file lacks %s", tt.want)
			}
		})
	}
}

func TestHelpShowsTheOwnerDefault(t *testing.T) {
	r := testRunner(new(bool))
	r.osUser = func() (string, error) { return "carol", nil }
	_, _, stderr := runT(t, r, "-h")
	for _, want := range []string{"-write-owners", "-owner", `(default "carol")`, "-users", "FILE.draft"} {
		if !strings.Contains(stderr, want) {
			t.Errorf("usage lacks %q:\n%s", want, stderr)
		}
	}
}

func TestPrintTable(t *testing.T) {
	res := discover.Result{Pipelines: []discover.Pipeline{
		{ID: "a", Connector: "a-connector", Topics: []string{"p.public.t1"}, SourceTables: []string{"public.t1"}},
		{ID: "b", Connector: "b\tconnector\nline", Topics: []string{"1", "2", "3", "4", "5"},
			SourceTables: []string{"public.x", "public.y"}, SinkTables: []string{"w.x", "w.y"}},
		{ID: "c", Connector: "c"},
	}}
	var buf bytes.Buffer
	printTable(&buf, res, []string{"owners.yaml", "users.yaml"}, newRedactor(config.Config{}))
	out := buf.String()

	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) != 5 { // header, three rows, TODO line
		t.Fatalf("got %d lines:\n%s", len(lines), out)
	}
	cols := func(l string) []string { return strings.Fields(l) }
	if got := cols(lines[0]); !slices.Equal(got, []string{"PIPELINE", "CONNECTORS", "TOPICS", "SOURCE", "TABLES", "SINK", "TABLES", "FILES"}) {
		t.Errorf("header = %v", got)
	}
	// A missing sink table is MISSING; a found one is named and still a TODO.
	if !strings.Contains(lines[1], "MISSING") || !strings.Contains(lines[3], "MISSING") {
		t.Errorf("rows without a sink table must say MISSING:\n%s", out)
	}
	if strings.Contains(lines[2], "MISSING") || !strings.Contains(lines[2], "w.x, w.y (TODO)") {
		t.Errorf("row b = %q", lines[2])
	}
	// More than three names are cut and counted; an empty list is "-".
	if !strings.Contains(lines[2], "1, 2, 3 +2 more") {
		t.Errorf("row b does not cut its topics: %q", lines[2])
	}
	if !strings.Contains(lines[3], " - ") {
		t.Errorf("row c has no '-' for empty lists: %q", lines[3])
	}
	// A hostile connector name cannot add a column or a line.
	if !strings.Contains(lines[2], "b connector line") {
		t.Errorf("row b connector = %q", lines[2])
	}
	for _, l := range lines[1:4] {
		if !strings.HasSuffix(l, "owners.yaml, users.yaml") {
			t.Errorf("row %q does not end with the files", l)
		}
	}
	if !strings.HasPrefix(lines[4], "TODO: fill in sink_tables") {
		t.Errorf("last line = %q", lines[4])
	}

	buf.Reset()
	printTable(&buf, discover.Result{}, nil, newRedactor(config.Config{}))
	if strings.Contains(buf.String(), "TODO") {
		t.Errorf("an empty table should not ask for sink tables:\n%s", buf.String())
	}
}

func TestPrintTableHidesAddresses(t *testing.T) {
	setSecretEnv(t)
	res := discover.Result{Pipelines: []discover.Pipeline{{ID: "a", Connector: "http://secret-host:8083/x", SourceTables: []string{"secret-host.t"}}}}
	var buf bytes.Buffer
	printTable(&buf, res, []string{"o.yaml"}, newRedactor(config.Load()))
	assertNoSecrets(t, buf.String())
}
