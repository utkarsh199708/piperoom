package evals

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gowebpki/jcs"
	"gopkg.in/yaml.v3"

	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/redact"
)

// The fixtures in testdata/rooms are real export documents: go run -tags
// evalgen ./evals/gen -rooms builds them with the gen fakes, the real tools and
// room.Service.PrepareExport (hash included). Regenerate them, never edit.

const (
	fixConfirmed   = "testdata/rooms/orders-confirmed.json"
	fixHard        = "testdata/rooms/orders-hard.json"
	fixShadow      = "testdata/rooms/orders-shadow.json"
	fixUnlabelled  = "testdata/rooms/orders-unlabelled.json"
	fixOpen        = "testdata/rooms/orders-open.json"
	fixUnrecovered = "testdata/rooms/orders-unrecovered.json"
	fixSecret      = "testdata/rooms/orders-secret.json"
	plantedSecret  = "hunter2-fixture-only"
)

func fixture(t *testing.T, path string) []byte {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func devOwners(t *testing.T) *owners.Owners {
	t.Helper()
	o, err := owners.Load("../dev/owners.yaml")
	if err != nil {
		t.Fatal(err)
	}
	return o
}

func buildFixture(t *testing.T, path string, mod func(*LearnedOptions)) (LearnedResult, error) {
	t.Helper()
	o := LearnedOptions{Owners: devOwners(t), Validate: true}
	if mod != nil {
		mod(&o)
	}
	return BuildLearned(fixture(t, path), o)
}

func TestFixturesVerify(t *testing.T) {
	paths, _ := filepath.Glob("testdata/rooms/*.json")
	if len(paths) < 7 {
		t.Fatalf("expected the 7 room fixtures, found %v", paths)
	}
	for _, p := range paths {
		if err := VerifyExportHash(fixture(t, p)); err != nil {
			t.Errorf("%s: %v", p, err)
		}
	}
}

func TestLearnedMapping(t *testing.T) {
	res, err := buildFixture(t, fixConfirmed, nil)
	if err != nil {
		t.Fatal(err)
	}
	sc := res.Scenario
	if res.Name != "orders-2026-10-11-9c4e0001" || sc.Name != res.Name {
		t.Errorf("name = %q / %q", res.Name, sc.Name)
	}
	if res.Hard || !strings.Contains(sc.Description, "orders stale") || !strings.Contains(sc.Description, "409") {
		t.Errorf("description = %q (hard %v)", sc.Description, res.Hard)
	}
	if sc.Pipeline.ID != "orders" || sc.Pipeline.TopicPrefix != "pg" || !reflect.DeepEqual(sc.Pipeline.Connectors, []string{"pg-orders-connector"}) ||
		!reflect.DeepEqual(sc.Pipeline.ConsumerGroups, []string{"order-sync"}) || sc.Pipeline.Owners != nil {
		t.Errorf("pipeline = %+v", sc.Pipeline)
	}
	if sc.Alert.Title != "orders stale" || !strings.HasPrefix(sc.Alert.PipelineMap, "pipeline orders\n") {
		t.Errorf("alert = %+v", sc.Alert)
	}
	if !reflect.DeepEqual(sc.Phases, []string{"broken", "recovered"}) || len(sc.Transitions) != 1 ||
		sc.Transitions[0].On.Action != "connect_restart_task" || sc.Transitions[0].On.OK == nil || !*sc.Transitions[0].On.OK || sc.Transitions[0].To != "recovered" {
		t.Errorf("phases %v transitions %+v", sc.Phases, sc.Transitions)
	}

	// Recordings: one per distinct (tool, input, phase); the repeated call keeps
	// its sequence; the dropped keys are gone; an error result is an error.
	type rec struct {
		tool, phase string
		input       map[string]any
		seq, hasErr int
	}
	var got []rec
	for _, r := range sc.Recordings {
		x := rec{tool: r.Tool, phase: r.Phase, input: r.Input, seq: len(r.Sequence)}
		if r.Error != "" {
			x.hasErr = 1
		}
		got = append(got, x)
	}
	want := []rec{
		{tool: "pipeline_map", input: map[string]any{"pipeline_id": "orders"}},
		{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector"}, seq: 2},
		{tool: "pg_recent_ddl", input: map[string]any{"table": "public.orders"}}, // since is dropped
		{tool: "connect_status", input: map[string]any{"connector": "pg-orders-source"}, hasErr: 1},
		{tool: "kafka_consumer_lag", input: map[string]any{"group": "order-sync"}},
		{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector"}, phase: "recovered"},
		{tool: "warehouse_freshness", input: map[string]any{"table": "analytics.orders", "ts_column": "updated_at"}, phase: "recovered"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("recordings\n got %+v\nwant %+v", got, want)
	}
	for _, r := range sc.Recordings {
		if r.Error != "" && strings.HasPrefix(r.Error, "error: ") {
			t.Errorf("error keeps the agent loop's prefix: %q", r.Error)
		}
		if r.Tool == "similar_incidents" {
			t.Error("similar_incidents must never be recorded")
		}
	}

	// Inputs in log order; the executed step carries its action and params.
	var kinds []string
	for _, in := range sc.Inputs {
		kinds = append(kinds, in.Kind)
	}
	if !reflect.DeepEqual(kinds, []string{"steer", "comment", "action_result"}) {
		t.Fatalf("input kinds = %v", kinds)
	}
	if in := sc.Inputs[0]; in.From != "Priya" || in.Text == "" {
		t.Errorf("steer = %+v", in)
	}
	ar := sc.Inputs[2]
	if ar.Action != "connect_restart_task" || ar.OK == nil || !*ar.OK || ar.Params["connector"] != "pg-orders-connector" {
		t.Errorf("action_result = %+v", ar)
	}

	// Expectations.
	e := sc.Expect
	if e.outcome() != OutcomeDiagnose || e.RootCauseLayer != "postgres" || !reflect.DeepEqual(e.Fix, [][]string{{"connect_restart_task"}}) {
		t.Errorf("expect = %+v", e)
	}
	terms := map[string]bool{}
	for _, g := range e.RootCause {
		if len(g) != 1 {
			t.Errorf("each group is one term: %v", g)
		}
		terms[g[0]] = true
	}
	for _, w := range []string{"pg orders connector", "409", "registry"} {
		if !terms[w] {
			t.Errorf("root_cause lacks %q: %v", w, e.RootCause)
		}
	}
	// "orders" is in the alert title: a hypothesis that only repeats the alert
	// must not match, so the exporter does not ask for it.
	for _, lazy := range []string{"orders", "stale"} {
		if terms[lazy] {
			t.Errorf("root_cause holds the title word %q", lazy)
		}
	}
	if res.Fingerprint != "orders|postgres|409,numeric,pg orders connector,registry,rejected" ||
		!strings.Contains(string(res.YAML), "# fingerprint: "+res.Fingerprint+"\n") {
		t.Errorf("fingerprint = %q", res.Fingerprint)
	}
}

func TestLearnedHardCase(t *testing.T) {
	res, err := buildFixture(t, fixHard, nil)
	if err != nil {
		t.Fatal(err)
	}
	sc := res.Scenario
	if !res.Hard || !strings.HasPrefix(sc.Description, "hard: true;") {
		t.Errorf("description = %q", sc.Description)
	}
	if sc.Expect.outcome() != OutcomeAbstain || len(sc.Expect.RootCause) != 0 || len(sc.Expect.Fix) != 0 {
		t.Errorf("expect = %+v", sc.Expect)
	}
	// The disputed story is named so a reviewer sees what the agent must not claim.
	if !strings.Contains(sc.Description, "warehouse") || len(sc.Phases) != 0 {
		t.Errorf("description %q phases %v", sc.Description, sc.Phases)
	}
}

func TestLearnedRefusals(t *testing.T) {
	for _, tc := range []struct {
		name string
		path string
		want error
	}{
		{"shadow room", fixShadow, ErrLearnedShadow},
		{"unlabelled room", fixUnlabelled, ErrLearnedUnlabelled},
		{"open room", fixOpen, ErrLearnedOpen},
		{"fix executed but not recovered", fixUnrecovered, ErrLearnedNotRecovered},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := buildFixture(t, tc.path, nil)
			if !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
		})
	}
}

func TestLearnedHashAndSchema(t *testing.T) {
	good := string(fixture(t, fixConfirmed))
	for name, mutate := range map[string]func(string) string{
		"edited event text": func(s string) string { return strings.Replace(s, "orders stale", "orders stalf", 1) },
		"edited hash":       func(s string) string { return strings.Replace(s, `"sha256":"`, `"sha256":"0`, 1) },
	} {
		bad := mutate(good)
		if bad == good {
			t.Fatalf("%s: mutation changed nothing", name)
		}
		if _, err := BuildLearned([]byte(bad), LearnedOptions{Owners: devOwners(t)}); !errors.Is(err, ErrExportHash) {
			t.Errorf("%s: err = %v, want ErrExportHash", name, err)
		}
	}
	for name, in := range map[string]string{
		"not json":     "nope",
		"no integrity": `{"schema":"piperoom.room-export/v1"}`,
		"empty":        "",
	} {
		if _, err := BuildLearned([]byte(in), LearnedOptions{Owners: devOwners(t)}); !errors.Is(err, ErrExportMalformed) {
			t.Errorf("%s: err = %v, want ErrExportMalformed", name, err)
		}
	}
}

func TestLearnedUnknownPipeline(t *testing.T) {
	o, err := owners.Load("scenarios/heldout/fixtures/owners.yaml")
	if err != nil {
		t.Skipf("no alternative owners file: %v", err)
	}
	if _, ok := o.Pipeline("orders"); ok {
		t.Skip("alternative owners file knows orders")
	}
	if _, err := BuildLearned(fixture(t, fixConfirmed), LearnedOptions{Owners: o}); !errors.Is(err, ErrLearnedUnknownPipeline) {
		t.Fatalf("err = %v", err)
	}
}

// ---- guardrail 2: redaction ----

func TestLearnedPlantedSecretNeverReachesTheFile(t *testing.T) {
	raw := string(fixture(t, fixSecret))
	// The fixture really holds it in the three places a room can: a tool
	// summary, a steer and a comment.
	if n := strings.Count(raw, plantedSecret); n < 3 {
		t.Fatalf("the secret fixture holds the secret %d times, want at least 3", n)
	}
	res, err := buildFixture(t, fixSecret, nil)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(res.YAML), plantedSecret) {
		t.Fatalf("the secret reached the file:\n%s", res.YAML)
	}
	if !strings.Contains(string(res.YAML), "check whether the schema is compatible") {
		t.Error("the steer's own words must survive the redaction")
	}
}

// writtenStrings are the string scalars of a YAML file.
func writtenStrings(t *testing.T, b []byte) []string {
	t.Helper()
	var doc yaml.Node
	if err := yaml.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	var out []string
	var walk func(*yaml.Node)
	walk = func(n *yaml.Node) {
		if n.Kind == yaml.ScalarNode && n.Tag == "!!str" {
			out = append(out, n.Value)
		}
		for _, c := range n.Content {
			walk(c)
		}
	}
	walk(&doc)
	return out
}

func TestLearnedSecondRedactionPassIsANoOp(t *testing.T) {
	for _, p := range []string{fixConfirmed, fixSecret, fixHard} {
		res, err := buildFixture(t, p, nil)
		if err != nil {
			t.Fatalf("%s: %v", p, err)
		}
		strs := writtenStrings(t, res.YAML)
		if len(strs) < 20 {
			t.Fatalf("%s: only %d strings checked", p, len(strs))
		}
		for _, s := range strs {
			if got, _ := redact.Text(s); got != s {
				t.Errorf("%s: a second pass changes %q to %q", p, s, got)
			}
		}
	}
}

// nonIdempotent masks by appending a dot: a second pass changes what the first
// wrote, which the exporter must refuse instead of writing.
type nonIdempotent struct{ *redact.Redactor }

func (nonIdempotent) Text(s string) (string, redact.Stats) { return s + ".", redact.Stats{} }

func TestLearnedRefusesWhenASecondPassChangesTheFile(t *testing.T) {
	_, err := buildFixture(t, fixConfirmed, func(o *LearnedOptions) {
		o.Redactor = nonIdempotent{redact.New(redact.DefaultOptions())}
	})
	if !errors.Is(err, ErrLearnedRedaction) {
		t.Fatalf("err = %v, want ErrLearnedRedaction", err)
	}
}

// ---- guardrail 5: dedup and cap ----

func TestLearnedPendingLandsInPendingAndIsNotReplayed(t *testing.T) {
	dir := t.TempDir() // stands for the private learned directory
	res, path, err := ExportLearned(fixture(t, fixConfirmed), LearnedOptions{Owners: devOwners(t), Dir: dir, Validate: true})
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(dir, "pending", res.Name+".yaml"); path != want {
		t.Fatalf("path = %s, want %s", path, want)
	}
	// What was written is what -check parses.
	if _, err := LoadScenario(path); err != nil {
		t.Fatal(err)
	}
	if got, err := LoadLearned(dir); err != nil || len(got) != 0 {
		t.Errorf("LoadLearned = %d scenarios, %v: a pending file must not be replayed", len(got), err)
	}
	pend, err := LoadPending(dir)
	if err != nil || len(pend) != 1 || pend[0].Name != res.Name {
		t.Fatalf("LoadPending = %v, %v", pend, err)
	}
	// The person's review: moving the file makes it part of the replayed set.
	if err := os.Rename(path, filepath.Join(dir, res.Name+".yaml")); err != nil {
		t.Fatal(err)
	}
	if got, err := LoadLearned(dir); err != nil || len(got) != 1 {
		t.Errorf("after the move LoadLearned = %d, %v", len(got), err)
	}
}

func TestLearnedDedup(t *testing.T) {
	dir := t.TempDir()
	o := LearnedOptions{Owners: devOwners(t), Dir: dir}
	_, path, err := ExportLearned(fixture(t, fixConfirmed), o)
	if err != nil {
		t.Fatal(err)
	}
	// The same incident again (a different room with the same pipeline, layer
	// and cause terms: here the same export under another name is enough).
	if _, _, err := ExportLearned(fixture(t, fixConfirmed), o); !errors.Is(err, ErrLearnedDuplicate) {
		t.Fatalf("second export: err = %v, want ErrLearnedDuplicate", err)
	}
	// A person moved the file into learned/: still a duplicate.
	moved := filepath.Join(dir, filepath.Base(path))
	if err := os.Rename(path, moved); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ExportLearned(fixture(t, fixConfirmed), o); !errors.Is(err, ErrLearnedDuplicate) {
		t.Fatalf("after the move: err = %v, want ErrLearnedDuplicate", err)
	}
	// A file without the header (hand written) is compared by what it expects.
	b, _ := os.ReadFile(moved)
	var kept []string
	for _, l := range strings.Split(string(b), "\n") {
		if !strings.HasPrefix(l, "# fingerprint:") {
			kept = append(kept, l)
		}
	}
	if err := os.WriteFile(moved, []byte(strings.Join(kept, "\n")), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ExportLearned(fixture(t, fixConfirmed), o); !errors.Is(err, ErrLearnedDuplicate) {
		t.Fatalf("headerless file: err = %v, want ErrLearnedDuplicate", err)
	}
	// -force writes it anyway.
	o.Force = true
	if _, _, err := ExportLearned(fixture(t, fixConfirmed), o); err != nil {
		t.Fatalf("force: %v", err)
	}
	// Without -force an output file is never overwritten.
	o.Force = false
	if _, _, err := ExportLearned(fixture(t, fixHard), o); err != nil {
		t.Fatalf("a different incident is not a duplicate: %v", err)
	}
}

func TestLearnedCapPerPipeline(t *testing.T) {
	dir := t.TempDir()
	pend := filepath.Join(dir, PendingDir)
	if err := os.MkdirAll(pend, 0o755); err != nil {
		t.Fatal(err)
	}
	other := "pipeline:\n  id: payments\nexpect:\n  root_cause_layer: kafka\n  root_cause: [[x]]\n"
	if err := os.WriteFile(filepath.Join(pend, "other.yaml"), []byte(other), 0o644); err != nil {
		t.Fatal(err)
	}
	o := LearnedOptions{Owners: devOwners(t), Dir: dir}
	// Nine for orders (half in learned/, half in pending/) plus one for another
	// pipeline: the tenth orders scenario fits, the eleventh does not.
	for i := 0; i < MaxLearnedPerPipeline-1; i++ {
		target := pend
		if i%2 == 0 {
			target = dir
		}
		body := fmt.Sprintf("# fingerprint: orders|postgres|filler%d\npipeline:\n  id: orders\nexpect:\n  root_cause_layer: postgres\n", i)
		if err := os.WriteFile(filepath.Join(target, fmt.Sprintf("orders-%d.yaml", i)), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	if _, _, err := ExportLearned(fixture(t, fixConfirmed), o); err != nil {
		t.Fatalf("the tenth fits: %v", err)
	}
	if _, _, err := ExportLearned(fixture(t, fixHard), o); !errors.Is(err, ErrLearnedCap) {
		t.Fatalf("the eleventh: err = %v, want ErrLearnedCap", err)
	}
	// -force lifts the duplicate rule, never the cap.
	o.Force = true
	if _, _, err := ExportLearned(fixture(t, fixHard), o); !errors.Is(err, ErrLearnedCap) {
		t.Fatalf("force must not lift the cap: err = %v", err)
	}
}

func TestLearnedCorruptNeighbourIsAnError(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "broken.yaml"), []byte("pipeline: [unclosed"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := BuildLearned(fixture(t, fixConfirmed), LearnedOptions{Owners: devOwners(t), Dir: dir}); err == nil || !strings.Contains(err.Error(), "broken.yaml") {
		t.Fatalf("err = %v: an unreadable file must not silently lift the cap", err)
	}
}

// ---- units ----

func TestRootCauseTerms(t *testing.T) {
	title := set(tokenize("orders stale"))
	for _, tc := range []struct {
		name, cause string
		skip        map[string]bool
		want        []string
	}{
		{"identifier, number, words", "pg-orders-connector task 0 failed: registry returned 409 on numeric amount", nil,
			[]string{"pg orders connector", "409", "registry", "returned", "numeric"}},
		{"title words are skipped and take no slot", "orders stale because the replication slot lagged behind", title,
			[]string{"replication", "lagged", "behind"}},
		{"one-digit numbers are noise", "task 0 restarted on node 3", nil, []string{"restarted"}},
		{"duplicates once", "registry registry registry", nil, []string{"registry"}},
		{"nothing specific", "it broke", nil, nil},
		{"table identifier", "public.orders lost its publication", nil, []string{"public orders", "publication"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := rootCauseTerms(tc.cause, tc.skip)
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("rootCauseTerms(%q) = %v, want %v", tc.cause, got, tc.want)
			}
			// Every term the exporter writes is one validation accepts.
			if errs := checkGroups("t", [][]string{got}, nil); len(got) > 0 && len(errs) > 0 {
				t.Errorf("checkGroups: %v", errs)
			}
		})
	}
}

// resign edits an export and re-computes its integrity hash the documented
// way, so a test can change what a person typed without breaking the seal.
func resign(t *testing.T, raw []byte, old, new string) []byte {
	t.Helper()
	s := string(raw)
	if !strings.Contains(s, old) {
		t.Fatalf("export does not hold %q", old)
	}
	s = strings.Replace(s, old, new, 1)
	var members map[string]json.RawMessage
	if err := json.Unmarshal([]byte(s), &members); err != nil {
		t.Fatal(err)
	}
	var integ struct {
		SHA256 string `json:"sha256"`
	}
	if err := json.Unmarshal(members["integrity"], &integ); err != nil {
		t.Fatal(err)
	}
	delete(members, "integrity")
	rest, _ := json.Marshal(members)
	canon, err := jcs.Transform(rest)
	if err != nil {
		t.Fatal(err)
	}
	sum := sha256.Sum256(canon)
	return []byte(strings.Replace(s, integ.SHA256, hex.EncodeToString(sum[:]), -1))
}

func TestLearnedNoTermsBeyondTheTitle(t *testing.T) {
	raw := fixture(t, fixConfirmed)
	cause := "Schema Registry rejected the new Avro schema (409) after ALTER TABLE orders ALTER COLUMN amount TYPE numeric; pg-orders-connector task 0 failed"
	// A cause that only repeats the title cannot be judged: refuse, and say why.
	_, err := BuildLearned(resign(t, raw, cause, "orders stale"), LearnedOptions{Owners: devOwners(t)})
	if !errors.Is(err, ErrLearnedNoTerms) {
		t.Fatalf("err = %v, want ErrLearnedNoTerms", err)
	}
	// The re-signed document is otherwise good: the same edit with a real cause builds.
	if _, err := BuildLearned(resign(t, raw, cause, "the replication slot was dropped during failover"), LearnedOptions{Owners: devOwners(t), Validate: true}); err != nil {
		t.Fatal(err)
	}
}

func TestLearnedDroppedInputKeysTable(t *testing.T) {
	want := []string{"at", "from", "limit", "max_results", "max_scan", "older_than_minutes", "query", "since", "timestamp", "to", "until", "window"}
	var got []string
	for k := range droppedInputKeys {
		got = append(got, k)
	}
	sortStrings(got)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("droppedInputKeys = %v, want %v: the list lives in one table", got, want)
	}
}

func sortStrings(s []string) {
	for i := range s {
		for j := i + 1; j < len(s); j++ {
			if s[j] < s[i] {
				s[i], s[j] = s[j], s[i]
			}
		}
	}
}

func TestKeptData(t *testing.T) {
	rd := redact.New(redact.DefaultOptions())
	big := `{"x":"` + strings.Repeat("a", maxLearnedData+10) + `"}`
	for name, tc := range map[string]struct {
		raw  string
		keep bool
	}{
		"small":            {`{"state":"RUNNING"}`, true},
		"over 4 KB":        {big, false},
		"cut data":         {`{"truncated":true,"original_bytes":9000,"preview":"{\"a\""}`, false},
		"empty":            {``, false},
		"not json":         {`{`, false},
		"password by name": {`{"password":"p4ss"}`, true},
	} {
		got := keptData([]byte(tc.raw), rd)
		if (got != nil) != tc.keep {
			t.Errorf("%s: kept = %v, want %v", name, got != nil, tc.keep)
		}
		if name == "password by name" && fmt.Sprint(got) == `map[password:p4ss]` {
			t.Errorf("%s: a value under a secret key survived: %v", name, got)
		}
	}
}

func TestLearnedName(t *testing.T) {
	for _, tc := range []struct{ pipe, id, want string }{
		{"orders", "room_9c4e0001", "orders-2026-10-11-9c4e0001"},
		{"my pipe!", "ab", "my-pipe--2026-10-11-ab"},
		{"orders", "", "orders-2026-10-11-room"},
	} {
		got := learnedName(tc.pipe, mustTime(t, "2026-10-11T09:00:00Z"), tc.id)
		if got != tc.want || !shadowNameRE.MatchString(got) {
			t.Errorf("learnedName(%q,%q) = %q, want %q", tc.pipe, tc.id, got, tc.want)
		}
	}
}

func mustTime(t *testing.T, s string) time.Time {
	t.Helper()
	v, err := time.Parse(time.RFC3339, s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

// TestLearnedScenarioReplaysThroughTheRoomPath: the exported file is a working
// scenario, not just a well-formed one. A scripted agent that investigates and
// states the confirmed cause passes it through the room path; one that only
// repeats the alert does not.
func TestLearnedScenarioReplaysThroughTheRoomPath(t *testing.T) {
	res, err := buildFixture(t, fixConfirmed, nil)
	if err != nil {
		t.Fatal(err)
	}
	good := []step{
		{tool: "pipeline_map", input: map[string]any{"pipeline_id": "orders"}},
		{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector"}},
		{tool: "pg_recent_ddl", input: map[string]any{"table": "public.orders", "since": "2026-10-10T09:40:00Z"}},
		{hypothesis: "pg-orders-connector task 0 failed because Schema Registry rejected the new orders schema (409): " +
			"ALTER TABLE changed amount to numeric, which is not backward compatible"},
		{proposal: &events.ProposalCreatedPayload{
			Title: "Restart task 0",
			Steps: []events.ProposalStep{{Action: "connect_restart_task", Params: json.RawMessage(`{"connector":"pg-orders-connector","task":0}`), Description: "restart"}},
		}},
		{idle: true}, {idle: true}, {idle: true},
		// The re-check after the fix the people executed.
		{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector"}},
		{tool: "warehouse_freshness", input: map[string]any{"table": "analytics.orders", "ts_column": "updated_at"}},
		{idle: true},
	}
	lazy := []step{
		{tool: "pipeline_map", input: map[string]any{"pipeline_id": "orders"}},
		{hypothesis: "orders is stale"},
		{idle: true}, {idle: true}, {idle: true},
	}
	run := func(script []step) RunResult {
		rep, err := Evaluate(context.Background(), []Scenario{res.Scenario}, Config{
			Label: "learned", Runs: 1, Runner: scriptedRunner(script), Logger: quietLogger(),
		})
		if err != nil || len(rep.Results) != 1 {
			t.Fatalf("Evaluate: %v (%d results)", err, len(rep.Results))
		}
		return rep.Results[0]
	}
	if r := run(good); !r.Pass {
		t.Errorf("the recorded cause was not found: %v", r.Failures)
	}
	if r := run(lazy); r.Pass {
		t.Error("a hypothesis that repeats the alert must not pass a learned scenario")
	}
}
