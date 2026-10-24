package room

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"piperoom/internal/config"
	"piperoom/internal/discover"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/setup/plan"
)

// The pipeline_register internals (U1-14): the gate, the fold fields the gate
// and the approval guard read, the owners entry rendering (byte-equal to
// discover -write-owners for the same pipeline), the connections: file
// references, watched_by, and the write's idempotency.

func greenRungs() []events.SetupRung {
	names := []string{plan.RungConnector, plan.RungTasks, plan.RungCanaryTopic, plan.RungSnapshot,
		plan.RungCanaryEvent, plan.RungCanarySubject, plan.RungSinkRow}
	rungs := make([]events.SetupRung, len(names))
	for i, n := range names {
		rungs[i] = events.SetupRung{Name: n, OK: true}
	}
	return rungs
}

func mustJSON(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestSetupRegisterGate(t *testing.T) {
	green := greenRungs()
	withoutRung := func(name string) []events.SetupRung {
		var out []events.SetupRung
		for _, r := range green {
			if r.Name != name {
				out = append(out, r)
			}
		}
		return out
	}
	cases := []struct {
		name     string
		snap     func() *RoomSnapshot
		wantErr  bool
		contains string
	}{
		{"no snapshot", func() *RoomSnapshot { return nil }, true, ""},
		{"not a setup room", func() *RoomSnapshot { return &RoomSnapshot{} }, true, ""},
		{"ladder never ran", func() *RoomSnapshot {
			return &RoomSnapshot{Setup: &SetupSnapshot{Mode: true, Verified: green}}
		}, true, "ladder has not run"},
		{"executed step after the verification", func() *RoomSnapshot {
			return &RoomSnapshot{Setup: &SetupSnapshot{Mode: true, Verified: green, verifiedSeq: 5, execSeq: 6}}
		}, true, "executed after"},
		{"a rung missing", func() *RoomSnapshot {
			return &RoomSnapshot{Setup: &SetupSnapshot{Mode: true, Verified: withoutRung(plan.RungCanaryTopic), verifiedSeq: 5}}
		}, true, `rung "canary_topic"`},
		{"a rung red", func() *RoomSnapshot {
			rungs := append([]events.SetupRung{}, green...)
			rungs[2].OK = false
			return &RoomSnapshot{Setup: &SetupSnapshot{Mode: true, Verified: rungs, verifiedSeq: 5}}
		}, true, `rung "canary_topic"`},
		{"green ladder, nothing executed", func() *RoomSnapshot {
			return &RoomSnapshot{Setup: &SetupSnapshot{Mode: true, Verified: green, verifiedSeq: 5}}
		}, false, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := setupRegisterGate(c.snap())
			if !c.wantErr {
				if err != nil {
					t.Fatalf("gate refused a green setup: %v", err)
				}
				return
			}
			var re *Error
			if !errors.As(err, &re) || re.Status != 409 || re.Code != "not_verified" {
				t.Fatalf("gate = %v, want 409 not_verified", err)
			}
			if c.contains != "" && !strings.Contains(re.Msg, c.contains) {
				t.Fatalf("gate message %q misses %q", re.Msg, c.contains)
			}
		})
	}
}

// TestRegisterSnapshotFoldFields pins the fold state the registration reads:
// the creator, the verification and execution seqs (reset by a new plan), the
// registration PR url, and the approvers' actors (copy-on-write).
func TestRegisterSnapshotFoldFields(t *testing.T) {
	req := events.SetupRequest{
		PipelineID: "orders-x", Tenant: "a", Title: "Orders", SourceKind: "postgres", Target: "compose",
		TopicPrefix: "orders", Tables: []string{"public.orders"}, Connectors: []string{"orders-src"},
		Targets: []events.SetupTarget{{Kind: "postgres", Address: "db.acme:5432"}},
	}
	ev := func(seq int64, typ events.Type, actor events.Actor, payload any) events.Event {
		return events.Event{Seq: seq, Type: typ, Actor: actor, Payload: mustJSON(t, payload)}
	}
	alice := events.Actor{Kind: events.ActorHuman, ID: "alice", Name: "Alice"}
	ann := events.Actor{Kind: events.ActorHuman, ID: "ann", Name: "Ann"}

	snap := Apply(RoomSnapshot{}, ev(1, events.RoomCreated, alice, events.RoomCreatedPayload{
		Mode: events.RoomModeSetup, Alert: mustJSON(t, req),
	}))
	if snap.Setup == nil || snap.Setup.creator.ID != "alice" {
		t.Fatalf("creator = %+v", snap.Setup)
	}

	snap = Apply(snap, ev(2, events.SetupVerified, systemActor, events.SetupVerifiedPayload{Rungs: greenRungs()}))
	if snap.Setup.verifiedSeq != 2 || len(snap.Setup.Verified) != 7 {
		t.Fatalf("verified seq/rungs = %d/%d", snap.Setup.verifiedSeq, len(snap.Setup.Verified))
	}

	snap = Apply(snap, ev(3, events.ActionExecuted, systemActor, events.ActionExecutedPayload{
		ProposalID: "p1", StepIndex: 0, OK: true,
	}))
	if snap.Setup.execSeq != 3 {
		t.Fatalf("exec seq = %d, want 3", snap.Setup.execSeq)
	}

	// A new plan supersedes the verification: the stale rungs and seq go.
	snap = Apply(snap, ev(4, events.SetupPlan, systemActor, events.SetupPlanPayload{
		PlanID: "pl2", Phases: []events.SetupPlanPhase{{Name: "stack", Steps: []events.SetupPlanStep{{Index: 0, Op: "compose_up"}}}},
	}))
	if snap.Setup.verifiedSeq != 0 || len(snap.Setup.Verified) != 0 {
		t.Fatalf("a new plan kept the old verification: seq %d rungs %v", snap.Setup.verifiedSeq, snap.Setup.Verified)
	}

	snap = Apply(snap, ev(5, events.PipelineRegistered, systemActor, events.PipelineRegisteredPayload{
		PipelineID: "orders-x", WatchedBy: events.WatchedByLocal, PullRequestURL: "https://forge.acme/pr/7",
	}))
	if snap.Setup.WatchedBy != events.WatchedByLocal || snap.Setup.regPullRequestURL != "https://forge.acme/pr/7" {
		t.Fatalf("registered fold = %+v", snap.Setup)
	}

	// The approvers' actors: what ApprovedBy and the overlap re-check read.
	snap = Apply(snap, ev(6, events.ProposalCreated, systemActor, events.ProposalCreatedPayload{
		ProposalID: "p9", RequiredLayers: []string{"environment"},
		Steps: []events.ProposalStep{{Action: registerActionName, Layer: events.LayerEnvironment}},
	}))
	snap = Apply(snap, ev(7, events.ApprovalGiven, ann, events.ApprovalGivenPayload{
		ProposalID: "p9", Layer: events.LayerEnvironment,
	}))
	prop, ok := openProposal(snap, "p9")
	if !ok {
		t.Fatal("the proposal is not open")
	}
	actor, ok := prop.approvalActors[events.LayerEnvironment]
	if !ok || actor.ID != "ann" {
		t.Fatalf("approval actors = %+v", prop.approvalActors)
	}
}

// entrySnapshot builds the snapshot registerEntry reads: the request, the
// room id and the fold-only fields.
func entrySnapshot(id, tenant, title string, targets []SetupTargetState, secretNames, consumerGroups, sinkTables []string) RoomSnapshot {
	req := events.SetupRequest{
		PipelineID: id, Tenant: tenant, Title: title, SourceKind: "postgres", Target: "compose",
		TopicPrefix: "orders", Tables: []string{"public.orders"}, Connectors: []string{"orders-src"},
		Targets: []events.SetupTarget{{Kind: "postgres", Address: "db.acme:5432"}},
	}
	return RoomSnapshot{
		ID:         "room_reg1",
		PipelineID: id,
		Setup: &SetupSnapshot{
			Mode:    true,
			tenant:  tenant,
			creator: events.Actor{Kind: events.ActorHuman, ID: "alice", Name: "Alice"},
			req:     req,
			Targets: targets,
			Provisional: SetupProvisionalState{
				TopicPrefix: "orders", Tables: []string{"public.orders"}, Connectors: []string{"orders-src"},
				ConsumerGroups: consumerGroups, SinkTables: sinkTables,
			},
			secretNames: secretNames,
		},
	}
}

func registerTestService(t *testing.T) *Service {
	t.Helper()
	svc := New(Deps{Setup: config.Setup{
		Enabled: true, SecretsDir: t.TempDir(), OwnersDir: t.TempDir(),
	}})
	t.Cleanup(svc.Close)
	return svc
}

// TestRegisterEntryByteEqualsDiscover pins the two renderings together (the
// row's done-when): for the same pipeline values, the entry's non-secret part
// — everything up to and including sink_ts_column — is byte-equal to what
// discover -write-owners renders, modulo the discover document's list
// indentation. discover's draft title is the connector name, so the title
// equals it here.
func TestRegisterEntryByteEqualsDiscover(t *testing.T) {
	svc := registerTestService(t)
	snap := entrySnapshot("orders-x", "", "orders-src", nil, nil,
		[]string{"orders-loader"}, nil)
	entry, watchedBy, err := svc.registerEntry(&snap)
	if err != nil {
		t.Fatal(err)
	}
	if watchedBy != events.WatchedByServer {
		t.Fatalf("watched_by = %q, want server", watchedBy)
	}

	var buf bytes.Buffer
	r := discover.Result{Pipelines: []discover.Pipeline{{
		ID: "orders-x", Connector: "orders-src", TopicPrefix: "orders",
		SourceTables: []string{"public.orders"}, ConsumerGroups: []string{"orders-loader"},
	}}}
	if err := discover.RenderOwned(&buf, r, "alice"); err != nil {
		t.Fatal(err)
	}
	want := discoverPipelineBlock(t, buf.String())
	mark := `sink_ts_column: ""`
	got := string(entry)
	got = got[:strings.Index(got, mark)+len(mark)]
	want = want[:strings.Index(want, mark)+len(mark)]
	if got != want {
		t.Fatalf("the entry's non-secret part is not byte-equal to discover -write-owners:\n--- entry ---\n%s\n--- discover ---\n%s", got, want)
	}
	// The entry is a complete owners document: owners.Load accepts it (a
	// pipeline without a tenant, the standalone case).
	path := filepath.Join(t.TempDir(), "orders-x.yaml")
	if err := os.WriteFile(path, entry, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := owners.Load(path); err != nil {
		t.Fatalf("the written entry does not load with owners.Load: %v", err)
	}
}

// discoverPipelineBlock extracts discover -write-owners' first pipeline
// mapping: the lines from "pipelines:" to the top-level "layers:" key. The
// per-pipeline entry is itself a complete owners document with exactly that
// pipelines: block, so the bytes compare directly.
func discoverPipelineBlock(t *testing.T, doc string) string {
	t.Helper()
	lines := strings.Split(doc, "\n")
	start := -1
	for i, l := range lines {
		if l == "pipelines:" {
			start = i
			break
		}
	}
	if start < 0 {
		t.Fatalf("no pipelines: block in discover output:\n%s", doc)
	}
	var out []string
	for _, l := range lines[start:] {
		if l == "layers:" {
			break
		}
		out = append(out, l)
	}
	return strings.Join(out, "\n")
}

// entryPipeline parses the entry and returns its one pipeline mapping.
func entryPipeline(t *testing.T, entry []byte) map[string]any {
	t.Helper()
	var doc struct {
		Pipelines []map[string]any `yaml:"pipelines"`
	}
	if err := yaml.Unmarshal(entry, &doc); err != nil {
		t.Fatalf("the entry does not parse: %v\n%s", err, entry)
	}
	if len(doc.Pipelines) != 1 {
		t.Fatalf("pipelines = %d entries, want 1:\n%s", len(doc.Pipelines), entry)
	}
	return doc.Pipelines[0]
}

// TestRegisterEntryShape pins what the entry carries for a tenant pipeline:
// tenant:, the creator owning every layer of the pipeline, auto_approve: [],
// and the setup: block with target, watched_by and the room id (design
// §3.9). The file loads through the directory loader, the team server's
// path: a tenant needs the main file's tenants: block, which a per-pipeline
// file must not carry itself.
func TestRegisterEntryShapeAndLoad(t *testing.T) {
	svc := registerTestService(t)
	snap := entrySnapshot("orders-x", "a", "Orders setup", nil, nil, nil, nil)
	entry, watchedBy, err := svc.registerEntry(&snap)
	if err != nil {
		t.Fatal(err)
	}
	if watchedBy != events.WatchedByServer {
		t.Fatalf("watched_by = %q", watchedBy)
	}
	doc := entryPipeline(t, entry)
	if doc["tenant"] != "a" {
		t.Fatalf("tenant = %v", doc["tenant"])
	}
	own, ok := doc["owners"].(map[string]any)
	if !ok {
		t.Fatalf("owners = %v", doc["owners"])
	}
	for _, layer := range events.LayersOf(events.PipelineKindCDC) {
		rights, ok := own[layer].(map[string]any)
		if !ok || rights["approvers"] != "user:alice" {
			t.Fatalf("owners.%s = %v, want the creator as approver", layer, own[layer])
		}
	}
	if aa, ok := doc["auto_approve"]; !ok || len(aa.([]any)) != 0 {
		t.Fatalf("auto_approve = %v, want []", doc["auto_approve"])
	}
	setup, ok := doc["setup"].(map[string]any)
	if !ok {
		t.Fatalf("setup = %v", doc["setup"])
	}
	if setup["target"] != "compose" || setup["watched_by"] != "server" || setup["room_id"] != "room_reg1" {
		t.Fatalf("setup = %v", setup)
	}
	main := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(main, []byte("tenants:\n  a: {members: [user:alice]}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(t.TempDir(), "owners.d")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "orders-x.yaml"), entry, 0o600); err != nil {
		t.Fatal(err)
	}
	o, err := owners.LoadDir(main, dir)
	if err != nil {
		t.Fatalf("the written entry does not load with the directory loader: %v", err)
	}
	if p, ok := o.Pipeline("orders-x"); !ok || p.TenantName() != "a" {
		t.Fatalf("the loaded pipeline = %+v, ok %v", p, ok)
	}
}

// TestRegisterEntryLocalTargetWatchedLocally pins §3.7's table: a local
// target registers watched_by: local, so the team server skips it.
func TestRegisterEntryLocalTargetWatchedLocally(t *testing.T) {
	svc := registerTestService(t)
	snap := entrySnapshot("orders-x", "a", "Orders", []SetupTargetState{{
		Kind: "postgres", Address: "127.0.0.1:5432", Class: "local",
	}}, nil, nil, nil)
	entry, watchedBy, err := svc.registerEntry(&snap)
	if err != nil {
		t.Fatal(err)
	}
	if watchedBy != events.WatchedByLocal {
		t.Fatalf("watched_by = %q, want local", watchedBy)
	}
	doc := entryPipeline(t, entry)
	setup := doc["setup"].(map[string]any)
	if setup["watched_by"] != "local" {
		t.Fatalf("setup = %v", setup)
	}
}

// TestRegisterConnectionsFileRefs: only the connection keys whose read
// credential the room recorded become file: references; every other key is
// left out, so the pipeline fails closed rather than read a process-wide
// setting of the server.
func TestRegisterConnectionsFileRefs(t *testing.T) {
	svc := registerTestService(t)
	setup := &SetupSnapshot{secretNames: []string{"source_db_url"}}
	prov, err := owners.Provisional(events.SetupRequest{
		PipelineID: "orders-x", TopicPrefix: "orders", Tables: []string{"public.orders"},
		Connectors: []string{"orders-src"},
		Targets: []events.SetupTarget{
			{Kind: "postgres", Address: "db.acme:5432"},
			{Kind: "kafka", Address: "kafka.acme:9092"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	conns, err := svc.registerConnections(setup, prov)
	if err != nil {
		t.Fatal(err)
	}
	want := owners.FileRef(filepath.Join(svc.setupCfg.SecretsDir, "orders-x", "source_db_url"))
	if len(conns) != 1 || conns["source_db_url"] != want {
		t.Fatalf("connections = %v, want {source_db_url: %s}", conns, want)
	}
}

func TestRegisterWriteFileIdempotent(t *testing.T) {
	svc := registerTestService(t)
	ctx := context.Background()
	snap := entrySnapshot("orders-x", "a", "Orders", nil, nil, nil, nil)
	entry, _, err := svc.registerEntry(&snap)
	if err != nil {
		t.Fatal(err)
	}
	path, err := svc.writeRegisterFile(ctx, &snap, entry)
	if err != nil {
		t.Fatal(err)
	}
	if path != filepath.Join(svc.setupCfg.OwnersDir, "orders-x.yaml") {
		t.Fatalf("path = %q", path)
	}
	// The second write is refused: a registration never overwrites a file.
	if _, err := svc.writeRegisterFile(ctx, &snap, entry); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("second write = %v, want already exists", err)
	}

	// A symlink in the way is refused, never followed.
	other := entrySnapshot("orders-y", "a", "Orders", nil, nil, nil, nil)
	outside := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(outside, entry, 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(svc.setupCfg.OwnersDir, "orders-y.yaml")
	if err := os.Symlink(outside, link); err != nil {
		t.Skipf("the OS refused to create a symlink: %v", err)
	}
	if _, err := svc.writeRegisterFile(ctx, &other, entry); err == nil || !strings.Contains(err.Error(), "symbolic link") {
		t.Fatalf("symlink write = %v, want symbolic link refusal", err)
	}
	if b, err := os.ReadFile(outside); err != nil || !bytes.Equal(b, entry) {
		t.Fatalf("the symlink target changed: %v", err)
	}
}

// loadOwnersString loads an owners YAML string from a temp file, for tests
// that build an owners set in code.
func loadOwnersString(t *testing.T, y string) *owners.Owners {
	t.Helper()
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(y), 0o600); err != nil {
		t.Fatal(err)
	}
	o, err := owners.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	return o
}

// TestRegisterApprovalsHold pins the overlap re-check (design §3.6): the
// recorded approvers must include, per overlapping pipeline, one of its own
// owners. A tenant admin or a system approval does not count.
func TestRegisterApprovalsHold(t *testing.T) {
	own := loadOwnersString(t, registerOwnersOnlyYAML())
	users, err := ParseUsers([]byte(setupUsersForRegister()))
	if err != nil {
		t.Fatal(err)
	}
	svc := New(Deps{Owners: own, Users: users, Setup: config.Setup{Enabled: true}})
	t.Cleanup(svc.Close)
	existing, ok := own.Pipeline("existing")
	if !ok {
		t.Fatal("no existing pipeline")
	}
	pipes := []owners.Pipeline{existing}

	snapFor := func(actors map[string]events.Actor) *RoomSnapshot {
		return &RoomSnapshot{
			Setup: &SetupSnapshot{Mode: true, req: events.SetupRequest{PipelineID: "orders-x", Tenant: "a"}},
			OpenProposals: []Proposal{{
				ProposalID:     "p1",
				approvalActors: actors,
			}},
		}
	}
	cases := []struct {
		name   string
		actors map[string]events.Actor
		want   bool
	}{
		{"no approvals", nil, false},
		{"a tenant admin who is not an owner", map[string]events.Actor{
			"environment": {Kind: events.ActorHuman, ID: "ann", Name: "Ann"},
		}, false},
		{"the system actor", map[string]events.Actor{
			"environment": systemActor,
		}, false},
		{"the overlapping pipeline's owner", map[string]events.Actor{
			"environment": {Kind: events.ActorHuman, ID: "alice", Name: "Alice"},
		}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := svc.registerApprovalsHold(snapFor(c.actors), "p1", pipes); got != c.want {
				t.Fatalf("approvals hold = %v, want %v", got, c.want)
			}
		})
	}
}

func registerOwnersOnlyYAML() string {
	var b strings.Builder
	b.WriteString("pipelines:\n  - id: existing\n    topic_prefix: ex\n    source_tables: [public.e]\n    connectors: [ex-c]\n    connections: {source_db_url: {env: EX_SRC}, warehouse_db_url: {env: EX_WH}, kafka_brokers: {env: EX_K}, connect_url: {env: EX_C}, schema_registry_url: {env: EX_SR}}\n    owners:")
	for _, l := range events.LayersOf(events.PipelineKindCDC) {
		b.WriteString("\n      " + l + ": {approvers: [user:alice]}")
	}
	return b.String()
}

func setupUsersForRegister() string {
	return "users:\n  - {id: alice, name: Alice}\n  - {id: ann, name: Ann}\n"
}

// TestRegisterCreateRefusal pins the Create gate: an overlap whose owners can
// never approve refuses; an owned overlap is allowed.
func TestRegisterCreateRefusal(t *testing.T) {
	const conns = "\n    connections: {source_db_url: {env: EX_SRC}, warehouse_db_url: {env: EX_WH}, kafka_brokers: {env: EX_K}, connect_url: {env: EX_C}, schema_registry_url: {env: EX_SR}}"
	own := loadOwnersString(t, "pipelines:\n"+
		"  - id: owned\n    topic_prefix: ow\n    source_tables: [public.a]\n    connectors: [ow-c]"+conns+"\n    owners:\n      postgres: {approvers: [user:alice]}\n"+
		"  - id: unowned\n    topic_prefix: un\n    source_tables: [public.b]\n    connectors: [un-c]"+conns+"\n")
	svc := New(Deps{Owners: own})
	t.Cleanup(svc.Close)
	snapFor := func(prefix string) *RoomSnapshot {
		return &RoomSnapshot{
			PipelineID: "orders-x",
			Setup: &SetupSnapshot{
				Mode: true,
				req: events.SetupRequest{
					PipelineID: "orders-x", TopicPrefix: prefix, Tables: []string{"public.orders"},
					Connectors: []string{"orders-src"},
				},
			},
		}
	}
	if err := svc.registerCreateRefusal(snapFor("none")); err != nil {
		t.Fatalf("no overlap refused: %v", err)
	}
	if err := svc.registerCreateRefusal(snapFor("ow")); err != nil {
		t.Fatalf("an owned overlap is approvable: %v", err)
	}
	err := svc.registerCreateRefusal(snapFor("un"))
	var re *Error
	if !errors.As(err, &re) || re.Status != 409 || re.Code != "overlap" {
		t.Fatalf("an unowned overlap = %v, want 409 overlap", err)
	}
}
