package room

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"piperoom/internal/config"
	"piperoom/internal/discover"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/setup/plan"
	"piperoom/internal/store/memstore"
)

// The pipeline_register internals (U1-14): the gate, the fold fields the gate
// and the approval guard read, the owners entry rendering (byte-equal to
// discover -write-owners for the same pipeline), the connections: file
// references, watched_by, the server-side preconditions, the write's
// exclusivity and its removal when the reload fails, the repos: path's wait
// for its own PR, and the whole last mile with the real ladder runner.

// registerCDCKeys are the connection keys owners.yaml requires of a cdc
// connections: block, the names the credentials click registers (U1-5d).
func registerCDCKeys() []string {
	return []string{"source_db_url", "warehouse_db_url", "kafka_brokers", "connect_url", "schema_registry_url"}
}

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
		PipelineID: "orders-x", Mode: events.RoomModeSetup, Alert: mustJSON(t, req),
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

	// The principal each approval was granted under (identity.grant), which
	// the overlap re-check reads for an IdP group; an approval without an
	// identity records its grant field.
	snap = Apply(snap, ev(8, events.ApprovalGiven, ann, events.ApprovalGivenPayload{
		ProposalID: "p9", Layer: "kafka", Identity: &events.Identity{Grant: "group:platform"}, Grant: events.GrantOwner,
	}))
	prop, _ = openProposal(snap, "p9")
	if prop.approvalGrants["kafka"] != "group:platform" || prop.approvalGrants[events.LayerEnvironment] != "" {
		t.Fatalf("approval grants = %+v, want kafka granted as group:platform", prop.approvalGrants)
	}

	// The credentials click's names: this room's pipeline only, valid names
	// only, each once.
	snap = Apply(snap, ev(9, events.SetupCredentialsRegistered, systemActor, events.SetupCredentialsRegisteredPayload{
		PipelineID: "someone-else", Names: []string{"source_db_url"},
	}))
	if len(snap.Setup.credentialNames) != 0 {
		t.Fatalf("another pipeline's credentials were folded: %v", snap.Setup.credentialNames)
	}
	snap = Apply(snap, ev(10, events.SetupCredentialsRegistered, systemActor, events.SetupCredentialsRegisteredPayload{
		PipelineID: "orders-x", Names: []string{"source_db_url", "bad name", "source_db_url"},
	}))
	if got := snap.Setup.credentialNames; len(got) != 1 || got[0] != "source_db_url" {
		t.Fatalf("credential names = %v, want [source_db_url]", got)
	}
}

// entrySnapshot builds the snapshot registerEntry reads: the request, the
// room id and the fold-only fields (credentialNames are the registered read
// credentials the entry references as connections:).
func entrySnapshot(id, tenant, title string, targets []SetupTargetState, credentialNames, consumerGroups, sinkTables []string) RoomSnapshot {
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
			credentialNames: credentialNames,
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

// TestRegisterConnectionsFileRefs: every registered read credential whose name
// is a connection key becomes a file: reference, whether or not the room's
// targets name that key (warehouse_db_url never comes from a target, and the
// owners validation requires it); a registered name that is no connection
// key (the runner's own store name) and a secret that was set but never
// registered are left out.
func TestRegisterConnectionsFileRefs(t *testing.T) {
	svc := registerTestService(t)
	setup := &SetupSnapshot{
		credentialNames: []string{"source_db_url", "warehouse_db_url", "source_read_password"},
		secretNames:     []string{"source_db_url", "warehouse_db_url", "kafka_brokers"},
	}
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
	// The value is the plain absolute path (what the loader's file: accepts),
	// not the canonical owners.FileRef comparison form.
	path := func(key string) string { return filepath.Join(svc.setupCfg.SecretsDir, "orders-x", key) }
	if len(conns) != 2 || conns["source_db_url"] != path("source_db_url") || conns["warehouse_db_url"] != path("warehouse_db_url") {
		t.Fatalf("connections = %v, want source_db_url and warehouse_db_url as absolute file paths", conns)
	}
	if !filepath.IsAbs(conns["source_db_url"]) || strings.HasPrefix(conns["source_db_url"], "file:") {
		t.Fatalf("connections.source_db_url = %q, want a plain absolute path", conns["source_db_url"])
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
	// The second write is refused: a registration never overwrites a file,
	// and the first entry's bytes stay as they were.
	changed := append([]byte("# changed\n"), entry...)
	if _, err := svc.writeRegisterFile(ctx, &snap, changed); err == nil || !strings.Contains(err.Error(), "already exists") {
		t.Fatalf("second write = %v, want already exists", err)
	}
	if b, err := os.ReadFile(path); err != nil || !bytes.Equal(b, entry) {
		t.Fatalf("the existing entry changed: %v", err)
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
// owners. A tenant admin or a system approval does not count. An approver the
// server knows only from the IdP (not in users.yaml) counts through the
// principal the approval was granted under (identity.grant "group:<name>" or
// "backup:group:<name>", as the fold records it), never through a user grant
// or a group the pipeline does not name; with no recorded grant it fails
// closed.
func TestRegisterApprovalsHold(t *testing.T) {
	own := loadOwnersString(t, registerOwnersOnlyYAML())
	users, err := ParseUsers([]byte(setupUsersForRegister()))
	if err != nil {
		t.Fatal(err)
	}
	svc := New(Deps{Owners: own, Users: users, Setup: config.Setup{Enabled: true}})
	t.Cleanup(svc.Close)
	pipe := func(id string) []owners.Pipeline {
		p, ok := own.Pipeline(id)
		if !ok {
			t.Fatalf("no pipeline %s", id)
		}
		return []owners.Pipeline{p}
	}
	human := func(id string) events.Actor { return events.Actor{Kind: events.ActorHuman, ID: id, Name: id} }
	env := events.LayerEnvironment

	snapFor := func(actors map[string]events.Actor, grants map[string]string) *RoomSnapshot {
		return &RoomSnapshot{
			Setup: &SetupSnapshot{Mode: true, req: events.SetupRequest{PipelineID: "orders-x", Tenant: "a"}},
			OpenProposals: []Proposal{{
				ProposalID:     "p1",
				approvalActors: actors,
				approvalGrants: grants,
			}},
		}
	}
	cases := []struct {
		name   string
		pipes  []owners.Pipeline
		actors map[string]events.Actor
		grants map[string]string
		want   bool
	}{
		{"no approvals", pipe("existing"), nil, nil, false},
		{"a tenant admin who is not an owner", pipe("existing"),
			map[string]events.Actor{env: human("ann")}, map[string]string{env: events.GrantAdmin}, false},
		{"the system actor", pipe("existing"),
			map[string]events.Actor{env: systemActor}, map[string]string{env: events.GrantPolicy}, false},
		{"the overlapping pipeline's owner", pipe("existing"),
			map[string]events.Actor{env: human("alice")}, map[string]string{env: "user:alice"}, true},
		{"an IdP group member, granted as the owning group", pipe("grouped"),
			map[string]events.Actor{env: human("zoe")}, map[string]string{env: "group:platform"}, true},
		{"an IdP group member, granted as a backup of the owning group", pipe("grouped"),
			map[string]events.Actor{env: human("zoe")}, map[string]string{env: "backup:group:platform"}, true},
		{"an IdP user granted as a user principal the pipeline does not name", pipe("grouped"),
			map[string]events.Actor{env: human("zoe")}, map[string]string{env: "user:zoe"}, false},
		{"a group the pipeline does not name", pipe("grouped"),
			map[string]events.Actor{env: human("zoe")}, map[string]string{env: "group:data"}, false},
		{"an IdP group member with no recorded grant", pipe("grouped"),
			map[string]events.Actor{env: human("zoe")}, nil, false},
		{"the group grant covers only the layer it was given on", pipe("grouped"),
			map[string]events.Actor{env: human("zoe"), "kafka": human("zoe")},
			map[string]string{"kafka": "group:platform"}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := svc.registerApprovalsHold(snapFor(c.actors, c.grants), "p1", c.pipes); got != c.want {
				t.Fatalf("approvals hold = %v, want %v", got, c.want)
			}
		})
	}
}

// registerOwnersOnlyYAML has "existing", owned by alice on every layer, and
// "grouped", owned by the group platform (an IdP group no users.yaml lists)
// on the environment layer.
func registerOwnersOnlyYAML() string {
	var b strings.Builder
	b.WriteString("pipelines:\n  - id: existing\n    topic_prefix: ex\n    source_tables: [public.e]\n    connectors: [ex-c]\n    connections: {source_db_url: {env: EX_SRC}, warehouse_db_url: {env: EX_WH}, kafka_brokers: {env: EX_K}, connect_url: {env: EX_C}, schema_registry_url: {env: EX_SR}}\n    owners:")
	for _, l := range events.LayersOf(events.PipelineKindCDC) {
		b.WriteString("\n      " + l + ": {approvers: [user:alice]}")
	}
	b.WriteString("\n  - id: grouped\n    topic_prefix: gr\n    source_tables: [public.g]\n    connectors: [gr-c]\n    connections: {source_db_url: {env: GR_SRC}, warehouse_db_url: {env: GR_WH}, kafka_brokers: {env: GR_K}, connect_url: {env: GR_C}, schema_registry_url: {env: GR_SR}}\n    owners:\n      environment: {approvers: [group:platform]}\n")
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

// TestRegisterPreconditions pins what the server itself needs before a
// registration writes anything (registerPreconditions; checked at Create and
// before Execute): an owners dir and a reload for the file path, a forge for
// repos:, read credentials registered for a pipeline this server will watch
// (a secret only set does not count), and an id the owners set does not hold
// yet. A local target is watched locally and needs no credentials here.
func TestRegisterPreconditions(t *testing.T) {
	local := []SetupTargetState{{Kind: "postgres", Address: "127.0.0.1:5432", Class: "local"}}
	cases := []struct {
		name     string
		tweak    func(svc *Service, snap *RoomSnapshot)
		wantCode string
		contains string
	}{
		{"server-watched, credentials registered", func(_ *Service, snap *RoomSnapshot) {
			snap.Setup.credentialNames = registerCDCKeys()
		}, "", ""},
		{"local target needs no credentials here", func(_ *Service, snap *RoomSnapshot) {
			snap.Setup.Targets = local
		}, "", ""},
		{"server-watched without credentials", func(*Service, *RoomSnapshot) {}, "validation", "read credentials"},
		{"a secret set but not registered", func(_ *Service, snap *RoomSnapshot) {
			snap.Setup.secretNames = registerCDCKeys()
		}, "validation", "read credentials"},
		{"a registered name that is no connection key", func(_ *Service, snap *RoomSnapshot) {
			snap.Setup.credentialNames = []string{"source_read_password"}
		}, "validation", "read credentials"},
		{"no OWNERS_DIR", func(svc *Service, snap *RoomSnapshot) {
			snap.Setup.credentialNames = registerCDCKeys()
			svc.setupCfg.OwnersDir = ""
		}, "validation", "OWNERS_DIR"},
		{"no owners reload", func(svc *Service, snap *RoomSnapshot) {
			snap.Setup.credentialNames = registerCDCKeys()
			svc.reloadOwners = nil
		}, "validation", "reload"},
		{"repos: without a forge", func(svc *Service, snap *RoomSnapshot) {
			snap.Setup.credentialNames = registerCDCKeys()
			snap.Setup.req.Repos = []string{"infra"}
			svc.setupForge = nil
		}, "validation", "forge"},
		{"the id is in the owners set already", func(svc *Service, snap *RoomSnapshot) {
			snap.Setup.credentialNames = registerCDCKeys()
			svc.owners = loadOwnersString(t, strings.Replace(registerOwnersOnlyYAML(), "id: existing", "id: orders-x", 1))
		}, "pipeline_exists", ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			svc := registerTestService(t)
			svc.reloadOwners = func() error { return nil }
			snap := entrySnapshot("orders-x", "", "Orders", nil, nil, nil, nil)
			c.tweak(svc, &snap)
			err := svc.registerPreconditions(&snap)
			if c.wantCode == "" {
				if err != nil {
					t.Fatalf("preconditions refused: %v", err)
				}
				return
			}
			var re *Error
			if !errors.As(err, &re) || re.Code != c.wantCode {
				t.Fatalf("preconditions = %v, want %s", err, c.wantCode)
			}
			if c.contains != "" && !strings.Contains(re.Msg, c.contains) {
				t.Fatalf("message %q misses %q", re.Msg, c.contains)
			}
		})
	}
}

// TestRegisterFileIsRemovedWhenTheReloadFails: the write and the reload are
// one step. A reload that errors, or that skips the entry, leaves no file
// behind (and no pipeline in the published set), and the next attempt is not
// refused as "already exists".
func TestRegisterFileIsRemovedWhenTheReloadFails(t *testing.T) {
	ctx := context.Background()
	svc := registerTestService(t)
	mainFile := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(mainFile, []byte(registerOwnersOnlyYAML()), 0o600); err != nil {
		t.Fatal(err)
	}
	live, err := owners.NewLive(mainFile, svc.setupCfg.OwnersDir)
	if err != nil {
		t.Fatal(err)
	}
	svc.ownersNowFn = live.Load
	svc.reloadOwners = func() error { return errors.New("owners: the main file is unreadable") }
	path := filepath.Join(svc.setupCfg.OwnersDir, "orders-x.yaml")

	snap := entrySnapshot("orders-x", "", "Orders", nil, registerCDCKeys(), nil, nil)
	entry, _, err := svc.registerEntry(&snap)
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.registerFile(ctx, &snap, entry)
	if err == nil || !strings.Contains(err.Error(), "unreadable") || !strings.Contains(err.Error(), "removed") {
		t.Fatalf("registerFile with a failing reload = %v, want the reload error and the removal", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the entry of a failed registration is still there: %v", err)
	}

	// An entry the reload skips (a tenant the main file does not declare) is
	// removed too, with the loader's reason, and never published.
	svc.reloadOwners = live.Reload
	tenanted := entrySnapshot("orders-x", "a", "Orders", nil, registerCDCKeys(), nil, nil)
	entry, _, err = svc.registerEntry(&tenanted)
	if err != nil {
		t.Fatal(err)
	}
	_, err = svc.registerFile(ctx, &tenanted, entry)
	if err == nil || !strings.Contains(err.Error(), "not loaded") || !strings.Contains(err.Error(), "tenant") {
		t.Fatalf("registerFile with a skipped entry = %v, want not loaded with the loader's reason", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("the skipped entry is still there: %v", err)
	}
	if _, ok := live.Load().Pipeline("orders-x"); ok {
		t.Fatal("the skipped entry is in the published owners set")
	}

	// The next attempt, with a loadable entry, succeeds: nothing refuses it
	// as already existing, and the published set carries the pipeline.
	entry, _, err = svc.registerEntry(&snap)
	if err != nil {
		t.Fatal(err)
	}
	got, err := svc.registerFile(ctx, &snap, entry)
	if err != nil {
		t.Fatalf("registerFile after the failures: %v", err)
	}
	if got != path {
		t.Fatalf("path = %q, want %q", got, path)
	}
	p, ok := live.Load().Pipeline("orders-x")
	if !ok || !p.HasConnections() {
		t.Fatalf("the registered pipeline = %+v, ok %v; want it published with connections", p, ok)
	}
}

// TestRegisterPRMergedClosesOnlyItsOwnPR: with repos: the room waits for the
// merge of the registration PR it recorded, not for any merge the follower
// sees. A merge without a URL, or of another PR, leaves the room open; the
// registration PR's merge closes it "pipeline live" and labels it.
func TestRegisterPRMergedClosesOnlyItsOwnPR(t *testing.T) {
	ctx := context.Background()
	users, err := ParseUsers([]byte(tierUsersYAML))
	if err != nil {
		t.Fatal(err)
	}
	svc := New(Deps{
		Store: memstore.New(), Users: users, Owners: loadOwnersString(t, tierOwnersYAML()), Run: idleRun,
		Setup: config.Setup{Enabled: true, SecretsDir: t.TempDir(), OwnersDir: t.TempDir()},
	})
	t.Cleanup(svc.Close)
	alice, ok := svc.LookupUser("alice")
	if !ok {
		t.Fatal("no user alice")
	}
	res, err := svc.CreateSetupRoom(ctx, alice, events.SetupRequest{
		PipelineID: "shipments-setup", Tenant: "a", Title: "Setup shipments", SourceKind: "postgres", Target: "compose",
		TopicPrefix: "shipments", Tables: []string{"public.shipments"}, Connectors: []string{"shipments-src"},
		Targets: []events.SetupTarget{{Kind: "postgres", Address: "db.acme:5432"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	roomID := res.Room.ID
	const url = "https://forge.acme/acme/infra/pull/7"
	if err := svc.EmitSetup(ctx, roomID, events.PipelineRegistered, events.PipelineRegisteredPayload{
		PipelineID: "shipments-setup", Tenant: "a", PullRequestURL: url, WatchedBy: events.WatchedByServer,
	}); err != nil {
		t.Fatal(err)
	}
	open := func() bool { return mustRoom(t, svc, roomID).ClosedAt == nil }

	svc.registerPRMerged(ctx, roomID, "p_reg", "")
	if !open() {
		t.Fatal("a merge without a URL closed the room")
	}
	svc.registerPRMerged(ctx, roomID, "p_reg", "https://forge.acme/acme/infra/pull/8")
	if !open() {
		t.Fatal("another PR's merge closed the room")
	}
	svc.registerPRMerged(ctx, roomID, "p_reg", url)
	snap := mustRoom(t, svc, roomID)
	if snap.ClosedAt == nil || snap.Resolution != pipelineLiveResolution {
		t.Fatalf("after the registration PR merged: closed %v, resolution %q", snap.ClosedAt, snap.Resolution)
	}
	var labels []events.RoomLabelledPayload
	for _, e := range roomEvents(t, svc, roomID) {
		if e.Type == events.RoomLabelled {
			var p events.RoomLabelledPayload
			if err := json.Unmarshal(e.Payload, &p); err != nil {
				t.Fatal(err)
			}
			labels = append(labels, p)
		}
	}
	if len(labels) != 1 || labels[0].Cause != registerLabelCause || labels[0].FixProposalID != "p_reg" {
		t.Fatalf("room.labelled = %+v, want one with cause setup for p_reg", labels)
	}
}

// TestRegisterAfterRealLadder runs the last mile with the real producers: the
// ladder runner (U1-8b) writes setup.verified for the current plan, the
// credentials click's record names the read credentials, the driver proposes
// pipeline_register, the tenant admin approves (the creator's own approval is
// refused), and the executor writes the entry, reloads the owners set and
// closes the room "pipeline live". Nothing here fakes a setup.verified.
func TestRegisterAfterRealLadder(t *testing.T) {
	ctx := context.Background()
	r := newLadderRig(t, tierOpts{})
	// The registration deps a team server wires: a Live owners set over the
	// rig's owners config and an OWNERS_DIR, the reload and the watcher hook.
	mainFile := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(mainFile, []byte(tierOwnersYAML()), 0o600); err != nil {
		t.Fatal(err)
	}
	ownersDir := filepath.Join(t.TempDir(), "owners.d")
	live, err := owners.NewLive(mainFile, ownersDir)
	if err != nil {
		t.Fatal(err)
	}
	reloads := 0
	r.svc.ownersNowFn = live.Load
	r.svc.reloadOwners = live.Reload
	r.svc.onOwnersReloaded = func() { reloads++ }
	r.svc.setupCfg.OwnersDir = ownersDir
	r.svc.setupCfg.SecretsDir = t.TempDir()

	roomID := r.openSetup("shipments-setup", "a", "alice", "db.acme:5432", "")
	r.plan(roomID, "db.acme:5432", "compose_up", "connect_create_connector")
	r.reads.found = func(key string) bool { return key == r.canary.lastNonce() }
	if err := r.svc.startSetupLadder(roomID); err != nil {
		t.Fatalf("start ladder: %v", err)
	}
	canary, _ := waitLadderCanaryProposal(t, r.svc, roomID, 0)
	if err := r.approve(roomID, canary, "postgres", "bob"); err != nil {
		t.Fatalf("approve the canary insert: %v", err)
	}
	// Rungs 1 to 7 green: the gate's event, written by the runner itself.
	waitSetupVerified(t, r.svc, roomID, 1)
	if err := r.svc.EmitSetup(ctx, roomID, events.SetupCredentialsRegistered, events.SetupCredentialsRegisteredPayload{
		PipelineID: "shipments-setup", Names: registerCDCKeys(), RunnerLabel: "laptop",
	}); err != nil {
		t.Fatal(err)
	}

	pid := r.driverPropose(roomID, "alice", registerActionName, events.LayerEnvironment, "{}")
	approveNow := func(who string) error {
		p, ok := openProposal(mustRoom(t, r.svc, roomID), pid)
		if !ok {
			t.Fatalf("proposal %s is not open", pid)
		}
		_, err := r.svc.ApproveProposal(ctx, r.user(who), pid, events.LayerEnvironment, p.ParamsHash)
		return err
	}
	// The creator cannot approve her own registration on a team server.
	err = approveNow("alice")
	var re *Error
	if !errors.As(err, &re) || re.Code != "not_admin" {
		t.Fatalf("alice's self-approval = %v, want 403 not_admin", err)
	}
	if err := approveNow("bob"); err != nil {
		t.Fatalf("bob approves: %v", err)
	}
	waitForCond(t, 10*time.Second, func() bool { return mustRoom(t, r.svc, roomID).ClosedAt != nil }, "the room never closed after the registration")
	snap := mustRoom(t, r.svc, roomID)
	if snap.Resolution != pipelineLiveResolution {
		t.Fatalf("resolution = %q, want %q", snap.Resolution, pipelineLiveResolution)
	}
	if reloads != 1 {
		t.Fatalf("owners reloads = %d, want 1", reloads)
	}
	p, ok := live.Load().Pipeline("shipments-setup")
	if !ok {
		t.Fatal("the reloaded owners set does not know the pipeline")
	}
	if !p.HasConnections() {
		t.Fatal("the registered pipeline has no connections: block")
	}
	spec, ok := p.Setup()
	if !ok || spec.RoomID != roomID || spec.WatchedBy != events.WatchedByServer {
		t.Fatalf("setup spec = %+v, ok %v", spec, ok)
	}
	var regs []events.PipelineRegisteredPayload
	for _, e := range roomEvents(t, r.svc, roomID) {
		if e.Type == events.PipelineRegistered {
			var pl events.PipelineRegisteredPayload
			if err := json.Unmarshal(e.Payload, &pl); err != nil {
				t.Fatal(err)
			}
			regs = append(regs, pl)
		}
	}
	if len(regs) != 1 || regs[0].By.ID != "alice" || len(regs[0].ApprovedBy) != 1 || regs[0].ApprovedBy[0].ID != "bob" {
		t.Fatalf("pipeline.registered = %+v, want by alice, approved by bob", regs)
	}
	// GET /api/pipelines shows the setup row with the registration time.
	for _, info := range r.svc.PipelinesFor(r.user("alice")) {
		if info.ID == "shipments-setup" {
			if info.Setup == nil || info.Setup.RegisteredAt == "" || info.Setup.RoomID != roomID {
				t.Fatalf("pipelines row = %+v", info.Setup)
			}
			return
		}
	}
	t.Fatal("PipelinesFor does not list the registered pipeline")
}
