package room_test

import (
	"context"
	"encoding/json"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/setup/plan"
	"piperoom/internal/store/memstore"
)

// pipeline_register end to end (U1-14; docs/design/setup-agent.md §3.6,
// §3.9; docs/api.md "Registration"): the happy path through the proposal and
// approval routes with the real executor, the not-verified refusal, the
// requester's own approval refused on a team server, the overlap refusal at
// Create, and the double register refused once the room is closed.

// registerOwnersYAML is the setup test owners config with a second tenant
// admin: ann may approve on tenant a, which the non-self approver rule of
// §3.6 needs (alice, the creator, may not approve her own registration).
// "naked" is tenant a's pipeline that owns no layer. Each tenant names its
// own env references (Load refuses a connection reference shared by two
// tenants without shared_connections).
func registerOwnersYAML() string {
	conns := func(p string) string {
		return "\n    connections: {source_db_url: {env: " + p + "_SRC}, warehouse_db_url: {env: " + p + "_WH}, kafka_brokers: {env: " + p + "_K}, connect_url: {env: " + p + "_C}, schema_registry_url: {env: " + p + "_SR}}"
	}
	owner := func(user string) string {
		var b strings.Builder
		b.WriteString("\n    owners:")
		for _, l := range events.LayersOf(events.PipelineKindCDC) {
			b.WriteString("\n      " + l + ": {approvers: [user:" + user + "]}")
		}
		return b.String()
	}
	return "tenants:\n" +
		"  a: {members: [user:alice, user:ann], admins: [user:alice, user:ann]}\n" +
		"  b: {members: [user:bob, user:ben]}\n" +
		"pipelines:" +
		"\n  - id: existing\n    tenant: a\n    topic_prefix: ex\n    source_tables: [public.e]\n    connectors: [ex-c]" + conns("EX") + owner("alice") +
		"\n  - id: other\n    tenant: b\n    topic_prefix: ot\n    source_tables: [public.o]\n    connectors: [ot-c]" + conns("OT") + owner("bob") +
		"\n  - id: naked\n    tenant: a\n    topic_prefix: nk\n    source_tables: [public.n]\n    connectors: [nk-c]" + conns("NK") + "\n"
}

// registerEnv wires the registration deps a team server provides: an
// owners.Live over OWNERS_FILE and OWNERS_DIR (OwnersNow, ReloadOwners) and
// the watcher rebuild hook (OnOwnersReloaded).
type registerEnv struct {
	setupEnv
	live      *owners.Live
	mainFile  string
	ownersDir string
	reloads   *int
}

func newRegisterEnv(t *testing.T) *registerEnv {
	t.Helper()
	users, err := room.ParseUsers([]byte(setupUsers))
	if err != nil {
		t.Fatal(err)
	}
	mainFile := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(mainFile, []byte(registerOwnersYAML()), 0o600); err != nil {
		t.Fatal(err)
	}
	ownersDir := filepath.Join(t.TempDir(), "owners.d")
	live, err := owners.NewLive(mainFile, ownersDir)
	if err != nil {
		t.Fatal(err)
	}
	secretsDir := t.TempDir()
	reloads := 0
	st := memstore.New()
	now := time.Date(2026, 10, 20, 12, 0, 0, 0, time.UTC)
	svc := room.New(room.Deps{
		Store:            st,
		Users:            users,
		Owners:           live.Load(),
		OwnersNow:        func() *owners.Owners { return live.Load() },
		ReloadOwners:     live.Reload,
		OnOwnersReloaded: func() { reloads++ },
		Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
			<-ctx.Done()
			return nil
		},
		Now:           func() time.Time { return now },
		Setup:         config.Setup{Enabled: true, SecretsDir: secretsDir, OwnersDir: ownersDir},
		SetupResolver: fakeResolver{"db.acme": {netip.MustParseAddr("10.1.2.3")}},
	})
	t.Cleanup(svc.Close)
	return &registerEnv{
		setupEnv:  setupEnv{svc: svc, store: st, own: live.Load(), now: now, secretsDir: secretsDir},
		live:      live,
		mainFile:  mainFile,
		ownersDir: ownersDir,
		reloads:   &reloads,
	}
}

// greenLadder is a setup.verified with rungs 1 to 7 green, the ladder
// pipeline_register is gated on (design §3.4, §3.9).
func greenLadder() []events.SetupRung {
	names := []string{plan.RungConnector, plan.RungTasks, plan.RungCanaryTopic, plan.RungSnapshot,
		plan.RungCanaryEvent, plan.RungCanarySubject, plan.RungSinkRow}
	rungs := make([]events.SetupRung, len(names))
	for i, n := range names {
		rungs[i] = events.SetupRung{Name: n, OK: true}
	}
	return rungs
}

func emitVerified(t *testing.T, svc *room.Service, roomID string, rungs []events.SetupRung) {
	t.Helper()
	if err := svc.EmitSetup(context.Background(), roomID, events.SetupVerified, events.SetupVerifiedPayload{
		Rungs: rungs,
	}); err != nil {
		t.Fatalf("setup.verified: %v", err)
	}
}

func registerProposal(title string) events.ProposalCreatedPayload {
	return events.ProposalCreatedPayload{
		Title: title, Risk: "low", Rollback: "remove the written owners file and close the room",
		Steps: []events.ProposalStep{{
			Action: "pipeline_register", Layer: "environment", Params: json.RawMessage("{}"),
		}},
	}
}

func TestRegisterHappyPathTeamServer(t *testing.T) {
	env := newRegisterEnv(t)
	ctx := context.Background()
	res := createSetup(t, &env.setupEnv, setupRequest("orders-setup"))
	roomID := res.Room.ID
	emitVerified(t, env.svc, roomID, greenLadder())

	// The driver proposes the registration; a driver-origin step needs no
	// plan step, and the action is in the service's own registry.
	ev, err := env.svc.CreateDriverProposal(ctx, alice(), roomID, registerProposal("register the pipeline"))
	if err != nil {
		t.Fatalf("driver proposal: %v", err)
	}
	dec, err := events.Decode[events.ProposalCreatedPayload](ev)
	if err != nil {
		t.Fatal(err)
	}
	pid := dec.ProposalID

	// On a team server the requester's own approval is refused, admin or not.
	_, err = env.svc.ApproveProposal(ctx, alice(), pid, "environment", proposalHash(t, env.svc, pid))
	wantSetupError(t, err, 403, "not_admin")

	// The other tenant admin approves; the executor runs the registration.
	if _, err := env.svc.ApproveProposal(ctx, ann(), pid, "environment", proposalHash(t, env.svc, pid)); err != nil {
		t.Fatalf("ann approves: %v", err)
	}
	waitFor(t, 5*time.Second, func() bool {
		snap, err := env.svc.GetRoom(ctx, roomID)
		return err == nil && snap.ClosedAt != nil
	})
	snap, err := env.svc.GetRoom(ctx, roomID)
	if err != nil {
		t.Fatal(err)
	}
	if snap.Resolution != "pipeline live" {
		t.Fatalf("resolution = %q, want %q", snap.Resolution, "pipeline live")
	}
	if snap.Setup == nil || snap.Setup.WatchedBy != events.WatchedByServer {
		t.Fatalf("setup.watched_by = %+v, want server", snap.Setup)
	}

	// The owners entry: written to OWNERS_DIR and carried by the directory
	// loader (a tenanted per-pipeline file needs the main file's tenants:
	// block, so owners.LoadDir is its check; owners.Load covers the
	// un-tenanted entry in the internal tests).
	path := filepath.Join(env.ownersDir, "orders-setup.yaml")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the written entry: %v", err)
	}
	if _, err := owners.LoadDir(env.mainFile, env.ownersDir); err != nil {
		t.Fatalf("the written file does not load with the directory loader: %v", err)
	}
	for _, want := range []string{
		"tenant: a", "auto_approve: []", "target: compose", "watched_by: server",
		"room_id: " + roomID, "approvers: 'user:alice'",
	} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("the entry misses %q:\n%s", want, raw)
		}
	}
	// The reload happened once and the watcher rebuild hook ran.
	if *env.reloads != 1 {
		t.Fatalf("owners reloads = %d, want 1", *env.reloads)
	}
	p, ok := env.live.Load().Pipeline("orders-setup")
	if !ok {
		t.Fatal("the reloaded owners set does not know the pipeline")
	}
	spec, ok := p.Setup()
	if !ok || spec.RoomID != roomID || spec.Target != "compose" || spec.WatchedBy != events.WatchedByServer {
		t.Fatalf("setup spec = %+v, ok %v", spec, ok)
	}

	// pipeline.registered: the file path, the watcher, the requester and the
	// approver who is not the requester.
	regs := eventsOf[events.PipelineRegisteredPayload](t, env.svc, roomID, events.PipelineRegistered)
	if len(regs) != 1 {
		t.Fatalf("pipeline.registered = %d events, want 1", len(regs))
	}
	reg := regs[0]
	if reg.PipelineID != "orders-setup" || reg.Tenant != "a" || reg.Path != path || reg.WatchedBy != events.WatchedByServer {
		t.Fatalf("pipeline.registered = %+v", reg)
	}
	if reg.By.ID != "alice" {
		t.Fatalf("pipeline.registered.by = %+v, want alice", reg.By)
	}
	if len(reg.ApprovedBy) != 1 || reg.ApprovedBy[0].ID != "ann" {
		t.Fatalf("pipeline.registered.approved_by = %+v, want [ann]", reg.ApprovedBy)
	}

	// room.labelled gets cause "setup".
	labels := eventsOf[events.RoomLabelledPayload](t, env.svc, roomID, events.RoomLabelled)
	if len(labels) != 1 || labels[0].Cause != "setup" {
		t.Fatalf("room.labelled = %+v, want one with cause setup", labels)
	}

	// GET /api/pipelines shows the setup row.
	var info room.PipelineInfo
	found := false
	for _, pi := range env.svc.PipelinesFor(alice()) {
		if pi.ID == "orders-setup" {
			info, found = pi, true
			break
		}
	}
	if !found {
		t.Fatal("PipelinesFor does not list the registered pipeline")
	}
	if info.Setup == nil || info.Setup.RoomID != roomID || info.Setup.Target != "compose" || info.Setup.WatchedBy != events.WatchedByServer {
		t.Fatalf("pipeline setup row = %+v", info.Setup)
	}
	if _, err := time.Parse(time.RFC3339, info.Setup.RegisteredAt); err != nil {
		t.Fatalf("registered_at %q does not parse: %v", info.Setup.RegisteredAt, err)
	}
	// It is the pipeline.registered event's own time.
	evs, _, err := env.svc.Events(ctx, roomID, 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	var regAt string
	for _, e := range evs {
		if e.Type == events.PipelineRegistered {
			regAt = e.At.UTC().Format(time.RFC3339)
		}
	}
	if info.Setup.RegisteredAt != regAt {
		t.Fatalf("registered_at = %q, want the event time %q", info.Setup.RegisteredAt, regAt)
	}

	// A second register is refused: the room is closed, and the file exists.
	_, err = env.svc.CreateDriverProposal(ctx, alice(), roomID, registerProposal("register again"))
	wantSetupError(t, err, 409, "room_closed")
}

func TestRegisterRefusedWithoutGreenLadder(t *testing.T) {
	env := newRegisterEnv(t)
	ctx := context.Background()
	roomID := createSetup(t, &env.setupEnv, setupRequest("orders-setup")).Room.ID

	// The ladder never ran.
	_, err := env.svc.CreateDriverProposal(ctx, alice(), roomID, registerProposal("register"))
	wantSetupError(t, err, 409, "not_verified")

	// Green, then a step executed after the verification: the machine moved,
	// so the gate closes again until the ladder re-runs.
	emitVerified(t, env.svc, roomID, greenLadder())
	if _, err := env.svc.CreateDriverProposal(ctx, alice(), roomID, registerProposal("register")); err != nil {
		t.Fatalf("a green ladder should allow the proposal: %v", err)
	}
	if err := env.svc.EmitSetup(ctx, roomID, events.ActionExecuted, events.ActionExecutedPayload{
		ProposalID: "p_other", StepIndex: 0, OK: true, Output: "ran",
	}); err != nil {
		t.Fatalf("action.executed: %v", err)
	}
	_, err = env.svc.CreateDriverProposal(ctx, alice(), roomID, registerProposal("register"))
	wantSetupError(t, err, 409, "not_verified")

	// The ladder runs again: green once more, the gate opens.
	emitVerified(t, env.svc, roomID, greenLadder())
	if _, err := env.svc.CreateDriverProposal(ctx, alice(), roomID, registerProposal("register")); err != nil {
		t.Fatalf("a fresh verification should allow the proposal: %v", err)
	}
}

// TestRegisterRefusedInIncidentRoom pins that pipeline_register is a
// setup-room step: an incident room has no setup state to register.
func TestRegisterRefusedInIncidentRoom(t *testing.T) {
	env := newRegisterEnv(t)
	ctx := context.Background()
	incident, err := env.svc.CreateRoom(ctx, alice(), "incident", "sev2", "existing")
	if err != nil {
		t.Fatal(err)
	}
	_, err = env.svc.CreateDriverProposal(ctx, alice(), incident.ID, registerProposal("register"))
	wantSetupError(t, err, 400, "validation")
}

func TestRegisterOverlapRefusedAtCreate(t *testing.T) {
	env := newRegisterEnv(t)
	ctx := context.Background()

	// The provisional pipeline shares tenant b's "other" topic prefix: its
	// owners cannot approve here, so Create refuses with 409 overlap and
	// never names the other tenant's pipeline.
	foreign := setupRequest("orders-foreign")
	foreign.TopicPrefix = "ot"
	roomID := createSetup(t, &env.setupEnv, foreign).Room.ID
	emitVerified(t, env.svc, roomID, greenLadder())
	_, err := env.svc.CreateDriverProposal(ctx, alice(), roomID, registerProposal("register"))
	wantSetupError(t, err, 409, "overlap")
	if err == nil || !strings.Contains(err.Error(), "another tenant") {
		t.Fatalf("the foreign overlap refusal names no other tenant's pipeline: %v", err)
	}

	// An own-tenant pipeline that owns no layer has no approver either.
	naked := setupRequest("orders-naked")
	naked.TopicPrefix = "nk"
	roomID = createSetup(t, &env.setupEnv, naked).Room.ID
	emitVerified(t, env.svc, roomID, greenLadder())
	_, err = env.svc.CreateDriverProposal(ctx, alice(), roomID, registerProposal("register"))
	wantSetupError(t, err, 409, "overlap")
	if err == nil || !strings.Contains(err.Error(), "naked") {
		t.Fatalf("the own-tenant unowned overlap refusal should name it: %v", err)
	}

	// An owned own-tenant overlap is approvable: Create allows it (the
	// Execute re-check then holds the approvals to the overlap rule).
	owned := setupRequest("orders-owned")
	owned.TopicPrefix = "ex"
	roomID = createSetup(t, &env.setupEnv, owned).Room.ID
	emitVerified(t, env.svc, roomID, greenLadder())
	ev, err := env.svc.CreateDriverProposal(ctx, alice(), roomID, registerProposal("register"))
	if err != nil {
		t.Fatalf("an owned overlap is approvable: %v", err)
	}
	dec, err := events.Decode[events.ProposalCreatedPayload](ev)
	if err != nil {
		t.Fatal(err)
	}
	pid := dec.ProposalID

	// A tenant admin who is not an owner of the overlapping pipeline passes
	// the approval guard on every required layer (the overlap's layers,
	// setupLayers), but the Execute re-check refuses: the overlap rule needs
	// the overlapping pipeline's OWNERS among the approvers (design §3.6),
	// and ann is none. The proposal fails and the room stays open.
	if len(dec.RequiredLayers) == 0 {
		t.Fatal("the overlap proposal has no required layers")
	}
	for _, layer := range dec.RequiredLayers {
		if _, err := env.svc.ApproveProposal(ctx, ann(), pid, layer, proposalHash(t, env.svc, pid)); err != nil {
			t.Fatalf("ann approves %s: %v", layer, err)
		}
	}
	waitFor(t, 5*time.Second, func() bool {
		p, err := env.store.GetProposal(ctx, pid)
		return err == nil && p.Status == "failed"
	})
	snap, err := env.svc.GetRoom(ctx, roomID)
	if err != nil {
		t.Fatal(err)
	}
	if snap.ClosedAt != nil {
		t.Fatalf("the room closed despite the refused overlap: %+v", snap.ClosedAt)
	}
}
