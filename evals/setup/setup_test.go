package setup

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
)

// The six U1-19 setup scenarios, in the recording directory.
var sixScenarios = []string{
	"setup/compose-postgres-clean",
	"setup/compose-privileged-refused",
	"setup/flat-plugin-dir",
	"setup/wal-level-replica",
	"setup/missing-grant",
	"setup/db-unreachable-from-connect",
}

// A digest of the shape the protocol produces (64 lowercase hex characters),
// for fixtures that only need a valid one.
var (
	digestA = strings.Repeat("a", 64)
	digestB = strings.Repeat("b", 64)
)

func TestLoadDirReadsTheSixSetupSessions(t *testing.T) {
	recs, err := LoadDir("recordings")
	if err != nil {
		t.Fatal(err)
	}
	// The other sources' sessions (U4d, U7d, U9d) are in the directory too;
	// TestLoadDirReadsEverySetupSession checks them.
	if len(recs) < len(sixScenarios) {
		t.Fatalf("recordings = %d, want at least %d", len(recs), len(sixScenarios))
	}
	for _, name := range sixScenarios {
		rec, ok := recs[name]
		if !ok {
			t.Errorf("%s: no recording", name)
			continue
		}
		if rec.SourceKind() != "postgres" {
			t.Errorf("%s: source = %q, want postgres", name, rec.SourceKind())
		}
		if rec.Target != "compose" {
			t.Errorf("%s: target = %q, want compose", name, rec.Target)
		}
		if len(rec.Ops) == 0 {
			t.Errorf("%s: no ops", name)
		}
		if rec.Ladder == nil || len(rec.Ladder.Reads) == 0 {
			t.Errorf("%s: no ladder reads", name)
			continue
		}
		rungs := rec.Ladder.Verified.Rungs
		if len(rungs) != 8 {
			t.Errorf("%s: %d rungs, want 8", name, len(rungs))
		}
		for _, r := range rungs[:7] {
			if !r.OK {
				t.Errorf("%s: rung %s is not green: %s", name, r.Name, r.Note)
			}
		}
		if rungs[7].OK {
			t.Errorf("%s: the person's change is green in a replay with no person", name)
		}
	}
}

// TestLoadDirReadsEverySetupSession: every recording, whatever its source,
// names a setup source and target kind, records ops, and ends with the
// eight-rung verification of plan.Ladder in which rung 8, the person's own
// change, is still awaited (no person acts in a replay). A rung before it may
// be red: a break's re-verification records where it failed, and the replay
// test recomputes every rung from the recorded reads.
func TestLoadDirReadsEverySetupSession(t *testing.T) {
	recs, err := LoadDir("recordings")
	if err != nil {
		t.Fatal(err)
	}
	for name, rec := range recs {
		if !ValidSourceKind(rec.SourceKind()) || !ValidTargetKind(rec.Target) {
			t.Errorf("%s: source %q, target %q: want setup kinds", name, rec.SourceKind(), rec.Target)
		}
		if len(rec.Ops) == 0 {
			t.Errorf("%s: no ops", name)
		}
		// A target plan.Ladder defines no ladder for (the Debezium Server,
		// U11c) records none: there is nothing it could recompute.
		if PlanLadderUndefined(*rec) != "" {
			if rec.Ladder != nil {
				t.Errorf("%s: target %s has no plan.Ladder, but the session records a ladder", name, rec.Target)
			}
			continue
		}
		if rec.Ladder == nil || len(rec.Ladder.Reads) == 0 || rec.Ladder.Verified == nil {
			t.Errorf("%s: no ladder reads or verification", name)
			continue
		}
		rungs := rec.Ladder.Verified.Rungs
		if len(rungs) != 8 {
			t.Errorf("%s: %d rungs, want 8", name, len(rungs))
			continue
		}
		if rungs[7].OK {
			t.Errorf("%s: the person's change is green in a replay with no person", name)
		}
	}
}

func TestLoadRejectsBrokenRecordings(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{"unknown field", `{"scenario":"x","ops":[{"op":"env_detect","result":{"ok":true,"summary":"s","receipt":{"out_mac":"m"}}}],"bogus":1}`, "unknown field"},
		{"no scenario", `{"ops":[{"op":"env_detect","result":{"ok":true,"summary":"s","receipt":{"out_mac":"m"}}}]}`, "scenario is required"},
		{"no ops", `{"scenario":"x","ops":[]}`, "ops is empty"},
		{"both outcomes", `{"scenario":"x","ops":[{"op":"env_detect","result":{"ok":true,"summary":"s","receipt":{"out_mac":"m"}},"refusal":{"reason":"guard"}}]}`, "never both"},
		{"no outcome", `{"scenario":"x","ops":[{"op":"env_detect"}]}`, "records a result or a refusal"},
		{"result id", `{"scenario":"x","ops":[{"op":"env_detect","result":{"id":"op_1","ok":true,"summary":"s","receipt":{"out_mac":"m"}}}]}`, "result.id is filled by the replay"},
		{"no summary", `{"scenario":"x","ops":[{"op":"env_detect","result":{"ok":true,"receipt":{"out_mac":"m"}}}]}`, "result.summary is required"},
		{"ok without receipt", `{"scenario":"x","ops":[{"op":"env_detect","result":{"ok":true,"summary":"s"}}]}`, "receipt.out_mac"},
		{"bad reason", `{"scenario":"x","ops":[{"op":"env_detect","refusal":{"reason":"nope"}}]}`, "not a protocol.Refuse"},
		{"bad source", `{"scenario":"x","source":"sqlite","ops":[{"op":"env_detect","refusal":{"reason":"guard"}}]}`, "not a setup source kind"},
		{"bad target", `{"scenario":"x","target":"laptop","ops":[{"op":"env_detect","refusal":{"reason":"guard"}}]}`, "not a setup target kind"},
		{"bad tier", `{"scenario":"x","ops":[{"op":"env_detect","tier":"T9","refusal":{"reason":"guard"}}]}`, "not a protocol tier"},
		{"short out_mac", `{"scenario":"x","ops":[{"op":"env_detect","result":{"ok":true,"summary":"s","receipt":{"out_mac":"abc"}}}]}`, "out_mac is not a 64-character lowercase hex digest"},
		{"uppercase out_mac", `{"scenario":"x","ops":[{"op":"env_detect","result":{"ok":true,"summary":"s","receipt":{"out_mac":"` + strings.ToUpper(digestA) + `"}}}]}`, "out_mac is not a 64-character lowercase hex digest"},
		{"bad params_hash", `{"scenario":"x","ops":[{"op":"compose_up","tier":"T1","params_hash":"h1","refusal":{"reason":"guard"}}]}`, "params_hash is not a 64-character lowercase hex digest"},
		{"bad nonce_hash", `{"scenario":"x","ops":[{"op":"env_detect","refusal":{"reason":"guard"}}],"ladder":{"plan_id":"pl_1","nonce_hash":"abc","verified":{"rungs":[{"name":"r","ok":true}]}}}`, "nonce_hash is not a 64-character lowercase hex digest"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]byte(tt.raw), "fixture.json")
			if err == nil {
				t.Fatal("Parse accepted a broken recording")
			}
			if !errors.Is(err, ErrRecording) || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("err = %v, want ErrRecording with %q", err, tt.want)
			}
		})
	}
}

func TestLoadMissingFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "absent.json"))
	if err == nil || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("err = %v, want a not-exist error", err)
	}
}

// fixture is a small session that exercises results, a mismatch and a
// refusal.
func fixture(t *testing.T) Recording {
	t.Helper()
	raw := `{
	  "scenario": "setup/fixture",
	  "ops": [
	    {"op": "env_detect", "tier": "T0", "params": {},
	     "result": {"ok": true, "summary": "docker present", "receipt": {"kind": "exec", "target": "env", "out_mac": "` + digestA + `"}}},
	    {"op": "compose_write", "tier": "T2", "params": {"templates": ["postgres"]},
	     "refusal": {"reason": "guard"}}
	  ],
	  "ladder": {"plan_id": "pl_1", "verified": {"rungs": [{"name": "connector_running", "ok": true}]}}
	}`
	rec, err := Parse([]byte(raw), "fixture.json")
	if err != nil {
		t.Fatal(err)
	}
	return rec
}

func TestSessionServesRecordedOps(t *testing.T) {
	s := NewSession(fixture(t))
	ctx := context.Background()

	res, err := s.Execute(ctx, "r_1", protocol.OpRequest{ID: "op_1", Op: "env_detect"})
	if err != nil {
		t.Fatal(err)
	}
	if !res.OK || res.Summary != "docker present" || res.ID != "op_1" || res.Receipt.OutMAC != digestA {
		t.Errorf("result = %+v", res)
	}
	if s.Remaining() != 1 {
		t.Errorf("remaining = %d, want 1", s.Remaining())
	}

	_, err = s.Execute(ctx, "r_1", protocol.OpRequest{
		ID: "op_2", Op: "compose_write", Tier: protocol.TierT2,
		Params: json.RawMessage(`{"templates":["postgres"]}`),
	})
	var refused *protocol.RefusedError
	if !errors.As(err, &refused) || refused.Refusal.Reason != protocol.RefuseGuard || refused.Refusal.ID != "op_2" {
		t.Fatalf("refusal = %v", err)
	}
	if s.Remaining() != 0 {
		t.Errorf("remaining = %d, want 0", s.Remaining())
	}
	if got := s.Unexecuted(); len(got) != 0 {
		t.Errorf("unexecuted = %v", got)
	}
}

func TestSessionMissingRecording(t *testing.T) {
	s := NewSession(fixture(t))
	ctx := context.Background()
	// The params name another template: the recorded write does not answer it.
	_, err := s.Execute(ctx, "r_1", protocol.OpRequest{
		Op: "compose_write", Params: json.RawMessage(`{"templates":["kafka"]}`),
	})
	if !errors.Is(err, ErrNoRecording) || !strings.Contains(err.Error(), "params differ") {
		t.Fatalf("err = %v, want ErrNoRecording with the params reason", err)
	}
	// An op the session never recorded.
	_, err = s.Execute(ctx, "r_1", protocol.OpRequest{Op: "compose_up", Params: json.RawMessage(`{}`)})
	if !errors.Is(err, ErrNoRecording) || !strings.Contains(err.Error(), "recorded no compose_up") {
		t.Fatalf("err = %v, want ErrNoRecording naming the op", err)
	}
	if got := s.Misses(); len(got) != 2 || got[0].Op != "compose_write" || got[1].Op != "compose_up" {
		t.Errorf("misses = %+v", got)
	}
	// A second call for an op already served is a miss too, as a real runner
	// answers it once.
	if _, err := s.Execute(ctx, "r_1", protocol.OpRequest{Op: "env_detect"}); err != nil {
		t.Fatalf("first env_detect err = %v", err)
	}
	if _, err := s.Execute(ctx, "r_1", protocol.OpRequest{Op: "env_detect"}); !errors.Is(err, ErrNoRecording) {
		t.Fatalf("second env_detect err = %v, want ErrNoRecording", err)
	}
}

func TestSessionHonoursContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewSession(fixture(t)).Execute(ctx, "r_1", protocol.OpRequest{Op: "env_detect"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context.Canceled", err)
	}
}

// TestSessionRefusesRequestsARunnerWouldRefuse: a recorded success answers
// only a request with the recorded semantics. A lower tier, an extra key, a
// longer array or params against a {} recording is a miss. The proposal ID
// and params hash are not part of that: they are per-run values, see
// TestSessionMatchesLiveShapedRequest.
func TestSessionRefusesRequestsARunnerWouldRefuse(t *testing.T) {
	raw := `{
	  "scenario": "setup/strict",
	  "ops": [
	    {"op": "env_detect", "tier": "T0", "params": {},
	     "result": {"ok": true, "summary": "docker present", "receipt": {"out_mac": "` + digestA + `"}}},
	    {"op": "compose_write", "tier": "T2", "proposal_id": "pr_1", "params_hash": "` + digestB + `",
	     "params": {"templates": ["postgres"], "dir": "/srv"},
	     "result": {"ok": true, "summary": "written", "receipt": {"out_mac": "` + digestA + `"}}}
	  ]
	}`
	rec, err := Parse([]byte(raw), "strict.json")
	if err != nil {
		t.Fatal(err)
	}
	good := protocol.OpRequest{
		Op: "compose_write", Tier: protocol.TierT2, ProposalID: "pr_1", ParamsHash: digestB,
		Params: json.RawMessage(`{"dir":"/srv","templates":["postgres"]}`),
	}
	tests := []struct {
		name string
		req  func(r protocol.OpRequest) protocol.OpRequest
		want string
	}{
		{"lower tier", func(r protocol.OpRequest) protocol.OpRequest { r.Tier = protocol.TierT1; return r }, "below the recorded"},
		{"no tier", func(r protocol.OpRequest) protocol.OpRequest { r.Tier = ""; return r }, "below the recorded"},
		{"extra key", func(r protocol.OpRequest) protocol.OpRequest {
			r.Params = json.RawMessage(`{"dir":"/srv","templates":["postgres"],"privileged":true}`)
			return r
		}, "params differ"},
		{"longer array", func(r protocol.OpRequest) protocol.OpRequest {
			r.Params = json.RawMessage(`{"dir":"/srv","templates":["postgres","kafka"]}`)
			return r
		}, "params differ"},
		{"{} recording with params", func(protocol.OpRequest) protocol.OpRequest {
			return protocol.OpRequest{Op: "env_detect", Tier: protocol.TierT0, Params: json.RawMessage(`{"path":"/"}`)}
		}, "params differ"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewSession(rec)
			_, err := s.Execute(context.Background(), "r_1", tt.req(good))
			if !errors.Is(err, ErrNoRecording) || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want ErrNoRecording with %q", err, tt.want)
			}
			if s.Remaining() != 2 {
				t.Errorf("remaining = %d, want 2: a miss uses no op", s.Remaining())
			}
		})
	}

	// The recorded request itself, and a higher tier, still answer.
	s := NewSession(rec)
	if res, err := s.Execute(context.Background(), "r_1", good); err != nil || !res.OK {
		t.Fatalf("recorded request: res %+v err %v", res, err)
	}
	if _, err := s.Execute(context.Background(), "r_1", protocol.OpRequest{Op: "env_detect", Tier: protocol.TierT1, Params: json.RawMessage(`null`)}); err != nil {
		t.Fatalf("higher tier, null params: %v", err)
	}
}

// TestSessionMatchesLiveShapedRequest is the U1-20 follow-up: a live request
// for a proposal op carries a proposal ID and params hash the recording never
// held (the room mints a ULID per proposal and hashes it with the room), and
// it must still be answered. The op, params, tier and step index are the
// semantics; the ids are per-run.
func TestSessionMatchesLiveShapedRequest(t *testing.T) {
	raw := `{
	  "scenario": "setup/live",
	  "ops": [
	    {"op": "source_grant_select", "tier": "T2", "proposal_id": "p_recorded",
	     "step_index": 1, "params_hash": "` + digestA + `",
	     "params": {"connector": "pg-orders-connector", "role": "devctl_setup_grant", "tables": ["public.orders"]},
	     "result": {"ok": true, "summary": "granted", "receipt": {"kind": "sql", "target": "orders-db", "out_mac": "` + digestB + `"}}}
	  ]
	}`
	rec, err := Parse([]byte(raw), "live.json")
	if err != nil {
		t.Fatal(err)
	}
	step := 1
	req := protocol.OpRequest{
		ID: "op_live_1", Op: "source_grant_select", Tier: protocol.TierT2,
		ProposalID: "p_01JAVERYLIVEPROPOSALID000", ParamsHash: digestB,
		StepIndex: &step,
		Params:    json.RawMessage(`{"connector":"pg-orders-connector","role":"devctl_setup_grant","tables":["public.orders"]}`),
	}
	s := NewSession(rec)
	res, err := s.Execute(context.Background(), "r_live", req)
	if err != nil {
		t.Fatalf("a live-shaped request did not match: %v", err)
	}
	if !res.OK || res.ID != req.ID || res.Receipt.OutMAC != digestB {
		t.Errorf("result = %+v, want the recorded success under the live request's id", res)
	}
	if s.Remaining() != 0 {
		t.Errorf("remaining = %d, want 0", s.Remaining())
	}
}

// TestSessionServesOutOfOrderRequests pins the documented matching rule: the
// recording's order breaks ties between identical ops, it does not constrain
// the caller's order. A model that reads the stack in another order still
// gets the same answers.
func TestSessionServesOutOfOrderRequests(t *testing.T) {
	rec := fixture(t)
	s := NewSession(rec)
	ctx := context.Background()
	// Ask for the recording's second op first, with the tier a live run sends.
	_, err := s.Execute(ctx, "r_1", protocol.OpRequest{Op: "compose_write", Tier: protocol.TierT2, Params: json.RawMessage(`{"templates":["postgres"]}`)})
	var refused *protocol.RefusedError
	if !errors.As(err, &refused) {
		t.Fatalf("out-of-order compose_write: err = %v, want the recorded refusal", err)
	}
	if res, err := s.Execute(ctx, "r_1", protocol.OpRequest{Op: "env_detect"}); err != nil || !res.OK {
		t.Fatalf("out-of-order env_detect: res %+v err %v", res, err)
	}
}

// TestSessionServesIdenticalOpsInRecordedOrder: when two recorded ops have the
// same name and params, one request takes the first and the next takes the
// second, in recording order.
func TestSessionServesIdenticalOpsInRecordedOrder(t *testing.T) {
	raw := `{
	  "scenario": "setup/repeated",
	  "ops": [
	    {"op": "net_probe", "tier": "T0", "params": {"from": "connect", "target": "postgres"},
	     "result": {"ok": false, "summary": "timeout", "receipt": {"kind": "exec", "target": "connect", "out_mac": "` + digestA + `"}}},
	    {"op": "net_probe", "tier": "T0", "params": {"from": "connect", "target": "postgres"},
	     "result": {"ok": true, "summary": "ok", "receipt": {"kind": "exec", "target": "connect", "out_mac": "` + digestB + `"}}}
	  ],
	  "ladder": {"plan_id": "pl_1", "verified": {"rungs": [{"name": "r", "ok": true}]}}
	}`
	rec, err := Parse([]byte(raw), "repeated.json")
	if err != nil {
		t.Fatal(err)
	}
	s := NewSession(rec)
	req := protocol.OpRequest{Op: "net_probe", Tier: protocol.TierT0, Params: json.RawMessage(`{"from":"connect","target":"postgres"}`)}
	first, err := s.Execute(context.Background(), "r_1", req)
	if err != nil || first.OK || first.Receipt.OutMAC != digestA {
		t.Fatalf("first: res %+v err %v, want the recorded failure", first, err)
	}
	second, err := s.Execute(context.Background(), "r_1", req)
	if err != nil || !second.OK || second.Receipt.OutMAC != digestB {
		t.Fatalf("second: res %+v err %v, want the recorded success", second, err)
	}
}

// TestVerifyReplaysTheSixRecordings is the recording-level half of the U1-20
// scoring check: every recording answers live-shaped requests and its ladder
// recomputes the recorded verification.
func TestVerifyReplaysTheSixRecordings(t *testing.T) {
	recs, err := LoadDir("recordings")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range sixScenarios {
		t.Run(name, func(t *testing.T) {
			rec, ok := recs[name]
			if !ok {
				t.Fatalf("no recording")
			}
			c := Verify(context.Background(), rec)
			if !c.OK() {
				t.Fatalf("Verify failed: %v", c.Failures)
			}
			if c.Ops != len(rec.Ops) || c.Rungs != len(rec.Ladder.Verified.Rungs) {
				t.Errorf("ops/rungs = %d/%d, want %d/%d", c.Ops, c.Rungs, len(rec.Ops), len(rec.Ladder.Verified.Rungs))
			}
		})
	}
}

// TestVerifyFailsABrokenRecording: a ladder that does not reach the recorded
// verdict, a missing verdict and a missing ladder are failures, never a pass.
func TestVerifyFailsABrokenRecording(t *testing.T) {
	recs, err := LoadDir("recordings")
	if err != nil {
		t.Fatal(err)
	}
	t.Run("rung differs", func(t *testing.T) {
		rec := *recs["setup/flat-plugin-dir"]
		ladder := *rec.Ladder
		verified := *ladder.Verified
		verified.Rungs = append([]Rung(nil), verified.Rungs...)
		verified.Rungs[0].OK = false // the reads still say RUNNING
		ladder.Verified = &verified
		rec.Ladder = &ladder
		c := Verify(context.Background(), &rec)
		if c.OK() || !strings.Contains(strings.Join(c.Failures, ";"), "rung 0") {
			t.Fatalf("Check = %+v, want a rung failure", c)
		}
	})
	t.Run("no verdict", func(t *testing.T) {
		rec := *recs["setup/missing-grant"]
		ladder := *rec.Ladder
		ladder.Verified = nil
		rec.Ladder = &ladder
		c := Verify(context.Background(), &rec)
		if c.OK() || !strings.Contains(strings.Join(c.Failures, ";"), "no setup.verified verdict") {
			t.Fatalf("Check = %+v, want a missing-verdict failure", c)
		}
	})
	t.Run("no ladder", func(t *testing.T) {
		rec := *recs["setup/wal-level-replica"]
		rec.Ladder = nil
		c := Verify(context.Background(), &rec)
		if c.OK() || !strings.Contains(strings.Join(c.Failures, ";"), "records no ladder") {
			t.Fatalf("Check = %+v, want a missing-ladder failure", c)
		}
	})
	t.Run("missing read", func(t *testing.T) {
		rec := *recs["setup/missing-grant"]
		ladder := *rec.Ladder
		ladder.Reads = nil // the rungs have nothing to read
		ladder.Verified = recs["setup/missing-grant"].Ladder.Verified
		rec.Ladder = &ladder
		c := Verify(context.Background(), &rec)
		if c.OK() {
			t.Fatalf("Check = %+v, want a ladder failure with no reads", c)
		}
	})
}

// TestVerifyTargetWithoutPlanLadder: a session on a target plan.Ladder
// defines no ladder for replays its ops and reports the missing ladder as
// NoLadder, never as a pass of rungs nobody defined; the same session with a
// ladder fails, and a Connect target's session without one still fails.
func TestVerifyTargetWithoutPlanLadder(t *testing.T) {
	op := Op{Op: "server_status", Tier: protocol.TierT0, Result: &protocol.OpResult{OK: true, Summary: "dbzserver: running",
		Receipt: events.SetupReceipt{Kind: "exec", OutMAC: digestA}}}
	dbz := Recording{Scenario: "setup/dbz", Source: "postgres", Target: "dbzserver", Ops: []Op{op}}
	if why := PlanLadderUndefined(dbz); !strings.Contains(why, "dbzserver") {
		t.Fatalf("PlanLadderUndefined(dbzserver) = %q, want a reason naming the target", why)
	}
	c := Verify(context.Background(), &dbz)
	if !c.OK() || c.Ops != 1 || c.Rungs != 0 || c.NoLadder == "" {
		t.Fatalf("Check = %+v, want the op replayed and NoLadder set", c)
	}

	withLadder := dbz
	withLadder.Ladder = &Ladder{PlanID: "pl_1", Verified: &Verified{Rungs: []Rung{{Name: "connector_running", OK: true}}}}
	if c := Verify(context.Background(), &withLadder); c.OK() || !strings.Contains(strings.Join(c.Failures, ";"), "records a ladder") {
		t.Fatalf("Check = %+v, want a failure: no plan ladder to recompute", c)
	}

	compose := dbz
	compose.Target = "compose"
	if why := PlanLadderUndefined(compose); why != "" {
		t.Fatalf("PlanLadderUndefined(compose) = %q, want none", why)
	}
	if c := Verify(context.Background(), &compose); c.OK() || !strings.Contains(strings.Join(c.Failures, ";"), "records no ladder") {
		t.Fatalf("Check = %+v, want the missing-ladder failure", c)
	}

	// An unknown sink is not excused by the target.
	badSink := dbz
	badSink.Sink = "Not-A-Kind"
	if why := PlanLadderUndefined(badSink); why != "" {
		t.Fatalf("PlanLadderUndefined(bad sink) = %q, want none", why)
	}
}

// TestVerifyNilRecording is the defensive path: no recording is a failure, not
// a panic and not a pass.
func TestVerifyNilRecording(t *testing.T) {
	if c := Verify(context.Background(), nil); c.OK() || len(c.Failures) != 1 {
		t.Fatalf("Check = %+v, want one failure", c)
	}
}
