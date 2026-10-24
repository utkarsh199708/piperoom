package setup

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

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

func TestLoadDirReadsTheSixSetupSessions(t *testing.T) {
	recs, err := LoadDir("recordings")
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != len(sixScenarios) {
		t.Fatalf("recordings = %d, want %d", len(recs), len(sixScenarios))
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
	const raw = `{
	  "scenario": "setup/fixture",
	  "ops": [
	    {"op": "env_detect", "tier": "T0", "params": {},
	     "result": {"ok": true, "summary": "docker present", "receipt": {"kind": "exec", "target": "env", "out_mac": "aa"}}},
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
	if !res.OK || res.Summary != "docker present" || res.ID != "op_1" || res.Receipt.OutMAC != "aa" {
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
// only the request it records. A lower tier, another proposal or params hash,
// an extra key, a longer array or params against a {} recording is a miss.
func TestSessionRefusesRequestsARunnerWouldRefuse(t *testing.T) {
	const raw = `{
	  "scenario": "setup/strict",
	  "ops": [
	    {"op": "env_detect", "tier": "T0", "params": {},
	     "result": {"ok": true, "summary": "docker present", "receipt": {"out_mac": "aa"}}},
	    {"op": "compose_write", "tier": "T2", "proposal_id": "pr_1", "params_hash": "h1",
	     "params": {"templates": ["postgres"], "dir": "/srv"},
	     "result": {"ok": true, "summary": "written", "receipt": {"out_mac": "bb"}}}
	  ]
	}`
	rec, err := Parse([]byte(raw), "strict.json")
	if err != nil {
		t.Fatal(err)
	}
	good := protocol.OpRequest{
		Op: "compose_write", Tier: protocol.TierT2, ProposalID: "pr_1", ParamsHash: "h1",
		Params: json.RawMessage(`{"dir":"/srv","templates":["postgres"]}`),
	}
	tests := []struct {
		name string
		req  func(r protocol.OpRequest) protocol.OpRequest
		want string
	}{
		{"lower tier", func(r protocol.OpRequest) protocol.OpRequest { r.Tier = protocol.TierT1; return r }, "below the recorded"},
		{"no tier", func(r protocol.OpRequest) protocol.OpRequest { r.Tier = ""; return r }, "below the recorded"},
		{"wrong params hash", func(r protocol.OpRequest) protocol.OpRequest { r.ParamsHash = "h2"; return r }, "params_hash differs"},
		{"wrong proposal", func(r protocol.OpRequest) protocol.OpRequest { r.ProposalID = "pr_2"; return r }, "proposal_id differs"},
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
