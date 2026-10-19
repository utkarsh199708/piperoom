package protocol_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
)

// The wire shapes of design §2.2: field names and tags as the runner and the
// server will both read them.
func TestFrameShapes(t *testing.T) {
	deadline := time.Date(2026, 10, 19, 10, 0, 0, 0, time.UTC)
	f := protocol.Frame{Type: protocol.FrameOp, Op: &protocol.OpRequest{
		ID: "op_1", Op: "compose_up", Params: json.RawMessage(`{"services":["connect"]}`), Tier: protocol.TierT1,
		Class: protocol.ClassLocal, Deadline: deadline, ProposalID: "p_1", StepIndex: 2, ParamsHash: "ab",
	}}
	raw, err := json.Marshal(f)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"type":"op","op":{"id":"op_1","op":"compose_up","params":{"services":["connect"]},"tier":"T1","class":"local","deadline":"2026-10-19T10:00:00Z","proposal_id":"p_1","step_index":2,"params_hash":"ab"}}`
	if string(raw) != want {
		t.Errorf("op frame = %s\nwant       %s", raw, want)
	}

	var in protocol.Frame
	if err := json.Unmarshal([]byte(`{"type":"result","result":{"id":"op_1","ok":true,"summary":"up","receipt":{"kind":"exec","argv":["docker","compose","up","-d"],"target":"/p","exit_code":0,"at":"2026-10-19T10:00:01Z","took_ms":900,"out_mac":"ff"},"truncated":true}}`), &in); err != nil {
		t.Fatal(err)
	}
	if in.Type != protocol.FrameResult || in.Result == nil || !in.Result.OK || in.Result.Receipt.Kind != "exec" || !in.Result.Truncated || in.Op != nil {
		t.Errorf("result frame decoded as %+v", in)
	}

	raw, err = json.Marshal(protocol.Frame{Type: protocol.FrameResume, Resume: &protocol.Resume{InFlight: []protocol.OpStatus{{ID: "op_1", ProposalID: "p_1", StepIndex: 2, State: protocol.StateRunning}}}})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"type":"resume","resume":{"in_flight":[{"id":"op_1","proposal_id":"p_1","step_index":2,"state":"running"}]}}`; string(raw) != want {
		t.Errorf("resume frame = %s", raw)
	}

	raw, err = json.Marshal(protocol.Frame{Type: protocol.FrameCredentials, Credentials: &protocol.Credentials{PipelineID: "orders", Secrets: []protocol.Secret{{Name: "pg_ro", Value: "s3cret-value"}}}})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"type":"credentials","credentials":{"pipeline_id":"orders","secrets":[{"name":"pg_ro","value":"s3cret-value"}]}}`; string(raw) != want {
		t.Errorf("credentials frame = %s", raw)
	}
	raw, err = json.Marshal(protocol.Frame{Type: protocol.FrameSecret, Secret: &protocol.SecretValue{Secret: protocol.Secret{Name: "pg_pw", Value: "typed-in-ui"}}})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"type":"secret","secret":{"name":"pg_pw","value":"typed-in-ui"}}`; string(raw) != want {
		t.Errorf("secret frame = %s", raw)
	}
	raw, err = json.Marshal(protocol.Frame{Type: protocol.FrameRegisterCredentials, RegisterCredentials: &protocol.RegisterCredentials{PipelineID: "orders", Names: []string{"pg_ro"}}})
	if err != nil {
		t.Fatal(err)
	}
	if want := `{"type":"register_credentials","register_credentials":{"pipeline_id":"orders","names":["pg_ro"]}}`; string(raw) != want {
		t.Errorf("register_credentials frame = %s", raw)
	}
	raw, err = json.Marshal(protocol.Frame{Type: protocol.FramePing})
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != `{"type":"ping"}` {
		t.Errorf("ping frame = %s", raw)
	}
}

// The pairing code alphabet has 32 symbols with no 0/O or 1/I.
func TestCodeAlphabet(t *testing.T) {
	if len(protocol.CodeAlphabet) != 32 || protocol.CodeLength != 8 {
		t.Fatalf("alphabet %q (%d), length %d", protocol.CodeAlphabet, len(protocol.CodeAlphabet), protocol.CodeLength)
	}
	for _, r := range "0O1Il" {
		if strings.ContainsRune(protocol.CodeAlphabet, r) {
			t.Errorf("alphabet holds %q", r)
		}
	}
	seen := map[rune]bool{}
	for _, r := range protocol.CodeAlphabet {
		if seen[r] {
			t.Errorf("alphabet repeats %q", r)
		}
		seen[r] = true
	}
}

// A refusal is matched with errors.Is on the two sentinels an executor must
// never retry, and on nothing else.
func TestRefusedErrorIs(t *testing.T) {
	hash := &protocol.RefusedError{Refusal: protocol.Refusal{Op: "compose_up", Reason: protocol.RefuseParamsHash}}
	dup := &protocol.RefusedError{Refusal: protocol.Refusal{Op: "compose_up", Reason: protocol.RefuseDuplicateStep}}
	guard := &protocol.RefusedError{Refusal: protocol.Refusal{Op: "compose_down", Reason: protocol.RefuseGuard}}
	if !errors.Is(hash, protocol.ErrParamsHash) || errors.Is(hash, protocol.ErrDuplicateStep) {
		t.Error("params_hash refusal")
	}
	if !errors.Is(dup, protocol.ErrDuplicateStep) || errors.Is(dup, protocol.ErrParamsHash) {
		t.Error("duplicate_step refusal")
	}
	if errors.Is(guard, protocol.ErrParamsHash) || errors.Is(guard, protocol.ErrDuplicateStep) || errors.Is(guard, protocol.ErrNoRunner) {
		t.Error("guard refusal matched a sentinel")
	}
	var re *protocol.RefusedError
	wrapped := errors.Join(errors.New("executor: step 2"), guard)
	if !errors.As(wrapped, &re) || re.Refusal.Reason != protocol.RefuseGuard {
		t.Error("errors.As through a wrap")
	}
	if got := guard.Error(); got != "setup: runner refused compose_down: guard" {
		t.Errorf("Error() = %q", got)
	}
}

// A fake executor compiles against the interface and the sentinel is usable
// by a caller that has no runner.
type noRunner struct{}

func (noRunner) Execute(context.Context, string, protocol.OpRequest) (protocol.OpResult, error) {
	return protocol.OpResult{}, protocol.ErrNoRunner
}

func TestRunnerExecutorInterface(t *testing.T) {
	var ex protocol.RunnerExecutor = noRunner{}
	_, err := ex.Execute(context.Background(), "r_1", protocol.OpRequest{Op: "env_detect", Tier: protocol.TierT0})
	if !errors.Is(err, protocol.ErrNoRunner) {
		t.Fatalf("err = %v", err)
	}
	// The receipt type is the events one, so an executor stores it unchanged.
	var _ events.SetupReceipt = protocol.OpResult{}.Receipt
}
