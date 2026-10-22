package actions

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/events"
)

// classAction is the smallest Action; class, when set, makes it Classed.
type classAction struct{ name string }

func (a classAction) Name() string                                  { return a.name }
func (classAction) Layer() string                                   { return "postgres" }
func (classAction) Describe() string                                { return "test action" }
func (classAction) ParamsSchema() map[string]any                    { return map[string]any{"type": "object"} }
func (classAction) Validate(context.Context, json.RawMessage) error { return nil }
func (classAction) Execute(context.Context, json.RawMessage) (string, error) {
	return "", errors.New("never run in this test")
}

type classedAction struct {
	classAction
	class string
}

func (a classedAction) Class() string { return a.class }

// verifiedRecovery is the shape X2a and X2b implement: Classed recovery plus
// Verifier, and it uses only the X2-0 names (E5).
type verifiedRecovery struct {
	classedAction
	after int64
}

func (a verifiedRecovery) Verify(ctx context.Context, _ json.RawMessage, executedAt time.Time) (VerifyResult, error) {
	if err := ctx.Err(); err != nil {
		return VerifyResult{}, err
	}
	if executedAt.IsZero() {
		return VerifyResult{}, errors.New("no executedAt")
	}
	return VerifyResult{Before: 412, After: a.after, Unit: events.RecoveryUnitRows}, nil
}

var (
	_ Classed  = verifiedRecovery{}
	_ Verifier = verifiedRecovery{}
)

// TC1, TC2: a recovery action without Verify is refused; one with it, and
// every action that does not say its class, registers as before.
func TestRegisterRecoveryClass(t *testing.T) {
	tests := []struct {
		name    string
		a       Action
		class   string
		wantErr string
	}{
		{"plain action is a fix", classAction{name: "connect_restart_task"}, ClassFix, ""},
		{"empty class reads as fix", classedAction{classAction{name: "a_empty"}, ""}, ClassFix, ""},
		{"explicit fix", classedAction{classAction{name: "a_fix"}, ClassFix}, ClassFix, ""},
		{"recovery with Verify", verifiedRecovery{classedAction: classedAction{classAction{name: "pg_resnapshot_rows"}, ClassRecovery}}, ClassRecovery, ""},
		{"recovery without Verify", classedAction{classAction{name: "kafka_replay_range"}, ClassRecovery}, ClassRecovery, "does not implement Verifier"},
		{"unknown class", classedAction{classAction{name: "a_odd"}, "Recovery"}, "Recovery", "unknown class"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClassOf(tt.a); got != tt.class {
				t.Errorf("ClassOf = %q, want %q", got, tt.class)
			}
			r := NewRegistry()
			err := r.Register(tt.a)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Register: %v", err)
				}
				if _, ok := r.Get(tt.a.Name()); !ok {
					t.Fatal("registered action not found")
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Register err = %v, want %q", err, tt.wantErr)
			}
			if _, ok := r.Get(tt.a.Name()); ok || len(r.All()) != 0 {
				t.Fatal("a refused action is in the registry")
			}
		})
	}
}

func TestRecoveryConstantsMatchEvents(t *testing.T) {
	if ClassFix != "fix" || ClassRecovery != "recovery" {
		t.Fatalf("class values changed: %q %q", ClassFix, ClassRecovery)
	}
	if RecoveryVerifyFirstPoll > RecoveryVerifyMaxPoll || RecoveryVerifyMaxPoll > RecoveryVerifyTimeout {
		t.Fatal("poll schedule out of order")
	}
}

func TestVerifierShape(t *testing.T) {
	var v Verifier = verifiedRecovery{after: 0}
	got, err := v.Verify(context.Background(), json.RawMessage(`{}`), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if out := events.RecoveryOutcomeFor(got.Before, got.After); out != events.RecoveryRecovered {
		t.Fatalf("outcome %q", out)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := v.Verify(ctx, nil, time.Now()); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled ctx: %v", err)
	}
}

// E5: the X2a/X2b writers can be faked against the client shapes without an
// adapter, and the existing SourceWriter and SnapshotSignaler shapes do not
// require the new methods (E2).
type fakeConditionSource struct{ got string }

func (f *fakeConditionSource) InsertSnapshotSignalWhere(_ context.Context, _, _ string, _ []string, cond string) (string, error) {
	if err := clients.ValidateSnapshotCondition(cond); err != nil {
		return "", err
	}
	f.got = cond
	return "sig-1", nil
}

type fakeConditionKafka struct{}

func (fakeConditionKafka) SendSnapshotSignalWhere(_ context.Context, topic, _, _ string, _ []string, cond string) (clients.SnapshotSignal, error) {
	if err := clients.ValidateSnapshotCondition(cond); err != nil {
		return clients.SnapshotSignal{}, err
	}
	return clients.SnapshotSignal{ID: "7", Topic: topic}, nil
}

var (
	_ clients.SnapshotConditionWriter   = (*fakeConditionSource)(nil)
	_ clients.SnapshotConditionSignaler = fakeConditionKafka{}
)

func TestConditionWritersRefuseBadConditionBeforeIO(t *testing.T) {
	var w clients.SnapshotConditionWriter = &fakeConditionSource{}
	if _, err := w.InsertSnapshotSignalWhere(context.Background(), "public.sig", "c", []string{"public.t"}, `"id" >= 1; drop table t`); !errors.Is(err, clients.ErrBadSnapshotCondition) {
		t.Fatalf("err = %v", err)
	}
	var k clients.SnapshotConditionSignaler = fakeConditionKafka{}
	if _, err := k.SendSnapshotSignalWhere(context.Background(), "sig", "c", "p", []string{"public.t"}, ""); !errors.Is(err, clients.ErrBadSnapshotCondition) {
		t.Fatalf("empty condition err = %v", err)
	}
}
