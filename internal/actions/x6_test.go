package actions

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
)

// tuningAction is the shape connect_tune_throughput (X6c) implements, using
// only the X6-0 names (E5): Classed performance, Preview with the tuning
// list, Verify with throughput and lag slopes.
type tuningAction struct {
	classedAction
	before, after int64
}

func (a tuningAction) Preview(ctx context.Context, _ json.RawMessage) (PreviewResult, error) {
	if err := ctx.Err(); err != nil {
		return PreviewResult{}, err
	}
	changes := []TuningChange{{Key: "tasks.max", Current: "1", Proposed: "3"}}
	for _, c := range changes {
		if err := CheckTunable("sink", c.Key, c.Proposed); err != nil {
			return PreviewResult{}, err
		}
	}
	return PreviewResult{Target: "connector orders-sink", Current: "tasks.max 1", Expected: "tasks.max 3",
		Reversible: true, Tuning: changes}, nil
}

func (a tuningAction) Verify(ctx context.Context, _ json.RawMessage, executedAt time.Time) (VerifyResult, error) {
	if err := ctx.Err(); err != nil {
		return VerifyResult{}, err
	}
	if executedAt.IsZero() {
		return VerifyResult{}, errors.New("no executedAt")
	}
	lb, la := int64(600), int64(-120)
	return VerifyResult{Before: a.before, After: a.after, Unit: events.PerformanceUnitRecordsPerMinute,
		LagSlopeBefore: &lb, LagSlopeAfter: &la}, nil
}

// verifiedOnly has Verify but no Preview.
type verifiedOnly struct{ classedAction }

func (verifiedOnly) Verify(context.Context, json.RawMessage, time.Time) (VerifyResult, error) {
	return VerifyResult{}, nil
}

// previewOnly has Preview but no Verify.
type previewOnly struct{ classedAction }

func (previewOnly) Preview(context.Context, json.RawMessage) (PreviewResult, error) {
	return PreviewResult{}, nil
}

var (
	_ Classed  = tuningAction{}
	_ Verifier = tuningAction{}
	_ Preview  = tuningAction{}
)

// TC1: the registry refuses a performance action without Verify (and without
// Preview, whose tuning list is the rollback); with both it registers.
func TestRegisterPerformanceClass(t *testing.T) {
	perf := func(name string) classedAction { return classedAction{classAction{name: name}, ClassPerformance} }
	tests := []struct {
		name    string
		a       Action
		wantErr string
	}{
		{"performance with Verify and Preview", tuningAction{classedAction: perf("connect_tune_throughput")}, ""},
		{"performance without Verify", previewOnly{perf("tune_no_verify")}, "does not implement Verifier"},
		{"performance without Preview", verifiedOnly{perf("tune_no_preview")}, "does not implement Preview"},
		{"performance with neither", perf("tune_bare"), "does not implement Verifier"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ClassOf(tt.a); got != ClassPerformance {
				t.Fatalf("ClassOf = %q", got)
			}
			r := NewRegistry()
			err := r.Register(tt.a)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Register: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Register err = %v, want %q", err, tt.wantErr)
			}
			if len(r.All()) != 0 {
				t.Fatal("a refused action is in the registry")
			}
		})
	}
}

// TC2: the allow-list is pinned. Adding, removing or re-bounding a key is a
// contract change: update this test in the leads' PR that changes it.
func TestTunableKeysPinned(t *testing.T) {
	want := []TunableKey{
		{Key: "tasks.max", Min: 1, Max: 128},
		{Key: "max.batch.size", Connector: "source", Min: 1, Max: 20480},
		{Key: "max.queue.size", Connector: "source", Min: 2, Max: 163840},
		{Key: "poll.interval.ms", Connector: "source", Min: 10, Max: 60000},
		{Key: "snapshot.fetch.size", Connector: "source", Min: 1, Max: 100000},
		{Key: "snapshot.max.threads", Connector: "source", Min: 1, Max: 16},
		{Key: "consumer.override.max.poll.records", Connector: "sink", Min: 1, Max: 10000},
	}
	got := TunableKeys()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("allow-list changed:\n got %+v\nwant %+v", got, want)
	}
	// A fresh slice: a caller cannot change the allow-list.
	got[0].Max = 1 << 40
	if TunableKeys()[0].Max != 128 {
		t.Fatal("TunableKeys shares its backing array")
	}
	for _, k := range want {
		if LooksSecret(k.Key) {
			t.Errorf("allow-listed key %q looks secret", k.Key)
		}
		if k.Min < 1 || k.Min > k.Max {
			t.Errorf("%s bounds %d..%d", k.Key, k.Min, k.Max)
		}
	}
}

// E4: CheckTunable enforces the allow-list, the connector type and the static
// bounds at the boundary.
func TestCheckTunable(t *testing.T) {
	tests := []struct {
		name, typ, key, value string
		want                  error
	}{
		{"tasks.max on a sink", "sink", "tasks.max", "3", nil},
		{"tasks.max on a source", "source", "tasks.max", "1", nil},
		{"lower bound", "source", "max.batch.size", "1", nil},
		{"upper bound", "source", "max.batch.size", "20480", nil},
		{"above the bound", "source", "max.batch.size", "20481", ErrTuningBounds},
		{"zero", "sink", "tasks.max", "0", ErrTuningBounds},
		{"negative", "source", "snapshot.max.threads", "-1", ErrTuningBounds},
		{"not an integer", "source", "poll.interval.ms", "1s", ErrTuningBounds},
		{"empty value", "sink", "consumer.override.max.poll.records", "", ErrTuningBounds},
		{"sign and space", "sink", "tasks.max", " 2", ErrTuningBounds},
		{"source key on a sink", "sink", "max.batch.size", "100", ErrNotTunable},
		{"sink key on a source", "source", "consumer.override.max.poll.records", "100", ErrNotTunable},
		{"bare consumer key", "sink", "max.poll.records", "100", ErrNotTunable},
		{"not allow-listed", "source", "decimal.handling.mode", "double", ErrNotTunable},
		{"secret-looking", "source", "database.password", "1", ErrNotTunable},
		{"unknown connector type", "", "tasks.max", "2", ErrNotTunable},
		{"case matters", "sink", "Tasks.Max", "2", ErrNotTunable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckTunable(tt.typ, tt.key, tt.value)
			if tt.want == nil {
				if err != nil {
					t.Fatalf("err = %v", err)
				}
				return
			}
			if !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
			if tt.value != "" && strings.Contains(err.Error(), tt.value) && tt.want == ErrNotTunable {
				t.Errorf("error echoes the value: %v", err)
			}
		})
	}
	err := CheckTunable("sink", "tasks.max", "64000")
	if err == nil || !strings.Contains(err.Error(), "from 1 to 128") {
		t.Fatalf("the bound is not named: %v", err)
	}
}

func TestPerformanceConstants(t *testing.T) {
	if ClassPerformance != "performance" {
		t.Fatalf("class value changed: %q", ClassPerformance)
	}
	if PerformanceVerifySettle+PerformanceVerifyWindow >= PerformanceVerifyTimeout {
		t.Fatal("the timeout ends before the After window closes")
	}
	if PerformanceVerifyWindow != 15*time.Minute {
		t.Fatal("the ticket's window is 15 min")
	}
}

// E5: the shape X6c implements composes with the outcome rule, and a
// recovery VerifyResult marshals without the X6 fields.
func TestPerformanceVerifyShape(t *testing.T) {
	a := tuningAction{classedAction: classedAction{classAction{name: "connect_tune_throughput"}, ClassPerformance}, before: 1000, after: 1150}
	pv, err := a.Preview(context.Background(), nil)
	if err != nil || len(pv.Tuning) != 1 || pv.Tuning[0].Current != "1" {
		t.Fatalf("preview %+v, %v", pv, err)
	}
	vr, err := a.Verify(context.Background(), nil, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if got := events.PerformanceOutcomeFor(vr.Before, vr.After, vr.LagSlopeBefore, vr.LagSlopeAfter); got != events.PerformanceImproved {
		t.Fatalf("outcome %q", got)
	}
	raw, err := json.Marshal(VerifyResult{Before: 412, After: 0, Unit: events.RecoveryUnitRows})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "lag_slope") {
		t.Fatalf("a recovery result carries X6 fields: %s", raw)
	}
}
