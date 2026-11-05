package ops

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"piperoom/internal/setup/protocol"
)

// deadlineOp records the deadline Execute ran it under; it does not
// implement Deadliner.
type deadlineOp struct {
	name string
	got  *time.Duration
}

func (o deadlineOp) Name() string                      { return o.name }
func (deadlineOp) Target() string                      { return "" }
func (deadlineOp) Tier() string                        { return protocol.TierT0 }
func (deadlineOp) Layer() string                       { return "environment" }
func (deadlineOp) Describe() string                    { return "records its deadline" }
func (deadlineOp) ParamsSchema() map[string]any        { return map[string]any{"type": "object"} }
func (deadlineOp) Validate(json.RawMessage, Env) error { return nil }
func (o deadlineOp) Execute(ctx context.Context, _ json.RawMessage, _ Env) (Result, error) {
	if dl, ok := ctx.Deadline(); ok {
		*o.got = time.Until(dl)
	}
	return Result{Summary: "ok"}, nil
}

// ownDeadlineOp is a deadlineOp that asks for its own bound.
type ownDeadlineOp struct {
	deadlineOp
	own time.Duration
}

func (o ownDeadlineOp) Deadline() time.Duration { return o.own }

// TestOpDeadline: an op runs under the default bound unless it implements
// Deadliner, whose bound is clamped to [default, MaxOpTimeout]; a nearer
// request deadline always wins.
func TestOpDeadline(t *testing.T) {
	cases := []struct {
		name     string
		own      time.Duration // < 0: not a Deadliner
		request  time.Duration // 0: no request deadline
		want     time.Duration
		maxSlack time.Duration
	}{
		{"default", -1, 0, defaultOpTimeout, time.Second},
		{"zero keeps the default", 0, 0, defaultOpTimeout, time.Second},
		{"below the default is raised to it", 2 * time.Second, 0, defaultOpTimeout, time.Second},
		{"own deadline", 15 * time.Minute, 0, 15 * time.Minute, time.Second},
		{"capped", 3 * time.Hour, 0, MaxOpTimeout, time.Second},
		{"a request deadline beyond the own one", 15 * time.Minute, time.Hour, 15 * time.Minute, time.Second},
		{"a nearer request deadline wins", 15 * time.Minute, 3 * time.Second, 3 * time.Second, time.Second},
		{"a nearer request deadline wins over the default", -1, 2 * time.Second, 2 * time.Second, time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r, err := New(Config{ProjectDir: t.TempDir()})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			var got time.Duration
			base := deadlineOp{name: "record_deadline", got: &got}
			var op Op = base
			if tc.own >= 0 {
				op = ownDeadlineOp{deadlineOp: base, own: tc.own}
			}
			if err := r.Register(op); err != nil {
				t.Fatal(err)
			}
			req := protocol.OpRequest{ID: "d1", Op: "record_deadline", Params: json.RawMessage(`{}`)}
			if tc.request > 0 {
				req.Deadline = time.Now().Add(tc.request)
			}
			if res, err := r.Execute(context.Background(), req); err != nil || !res.OK {
				t.Fatalf("Execute = %v, %+v", err, res)
			}
			if got > tc.want || got < tc.want-tc.maxSlack {
				t.Errorf("ran under %s, want about %s", got, tc.want)
			}
		})
	}
}
