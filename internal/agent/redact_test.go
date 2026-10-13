package agent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// A synthetic credential: it exists only in this test.
const testPassword = "Zq7-Rk2pW9xT"

func TestReadToolSecretsNeverReachModelOrEvents(t *testing.T) {
	dsn := "postgres://debezium:" + testPassword + "@pg-primary:5432/orders"
	errTarget := errors.New("connect refused")
	cases := []struct {
		name string
		res  tools.Result
		err  error
		want string // evidence that must survive
	}{
		{
			name: "summary",
			res: tools.Result{
				Summary: "task 0 FAILED: org.postgresql.util.PSQLException: connection to " + dsn + " refused; slot debezium_orders",
				Data: map[string]any{
					"trace":  "dial " + dsn,
					"config": map[string]any{"database.password": testPassword, "slot.name": "debezium_orders"},
					"tasks":  []taskRow{{ID: 0, State: "FAILED", Trace: "PSQLException: connect " + dsn}},
				},
			},
			want: "debezium_orders",
		},
		{
			name: "error",
			err:  fmt.Errorf("connect: dial %s: %w", dsn, errTarget),
			want: "pg-primary:5432/orders",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rec := &recordingTool{name: "connect_status", schema: testSchema("connector"), result: tc.res, err: tc.err}
			model := &scriptedModel{turns: []anthropic.BetaMessage{
				mustMessage(t, toolTurnJSON("connect_status", `{"connector":"pg-orders-connector"}`)),
			}}
			sink := &fakeSink{}
			var logs bytes.Buffer
			r := newTestRunner(t, model, &fakeInbox{waitErr: context.Canceled}, sink, rec, Options{MaxTurns: 3})
			r.log = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
			if err := r.run(context.Background()); err != nil {
				t.Fatalf("run: %v", err)
			}

			if len(model.calls) < 2 {
				t.Fatalf("model saw %d requests, want the tool result turn", len(model.calls))
			}
			seen := messagesJSON([]anthropic.BetaMessageParam{lastMessage(model.calls)})
			if strings.Contains(seen, testPassword) {
				t.Errorf("model saw the password: %s", seen)
			}
			if !strings.Contains(seen, "[redacted") || !strings.Contains(seen, tc.want) {
				t.Errorf("model result lost the mask or the evidence %q: %s", tc.want, seen)
			}

			results := sink.ofType(events.AgentToolResult)
			if len(results) != 1 {
				t.Fatalf("got %d tool_result events, want 1", len(results))
			}
			for _, e := range sink.events {
				if bytes.Contains(e.payload, []byte(testPassword)) {
					t.Errorf("%s event carries the password: %s", e.typ, e.payload)
				}
			}
			if !bytes.Contains(results[0].payload, []byte(tc.want)) {
				t.Errorf("event lost the evidence %q: %s", tc.want, results[0].payload)
			}
			if strings.Contains(logs.String(), testPassword) {
				t.Errorf("log carries the password: %s", logs.String())
			}
		})
	}
}

// taskRow is shaped like connect_status's typed task rows: a typed slice
// inside the Data map, which a shallow walk would miss.
type taskRow struct {
	ID    int    `json:"id"`
	State string `json:"state"`
	Trace string `json:"trace,omitempty"`
}

func TestVerifierStepSecretsMasked(t *testing.T) {
	dsn := "postgres://debezium:" + testPassword + "@pg-primary:5432/orders"
	cases := []struct {
		name string
		tool *fakeVerifyTool
	}{
		{"result", &fakeVerifyTool{name: "connect_status", schema: connectSchema(), result: tools.Result{
			Summary: "pg-orders-connector (source): RUNNING.\nTask 0: FAILED: dial " + dsn,
			Data: map[string]any{
				"name":  "pg-orders-connector",
				"state": "RUNNING",
				"tasks": []taskRow{{ID: 0, State: "FAILED", Trace: "PSQLException: connect " + dsn}},
			},
		}}},
		{"error", &fakeVerifyTool{name: "connect_status", schema: connectSchema(),
			err: fmt.Errorf("connect: GET status: dial %s: %w", dsn, context.DeadlineExceeded)}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			model := &scriptedModel{}
			sink := &fakeSink{}
			inbox := &fakeInbox{
				drains: [][]HumanInput{
					nil,
					lastActionResult("proposal p_1 step 0: restarted pg-orders-connector task 0"),
				},
				waitErr: context.Canceled,
			}
			steps := []events.ProposalStep{
				{Action: "connect_restart_task", Layer: "kafka-connect", Params: []byte(`{"connector":"pg-orders-connector","task":0}`), Description: "restart task 0"},
			}
			r := newVerifyRunner(t, model, inbox, sink, []tools.Tool{tc.tool}, steps)
			var logs bytes.Buffer
			r.log = slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
			if err := r.run(context.Background()); err != nil {
				t.Fatalf("run: %v", err)
			}
			if tc.tool.calls == 0 {
				t.Fatal("verifier never ran the tool")
			}
			if len(sink.ofType(events.AgentToolResult)) == 0 {
				t.Fatal("no verifier tool_result event")
			}
			for _, e := range sink.events {
				if bytes.Contains(e.payload, []byte(testPassword)) {
					t.Errorf("%s event carries the password: %s", e.typ, e.payload)
				}
			}
			for i, c := range model.calls {
				if js := messagesJSON(c); strings.Contains(js, testPassword) {
					t.Errorf("model request %d carries the password: %s", i, js)
				}
			}
			if strings.Contains(logs.String(), testPassword) {
				t.Errorf("log carries the password: %s", logs.String())
			}
			// The verdict still names the evidence.
			if !strings.Contains(strings.Join(messageTexts(sink), "\n")+messagesJSON(model.calls[len(model.calls)-1]), "pg-primary:5432/orders") {
				t.Error("verdict lost the evidence")
			}
		})
	}
}

// secretPanicTool panics with an error that carries a DSN password.
type secretPanicTool struct{ panicTool }

func TestReadToolPanicValueIsMasked(t *testing.T) {
	reg := tools.NewRegistry()
	value := fmt.Errorf("pgx: dial postgres://debezium:%s@pg-primary:5432/orders", testPassword)
	if err := reg.Register(&secretPanicTool{panicTool{name: "connect_status", value: value}}); err != nil {
		t.Fatal(err)
	}
	turn := mustMessage(t, toolTurnJSON("connect_status", `{"why":"check","connector":"c"}`))
	ep := testEndpoint(&scriptedModel{turns: []anthropic.BetaMessage{turn}}, 0)
	var logs bytes.Buffer
	got := expectPanic(t, func() {
		_ = ep.Run(context.Background(), Alert{RoomID: "r", Title: "orders stale", PipelineID: "orders"}, Deps{
			Tools: reg, Inbox: &fakeInbox{waitErr: context.Canceled}, Sink: &fakeSink{},
			Logger: slog.New(slog.NewTextHandler(&logs, nil)),
		}, Options{})
	})
	s := fmt.Sprint(got)
	if strings.Contains(s, testPassword) || !strings.Contains(s, "pg-primary:5432/orders") {
		t.Errorf("re-raised panic value %q", s)
	}
	if strings.Contains(logs.String(), testPassword) {
		t.Errorf("log carries the password: %s", logs.String())
	}
}

func TestScrubToolErrorKeepsChain(t *testing.T) {
	r := &runner{log: discardLogger()}
	err := r.scrubToolError("t", fmt.Errorf("dial postgres://u:%s@db/x: %w", testPassword, context.DeadlineExceeded))
	if strings.Contains(err.Error(), testPassword) {
		t.Fatalf("error text kept the password: %v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("errors.Is lost the wrapped cause")
	}
	clean := errors.New("not found")
	if got := r.scrubToolError("t", clean); got != clean {
		t.Fatalf("clean error replaced: %v", got)
	}
}
