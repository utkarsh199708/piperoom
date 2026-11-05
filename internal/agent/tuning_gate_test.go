package agent

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/actions"
	"piperoom/internal/tools"
)

// tuningRunner runs a CDC room whose model is scripted by fn, with two read
// tools (kafka_throughput reporting a skewed partition, connect_status) and
// one performance action, connect_tune_throughput. It returns the
// proposals, the model and the tool results the model was shown.
func tuningRunner(t *testing.T, fn func(i int, p anthropic.BetaMessageNewParams) string) (*fakeProposals, []string) {
	t.Helper()
	reg := tools.NewRegistry()
	for _, tl := range []tools.Tool{
		&recordingTool{name: "kafka_throughput", schema: testSchema(), result: tools.Result{
			Summary: "orders last 15m: pg.public.orders p2 holds 91% of connect-orders-sink's lag; falling behind at 180/s",
		}},
		&recordingTool{name: "connect_status", schema: testSchema(), result: tools.Result{Summary: "orders-sink RUNNING, 1 of 1 tasks RUNNING"}},
	} {
		if err := reg.Register(tl); err != nil {
			t.Fatal(err)
		}
	}
	acts := actions.NewRegistry()
	perf := &fakePerfAction{fakeRecoveryAction: &fakeRecoveryAction{fakeAction: fakeAction{name: "connect_tune_throughput", layer: "kafka-connect"}}}
	if err := acts.Register(perf); err != nil {
		t.Fatal(err)
	}
	props := &fakeProposals{id: "p_1"}
	r, err := newRunner(Alert{Title: "orders lag", PipelineID: "orders"}, Deps{
		Tools: reg, Actions: acts, Proposals: props, Sink: &fakeSink{},
		Inbox: &fakeInbox{waitErr: context.Canceled}, Logger: discardLogger(),
	}, Options{MaxTurns: 5})
	if err != nil {
		t.Fatal(err)
	}
	var results []string
	r.model = viaSDK(&funcModel{fn: func(_ context.Context, i int, p anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
		if i > 0 {
			results = append(results, toolResultTexts(t, p)...)
		}
		content := fn(i, p)
		if content == "" {
			return mustMessage(t, `{"stop_reason":"end_turn","content":[{"type":"text","text":"done"}]}`), nil
		}
		return mustMessage(t, `{"stop_reason":"tool_use","content":[`+content+`]}`), nil
	}})
	if err := r.run(context.Background()); err != nil {
		t.Fatalf("run: %v", err)
	}
	return props, results
}

func tuneProposal(description string) string {
	return `{"type":"tool_use","id":"tu9","name":"propose_fix","input":{"title":"add sink tasks","steps":[{"action":"connect_tune_throughput","params":"{\"connector\":\"orders-sink\",\"changes\":{\"tasks.max\":\"3\"}}","description":"` +
		description + `"}],"risk":"tasks restart once","rollback":"set tasks.max back to 1"}}`
}

// TC4: with no throughput evidence the agent's tuning is refused and nothing
// is proposed; citing a read that measures nothing (connect_status) is no
// better; with the skew finding cited, the proposal is created and cites the
// step.
func TestTuningNeedsThroughputEvidence(t *testing.T) {
	t.Run("no evidence", func(t *testing.T) {
		props, results := tuningRunner(t, func(i int, _ anthropic.BetaMessageNewParams) string {
			if i == 0 {
				return tuneProposal("add tasks to catch up")
			}
			return ""
		})
		if props.created.Title != "" {
			t.Fatalf("a tuning without evidence was proposed: %+v", props.created)
		}
		if len(results) != 1 || !strings.Contains(results[0], "a tuning needs measured evidence") {
			t.Fatalf("tool results = %q", results)
		}
	})
	t.Run("cites a read that measures nothing", func(t *testing.T) {
		props, results := tuningRunner(t, func(i int, p anthropic.BetaMessageNewParams) string {
			switch i {
			case 0:
				return `{"type":"tool_use","id":"tu1","name":"connect_status","input":{"why":"state"}}`
			case 1:
				return tuneProposal("connector is fine, add tasks (" + lastStepID(t, p) + ")")
			}
			return ""
		})
		if props.created.Title != "" || !strings.Contains(results[len(results)-1], "a tuning needs measured evidence") {
			t.Fatalf("proposal %+v results %q", props.created, results)
		}
	})
	t.Run("skew finding cited", func(t *testing.T) {
		var step string
		props, _ := tuningRunner(t, func(i int, p anthropic.BetaMessageNewParams) string {
			switch i {
			case 0:
				return `{"type":"tool_use","id":"tu1","name":"kafka_throughput","input":{"why":"is the sink keeping up"}}`
			case 1:
				step = lastStepID(t, p)
				return tuneProposal("one task reads 3 partitions and p2 holds 91% of the lag (" + step + "); give each partition a task")
			}
			return ""
		})
		if len(props.created.Steps) != 1 || props.created.Steps[0].Action != "connect_tune_throughput" || !strings.Contains(props.created.Steps[0].Description, step) {
			t.Fatalf("proposal %+v, want the tuning citing %s", props.created, step)
		}
	})
}

// The gate on its own: an errored evidence read, an unknown id and a nil
// ledger refuse; the error is ErrBadInput so the model sees it.
func TestTuningEvidenceGate(t *testing.T) {
	l := newStepLedger()
	l.add("s_01JAAAAAAAAAAAAAAAAAAAAAAA", "kafka_throughput", "falling behind", false)
	l.add("s_01JBBBBBBBBBBBBBBBBBBBBBBB", "connect_task_balance", "error: connect down", true)
	l.add("s_01JCCCCCCCCCCCCCCCCCCCCCCC", "debezium_snapshot_progress", "2.1 M of 10 M rows", false)
	cases := []struct {
		text string
		ok   bool
	}{
		{"see s_01JAAAAAAAAAAAAAAAAAAAAAAA", true},
		{"snapshot at 21 % (s_01JCCCCCCCCCCCCCCCCCCCCCCC)", true},
		{"errored read s_01JBBBBBBBBBBBBBBBBBBBBBBB", false},
		{"unknown s_01JDDDDDDDDDDDDDDDDDDDDDDD", false},
		{"no id at all", false},
	}
	for _, tc := range cases {
		err := l.tuningEvidence(1, "connect_tune_throughput", tc.text)
		if (err == nil) != tc.ok || (err != nil && !errors.Is(err, tools.ErrBadInput)) {
			t.Fatalf("%q: err = %v, want ok %v", tc.text, err, tc.ok)
		}
	}
	var none *stepLedger
	if err := none.tuningEvidence(1, "connect_tune_throughput", "s_01JAAAAAAAAAAAAAAAAAAAAAAA"); err == nil {
		t.Fatal("a nil ledger let a tuning through")
	}
}

// The setup tuning gate (U10b), table-driven for both halves: a step is a
// tuning step when its config keys map to a tuning entry of the knowledge
// index for the room's source connector, and such a step passes only when it
// cites a symptom read of this run.
func TestSetupTuningKeys(t *testing.T) {
	cases := []struct {
		name   string
		source string
		params string
		want   []string
	}{
		{"connector config with tuning keys", "postgres", `{"name":"orders-src","config":{"max.batch.size":"4096","max.queue.size":"16384","topic.prefix":"pg"}}`, []string{"max.batch.size", "max.queue.size"}},
		{"worker properties with a tuning key", "postgres", `{"file":"connect-distributed.properties","entries":{"offset.flush.interval.ms":"10000","group.id":"connect-cluster"}}`, []string{"offset.flush.interval.ms"}},
		{"env entries with the heap knob", "postgres", `{"file":".env","entries":{"KAFKA_HEAP_OPTS":"-Xms1g -Xmx4g"}}`, []string{"KAFKA_HEAP_OPTS"}},
		{"no tuning keys", "postgres", `{"config":{"topic.prefix":"pg","table.include.list":"public.orders"}}`, nil},
		{"no key-bearing fields", "postgres", `{"templates":["kafka-kraft"]}`, nil},
		{"unparsable params gate nothing here", "postgres", `"not an object"`, nil},
		{"another source's tuning key still gates every-source entries", "mysql", `{"config":{"max.batch.size":"4096"}}`, []string{"max.batch.size"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := setupTuningKeys(tc.source, []byte(tc.params))
			if len(got) != len(tc.want) {
				t.Fatalf("setupTuningKeys = %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("setupTuningKeys = %v, want %v", got, tc.want)
				}
			}
		})
	}
}

func TestSetupTuningEvidenceGate(t *testing.T) {
	l := newStepLedger()
	l.add("s_01JAAAAAAAAAAAAAAAAAAAAAAA", "container_logs", "connect: java.lang.OutOfMemoryError: Java heap space", false)
	l.add("s_01JBBBBBBBBBBBBBBBBBBBBBBB", "debezium_snapshot_progress", "snapshot stalled at 2.1 M of 10 M rows", false)
	l.add("s_01JCCCCCCCCCCCCCCCCCCCCCCC", "env_detect", "linux amd64, docker 27", false)
	l.add("s_01JDDDDDDDDDDDDDDDDDDDDDDD", "connect_worker_logs", "error: worker unreachable", true)
	keys := []string{"max.batch.size"}
	cases := []struct {
		text string
		ok   bool
	}{
		{"OOM in the worker logs, see s_01JAAAAAAAAAAAAAAAAAAAAAAA", true},
		{"snapshot stalled (s_01JBBBBBBBBBBBBBBBBBBBBBBB)", true},
		{"cites a read that shows no symptom s_01JCCCCCCCCCCCCCCCCCCCCCCC", false},
		{"cites an errored log read s_01JDDDDDDDDDDDDDDDDDDDDDDD", false},
		{"no citation at all", false},
	}
	for _, tc := range cases {
		err := l.setupTuningEvidence(1, "properties_write", keys, tc.text)
		if (err == nil) != tc.ok || (err != nil && !errors.Is(err, tools.ErrBadInput)) {
			t.Fatalf("%q: err = %v, want ok %v", tc.text, err, tc.ok)
		}
		if err != nil && !strings.Contains(err.Error(), "max.batch.size") {
			t.Fatalf("%q: the refusal does not name the tuning key: %v", tc.text, err)
		}
	}
	var none *stepLedger
	if err := none.setupTuningEvidence(1, "properties_write", keys, "s_01JAAAAAAAAAAAAAAAAAAAAAAA"); err == nil {
		t.Fatal("a nil ledger let a setup tuning through")
	}
}
