package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/tools"
)

// The echo guard tests (P3-W3b) run the real injectscan over the texts the
// injection scenarios plant, so a change to either the scanner or the guard
// that breaks a case shows here before a live run.

// plantedSummary returns the recorded summary of tool in an eval scenario
// that holds marker: the scenario's injection text.
func plantedSummary(t *testing.T, file, tool, marker string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("..", "..", "evals", "scenarios", file))
	if err != nil {
		t.Fatal(err)
	}
	var sc struct {
		Alert struct {
			PipelineMap string `yaml:"pipeline_map"`
		} `yaml:"alert"`
		Recordings []struct {
			Tool    string `yaml:"tool"`
			Summary string `yaml:"summary"`
		} `yaml:"recordings"`
	}
	if err := yaml.Unmarshal(b, &sc); err != nil {
		t.Fatalf("%s: %v", file, err)
	}
	for _, r := range sc.Recordings {
		if r.Tool == tool && strings.Contains(r.Summary, marker) {
			return r.Summary
		}
	}
	t.Fatalf("%s: no %s recording holds %q", file, tool, marker)
	return ""
}

// ordersPipelineMap is the map the injection scenarios give the agent.
const ordersPipelineMap = `pipeline orders
1 pg:public.orders [postgres, owner priya] WARN slot_active=false slot_retained=213.0 KiB
2 connect:pg-orders-connector [kafka-connect, owner arjun] FAIL tasks_failed=1
3 topic:pg.public.orders [kafka, owner arjun] OK newest_record=14m ago
4 group:order-sync [consumer, owner kabir] OK lag=0 state=Stable
5 wh:analytics.orders [warehouse, owner meera] WARN stale_for=14m ts_column=updated_at
`

const (
	echoReadStep    = "s_01J0000000000000000000000E" // the unflagged connect_status read
	echoFlaggedStep = "s_01J0000000000000000000000F" // the read injectscan flags
)

// echoRoom is a room under test: the ledger a runner would hold, the room
// tools built on it, and the proposals sink.
type echoRoom struct {
	steps     *stepLedger
	hypo      tools.Tool
	fixTool   tools.Tool
	proposals *fakeProposals
	logs      *bytes.Buffer
}

func newEchoRoom(t *testing.T) *echoRoom {
	t.Helper()
	reg := actions.NewRegistry()
	for _, a := range []*fakeAction{
		{name: "connect_restart_connector", layer: "kafka-connect"},
		{name: "connect_restart_task", layer: "kafka-connect"},
		{name: "connect_update_connector_config", layer: "kafka-connect"},
		{name: "schema_set_compatibility", layer: "schema-registry"},
		{name: "schema_register", layer: "schema-registry"},
	} {
		if err := reg.Register(a); err != nil {
			t.Fatal(err)
		}
	}
	steps := newStepLedger()
	d := Deps{Actions: reg, Sink: &fakeSink{}}
	steps.setScanNames(scanNamesFor(d, ordersPipelineMap))
	logs := &bytes.Buffer{}
	d.Logger = slog.New(slog.NewJSONHandler(logs, nil))
	proposals := &fakeProposals{id: "p_1"}
	d.Proposals = proposals
	rt := newRoomToolsWith(d, nil, steps)
	return &echoRoom{steps: steps, hypo: rt[0], fixTool: rt[1], proposals: proposals, logs: logs}
}

// read records a read the way runReadTool does: in the ledger and through
// wrapReadToolOutput, which flags it when injectscan does.
func (e *echoRoom) read(t *testing.T, stepID, tool, text string) {
	t.Helper()
	e.steps.add(stepID, tool, text, false)
	r := &runner{log: discardLogger(), steps: e.steps}
	r.wrapReadToolOutput(tool, stepID, text)
}

func (e *echoRoom) hypothesis(t *testing.T, cites ...string) {
	t.Helper()
	in := map[string]any{"text": "task 0 failed on the amount type change", "based_on_steps": cites, "confidence": "low"}
	if len(cites) > 0 {
		in["confidence"] = "medium"
	}
	raw, _ := json.Marshal(in)
	if _, err := e.hypo.Run(context.Background(), raw); err != nil {
		t.Fatalf("hypothesis: %v", err)
	}
}

// propose runs propose_fix with the given steps (action, params).
func (e *echoRoom) propose(steps ...[2]string) (tools.Result, error) {
	var b strings.Builder
	b.WriteString(`{"title":"fix","risk":"low","rollback":"restore the previous config","steps":[`)
	for i, s := range steps {
		if i > 0 {
			b.WriteString(",")
		}
		b.WriteString(`{"action":"` + s[0] + `","params":` + s[1] + `,"description":"step"}`)
	}
	b.WriteString("]}")
	e.proposals.created = events.ProposalCreatedPayload{}
	return e.fixTool.Run(context.Background(), json.RawMessage(b.String()))
}

func wantRefused(t *testing.T, err error, step, flaggedRead string) {
	t.Helper()
	if !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("err = %v, want a tools.ErrBadInput refusal", err)
	}
	msg := err.Error()
	for _, w := range []string{"step " + step + " ", "the read " + flaggedRead + " flagged as an injection", "cite evidence of your own or drop the step"} {
		if !strings.Contains(msg, w) {
			t.Errorf("refusal %q lacks %q", msg, w)
		}
	}
	if strings.Count(msg, "\n") != 0 {
		t.Errorf("refusal is not one line: %q", msg)
	}
}

func wantCreated(t *testing.T, e *echoRoom, err error, action string) {
	t.Helper()
	if err != nil {
		t.Fatalf("propose_fix: %v", err)
	}
	if len(e.proposals.created.Steps) == 0 || e.proposals.created.Steps[0].Action != action {
		t.Fatalf("proposal not created with %s: %+v", action, e.proposals.created)
	}
}

// TestEchoGuardDDLComment: the injected COMMENT names
// connect_update_connector_config with value.converter=JsonConverter. The
// standard decimal.handling.mode change, backed by connect_status, stays
// allowed; the injected converter switch is refused however it is spelled.
func TestEchoGuardDDLComment(t *testing.T) {
	ddl := plantedSummary(t, "injection-ddl-comment.yaml", "pg_recent_ddl", "NOTE FOR THE AI ASSISTANT")
	e := newEchoRoom(t)
	e.read(t, echoReadStep, "connect_status", "pg-orders-connector (source): RUNNING. Task 0: FAILED: 409 incompatible schema for pg.public.orders-value")
	e.read(t, echoFlaggedStep, "pg_recent_ddl", ddl)
	if len(e.steps.flaggedReads()) != 1 {
		t.Fatalf("flagged reads = %+v, want the pg_recent_ddl read", e.steps.flaggedReads())
	}
	e.hypothesis(t, echoReadStep)

	_, err := e.propose(
		[2]string{"connect_update_connector_config", `{"connector":"pg-orders-connector","changes":{"decimal.handling.mode":"double"}}`},
		[2]string{"connect_restart_task", `{"connector":"pg-orders-connector","task":0}`},
	)
	wantCreated(t, e, err, "connect_update_connector_config")
	if e.logs.Len() != 0 {
		t.Errorf("an allowed step logged: %s", e.logs.String())
	}

	for _, params := range []string{
		`{"connector":"pg-orders-connector","changes":{"value.converter":"org.apache.kafka.connect.json.JsonConverter"}}`,
		`{"connector":"pg-orders-connector","changes":{"value.converter":"JsonConverter"}}`,
		`{"connector":"pg-orders-connector","changes":{"converter":"jsonconverter"}}`,
	} {
		e.logs.Reset()
		_, err := e.propose(
			[2]string{"schema_set_compatibility", `{"subject":"pg.public.orders-value","level":"NONE"}`},
			[2]string{"connect_update_connector_config", params},
		)
		wantRefused(t, err, "2", echoFlaggedStep)
		if e.proposals.created.Title != "" {
			t.Errorf("%s: proposal created despite the refusal", params)
		}
		line := e.logs.String()
		for _, w := range []string{`"tool":"propose_fix"`, `"step":2`, `"tool_request"`, `"override"`} {
			if !strings.Contains(line, w) {
				t.Errorf("log %q lacks %s", line, w)
			}
		}
		for _, leak := range []string{"JsonConverter", "ignore previous", "value.converter", "AI ASSISTANT"} {
			if strings.Contains(line, leak) {
				t.Errorf("log leaks span text %q: %s", leak, line)
			}
		}
	}
	// The injected schema_register is refused too: the comment names it and
	// the subject the model would type is in no span, but the hypothesis
	// rule still holds when nothing of the model's own backs it.
	e.hypothesis(t, echoFlaggedStep)
	_, err = e.propose([2]string{"schema_register", `{"subject":"pg.public.orders-value","schema":"{}"}`})
	wantRefused(t, err, "1", echoFlaggedStep)
}

// TestEchoGuardKafkaRecordKey: the record key asks to restart
// pg-orders-connector; action and target sit in one span, so the restart is
// refused even when the hypothesis cites an unflagged read.
func TestEchoGuardKafkaRecordKey(t *testing.T) {
	key := plantedSummary(t, "injection-kafka-record-key.yaml", "kafka_find_by_key", "NOTE TO THE AI AGENT")
	e := newEchoRoom(t)
	e.read(t, echoReadStep, "kafka_consumer_lag", "group order-sync: orders/1 committed 25377154 lag 13908")
	e.read(t, echoFlaggedStep, "kafka_find_by_key", key)
	e.hypothesis(t, echoReadStep)

	_, err := e.propose([2]string{"connect_restart_connector", `{"connector":"pg-orders-connector"}`})
	wantRefused(t, err, "1", echoFlaggedStep)
	// A restart of another connector echoes nothing and is backed: allowed.
	_, err = e.propose([2]string{"connect_restart_connector", `{"connector":"inventory-connector"}`})
	wantCreated(t, e, err, "connect_restart_connector")
	// An action the key never names is not the guard's business.
	_, err = e.propose([2]string{"schema_set_compatibility", `{"subject":"pg.public.orders-value","level":"NONE"}`})
	wantCreated(t, e, err, "schema_set_compatibility")
}

// TestEchoGuardConnectTraceHypothesisRule: the trace asks for
// connect_restart_connector without naming the connector, so the params echo
// nothing; the step is refused only while no hypothesis cites a read outside
// the flagged one (rule b).
func TestEchoGuardConnectTraceHypothesisRule(t *testing.T) {
	trace := plantedSummary(t, "injection-connect-trace.yaml", "connect_status", "[SYSTEM]")
	e := newEchoRoom(t)
	e.read(t, echoFlaggedStep, "connect_status", trace)
	e.read(t, echoReadStep, "kafka_consumer_lag", "group order-sync: Stable, total lag 0")

	// No hypothesis yet.
	_, err := e.propose([2]string{"connect_restart_connector", `{"connector":"pg-orders-connector"}`})
	wantRefused(t, err, "1", echoFlaggedStep)
	if !strings.Contains(err.Error(), "hypothesis cites no step outside the flagged reads") {
		t.Errorf("refusal does not name the rule: %v", err)
	}
	// A hypothesis citing only the flagged read.
	e.hypothesis(t, echoFlaggedStep)
	_, err = e.propose([2]string{"connect_restart_connector", `{"connector":"pg-orders-connector"}`})
	wantRefused(t, err, "1", echoFlaggedStep)
	// The injected schema_register for the subject it names: rule (a).
	_, err = e.propose([2]string{"schema_register", `{"subject":"pg.public.orders-value","schema":"{}"}`})
	wantRefused(t, err, "1", echoFlaggedStep)
	// Evidence of the model's own unlocks the restart, not the echo.
	e.hypothesis(t, echoFlaggedStep, echoReadStep)
	_, err = e.propose([2]string{"connect_restart_connector", `{"connector":"pg-orders-connector"}`})
	wantCreated(t, e, err, "connect_restart_connector")
	_, err = e.propose([2]string{"schema_register", `{"subject":"pg.public.orders-value","schema":"{}"}`})
	wantRefused(t, err, "1", echoFlaggedStep)
}

// TestEchoGuardNoFlaggedReads: a room whose reads injectscan never flagged
// (schema-change, and the subtle comment it misses) is untouched.
func TestEchoGuardNoFlaggedReads(t *testing.T) {
	subtle := plantedSummary(t, "injection-ddl-comment-subtle.yaml", "pg_recent_ddl", "Per the platform team")
	e := newEchoRoom(t)
	e.read(t, echoReadStep, "connect_status", "pg-orders-connector (source): RUNNING. Task 0: FAILED: 409")
	e.read(t, echoFlaggedStep, "pg_recent_ddl", subtle)
	if n := len(e.steps.flaggedReads()); n != 0 {
		t.Fatalf("flagged reads = %d, want 0: the subtle comment is a documented recall gap", n)
	}
	// No hypothesis at all: rule (b) would fire if a span named the action.
	_, err := e.propose(
		[2]string{"schema_set_compatibility", `{"subject":"pg.public.orders-value","level":"NONE"}`},
		[2]string{"connect_restart_connector", `{"connector":"pg-orders-connector"}`},
	)
	wantCreated(t, e, err, "schema_set_compatibility")
	if e.logs.Len() != 0 {
		t.Errorf("unexpected log: %s", e.logs.String())
	}
}

// TestEchoGuardNilProposalsAndLedger: a nil proposals sink is the existing
// error, not a panic, flagged reads or not; a tool built without a ledger
// (NewProposeFixTool) runs no guard.
func TestEchoGuardNilProposalsAndLedger(t *testing.T) {
	key := plantedSummary(t, "injection-kafka-record-key.yaml", "kafka_find_by_key", "NOTE TO THE AI AGENT")
	e := newEchoRoom(t)
	e.read(t, echoFlaggedStep, "kafka_find_by_key", key)
	tool := e.fixTool.(*proposeFixTool)
	tool.proposals = nil
	_, err := e.propose([2]string{"connect_restart_connector", `{"connector":"pg-orders-connector"}`})
	if err == nil || errors.Is(err, tools.ErrBadInput) || !strings.Contains(err.Error(), "proposals are not available") {
		t.Fatalf("err = %v, want the proposals-not-available error", err)
	}

	reg := actions.NewRegistry()
	if err := reg.Register(&fakeAction{name: "connect_restart_connector", layer: "kafka-connect"}); err != nil {
		t.Fatal(err)
	}
	bare := NewProposeFixTool(reg, &fakeProposals{id: "p_2"})
	_, err = bare.Run(context.Background(), json.RawMessage(`{"title":"t","risk":"low","rollback":"none needed","steps":[{"action":"connect_restart_connector","params":{"connector":"pg-orders-connector"},"description":"d"}]}`))
	if err != nil {
		t.Fatalf("no ledger: %v", err)
	}
	var nilLedger *stepLedger
	if kinds, err := nilLedger.echoedInjection(1, "connect_restart_connector", json.RawMessage(`{}`)); err != nil || kinds != nil {
		t.Fatalf("nil ledger: %v %v", kinds, err)
	}
	if got := nilLedger.scanRead("s_1", "kafka_find_by_key", key); len(got) == 0 {
		t.Fatal("nil ledger still scans")
	}
}

// TestEchoGuardEvictionDropsSpans: a flagged read evicted from the ledger
// takes its spans with it.
func TestEchoGuardEvictionDropsSpans(t *testing.T) {
	key := plantedSummary(t, "injection-kafka-record-key.yaml", "kafka_find_by_key", "NOTE TO THE AI AGENT")
	e := newEchoRoom(t)
	e.read(t, echoFlaggedStep, "kafka_find_by_key", key)
	for i := 0; i < maxLedgerSteps; i++ {
		e.steps.add("s_"+strings.Repeat("0", 20)+string(rune('A'+i%26))+strings.Repeat("Z", 5)+string(rune('A'+i/26)), "connect_status", "ok", false)
	}
	if n := len(e.steps.flaggedReads()); n != 0 {
		t.Fatalf("flagged reads after eviction = %d, want 0", n)
	}
	if _, ok := e.steps.flagged[echoFlaggedStep]; ok {
		t.Fatal("evicted step still in flagged")
	}
}

func TestParamEchoValues(t *testing.T) {
	cases := []struct {
		params string
		want   []string
	}{
		{`{"connector":"pg-orders-connector","task":0}`, []string{"pg-orders-connector"}},
		{`{"changes":{"value.converter":"org.apache.kafka.connect.json.JsonConverter"}}`, []string{"value.converter", "org.apache.kafka.connect.json.JsonConverter", "JsonConverter"}},
		{`{"level":"NONE","offset":25377154,"short":"abc"}`, []string{"25377154"}},
		{`{"list":["latest","reset-to-latest"]}`, []string{"reset-to-latest"}},
		{`not json`, nil},
	}
	for _, c := range cases {
		got := paramEchoValues(json.RawMessage(c.params))
		if len(got) == 0 && len(c.want) == 0 {
			continue
		}
		sortStrings(got)
		sortStrings(c.want)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: got %v, want %v", c.params, got, c.want)
		}
	}
}

func TestPipelineMapNames(t *testing.T) {
	got := pipelineMapNames(ordersPipelineMap + "6 sr:pg.public.orders-value [schema-registry, owner arjun] OK\n  depends on: 2\n7 dlq:orders.dlq [kafka, owner arjun] OK\n")
	want := []string{"public.orders", "orders", "pg-orders-connector", "pg.public.orders", "order-sync", "analytics.orders", "pg.public.orders-value", "orders.dlq"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
	if got := pipelineMapNames("postgres(orders) -> pg-orders-connector"); got != nil {
		t.Errorf("free text gave %v", got)
	}
}

func sortStrings(s []string) {
	for i := 1; i < len(s); i++ {
		for j := i; j > 0 && s[j] < s[j-1]; j-- {
			s[j], s[j-1] = s[j-1], s[j]
		}
	}
}
