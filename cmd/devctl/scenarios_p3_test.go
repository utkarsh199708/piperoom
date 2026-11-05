package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"gopkg.in/yaml.v3"

	"piperoom/internal/config"
	"piperoom/internal/owners"
)

var p3Scenarios = []string{"missed-table", "dead-slot", "consumer-bad-deploy", "dlq-backlog"}

// Every P3 break must trip the watcher (a signal enabled in dev/owners.p3.yaml)
// or carry an alert that opens a room for the dev pipeline (P-board P3h).
func TestP3ScenariosTripWatcherOrAlert(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range []string{filepath.Base(p3OwnersFile)} {
		own, err := owners.Load(filepath.Join(root, "dev", file))
		if err != nil {
			t.Fatalf("load dev/%s: %v", file, err)
		}
		var p *owners.Pipeline
		for i := range own.Pipelines {
			if own.Pipelines[i].ID == demoPipeline {
				p = &own.Pipelines[i]
			}
		}
		if p == nil {
			t.Fatalf("dev/%s has no pipeline %q", file, demoPipeline)
		}
		w := p.Watch()
		enabled := map[string]bool{
			owners.SignalConnectorDown: w.ConnectorDown.Enabled && w.ConnectorDown.Action == owners.ActionRoom,
			owners.SignalSlotInactive:  w.SlotInactive.Enabled && w.SlotInactive.Action == owners.ActionRoom,
			owners.SignalDLQGrowth:     w.DLQGrowth.Enabled && w.DLQGrowth.Action == owners.ActionRoom,
		}
		for _, name := range p3Scenarios {
			t.Run(file+"/"+name, func(t *testing.T) {
				sc, ok := scenarios[name]
				if !ok {
					t.Fatalf("scenario %q not registered", name)
				}
				if sc.breakIt == nil || sc.fix == nil {
					t.Fatalf("scenario %q has no break or fix", name)
				}
				if len(sc.signals) == 0 && sc.alert == "" {
					t.Fatalf("scenario %q trips neither the watcher nor an alert", name)
				}
				for _, sig := range sc.signals {
					if !enabled[sig] {
						t.Errorf("signal %s of %q does not open a room with dev/%s", sig, name, file)
					}
				}
				if sc.alert != "" {
					var a struct {
						Title      string `json:"title"`
						Severity   string `json:"severity"`
						PipelineID string `json:"pipeline_id"`
						DedupKey   string `json:"dedup_key"`
					}
					if err := json.Unmarshal([]byte(sc.alert), &a); err != nil {
						t.Fatalf("alert of %q is not JSON: %v", name, err)
					}
					if a.Title == "" || a.Severity == "" || a.PipelineID != p.ID || !strings.HasPrefix(a.DedupKey, "devctl-") {
						t.Errorf("alert of %q = %+v: want a title, a severity, pipeline %q and a devctl- dedup key", name, a, p.ID)
					}
				}
			})
		}
		// The actions refuse a table outside source_tables and a DLQ the
		// pipeline does not declare.
		if !slices.Contains(p.SourceTables, refundsTable) {
			t.Errorf("dev/%s source_tables %v lack %s (pg_add_publication_table would refuse it)", file, p.SourceTables, refundsTable)
		}
		if p.SourceTables[0] != "public.orders" {
			t.Errorf("dev/%s: public.orders must stay the first source table (devctl health reads it)", file)
		}
		if !slices.ContainsFunc(w.DLQTopics, func(d owners.DLQTopic) bool { return d.Topic == dlqTopic && d.WrittenBy == ordersGroup }) {
			t.Errorf("dev/%s watch.dlq_topics %v lack %s written by %s", file, w.DLQTopics, dlqTopic, ordersGroup)
		}
	}
}

// devconsumer must write rejects to the DLQ the scenario and owners.p3.yaml use,
// and pass the bad-build knobs the scenarios set.
func TestComposeDevconsumerKnobs(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join(root, "dev", "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var f struct {
		Services map[string]struct {
			Environment map[string]string `yaml:"environment"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(b, &f); err != nil {
		t.Fatal(err)
	}
	env := f.Services["devconsumer"].Environment
	want := map[string]string{
		"DLQ_TOPIC":     dlqTopic,
		"SKIP_OPS":      "${DEVCONSUMER_SKIP_OPS:-}",
		"REJECT_STATUS": "${DEVCONSUMER_REJECT_STATUS:-}",
	}
	for k, v := range want {
		if env[k] != v {
			t.Errorf("devconsumer %s = %q, want %q", k, env[k], v)
		}
	}
}

// dev/owners.p3.yaml is the baseline plus the P3 additions, nothing else; the
// baseline (which evals/gen derives fixtures from) has none of them.
func TestOwnersP3MatchesBaseline(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatal(err)
	}
	read := func(name string) map[string]any {
		b, err := os.ReadFile(filepath.Join(root, "dev", name))
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := yaml.Unmarshal(b, &m); err != nil {
			t.Fatalf("dev/%s: %v", name, err)
		}
		return m
	}
	base, p3 := read("owners.yaml"), read(filepath.Base(p3OwnersFile))
	pipe := func(m map[string]any) map[string]any { return m["pipelines"].([]any)[0].(map[string]any) }
	bp, pp := pipe(base), pipe(p3)
	if fmt.Sprint(bp["source_tables"]) != "[public.orders]" {
		t.Errorf("baseline source_tables = %v: the P3 table belongs in %s only", bp["source_tables"], p3OwnersFile)
	}
	if _, ok := bp["watch"].(map[string]any)["dlq_topics"]; ok {
		t.Errorf("baseline has watch.dlq_topics: the P3 DLQ belongs in %s only", p3OwnersFile)
	}
	// Strip the P3 additions; what is left must equal the baseline.
	pp["source_tables"] = []any{"public.orders"}
	w := pp["watch"].(map[string]any)
	delete(w, "dlq_topics")
	delete(w["signals"].(map[string]any), "dlq_growth")
	if fmt.Sprint(base) != fmt.Sprint(p3) {
		t.Errorf("%s drifted from dev/owners.yaml beyond the P3 additions:\nbase %v\np3   %v", p3OwnersFile, base, p3)
	}
	for _, name := range p3Scenarios {
		if scenarios[name].owners != p3OwnersFile {
			t.Errorf("scenario %q owners = %q, want %s", name, scenarios[name].owners, p3OwnersFile)
		}
	}
}

func TestScenarioNamesListed(t *testing.T) {
	names := scenarioNames()
	for _, n := range append([]string{"schema-change", "connector-auth"}, p3Scenarios...) {
		if !slices.Contains(names, n) {
			t.Errorf("scenarioNames() = %v, missing %q", names, n)
		}
	}
	if !slices.IsSorted(names) {
		t.Errorf("scenarioNames() not sorted: %v", names)
	}
	if got := alertScenarios(); !slices.Equal(got, []string{"blocked-slot", "consumer-bad-deploy", "consumer-skipped-range", "dag-failed", "dbt-model-broken", "late-batch", "missed-table", "missed-table-kafka-signal", "no-replica-identity", "rows-lost-after-retention", "server-id-clash"}) {
		t.Errorf("alertScenarios() = %v", got)
	}
}

func TestParseAlertFlags(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		want    alertOpts
		wantErr string
	}{
		{"defaults", []string{"missed-table"}, alertOpts{scenario: "missed-table", asUser: "arjun"}, ""},
		{"flags after", []string{"consumer-bad-deploy", "--server", "http://127.0.0.1:9", "--as", "kabir"}, alertOpts{scenario: "consumer-bad-deploy", server: "http://127.0.0.1:9", asUser: "kabir"}, ""},
		{"flags before", []string{"--as", "meera", "missed-table"}, alertOpts{scenario: "missed-table", asUser: "meera"}, ""},
		{"no scenario", nil, alertOpts{}, "usage: devctl alert"},
		{"two scenarios", []string{"missed-table", "dead-slot"}, alertOpts{}, "usage: devctl alert"},
		{"unknown", []string{"nope"}, alertOpts{}, `unknown scenario "nope"`},
		{"watcher scenario", []string{"dlq-backlog"}, alertOpts{}, "the watcher opens its room (signals dlq_growth)"},
		{"empty user", []string{"missed-table", "--as", " "}, alertOpts{}, "--as must not be empty"},
		{"bad flag", []string{"--nope", "missed-table"}, alertOpts{}, "flag provided but not defined"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseAlertFlags(tt.args)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Errorf("got %+v, want %+v", got, tt.want)
			}
		})
	}
}

func TestPostAlert(t *testing.T) {
	tests := []struct {
		name    string
		code    int
		reply   string
		wantOut string
		wantErr string
	}{
		{"new room", http.StatusCreated, `{"id":"r_1","title":"x"}`, "alert sent: opened room r_1", ""},
		{"joined", http.StatusOK, `{"id":"r_1"}`, "alert sent: joined room r_1", ""},
		{"tenants reply", http.StatusCreated, `{"room_id":"r_2","created":true}`, "opened room r_2", ""},
		{"oidc mode", http.StatusNotFound, `{}`, "", "mounted in dev auth mode only"},
		{"unknown user", http.StatusUnauthorized, `{}`, "", "--as sends X-User"},
		{"server error", http.StatusInternalServerError, `{}`, "", "HTTP 500"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var gotBody, gotUser, gotPath string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				b := new(bytes.Buffer)
				_, _ = b.ReadFrom(r.Body)
				gotBody, gotUser, gotPath = b.String(), r.Header.Get("X-User"), r.Method+" "+r.URL.Path
				writeJSON(w, tt.code, tt.reply)
			}))
			defer srv.Close()
			var out bytes.Buffer
			err := postAlert(context.Background(), srv.Client(), srv.URL+"/", "kabir", missedTableAlert, &out)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want %q", err, tt.wantErr)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(out.String(), tt.wantOut) {
				t.Errorf("out = %q, want %q", out.String(), tt.wantOut)
			}
			if gotPath != "POST /api/alerts/webhook" || gotUser != "kabir" || gotBody != missedTableAlert {
				t.Errorf("request = %s user=%q body=%q", gotPath, gotUser, gotBody)
			}
		})
	}
	if err := postAlert(context.Background(), http.DefaultClient, "ftp://x", "a", "{}", &bytes.Buffer{}); err == nil {
		t.Error("bad server URL accepted")
	}
}

func TestParseIDSpan(t *testing.T) {
	tests := []struct {
		in      string
		lo, hi  int64
		wantErr bool
	}{
		{"101 130", 101, 130, false},
		{" 5 5\n", 5, 5, false},
		{"", 0, 0, true},
		{"101", 0, 0, true},
		{"a 3", 0, 0, true},
		{"3 b", 0, 0, true},
		{"9 3", 0, 0, true},
	}
	for _, tt := range tests {
		lo, hi, err := parseIDSpan(tt.in)
		if (err != nil) != tt.wantErr || lo != tt.lo || hi != tt.hi {
			t.Errorf("parseIDSpan(%q) = %d, %d, %v", tt.in, lo, hi, err)
		}
	}
}

func TestSlotRowParse(t *testing.T) {
	tests := []struct {
		out     string
		want    slotState
		ok      bool
		wantErr bool
	}{
		{"true|26000", slotState{active: true, retained: 26000}, true, false},
		{"false|0", slotState{retained: 0}, true, false},
		{"", slotState{}, false, false},
		{"t|1", slotState{}, false, true},
		{"false|x", slotState{}, false, true},
		{"false", slotState{}, false, true},
	}
	for _, tt := range tests {
		s := &stack{sqlFn: func(context.Context, string, ...string) (string, error) { return tt.out, nil }}
		got, ok, err := s.slotRow(context.Background())
		if (err != nil) != tt.wantErr || ok != tt.ok || got != tt.want {
			t.Errorf("slotRow(%q) = %+v, %v, %v", tt.out, got, ok, err)
		}
	}
	s := &stack{sqlFn: func(context.Context, string, ...string) (string, error) { return "", errors.New("down") }}
	if _, _, err := s.slotRow(context.Background()); err == nil {
		t.Error("slotRow with Postgres down = nil error")
	}
}

func TestReplayRecordsDropsErrorHeaders(t *testing.T) {
	in := []*kgo.Record{
		{Topic: dlqTopic, Key: []byte("k1"), Value: []byte("v1"), Partition: 0, Offset: 7, Headers: []kgo.RecordHeader{
			{Key: "trace-id", Value: []byte("t")},
			{Key: "__connect.errors.topic", Value: []byte(ordersTopic)},
			{Key: "__connect.errors.exception.message", Value: []byte("boom")},
		}},
		{Topic: dlqTopic, Key: []byte("k2"), Value: []byte("v2")},
	}
	out := replayRecords(in, ordersTopic)
	if len(out) != 2 {
		t.Fatalf("got %d records", len(out))
	}
	for i, r := range out {
		if r.Topic != ordersTopic || string(r.Key) != string(in[i].Key) || string(r.Value) != string(in[i].Value) {
			t.Errorf("record %d = %s %q %q", i, r.Topic, r.Key, r.Value)
		}
		if r.Partition != 0 || r.Offset != 0 {
			t.Errorf("record %d keeps the DLQ position (%d@%d): the partitioner must pick it by key", i, r.Partition, r.Offset)
		}
	}
	if len(out[0].Headers) != 1 || out[0].Headers[0].Key != "trace-id" {
		t.Errorf("headers = %v, want only trace-id", out[0].Headers)
	}
}

// ---- flows against the fake stack (scenarios_p3_fake_test.go) ----

func TestMissedTableBreakAndFix(t *testing.T) {
	w := newP3World(t)
	s, logs := w.stack()
	ctx := w.ctx()
	if err := breakMissedTable(ctx, s); err != nil {
		t.Fatalf("break: %v\n%s", err, logs)
	}
	w.with(func() {
		if !w.refundsExists || !w.signalExists || w.refundsInPub || w.refundRows != refundRows {
			t.Errorf("after break: refunds=%v signal=%v inPub=%v rows=%d", w.refundsExists, w.signalExists, w.refundsInPub, w.refundRows)
		}
		if w.connCfg["publication.autocreate.mode"] != "disabled" || w.connCfg["signal.data.collection"] != signalTable ||
			!strings.Contains(w.connCfg["table.include.list"].(string), "public.devctl_refunds") {
			t.Errorf("connector config after break = %v", w.connCfg)
		}
		if w.connCfg["database.hostname"] != "postgres" {
			t.Errorf("break lost the base connector keys: %v", w.connCfg)
		}
	})
	if span, ok, err := s.topicSpan(ctx, refundsTopic); err != nil || (ok && span.records() > 0) {
		t.Fatalf("refunds reached Kafka during the break: %v %v %v", span, ok, err)
	}
	if err := fixMissedTable(ctx, s); err != nil {
		t.Fatalf("fix: %v\n%s", err, logs)
	}
	w.with(func() {
		if w.signals != 1 {
			t.Errorf("snapshot signals = %d, want 1", w.signals)
		}
		if w.refundsExists || w.signalExists || w.refundsInPub {
			t.Errorf("fix left devctl_* objects: refunds=%v signal=%v inPub=%v", w.refundsExists, w.signalExists, w.refundsInPub)
		}
		if w.connCfg["publication.autocreate.mode"] != "filtered" || w.connCfg["table.include.list"] != "public.orders" {
			t.Errorf("connector config after fix = %v, want the dev/connectors baseline", w.connCfg)
		}
		if len(w.subjectsDel) != 8 {
			t.Errorf("subject deletes = %v, want soft+permanent for 4 subjects", w.subjectsDel)
		}
	})
	for _, topic := range []string{refundsTopic, signalTopic} {
		if _, ok, err := s.topicSpan(ctx, topic); err != nil || ok {
			t.Errorf("topic %s still exists after fix (err %v)", topic, err)
		}
	}
	if !strings.Contains(logs.String(), "refund rows reached Kafka") {
		t.Errorf("fix did not prove the rows reached Kafka:\n%s", logs)
	}
	// Idempotent: a fixed stack is fixed again without error or new signal.
	if err := fixMissedTable(ctx, s); err != nil {
		t.Fatalf("second fix: %v", err)
	}
	w.with(func() {
		if w.signals != 1 {
			t.Errorf("second fix sent a snapshot signal (%d)", w.signals)
		}
	})
}

// The agent already ran pg_add_publication_table: the fix must not add the
// table again (Postgres would refuse) and still snapshot the history.
func TestMissedTableFixAfterAgentPublished(t *testing.T) {
	w := newP3World(t)
	s, logs := w.stack()
	ctx := w.ctx()
	if err := breakMissedTable(ctx, s); err != nil {
		t.Fatalf("break: %v", err)
	}
	w.with(func() { w.refundsInPub = true })
	if err := fixMissedTable(ctx, s); err != nil {
		t.Fatalf("fix: %v\n%s", err, logs)
	}
	if !strings.Contains(logs.String(), "skipped: "+refundsTable+" already in") {
		t.Errorf("fix did not skip the publication add:\n%s", logs)
	}
}

func TestMissedTableBreakRefusesPublishedTable(t *testing.T) {
	w := newP3World(t)
	w.refundsInPub = true
	s, _ := w.stack()
	err := breakMissedTable(w.ctx(), s)
	if err == nil || !strings.Contains(err.Error(), "already in "+publicationName) {
		t.Fatalf("break = %v, want already-published refusal", err)
	}
	w.with(func() {
		if len(w.sql) != 1 {
			t.Errorf("break ran %v after refusing", w.sql)
		}
	})
}

// The fix waits for the snapshot; when the rows never arrive it fails and
// does not tear the scenario down (the operator can look at it).
func TestMissedTableFixSnapshotNeverArrives(t *testing.T) {
	w := newP3World(t)
	s, _ := w.stack()
	if err := breakMissedTable(w.ctx(), s); err != nil {
		t.Fatalf("break: %v", err)
	}
	w.with(func() { w.connState = "STOPPED" }) // nothing streams
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err := fixMissedTable(ctx, s)
	if err == nil || !strings.Contains(err.Error(), "after the snapshot signal") {
		t.Fatalf("fix = %v, want a snapshot timeout", err)
	}
	w.with(func() {
		if !w.refundsExists {
			t.Error("failed fix dropped the scenario tables")
		}
	})
}

func TestDeadSlotBreakAndFix(t *testing.T) {
	w := newP3World(t)
	s, logs := w.stack()
	ctx := w.ctx()
	if err := breakDeadSlot(ctx, s); err != nil {
		t.Fatalf("break: %v\n%s", err, logs)
	}
	w.with(func() {
		if w.connState != "STOPPED" || w.slotActive || !w.slotExists || w.retained <= 26000 {
			t.Errorf("after break: conn=%s active=%v exists=%v retained=%d", w.connState, w.slotActive, w.slotExists, w.retained)
		}
	})
	if err := fixDeadSlot(ctx, s); err != nil {
		t.Fatalf("fix: %v\n%s", err, logs)
	}
	w.with(func() {
		if w.connState != "RUNNING" || !w.slotExists || !w.slotActive || w.retained != 0 {
			t.Errorf("after fix: conn=%s exists=%v active=%v retained=%d", w.connState, w.slotExists, w.slotActive, w.retained)
		}
		if len(w.touched) != 1 || !strings.HasPrefix(w.touched[0], "updated_at >= '2025-10-09 08:53:20.000001+00'") {
			t.Errorf("re-emit filter = %v", w.touched)
		}
	})
	if !strings.Contains(logs.String(), "dropped slot "+slotName) {
		t.Errorf("fix did not log the drop:\n%s", logs)
	}
	// Idempotent: on a healthy stack the second fix changes nothing.
	if err := fixDeadSlot(ctx, s); err != nil {
		t.Fatalf("second fix: %v", err)
	}
	w.with(func() {
		if len(w.touched) != 1 {
			t.Errorf("second fix re-emitted rows again: %v", w.touched)
		}
	})
	if !strings.Contains(logs.String(), "nothing is dead") {
		t.Errorf("second fix did not skip:\n%s", logs)
	}
}

// KAN-411: the connector's saved offset points into the slot that was just
// dropped, so the fix clears it before resuming. The fake fails task 0 when it
// resumes with an offset older than the slot's start (as Debezium did on the
// live stack), so a fix that skipped the delete would time out at RUNNING.
func TestDeadSlotFixClearsTheStaleOffset(t *testing.T) {
	w := newP3World(t)
	s, logs := w.stack()
	ctx := w.ctx()
	if err := breakDeadSlot(ctx, s); err != nil {
		t.Fatalf("break: %v\n%s", err, logs)
	}
	if err := fixDeadSlot(ctx, s); err != nil {
		t.Fatalf("fix: %v\n%s", err, logs)
	}
	w.with(func() {
		if w.offsetLSN != 0 {
			t.Errorf("stored offset %#x survived the fix", w.offsetLSN)
		}
		if w.taskFailed {
			t.Error("task failed on the recreated slot")
		}
		if mode, _ := w.connCfg["snapshot.mode"].(string); mode != "when_needed" {
			t.Errorf("snapshot.mode = %v, want when_needed so the connector snapshots without offsets", w.connCfg["snapshot.mode"])
		}
	})
	// TC1/TC6: stop → drop → create → delete offsets, then resume, in that
	// order. Connect refuses the offsets delete on a running connector, so the
	// stop has to come first.
	ops := w.snapshotOps()
	last := -1
	for _, op := range []string{"stop", "drop-slot", "create-slot", "delete-offsets"} {
		i := slices.Index(ops, op)
		if i < 0 {
			t.Fatalf("ops %v: missing %q", ops, op)
		}
		if i < last {
			t.Fatalf("ops %v: %q is out of order", ops, op)
		}
		last = i
	}
	if del, resume := slices.Index(ops, "delete-offsets"), slices.Index(ops, "resume"); del > resume {
		t.Errorf("ops %v: resumed before clearing the offsets", ops)
	}
	if !strings.Contains(logs.String(), "cleared connector offsets") {
		t.Errorf("fix did not log the cleared offsets:\n%s", logs)
	}
}

// TC4: on a healthy stack the fix changes nothing.
func TestDeadSlotFixOnHealthyStackIsNoOp(t *testing.T) {
	w := newP3World(t)
	s, logs := w.stack()
	if err := fixDeadSlot(w.ctx(), s); err != nil {
		t.Fatalf("fix: %v\n%s", err, logs)
	}
	w.with(func() {
		if len(w.ops) != 0 || len(w.touched) != 0 || w.offsetLSN == 0 {
			t.Errorf("healthy fix touched the stack: ops=%v touched=%v offset=%#x", w.ops, w.touched, w.offsetLSN)
		}
	})
	if !strings.Contains(logs.String(), "nothing is dead") {
		t.Errorf("no-op fix did not say why:\n%s", logs)
	}
}

// Error path: when Connect refuses the offsets delete, the fix stops with that
// error instead of resuming on an offset it knows cannot be replayed.
func TestDeadSlotFixFailsWhenOffsetsCannotBeCleared(t *testing.T) {
	w := newP3World(t)
	w.offsetDeleteStatus = http.StatusInternalServerError
	s, logs := w.stack()
	if err := breakDeadSlot(w.ctx(), s); err != nil {
		t.Fatalf("break: %v\n%s", err, logs)
	}
	err := fixDeadSlot(w.ctx(), s)
	if err == nil || !strings.Contains(err.Error(), "clear connector offsets") {
		t.Fatalf("fix = %v, want the offsets delete to fail", err)
	}
	w.with(func() {
		if slices.Contains(w.ops, "resume") {
			t.Errorf("fix resumed despite the failed delete: %v", w.ops)
		}
	})
}

// The agent dropped the slot (pg_drop_replication_slot) and nobody resumed:
// the fix resumes and re-emits without dropping again.
func TestDeadSlotFixAfterAgentDropped(t *testing.T) {
	w := newP3World(t)
	s, logs := w.stack()
	ctx := w.ctx()
	if err := breakDeadSlot(ctx, s); err != nil {
		t.Fatalf("break: %v", err)
	}
	w.with(func() { w.slotExists, w.retained = false, 0 })
	if err := fixDeadSlot(ctx, s); err != nil {
		t.Fatalf("fix: %v\n%s", err, logs)
	}
	if !strings.Contains(logs.String(), "skipped: slot "+slotName+" already dropped") {
		t.Errorf("fix did not skip the drop:\n%s", logs)
	}
	w.with(func() {
		if !w.slotActive || w.connState != "RUNNING" || len(w.touched) != 1 {
			t.Errorf("after fix: active=%v conn=%s touched=%v", w.slotActive, w.connState, w.touched)
		}
	})
}

// Error path: the slot is inactive while the connector runs (a restart in
// progress). The fix must not drop a slot the connector is about to use.
func TestDeadSlotFixRefusesSlotOfRunningConnector(t *testing.T) {
	w := newP3World(t)
	w.slotActive = false
	s, _ := w.stack()
	err := fixDeadSlot(w.ctx(), s)
	if err == nil || !strings.Contains(err.Error(), "not dropping") {
		t.Fatalf("fix = %v, want a refusal", err)
	}
	w.with(func() {
		if !w.slotExists {
			t.Error("slot dropped")
		}
	})
}

func TestDeadSlotBreakNeedsSlot(t *testing.T) {
	w := newP3World(t)
	w.slotExists = false
	s, _ := w.stack()
	if err := breakDeadSlot(w.ctx(), s); err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("break = %v", err)
	}
}

func TestConsumerBadDeployBreakAndFix(t *testing.T) {
	w := newP3World(t)
	s, logs := w.stack()
	ctx := w.ctx()
	if err := breakConsumerBadDeploy(ctx, s); err != nil {
		t.Fatalf("break: %v\n%s", err, logs)
	}
	w.with(func() {
		if w.consumerUp {
			t.Error("break left the consumer running: the reset action needs an inactive group")
		}
		for id := int64(1001); id <= w.nextID; id++ {
			if w.loaded[id] {
				t.Errorf("order %d loaded by the bad build", id)
			}
		}
	})
	if err := fixConsumerBadDeploy(ctx, s); err != nil {
		t.Fatalf("fix: %v\n%s", err, logs)
	}
	w.with(func() {
		if !w.consumerUp || w.skipOps != "" || !w.resetSeen {
			t.Errorf("after fix: up=%v skip=%q reset=%v", w.consumerUp, w.skipOps, w.resetSeen)
		}
		if len(w.loaded) != badDeployOrders {
			t.Errorf("warehouse has %d of the %d skipped orders", len(w.loaded), badDeployOrders)
		}
	})
	if err := fixConsumerBadDeploy(ctx, s); err != nil {
		t.Fatalf("second fix: %v", err)
	}
}

// Error path: the good build never joins (crash loop). The fix fails with a
// clear message instead of reporting success.
func TestConsumerBadDeployFixConsumerNeverJoins(t *testing.T) {
	w := newP3World(t)
	s, _ := w.stack()
	s.groupFn = func(context.Context, string) (groupView, error) { return groupView{state: "Empty"}, nil }
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	err := fixConsumerBadDeploy(ctx, s)
	if err == nil || !strings.Contains(err.Error(), "did not rejoin") {
		t.Fatalf("fix = %v", err)
	}
}

// Error path: the bad build loaded the orders (it is not bad): the break
// says so instead of claiming a broken stack.
func TestConsumerBadDeployBreakNotSkipped(t *testing.T) {
	w := newP3World(t)
	s, _ := w.stack()
	s.composeFn = func(ctx context.Context, env map[string]string, args ...string) error {
		delete(env, "DEVCONSUMER_SKIP_OPS") // the knob does not reach the container
		return w.compose(ctx, env, args...)
	}
	err := breakConsumerBadDeploy(w.ctx(), s)
	if err == nil || !strings.Contains(err.Error(), "did not skip them") {
		t.Fatalf("break = %v", err)
	}
}

func TestDLQBacklogBreakAndFix(t *testing.T) {
	w := newP3World(t)
	s, logs := w.stack()
	ctx := w.ctx()
	if err := breakDLQBacklog(ctx, s); err != nil {
		t.Fatalf("break: %v\n%s", err, logs)
	}
	span, ok, err := s.topicSpan(ctx, dlqTopic)
	if err != nil || !ok || span.records() != giftOrders {
		t.Fatalf("DLQ after break: %v %v %v", span, ok, err)
	}
	if err := fixDLQBacklog(ctx, s); err != nil {
		t.Fatalf("fix: %v\n%s", err, logs)
	}
	if _, ok, err := s.topicSpan(ctx, dlqTopic); err != nil || ok {
		t.Errorf("DLQ topic still exists after fix (err %v)", err)
	}
	recs, err := w.records(ordersTopic)
	if err != nil {
		t.Fatal(err)
	}
	if len(recs) != giftOrders {
		t.Fatalf("replayed %d records, want %d", len(recs), giftOrders)
	}
	// Order kept per key, error headers dropped, other headers kept.
	for _, r := range recs {
		for _, h := range r.Headers {
			if strings.HasPrefix(h.Key, connectErrorHeader) {
				t.Errorf("replayed record keeps %s", h.Key)
			}
		}
		if len(r.Headers) != 1 || r.Headers[0].Key != "trace-id" {
			t.Errorf("replayed headers = %v", r.Headers)
		}
	}
	// Idempotent: nothing left to replay, nothing produced twice.
	if err := fixDLQBacklog(ctx, s); err != nil {
		t.Fatalf("second fix: %v", err)
	}
	if recs, _ := w.records(ordersTopic); len(recs) != giftOrders {
		t.Errorf("second fix replayed again: %d records", len(recs))
	}
	if !strings.Contains(logs.String(), "skipped: "+dlqTopic+" is empty or gone") {
		t.Errorf("second fix did not skip:\n%s", logs)
	}
}

// Error path: the cause is not fixed (the build still rejects): the replayed
// records go straight back to the DLQ, and the fix must not claim success.
func TestDLQBacklogFixCauseNotFixed(t *testing.T) {
	w := newP3World(t)
	s, _ := w.stack()
	if err := breakDLQBacklog(w.ctx(), s); err != nil {
		t.Fatalf("break: %v", err)
	}
	s.composeFn = func(ctx context.Context, env map[string]string, args ...string) error {
		env["DEVCONSUMER_REJECT_STATUS"] = rejectStatus // the redeploy did not take
		return w.compose(ctx, env, args...)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	err := fixDLQBacklog(ctx, s)
	if err == nil || !strings.Contains(err.Error(), "after the replay") {
		t.Fatalf("fix = %v", err)
	}
}

// Error path: Kafka is down. Every Kafka step reports it, wrapped.
func TestKafkaOpsBrokerDown(t *testing.T) {
	s := &stack{cfg: config.Config{KafkaBrokers: "127.0.0.1:1"}}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if _, _, err := s.topicSpan(ctx, "x"); err == nil {
		t.Error("topicSpan with broker down = nil error")
	}
	if err := s.ensureTopic(ctx, "x", 1); err == nil {
		t.Error("ensureTopic with broker down = nil error")
	}
	if err := s.resetGroupToStart(ctx, "g", "x"); err == nil {
		t.Error("resetGroupToStart with broker down = nil error")
	}
}
