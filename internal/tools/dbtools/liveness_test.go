package dbtools

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/tools"
)

// Tests for KAN-400's streaming liveness check (TC1-TC4, TC8, TC9 of the
// ticket's lead review).

func livenessSlot(name, flush, restart string, retained int64) clients.ReplicationSlot {
	return clients.ReplicationSlot{
		Name: name, Plugin: "pgoutput", Active: true,
		ConfirmedFlushLSN: flush, RestartLSN: restart,
		RetainedWALBytes: retained, WALStatus: "reserved",
	}
}

// livenessDeps scripts the two slot reads and uses the shared orders pipeline.
// Wait is a no-op so the tests do not sleep.
func livenessDeps(reads ...[]clients.ReplicationSlot) (Deps, *fakeSource) {
	src := &fakeSource{slotReads: reads}
	d := Deps{
		Src: src,
		Connect: &fakeConnect{
			configs: map[string]map[string]string{
				"pg-orders-connector": {
					"connector.class": "io.debezium.connector.postgresql.PostgresConnector",
					"slot.name":       "orders_slot",
				},
				"pg-payments-connector": {
					"connector.class": "io.debezium.connector.postgresql.PostgresConnector",
					"slot.name":       "payments_slot",
				},
			},
			status: clients.ConnectorStatus{
				Name: "pg-orders-connector", State: "RUNNING",
				Tasks: []clients.TaskStatus{{ID: 0, State: "RUNNING"}},
			},
		},
		Owners: testOwners(),
		Wait:   func(context.Context, time.Duration) error { return nil },
	}
	return d, src
}

func runLiveness(t *testing.T, d Deps, ctx context.Context, in string) (tools.Result, error) {
	t.Helper()
	return pgStreamingLivenessTool(d).Run(ctx, json.RawMessage(in))
}

func TestStreamingLivenessStreaming(t *testing.T) {
	d, _ := livenessDeps(
		[]clients.ReplicationSlot{livenessSlot("orders_slot", "0/1000", "0/1000", 0)},
		[]clients.ReplicationSlot{livenessSlot("orders_slot", "0/2000", "0/1000", 0x1000)},
	)
	res, err := runLiveness(t, d, roomCtx(), `{"pipeline_id":"orders"}`)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`streaming: slot "orders_slot" confirmed_flush_lsn advanced 4096 bytes in 5s (0/1000 -> 0/2000)`,
		"connector pg-orders-connector state RUNNING",
	} {
		if !strings.Contains(res.Summary, want) {
			t.Errorf("summary lacks %q:\n%s", want, res.Summary)
		}
	}
	rep := res.Data.(livenessReport)
	if rep.Verdict != verdictStreaming || rep.Slot.ConfirmedFlushAdvanceBytes != 4096 {
		t.Errorf("report = %+v", rep)
	}
	if rep.ConnectorState != "RUNNING" || len(rep.ConnectorTasks) != 1 {
		t.Errorf("connector = %+v", rep)
	}
}

func TestStreamingLivenessStuckShowsOldestWriter(t *testing.T) {
	d, src := livenessDeps(
		[]clients.ReplicationSlot{livenessSlot("orders_slot", "0/1000", "0/1000", 0)},
		[]clients.ReplicationSlot{livenessSlot("orders_slot", "0/1000", "0/1000", 0x2000)},
	)
	src.txns = []clients.Transaction{{
		PID: 42, Username: "app", ApplicationName: "writer", State: "idle in transaction",
		Age: 12 * time.Minute, WaitEvent: "Client:ClientRead", HasXID: true,
		Query: "update orders set status = 'paid'",
	}}
	res, err := runLiveness(t, d, roomCtx(), `{"pipeline_id":"orders"}`)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		// TC2 wording, with the source WAL it wrote while the slot stood still:
		"stuck: confirmed_flush_lsn did not move while the source wrote 8192 bytes in 5s",
		"connector pg-orders-connector state RUNNING",
		"oldest open writer: pid=42 state=idle in transaction age=12m verb=UPDATE",
	} {
		if !strings.Contains(res.Summary, want) {
			t.Errorf("summary lacks %q:\n%s", want, res.Summary)
		}
	}
	// TC9: the transaction that explains a stuck slot without a broken
	// connector is in Data, with no query text.
	rep := res.Data.(livenessReport)
	if rep.Verdict != verdictStuck || rep.Slot.SourceWALAdvanceBytes != 8192 {
		t.Errorf("report = %+v", rep)
	}
	// The summary stays small; the detail is in Data.
	if len(res.Summary) > tools.MaxSummaryBytes {
		t.Errorf("summary is %d bytes, over the cap", len(res.Summary))
	}
	if rep.OldestTransaction == nil || rep.OldestTransaction.PID != 42 || rep.OldestTransaction.Verb != "UPDATE" {
		t.Fatalf("oldest transaction = %+v", rep.OldestTransaction)
	}
	b, _ := json.Marshal(rep)
	if strings.Contains(string(b), "paid") {
		t.Errorf("query text reached Data: %s", b)
	}
}

func TestStreamingLivenessStuckOutsidePipeline(t *testing.T) {
	// A writer outside the pipeline's tables holds back every logical slot's
	// restart_lsn; only the anonymised shape may be shown.
	d, src := livenessDeps(
		[]clients.ReplicationSlot{livenessSlot("orders_slot", "0/1000", "0/1000", 0)},
		[]clients.ReplicationSlot{livenessSlot("orders_slot", "0/1000", "0/1000", 0x2000)},
	)
	src.txns = nil
	d.Src = &otherWriterSource{
		fakeSource: src,
		tt: postgres.TableTransactions{OldestOtherWriters: []postgres.OtherWriter{{
			PID: 9, SameDatabase: true, State: "idle in transaction",
			AgeSeconds: 600, XIDAge: 30, WaitEventType: "Client",
		}}},
	}
	res, err := runLiveness(t, d, roomCtx(), `{"pipeline_id":"orders"}`)
	if err != nil {
		t.Fatal(err)
	}
	if want := "oldest open writer outside this pipeline's tables: pid=9 same_database=true state=idle in transaction age=10m xid_age=30"; !strings.Contains(res.Summary, want) {
		t.Errorf("summary lacks %q:\n%s", want, res.Summary)
	}
	rep := res.Data.(livenessReport)
	if rep.OldestOtherWriter == nil || rep.OldestOtherWriter.PID != 9 || rep.OldestTransaction != nil {
		t.Errorf("report = %+v", rep)
	}
}

// otherWriterSource reports the pipeline-table filter's full answer.
type otherWriterSource struct {
	*fakeSource
	tt postgres.TableTransactions
}

func (s *otherWriterSource) LongTransactionsOnTables(context.Context, time.Duration, []string) (postgres.TableTransactions, error) {
	return s.tt, nil
}

func TestStreamingLivenessQuiet(t *testing.T) {
	// TC3: both LSNs flat is a quiet source, not a stall.
	d, _ := livenessDeps(
		[]clients.ReplicationSlot{livenessSlot("orders_slot", "0/1000", "0/1000", 0x10)},
		[]clients.ReplicationSlot{livenessSlot("orders_slot", "0/1000", "0/1000", 0x10)},
	)
	res, err := runLiveness(t, d, roomCtx(), `{"pipeline_id":"orders"}`)
	if err != nil {
		t.Fatal(err)
	}
	if want := `quiet: no source writes in 5s (slot "orders_slot", confirmed_flush_lsn 0/1000 unchanged, active=true); not a stall`; !strings.Contains(res.Summary, want) {
		t.Errorf("summary lacks %q:\n%s", want, res.Summary)
	}
	if rep := res.Data.(livenessReport); rep.Verdict != verdictQuiet || rep.Slot.SourceWALAdvanceBytes != 0 {
		t.Errorf("report = %+v", rep)
	}
}

func TestStreamingLivenessNoSlot(t *testing.T) {
	// TC4: a missing slot names the connector; it is never reported healthy,
	// and another pipeline's slot is not used as a substitute.
	d, _ := livenessDeps(nil, nil)
	d.Src.(*fakeSource).slotReads = [][]clients.ReplicationSlot{
		{livenessSlot("payments_slot", "0/1000", "0/1000", 0)},
		{livenessSlot("payments_slot", "0/2000", "0/1000", 0)},
	}
	_, err := runLiveness(t, d, roomCtx(), `{"pipeline_id":"orders"}`)
	if err == nil {
		t.Fatal("a pipeline without its slot must fail, not read healthy")
	}
	if want := `no slot for connector "pg-orders-connector"`; !strings.Contains(err.Error(), want) {
		t.Errorf("error = %v, want it to contain %q", err, want)
	}
}

func TestStreamingLivenessWindowAndInput(t *testing.T) {
	reads := [][]clients.ReplicationSlot{
		{livenessSlot("orders_slot", "0/1000", "0/1000", 0)},
		{livenessSlot("orders_slot", "0/2000", "0/1000", 0x1000)},
	}
	d, _ := livenessDeps(reads...)
	res, err := runLiveness(t, d, roomCtx(), `{"pipeline_id":"orders","window_seconds":2}`)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Summary, "advanced 4096 bytes in 2s") || res.Data.(livenessReport).WindowSeconds != 2 {
		t.Errorf("summary = %q data = %+v", res.Summary, res.Data)
	}

	for _, in := range []string{
		`{"window_seconds":5}`, // pipeline_id missing
		`{"pipeline_id":"orders","window_seconds":0}`,
		`{"pipeline_id":"orders","window_seconds":9}`,
		`{"pipeline_id":"orders","window_seconds":"5"}`,
		`{`,
	} {
		if _, err := runLiveness(t, d, roomCtx(), in); !errors.Is(err, tools.ErrBadInput) {
			t.Errorf("input %s: error %v does not wrap ErrBadInput", in, err)
		}
	}
}

func TestStreamingLivenessScope(t *testing.T) {
	d, src := livenessDeps(
		[]clients.ReplicationSlot{livenessSlot("orders_slot", "0/1000", "0/1000", 0)},
		[]clients.ReplicationSlot{livenessSlot("orders_slot", "0/2000", "0/1000", 0x1000)},
	)
	// Another pipeline's id in the input is refused before any client call.
	if _, err := runLiveness(t, d, roomCtx(), `{"pipeline_id":"payments"}`); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("error = %v, want ErrOutOfScope", err)
	}
	if src.slotCalls != 0 {
		t.Fatalf("out-of-scope call reached the source %d times", src.slotCalls)
	}
	// No room scope and no opt-out.
	if _, err := runLiveness(t, d, context.Background(), `{"pipeline_id":"orders"}`); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("error = %v, want ErrNoScope", err)
	}
	// CLI/dev opt-out works on the input pipeline.
	if _, err := runLiveness(t, d, tools.WithUnscoped(context.Background()), `{"pipeline_id":"orders"}`); err != nil {
		t.Fatalf("unscoped: %v", err)
	}
}

func TestStreamingLivenessErrorPaths(t *testing.T) {
	base := func() (Deps, *fakeSource) {
		return livenessDeps(
			[]clients.ReplicationSlot{livenessSlot("orders_slot", "0/1000", "0/1000", 0)},
			[]clients.ReplicationSlot{livenessSlot("orders_slot", "0/1000", "0/1000", 0x2000)},
		)
	}

	t.Run("no source client", func(t *testing.T) {
		d, _ := base()
		d.Src = nil
		_, err := runLiveness(t, d, roomCtx(), `{"pipeline_id":"orders"}`)
		if err == nil || !strings.Contains(err.Error(), "not configured") {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("source read fails", func(t *testing.T) {
		d, src := base()
		src.slotErr = errors.New("connection refused")
		_, err := runLiveness(t, d, roomCtx(), `{"pipeline_id":"orders"}`)
		if err == nil || !strings.Contains(err.Error(), "connection refused") {
			t.Fatalf("error = %v", err)
		}
	})
	t.Run("slot disappears", func(t *testing.T) {
		d, _ := livenessDeps(
			[]clients.ReplicationSlot{livenessSlot("orders_slot", "0/1000", "0/1000", 0)},
			nil,
		)
		res, err := runLiveness(t, d, roomCtx(), `{"pipeline_id":"orders"}`)
		if err != nil {
			t.Fatal(err)
		}
		rep := res.Data.(livenessReport)
		if rep.Verdict != verdictUnknown || !strings.Contains(rep.Note, "disappeared") || !strings.Contains(res.Summary, "unknown:") {
			t.Fatalf("report = %+v summary = %q", rep, res.Summary)
		}
	})
	t.Run("slot appears", func(t *testing.T) {
		d, _ := livenessDeps(
			nil,
			[]clients.ReplicationSlot{livenessSlot("orders_slot", "0/1000", "0/1000", 0)},
		)
		res, err := runLiveness(t, d, roomCtx(), `{"pipeline_id":"orders"}`)
		if err != nil {
			t.Fatal(err)
		}
		rep := res.Data.(livenessReport)
		if rep.Verdict != verdictUnknown || !strings.Contains(rep.Note, "appeared") {
			t.Fatalf("report = %+v", rep)
		}
	})
	t.Run("flush goes backwards", func(t *testing.T) {
		d, _ := livenessDeps(
			[]clients.ReplicationSlot{livenessSlot("orders_slot", "0/2000", "0/1000", 0)},
			[]clients.ReplicationSlot{livenessSlot("orders_slot", "0/1000", "0/1000", 0)},
		)
		res, err := runLiveness(t, d, roomCtx(), `{"pipeline_id":"orders"}`)
		if err != nil {
			t.Fatal(err)
		}
		rep := res.Data.(livenessReport)
		if rep.Verdict != verdictUnknown || !strings.Contains(rep.Note, "backwards") || !strings.Contains(res.Summary, "unknown:") {
			t.Fatalf("report = %+v summary = %q", rep, res.Summary)
		}
	})
	t.Run("no confirmed position", func(t *testing.T) {
		// A physical slot has no confirmed_flush_lsn: unknown, never healthy.
		d, _ := livenessDeps(
			[]clients.ReplicationSlot{{Name: "orders_slot", Active: true, RestartLSN: "0/1000"}},
			[]clients.ReplicationSlot{{Name: "orders_slot", Active: true, RestartLSN: "0/1000"}},
		)
		res, err := runLiveness(t, d, roomCtx(), `{"pipeline_id":"orders"}`)
		if err != nil {
			t.Fatal(err)
		}
		rep := res.Data.(livenessReport)
		if rep.Verdict != verdictUnknown || !strings.Contains(rep.Note, "confirmed_flush_lsn") {
			t.Fatalf("report = %+v", rep)
		}
	})
	t.Run("no restart position", func(t *testing.T) {
		d, _ := livenessDeps(
			[]clients.ReplicationSlot{livenessSlot("orders_slot", "0/1000", "", 0)},
			[]clients.ReplicationSlot{livenessSlot("orders_slot", "0/1000", "", 0)},
		)
		res, err := runLiveness(t, d, roomCtx(), `{"pipeline_id":"orders"}`)
		if err != nil {
			t.Fatal(err)
		}
		rep := res.Data.(livenessReport)
		if rep.Verdict != verdictUnknown || !strings.Contains(rep.Note, "restart_lsn") {
			t.Fatalf("report = %+v", rep)
		}
	})
	t.Run("connect down is a note, not a lost verdict", func(t *testing.T) {
		d, src := base()
		d.Connect.(*fakeConnect).statusErr = errors.New("connect down")
		src.txns = nil
		res, err := runLiveness(t, d, roomCtx(), `{"pipeline_id":"orders"}`)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(res.Summary, "stuck:") {
			t.Errorf("verdict lost: %q", res.Summary)
		}
		if !strings.Contains(res.Summary, "connector status unavailable: connect down") {
			t.Errorf("summary lacks the status note: %q", res.Summary)
		}
		if res.Data.(livenessReport).Verdict != verdictStuck {
			t.Errorf("data = %+v", res.Data)
		}
	})
	t.Run("deadline respected", func(t *testing.T) {
		d, _ := base()
		d.Wait = func(ctx context.Context, _ time.Duration) error {
			<-ctx.Done()
			return ctx.Err()
		}
		ctx, cancel := context.WithTimeout(roomCtx(), 20*time.Millisecond)
		defer cancel()
		_, err := runLiveness(t, d, ctx, `{"pipeline_id":"orders"}`)
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("error = %v, want context.DeadlineExceeded", err)
		}
	})
}

func TestStreamingLivenessReadOnlyKind(t *testing.T) {
	reg := tools.NewReadOnlyRegistry()
	if err := Register(reg, testDeps()); err != nil {
		t.Fatal(err)
	}
	tl, ok := reg.Get("pg_streaming_liveness")
	if !ok {
		t.Fatal("pg_streaming_liveness not registered")
	}
	if tl.Kind() != tools.KindRead {
		t.Fatalf("kind = %s, want read", tl.Kind())
	}
	if !strings.Contains(tl.Description(), "long transaction") {
		t.Errorf("description must record the long-transaction caveat: %q", tl.Description())
	}
}

func TestParseLSN(t *testing.T) {
	cases := []struct {
		in   string
		want uint64
		ok   bool
	}{
		{"0/0", 0, true},
		{"0/16B6C50", 0x16B6C50, true},
		{"16/B374D848", 0x16B374D848, true},
		{"FFFFFFFF/FFFFFFFF", 1<<64 - 1, true},
		{" 1/A ", 1<<32 | 0xA, true},
		{"", 0, false},
		{"abc", 0, false},
		{"1/", 0, false},
		{"/1", 0, false},
		{"1/2/3", 0, false},
		{"1/G", 0, false},
		{"100000000/0", 0, false}, // more than 8 hex digits per part
	}
	for _, c := range cases {
		got, err := parseLSN(c.in)
		if (err == nil) != c.ok || (c.ok && got != c.want) {
			t.Errorf("parseLSN(%q) = %d, %v; want %d, ok=%t", c.in, got, err, c.want, c.ok)
		}
	}
}

func TestLivenessWindow(t *testing.T) {
	five := 5
	two := 2
	zero := 0
	nine := 9
	cases := []struct {
		in   *int
		want time.Duration
		ok   bool
	}{
		{nil, 5 * time.Second, true},
		{&five, 5 * time.Second, true},
		{&two, 2 * time.Second, true},
		{&zero, 0, false},
		{&nine, 0, false},
	}
	for _, c := range cases {
		got, err := livenessWindow(c.in)
		if (err == nil) != c.ok || (c.ok && got != c.want) {
			t.Errorf("livenessWindow(%v) = %v, %v; want %v, ok=%t", c.in, got, err, c.want, c.ok)
		}
	}
}
