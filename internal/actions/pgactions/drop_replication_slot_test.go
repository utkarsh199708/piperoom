package pgactions

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// ---- fakes ----

type fakeConnect struct {
	cfg   map[string]map[string]string
	err   error
	calls int
}

func (f *fakeConnect) Config(ctx context.Context, name string) (map[string]string, error) {
	f.calls++
	if _, ok := ctx.Deadline(); !ok {
		return nil, errors.New("fake: Config called without a deadline")
	}
	if f.err != nil {
		return nil, f.err
	}
	c, ok := f.cfg[name]
	if !ok {
		return nil, clients.ErrNotFound
	}
	return c, nil
}

// fakeSource is the read-only source client. It implements the named read
// unless plain is set.
type fakeSource struct {
	slots    []clients.ReplicationSlot
	err      error
	reads    int
	noNamed  bool
	deadline bool
}

func (f *fakeSource) ReplicationSlots(ctx context.Context) ([]clients.ReplicationSlot, error) {
	f.reads++
	_, f.deadline = ctx.Deadline()
	return f.slots, f.err
}
func (f *fakeSource) RecentDDL(context.Context, time.Time, int) ([]clients.DDLChange, error) {
	return nil, nil
}
func (f *fakeSource) LongTransactions(context.Context, time.Duration) ([]clients.Transaction, error) {
	return nil, nil
}
func (f *fakeSource) Row(context.Context, string, string, string) (map[string]any, bool, error) {
	return nil, false, nil
}

type namedSource struct{ *fakeSource }

func (n namedSource) ReplicationSlotsNamed(ctx context.Context, names []string) ([]clients.ReplicationSlot, int, error) {
	n.reads++
	_, n.deadline = ctx.Deadline()
	var out []clients.ReplicationSlot
	for _, s := range n.slots {
		if len(names) == 1 && s.Name == names[0] {
			out = append(out, s)
		}
	}
	return out, len(n.slots) - len(out), n.err
}

// fakeWriter records writes. With checked it implements the checked drop and
// answers from seen/err as the drop transaction would.
type fakeWriter struct {
	drops     []string
	allowLost []bool
	seen      postgres.SlotSeen
	err       error
}

func (w *fakeWriter) AddPublicationTables(context.Context, string, []string) error {
	return errors.New("fake: unexpected AddPublicationTables")
}
func (w *fakeWriter) DropReplicationSlot(_ context.Context, slot string) error {
	w.drops = append(w.drops, slot)
	return w.err
}
func (w *fakeWriter) InsertSnapshotSignal(context.Context, string, string, []string) (string, error) {
	return "", errors.New("fake: unexpected InsertSnapshotSignal")
}

type checkedWriter struct{ *fakeWriter }

func (c checkedWriter) DropReplicationSlotChecked(ctx context.Context, slot string, allowLost bool) (postgres.SlotSeen, error) {
	if _, ok := ctx.Deadline(); !ok {
		return postgres.SlotSeen{}, errors.New("fake: drop without a deadline")
	}
	if c.err != nil {
		return c.seen, c.err
	}
	c.drops = append(c.drops, slot)
	c.allowLost = append(c.allowLost, allowLost)
	return c.seen, nil
}

// ---- fixtures ----

const (
	conn = "pg-orders-connector"
	slot = "orders_slot"
)

func pipeline() owners.Pipeline {
	return owners.Pipeline{ID: "orders", TopicPrefix: "pg", SourceTables: []string{"public.orders"}, Connectors: []string{conn}}
}

func otherPipeline() owners.Pipeline {
	return owners.Pipeline{ID: "billing", TopicPrefix: "bill", SourceTables: []string{"public.invoices"}, Connectors: []string{"pg-billing-connector"}}
}

func inactive(name string) clients.ReplicationSlot {
	return clients.ReplicationSlot{Name: name, Plugin: "pgoutput", WALStatus: "extended", RetainedWALBytes: 3 << 30, RestartLSN: "0/16B3748"}
}

type fixture struct {
	src *fakeSource
	cn  *fakeConnect
	w   *fakeWriter
	a   *dropSlot
}

func newFixture(slots ...clients.ReplicationSlot) fixture {
	src := &fakeSource{slots: slots}
	cn := &fakeConnect{cfg: map[string]map[string]string{
		conn:                   {"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "slot.name": slot},
		"pg-billing-connector": {"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "slot.name": "billing_slot"},
	}}
	w := &fakeWriter{}
	a := &dropSlot{d: DropSlotDeps{Source: namedSource{src}, Connect: cn, Writer: checkedWriter{w}}}
	return fixture{src: src, cn: cn, w: w, a: a}
}

func params(t *testing.T, v map[string]any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func good(t *testing.T) json.RawMessage {
	return params(t, map[string]any{"connector": conn, "slot": slot})
}

func roomCtx() context.Context { return tools.WithPipeline(context.Background(), pipeline()) }

// ---- tests ----

func TestMetadata(t *testing.T) {
	a := NewDropReplicationSlot(DropSlotDeps{})
	if a.Name() != "pg_drop_replication_slot" || a.Layer() != "postgres" || a.Describe() == "" {
		t.Fatalf("metadata: %q %q %q", a.Name(), a.Layer(), a.Describe())
	}
	s := a.ParamsSchema()
	if s["additionalProperties"] != false {
		t.Fatal("schema must refuse unknown params")
	}
	req, _ := s["required"].([]string)
	if strings.Join(req, ",") != "connector,slot" {
		t.Fatalf("required = %v", req)
	}
}

func TestValidateAcceptsInactiveOwnSlot(t *testing.T) {
	f := newFixture(inactive(slot), inactive("billing_slot"))
	// Validate does not need a scope in ctx.
	if err := f.a.Validate(context.Background(), good(t)); err != nil {
		t.Fatal(err)
	}
	if !f.src.deadline {
		t.Fatal("slot read had no deadline")
	}
	if len(f.w.drops) != 0 {
		t.Fatal("Validate wrote")
	}
}

func TestValidateDoesNotNeedAWriter(t *testing.T) {
	f := newFixture(inactive(slot))
	f.a.d.Writer = nil
	if err := f.a.Validate(context.Background(), good(t)); err != nil {
		t.Fatalf("Validate with write mode off: %v", err)
	}
}

func TestValidateRefusals(t *testing.T) {
	lost := inactive(slot)
	lost.WALStatus = "lost"
	active := inactive(slot)
	active.Active = true
	physical := clients.ReplicationSlot{Name: slot, WALStatus: "reserved"}

	cases := []struct {
		name   string
		slots  []clients.ReplicationSlot
		params map[string]any
		want   error
		text   string
		noIO   bool
	}{
		{name: "active slot", slots: []clients.ReplicationSlot{active}, want: clients.ErrSlotActive},
		{name: "lost without force", slots: []clients.ReplicationSlot{lost}, want: ErrLostNeedsForce, text: "new snapshot"},
		{name: "lost with force false", slots: []clients.ReplicationSlot{lost}, params: map[string]any{"connector": conn, "slot": slot, "force": false}, want: ErrLostNeedsForce},
		{name: "missing slot", want: clients.ErrNotFound},
		{name: "physical slot", slots: []clients.ReplicationSlot{physical}, want: clients.ErrNotFound},
		{name: "another pipeline's slot", slots: []clients.ReplicationSlot{inactive("billing_slot")}, params: map[string]any{"connector": conn, "slot": "billing_slot"}, want: tools.ErrOutOfScope},
		{name: "unknown connector", params: map[string]any{"connector": "nope", "slot": slot}, want: clients.ErrNotFound},
		{name: "injection in slot", params: map[string]any{"connector": conn, "slot": "x'); DROP TABLE t; --"}, noIO: true},
		{name: "upper-case slot", params: map[string]any{"connector": conn, "slot": "Orders_slot"}, noIO: true},
		{name: "leading digit", params: map[string]any{"connector": conn, "slot": "1slot"}, noIO: true},
		{name: "too long", params: map[string]any{"connector": conn, "slot": strings.Repeat("s", 64)}, noIO: true},
		{name: "empty slot", params: map[string]any{"connector": conn, "slot": ""}, noIO: true},
		{name: "no connector", params: map[string]any{"slot": slot}, noIO: true},
		{name: "unknown param", params: map[string]any{"connector": conn, "slot": slot, "cascade": true}, noIO: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(tc.slots...)
			p := good(t)
			if tc.params != nil {
				p = params(t, tc.params)
			}
			err := f.a.Validate(context.Background(), p)
			if err == nil {
				t.Fatal("accepted")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("%v does not wrap %v", err, tc.want)
			}
			if tc.text != "" && !strings.Contains(err.Error(), tc.text) {
				t.Fatalf("%v does not say %q", err, tc.text)
			}
			if tc.noIO && (f.cn.calls != 0 || f.src.reads != 0) {
				t.Fatal("bad params reached Connect or Postgres")
			}
		})
	}
}

func TestValidateLostWithForce(t *testing.T) {
	lost := inactive(slot)
	lost.WALStatus = "lost"
	f := newFixture(lost)
	if err := f.a.Validate(context.Background(), params(t, map[string]any{"connector": conn, "slot": slot, "force": true})); err != nil {
		t.Fatal(err)
	}
}

func TestValidateConnectorWithoutSlot(t *testing.T) {
	f := newFixture(inactive(slot))
	f.cn.cfg[conn] = map[string]string{"connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector"}
	if err := f.a.Validate(context.Background(), good(t)); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("sink connector: %v", err)
	}
	f.cn.cfg[conn] = map[string]string{"slot.name": "${file:/x:slot}"}
	if err := f.a.Validate(context.Background(), good(t)); !errors.Is(err, owners.ErrInvalidSlotName) {
		t.Fatalf("unresolved slot.name: %v", err)
	}
}

func TestValidateFallsBackToPlainSlotRead(t *testing.T) {
	f := newFixture(inactive("billing_slot"), inactive(slot))
	f.a.d.Source = f.src // no ReplicationSlotsNamed
	if err := f.a.Validate(context.Background(), good(t)); err != nil {
		t.Fatal(err)
	}
	if f.src.reads != 1 {
		t.Fatalf("reads = %d", f.src.reads)
	}
}

func TestValidateReadErrors(t *testing.T) {
	f := newFixture(inactive(slot))
	f.cn.err = errors.New("connect down")
	if err := f.a.Validate(context.Background(), good(t)); err == nil || !strings.Contains(err.Error(), "connect down") {
		t.Fatalf("connect down: %v", err)
	}
	f = newFixture(inactive(slot))
	f.src.err = context.DeadlineExceeded
	if err := f.a.Validate(context.Background(), good(t)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("postgres timeout: %v", err)
	}
}

func TestCheckScope(t *testing.T) {
	a := NewDropReplicationSlot(DropSlotDeps{}).(*dropSlot)
	if err := a.CheckScope(pipeline(), good(t)); err != nil {
		t.Fatal(err)
	}
	if err := a.CheckScope(otherPipeline(), good(t)); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("other pipeline: %v", err)
	}
	if err := a.CheckScope(pipeline(), params(t, map[string]any{"connector": conn, "slot": "Bad-Name"})); err == nil {
		t.Fatal("bad slot name in scope")
	}
}

func TestCheckScopeDeclaredWatchSlot(t *testing.T) {
	path := filepath.Join(t.TempDir(), "owners.yaml")
	yml := `
pipelines:
  - id: orders
    topic_prefix: pg
    source_tables: [public.orders]
    connectors: [pg-orders-connector]
    watch:
      slot: declared_slot
layers:
  postgres: priya
  kafka-connect: arjun
  schema-registry: priya
  kafka: arjun
  consumer: kabir
  warehouse: meera
`
	if err := os.WriteFile(path, []byte(yml), 0o600); err != nil {
		t.Fatal(err)
	}
	own, err := owners.Load(path)
	if err != nil {
		t.Fatalf("owners.Load: %v", err)
	}
	p, ok := own.Pipeline("orders")
	if !ok || p.Watch().Slot != "declared_slot" {
		t.Fatalf("fixture: %v %+v", ok, p.Watch())
	}
	a := &dropSlot{}
	if err := a.CheckScope(p, good(t)); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("slot other than watch.slot: %v", err)
	}
	if err := a.CheckScope(p, params(t, map[string]any{"connector": conn, "slot": "declared_slot"})); err != nil {
		t.Fatal(err)
	}
}

func TestExecuteDrops(t *testing.T) {
	f := newFixture(inactive(slot))
	f.w.seen = postgres.SlotSeen{Name: slot, Plugin: "pgoutput", WALStatus: "extended", RetainedWALBytes: 3 << 30}
	out, err := f.a.Execute(roomCtx(), good(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(f.w.drops) != 1 || f.w.drops[0] != slot || f.w.allowLost[0] {
		t.Fatalf("drops = %v allowLost = %v", f.w.drops, f.w.allowLost)
	}
	for _, want := range []string{"dropped replication slot orders_slot", "pgoutput", "extended", "3.0 GiB", "new snapshot"} {
		if !strings.Contains(out, want) {
			t.Fatalf("result %q lacks %q", out, want)
		}
	}
}

func TestExecuteLostWithForcePassesAllowLost(t *testing.T) {
	f := newFixture()
	_, err := f.a.Execute(roomCtx(), params(t, map[string]any{"connector": conn, "slot": slot, "force": true}))
	if err != nil {
		t.Fatal(err)
	}
	if !f.w.allowLost[0] {
		t.Fatal("force did not reach the writer")
	}
}

func TestExecuteRefusals(t *testing.T) {
	cases := []struct {
		name string
		ctx  context.Context
		raw  map[string]any
		werr error
		want error
		text string
		io   bool // the refusal needs the connector config
	}{
		{name: "no scope", ctx: context.Background(), want: tools.ErrNoScope},
		{name: "pipeline id only", ctx: tools.WithPipelineID(context.Background(), "orders"), want: tools.ErrNoScope},
		{name: "another pipeline's room", ctx: tools.WithPipeline(context.Background(), otherPipeline()), want: tools.ErrOutOfScope},
		{name: "another pipeline's slot", raw: map[string]any{"connector": conn, "slot": "billing_slot"}, want: tools.ErrOutOfScope, io: true},
		// The race: the slot was inactive at Validate and Preview, then a
		// consumer attached; the drop transaction's re-read (or Postgres)
		// refuses it and nothing is dropped.
		{name: "became active", werr: clients.ErrSlotActive, want: clients.ErrSlotActive, text: "nothing dropped"},
		{name: "became lost", werr: postgres.ErrSlotLost, want: ErrLostNeedsForce},
		{name: "gone", werr: clients.ErrNotFound, want: clients.ErrNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(inactive(slot))
			f.w.err = tc.werr
			ctx := tc.ctx
			if ctx == nil {
				ctx = roomCtx()
			}
			raw := good(t)
			if tc.raw != nil {
				raw = params(t, tc.raw)
			}
			_, err := f.a.Execute(ctx, raw)
			if !errors.Is(err, tc.want) {
				t.Fatalf("%v does not wrap %v", err, tc.want)
			}
			if tc.text != "" && !strings.Contains(err.Error(), tc.text) {
				t.Fatalf("%v lacks %q", err, tc.text)
			}
			if len(f.w.drops) != 0 {
				t.Fatalf("dropped %v", f.w.drops)
			}
			if tc.werr == nil && !tc.io && f.cn.calls != 0 {
				t.Fatal("a scope refusal did I/O")
			}
		})
	}
}

func TestExecuteNoWriteCredentials(t *testing.T) {
	f := newFixture(inactive(slot))
	f.a.d.Writer = nil
	if _, err := f.a.Execute(roomCtx(), good(t)); !errors.Is(err, errNoWriteCredentials) {
		t.Fatalf("no writer: %v", err)
	}
}

// A writer without the checked drop: Execute re-reads through the source
// client right before the drop and refuses what Validate would.
func TestExecutePlainWriterRereads(t *testing.T) {
	active := inactive(slot)
	active.Active = true
	f := newFixture(active)
	f.a.d.Writer = f.w
	if _, err := f.a.Execute(roomCtx(), good(t)); !errors.Is(err, clients.ErrSlotActive) {
		t.Fatalf("active: %v", err)
	}
	if len(f.w.drops) != 0 {
		t.Fatal("dropped an active slot")
	}
	f = newFixture(inactive(slot))
	f.a.d.Writer = f.w
	out, err := f.a.Execute(roomCtx(), good(t))
	if err != nil || len(f.w.drops) != 1 || !strings.Contains(out, "pgoutput") {
		t.Fatalf("plain writer: %q %v %v", out, err, f.w.drops)
	}
	// Postgres refuses a slot that turned active after the re-read.
	f = newFixture(inactive(slot))
	f.w.err = clients.ErrSlotActive
	f.a.d.Writer = f.w
	if _, err := f.a.Execute(roomCtx(), good(t)); !errors.Is(err, clients.ErrSlotActive) {
		t.Fatalf("race: %v", err)
	}
}

func TestPreview(t *testing.T) {
	f := newFixture(inactive(slot))
	f.a.d.Writer = nil // Preview never needs one
	res, err := f.a.Preview(roomCtx(), good(t))
	if err != nil {
		t.Fatal(err)
	}
	if res.Reversible || res.Rollback != "" {
		t.Fatal("a slot drop is not reversible")
	}
	all := res.Target + "\n" + res.Current + "\n" + res.Expected + "\n" + strings.Join(res.Downstream, "\n")
	for _, want := range []string{"orders_slot", conn, "pgoutput", "inactive", "wal_status extended", "3.0 GiB", "pg_wal_lsn_diff", "NOT reversible", "new snapshot", "cannot resume"} {
		if !strings.Contains(all, want) {
			t.Fatalf("preview lacks %q:\n%s", want, all)
		}
	}
}

func TestPreviewShowsRefusal(t *testing.T) {
	active := inactive(slot)
	active.Active = true
	f := newFixture(active)
	res, err := f.a.Preview(roomCtx(), good(t))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Current, "ACTIVE") || !strings.Contains(res.Expected, "refused") {
		t.Fatalf("preview of an active slot: %+v", res)
	}
}

func TestPreviewNeverWrites(t *testing.T) {
	f := newFixture(inactive(slot))
	if _, err := f.a.Preview(roomCtx(), good(t)); err != nil {
		t.Fatal(err)
	}
	if len(f.w.drops) != 0 {
		t.Fatal("Preview called the writer")
	}
}

func TestPreviewScope(t *testing.T) {
	f := newFixture(inactive(slot))
	if _, err := f.a.Preview(context.Background(), good(t)); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("no scope: %v", err)
	}
	if _, err := f.a.Preview(tools.WithPipeline(context.Background(), otherPipeline()), good(t)); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("other pipeline: %v", err)
	}
	if f.cn.calls != 0 || f.src.reads != 0 {
		t.Fatal("scope refusal did I/O")
	}
}

func TestTimeoutBoundsReads(t *testing.T) {
	f := newFixture(inactive(slot))
	f.a.d.Timeout = time.Nanosecond
	f.a.d.Connect = blockingConnect{}
	start := time.Now()
	err := f.a.Validate(context.Background(), good(t))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("hung Connect: %v", err)
	}
	if time.Since(start) > 5*time.Second {
		t.Fatal("deadline not applied")
	}
}

type blockingConnect struct{}

func (blockingConnect) Config(ctx context.Context, _ string) (map[string]string, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
