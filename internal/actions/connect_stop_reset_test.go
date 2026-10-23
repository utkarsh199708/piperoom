package actions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

const lifecycleConnector = "pg-orders-connector"

// pgSourceClass is the connector class of the dev stack's Debezium source.
const pgSourceClass = "io.debezium.connector.postgresql.PostgresConnector"

// lifecycleFake implements clients.ConnectClient, connect.SourceOffsetReader,
// connect.ConnectLifecycleWriter and the config validator. Writes are refused
// unless a test opts in, so a Preview that wrote would fail the test.
type lifecycleFake struct {
	names     []string
	status    map[string]clients.ConnectorStatus
	statusErr error
	configs   map[string]map[string]string
	configErr error
	offsets   map[string][]connect.SourceOffset
	offErr    error

	vr            connect.ValidateResult
	validateErr   error
	validateClass string
	validated     map[string]string

	createErr, deleteErr, stopErr, resetErr, alterErr error

	allowWrites bool
	refusals    atomic.Int32

	listCalls, statusCalls, configCalls, offsetCalls, validateCalls int
	stopped, reset, deleted                                         []string
	altered                                                         map[string][]connect.SourceOffset
	created                                                         map[string]map[string]string
}

func (f *lifecycleFake) refusedWrites() int32 { return f.refusals.Load() }

func (f *lifecycleFake) ListConnectors(context.Context) ([]string, error) {
	f.listCalls++
	return append([]string(nil), f.names...), nil
}

func (f *lifecycleFake) Status(_ context.Context, name string) (clients.ConnectorStatus, error) {
	f.statusCalls++
	if f.statusErr != nil {
		return clients.ConnectorStatus{}, f.statusErr
	}
	st, ok := f.status[name]
	if !ok {
		return clients.ConnectorStatus{}, fmt.Errorf("no such connector %q", name)
	}
	return st, nil
}

func (f *lifecycleFake) Config(_ context.Context, name string) (map[string]string, error) {
	f.configCalls++
	if f.configErr != nil {
		return nil, f.configErr
	}
	cfg := map[string]string{}
	for k, v := range f.configs[name] {
		cfg[k] = v
	}
	return cfg, nil
}

func (f *lifecycleFake) RestartTask(context.Context, string, int) error {
	return refuseWrite(&f.refusals, "lifecycleFake", "RestartTask")
}
func (f *lifecycleFake) RestartConnector(context.Context, string) error {
	return refuseWrite(&f.refusals, "lifecycleFake", "RestartConnector")
}

func (f *lifecycleFake) SourceOffsets(_ context.Context, name string) ([]connect.SourceOffset, error) {
	f.offsetCalls++
	if f.offErr != nil {
		return nil, f.offErr
	}
	return append([]connect.SourceOffset(nil), f.offsets[name]...), nil
}

func (f *lifecycleFake) StopConnector(_ context.Context, name string) error {
	if !f.allowWrites {
		return refuseWrite(&f.refusals, "lifecycleFake", "StopConnector")
	}
	if f.stopErr != nil {
		return f.stopErr
	}
	f.stopped = append(f.stopped, name)
	st := f.status[name]
	st.State = "STOPPED"
	for i := range st.Tasks {
		st.Tasks[i].State = "STOPPED"
	}
	f.status[name] = st
	return nil
}

func (f *lifecycleFake) ResetSourceOffsets(_ context.Context, name string) error {
	if !f.allowWrites {
		return refuseWrite(&f.refusals, "lifecycleFake", "ResetSourceOffsets")
	}
	if f.resetErr != nil {
		return f.resetErr
	}
	f.reset = append(f.reset, name)
	f.offsets[name] = nil
	return nil
}

func (f *lifecycleFake) AlterSourceOffsets(_ context.Context, name string, offs []connect.SourceOffset) error {
	if !f.allowWrites {
		return refuseWrite(&f.refusals, "lifecycleFake", "AlterSourceOffsets")
	}
	if f.alterErr != nil {
		return f.alterErr
	}
	if f.altered == nil {
		f.altered = map[string][]connect.SourceOffset{}
	}
	f.altered[name] = offs
	f.offsets[name] = append([]connect.SourceOffset(nil), offs...)
	return nil
}

func (f *lifecycleFake) CreateConnector(_ context.Context, name string, cfg map[string]string) error {
	if !f.allowWrites {
		return refuseWrite(&f.refusals, "lifecycleFake", "CreateConnector")
	}
	if f.createErr != nil {
		return f.createErr
	}
	if f.created == nil {
		f.created = map[string]map[string]string{}
	}
	cp := map[string]string{}
	for k, v := range cfg {
		cp[k] = v
	}
	f.created[name] = cp
	f.names = append(f.names, name)
	f.status[name] = clients.ConnectorStatus{Name: name, Type: "source", State: "RUNNING", Tasks: []clients.TaskStatus{{ID: 0, State: "RUNNING"}}}
	return nil
}

func (f *lifecycleFake) DeleteConnector(_ context.Context, name string) error {
	if !f.allowWrites {
		return refuseWrite(&f.refusals, "lifecycleFake", "DeleteConnector")
	}
	if f.deleteErr != nil {
		return f.deleteErr
	}
	f.deleted = append(f.deleted, name)
	var names []string
	for _, n := range f.names {
		if n != name {
			names = append(names, n)
		}
	}
	f.names = names
	delete(f.status, name)
	delete(f.configs, name)
	return nil
}

func (f *lifecycleFake) ValidateConfig(_ context.Context, class string, cfg map[string]string) (connect.ValidateResult, error) {
	f.validateCalls++
	f.validateClass = class
	f.validated = map[string]string{}
	for k, v := range cfg {
		f.validated[k] = v
	}
	return f.vr, f.validateErr
}

// noExtras hides the optional interfaces (writer, offset reader, validator),
// as a read-only or older client does.
type noExtras struct{ clients.ConnectClient }

func newLifecycleFake() *lifecycleFake {
	return &lifecycleFake{
		names:  []string{lifecycleConnector},
		status: map[string]clients.ConnectorStatus{lifecycleConnector: {Name: lifecycleConnector, Type: "source", State: "STOPPED", Tasks: []clients.TaskStatus{{ID: 0, State: "STOPPED"}}}},
		configs: map[string]map[string]string{lifecycleConnector: {
			"connector.class":    pgSourceClass,
			"snapshot.mode":      "initial",
			"topic.prefix":       "orders",
			"table.include.list": "public.orders",
		}},
		offsets: map[string][]connect.SourceOffset{lifecycleConnector: {{
			Partition: map[string]any{"server": "orders"},
			Offset: map[string]any{
				"lsn":     json.Number("12345678"),
				"txId":    json.Number("42"),
				"ts_usec": json.Number("1700000000000000"),
			},
		}}},
	}
}

func (f *lifecycleFake) setState(state string) {
	st := f.status[lifecycleConnector]
	st.State = state
	f.status[lifecycleConnector] = st
}

// lifecycleSource is the source client Preview consults for a snapshot's size.
type lifecycleSource struct {
	rows  map[string]int64
	err   error
	calls int
}

func (s *lifecycleSource) ReplicationSlots(context.Context) ([]clients.ReplicationSlot, error) {
	return nil, nil
}
func (s *lifecycleSource) RecentDDL(context.Context, time.Time, int) ([]clients.DDLChange, error) {
	return nil, nil
}
func (s *lifecycleSource) LongTransactions(context.Context, time.Duration) ([]clients.Transaction, error) {
	return nil, nil
}
func (s *lifecycleSource) Row(context.Context, string, string, string) (map[string]any, bool, error) {
	return nil, false, nil
}
func (s *lifecycleSource) EstimatedRows(_ context.Context, table string) (int64, error) {
	s.calls++
	if s.err != nil {
		return 0, s.err
	}
	if n, ok := s.rows[table]; ok {
		return n, nil
	}
	return -1, nil
}

func stopAction(t *testing.T, c clients.ConnectClient) *stopConnector {
	t.Helper()
	a := &stopConnector{d: LifecycleDeps{Connect: c, PollInterval: time.Millisecond}}
	if w, ok := c.(writeCounter); ok {
		guardWrites(t, w)
	}
	return a
}

func resetAction(t *testing.T, c clients.ConnectClient, src clients.SourceReader) *resetSourceOffsets {
	t.Helper()
	a := &resetSourceOffsets{d: LifecycleDeps{Connect: c, Source: src}}
	if w, ok := c.(writeCounter); ok {
		guardWrites(t, w)
	}
	return a
}

const stopIn = `{"connector":"pg-orders-connector"}`
const resetEmptyIn = `{"connector":"pg-orders-connector"}`

// resetAtIn is a position with the same partition and keys the fake stores.
const resetAtIn = `{"connector":"pg-orders-connector","stop_first":true,"position":{"partition":{"server":"orders"},"offset":{"lsn":999,"txId":42,"ts_usec":1700000000000000}}}`

func TestStopConnectorValidate(t *testing.T) {
	f := newLifecycleFake()
	a := stopAction(t, f)
	if err := a.Validate(scoped(), json.RawMessage(`{"connector":""}`)); err == nil {
		t.Fatal("empty connector accepted")
	}
	err := a.Validate(otherPipeline(), json.RawMessage(stopIn))
	if !errors.Is(err, tools.ErrOutOfScope) || f.listCalls != 0 {
		t.Fatalf("out of scope: err=%v listCalls=%d, want ErrOutOfScope before any I/O", err, f.listCalls)
	}
	if err := a.Validate(scoped(), json.RawMessage(`{"connector":"pg-inventory-connector"}`)); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("a connector of another pipeline: %v", err)
	}
	f.names = nil
	if err := a.Validate(scoped(), json.RawMessage(stopIn)); err == nil || !strings.Contains(err.Error(), "not found") {
		t.Fatalf("a missing connector: %v", err)
	}
	f.names = []string{lifecycleConnector}
	if err := a.Validate(scoped(), json.RawMessage(stopIn)); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if err := a.CheckScope(testPipeline(), json.RawMessage(stopIn)); err != nil {
		t.Fatalf("CheckScope: %v", err)
	}
	if err := a.CheckScope(owners.Pipeline{ID: "other"}, json.RawMessage(stopIn)); err == nil {
		t.Fatal("CheckScope accepted another pipeline")
	}
}

func TestStopConnectorPreviewAndExecute(t *testing.T) {
	f := newLifecycleFake()
	f.setState("RUNNING")
	a := stopAction(t, f)
	res, err := a.Preview(scoped(), json.RawMessage(stopIn))
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if !strings.Contains(res.Current, "RUNNING") || !strings.Contains(res.Expected, "STOPPED") {
		t.Fatalf("preview = %+v", res)
	}
	if !res.Reversible || res.Rollback != `connect_resume_connector {"connector":"pg-orders-connector"}` {
		t.Fatalf("rollback = %q reversible=%v", res.Rollback, res.Reversible)
	}
	if f.refusedWrites() != 0 || len(f.stopped) != 0 {
		t.Fatal("Preview wrote")
	}

	f.allowWrites = true
	out, err := a.Execute(scoped(), json.RawMessage(stopIn))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(f.stopped) != 1 || f.stopped[0] != lifecycleConnector || !strings.Contains(out, "stopped "+lifecycleConnector+" (STOPPED)") {
		t.Fatalf("stopped=%v out=%q", f.stopped, out)
	}
}

func TestStopConnectorAlreadyStoppedWritesNothing(t *testing.T) {
	f := newLifecycleFake() // STOPPED
	f.allowWrites = true
	a := stopAction(t, f)
	res, err := a.Preview(scoped(), json.RawMessage(stopIn))
	if err != nil || !strings.Contains(res.Expected, "no change") || res.Reversible {
		t.Fatalf("preview of a stopped connector: %+v %v", res, err)
	}
	out, err := a.Execute(scoped(), json.RawMessage(stopIn))
	if err != nil || !strings.Contains(out, "already STOPPED") || len(f.stopped) != 0 {
		t.Fatalf("out=%q err=%v stopped=%v", out, err, f.stopped)
	}
}

func TestStopConnectorWithoutWriter(t *testing.T) {
	f := newLifecycleFake()
	f.setState("RUNNING")
	a := &stopConnector{d: LifecycleDeps{Connect: noExtras{f}}}
	if _, err := a.Execute(scoped(), json.RawMessage(stopIn)); err == nil || !strings.Contains(err.Error(), "write mode") {
		t.Fatalf("Execute without a lifecycle writer: %v", err)
	}
}

func TestSourceOffsetResetRunningNeedsStopStep(t *testing.T) {
	f := newLifecycleFake()
	f.setState("RUNNING")
	a := resetAction(t, f, nil)

	if err := a.Validate(scoped(), json.RawMessage(resetEmptyIn)); !errors.Is(err, ErrStopStepRequired) {
		t.Fatalf("Validate: %v, want ErrStopStepRequired", err)
	}
	if _, err := a.Preview(scoped(), json.RawMessage(resetEmptyIn)); !errors.Is(err, ErrStopStepRequired) {
		t.Fatalf("Preview: %v, want ErrStopStepRequired", err)
	}
	// The stop step is promised: Validate passes at proposal creation.
	if err := a.Validate(scoped(), json.RawMessage(`{"connector":"pg-orders-connector","stop_first":true}`)); err != nil {
		t.Fatalf("Validate with stop_first: %v", err)
	}
	// Execute re-checks the real state: the promised stop did not run.
	if _, err := a.Execute(scoped(), json.RawMessage(`{"connector":"pg-orders-connector","stop_first":true}`)); !errors.Is(err, ErrStopStepRequired) {
		t.Fatalf("Execute on a still-running connector: %v, want ErrStopStepRequired", err)
	}
	if len(f.reset) != 0 {
		t.Fatal("Execute reset while running")
	}
	res, err := a.Preview(scoped(), json.RawMessage(`{"connector":"pg-orders-connector","stop_first":true}`))
	if err != nil || !strings.Contains(res.Current, "RUNNING; it must be stopped first") {
		t.Fatalf("preview with the stop step promised = %+v %v", res, err)
	}
}

func TestSourceOffsetResetRefusals(t *testing.T) {
	tests := []struct {
		name string
		prep func(*lifecycleFake)
		in   string
		want error
		text string
	}{
		{"sink connector", func(f *lifecycleFake) {
			st := f.status[lifecycleConnector]
			st.Type = "sink"
			f.status[lifecycleConnector] = st
		}, resetEmptyIn, nil, "only a source connector"},
		{"offsets read fails", func(f *lifecycleFake) { f.offErr = errors.New("connect 500") }, resetEmptyIn, nil, "read stored offsets"},
		{"another partition", nil, `{"connector":"pg-orders-connector","stop_first":true,"position":{"partition":{"server":"billing"},"offset":{"lsn":999,"txId":42,"ts_usec":1700000000000000}}}`, ErrSourcePosition, `{"server":"orders"}`},
		{"missing offset key", nil, `{"connector":"pg-orders-connector","stop_first":true,"position":{"partition":{"server":"orders"},"offset":{"lsn":999,"txId":42}}}`, ErrSourcePosition, "missing ts_usec"},
		{"extra offset key", nil, `{"connector":"pg-orders-connector","stop_first":true,"position":{"partition":{"server":"orders"},"offset":{"lsn":999,"txId":42,"ts_usec":1,"gtids":"x"}}}`, ErrSourcePosition, "not stored by the source: gtids"},
		{"string for a number", nil, `{"connector":"pg-orders-connector","stop_first":true,"position":{"partition":{"server":"orders"},"offset":{"lsn":"999","txId":42,"ts_usec":1}}}`, ErrSourcePosition, "is a number"},
		{"negative number", nil, `{"connector":"pg-orders-connector","stop_first":true,"position":{"partition":{"server":"orders"},"offset":{"lsn":-1,"txId":42,"ts_usec":1}}}`, ErrSourcePosition, "non-negative"},
		{"fraction", nil, `{"connector":"pg-orders-connector","stop_first":true,"position":{"partition":{"server":"orders"},"offset":{"lsn":1.5,"txId":42,"ts_usec":1}}}`, ErrSourcePosition, "non-negative"},
		{"no stored shape, another partition", func(f *lifecycleFake) { f.offsets[lifecycleConnector] = nil }, `{"connector":"pg-orders-connector","stop_first":true,"position":{"partition":{"server":"billing"},"offset":{"lsn":999}}}`, ErrSourcePosition, "Debezium partition is"},
		{"full reset of two offsets", func(f *lifecycleFake) {
			f.offsets[lifecycleConnector] = append(f.offsets[lifecycleConnector], connect.SourceOffset{Partition: map[string]any{"server": "orders2"}, Offset: map[string]any{"lsn": json.Number("1")}})
		}, resetEmptyIn, ErrMultipleSourceOffsets, "stores 2 source offsets"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newLifecycleFake()
			if tc.prep != nil {
				tc.prep(f)
			}
			a := resetAction(t, f, nil)
			err := a.Validate(scoped(), json.RawMessage(tc.in))
			if err == nil {
				t.Fatal("Validate accepted the step")
			}
			if tc.want != nil && !errors.Is(err, tc.want) {
				t.Fatalf("err = %v, want %v", err, tc.want)
			}
			if tc.text != "" && !strings.Contains(err.Error(), tc.text) {
				t.Fatalf("err %q does not contain %q", err, tc.text)
			}
			if f.refusedWrites() != 0 || len(f.reset) != 0 || len(f.altered) != 0 {
				t.Fatal("a refusal wrote")
			}
		})
	}
}

func TestSourceOffsetResetValidateOutOfScopeBeforeIO(t *testing.T) {
	f := newLifecycleFake()
	a := resetAction(t, f, nil)
	if err := a.Validate(otherPipeline(), json.RawMessage(resetEmptyIn)); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("Validate: %v, want ErrOutOfScope", err)
	}
	if f.listCalls != 0 || f.offsetCalls != 0 || f.statusCalls != 0 {
		t.Fatalf("reads before the scope refusal: list=%d status=%d offsets=%d", f.listCalls, f.statusCalls, f.offsetCalls)
	}
	if _, err := a.Preview(otherPipeline(), json.RawMessage(resetEmptyIn)); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("Preview: %v, want ErrOutOfScope", err)
	}
}

func TestSourceOffsetResetPreviewRollbackAndSnapshot(t *testing.T) {
	f := newLifecycleFake()
	src := &lifecycleSource{rows: map[string]int64{"public.orders": 500}}
	a := resetAction(t, f, src)
	res, err := a.Preview(scoped(), json.RawMessage(resetEmptyIn))
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if !strings.Contains(res.Current, `"lsn":12345678`) || !strings.Contains(res.Current, "STOPPED") {
		t.Fatalf("Current = %q", res.Current)
	}
	if !strings.Contains(res.Expected, "snapshot.mode=initial") || !strings.Contains(res.Expected, "~500 rows") || !strings.Contains(res.Expected, "op r") {
		t.Fatalf("Expected = %q", res.Expected)
	}
	if !res.Reversible || !strings.HasPrefix(res.Rollback, ResetSourceOffsetsName+" {") || !strings.Contains(res.Rollback, `"lsn":12345678`) {
		t.Fatalf("rollback = %q", res.Rollback)
	}
	if f.refusedWrites() != 0 || len(f.reset) != 0 {
		t.Fatal("Preview wrote")
	}

	pos, err := a.Preview(scoped(), json.RawMessage(resetAtIn))
	if err != nil {
		t.Fatalf("Preview of a position: %v", err)
	}
	if !strings.Contains(pos.Expected, "replaced by") || !strings.Contains(pos.Expected, "does not snapshot while an offset exists") {
		t.Fatalf("position Expected = %q", pos.Expected)
	}
	if !strings.Contains(pos.Rollback, `"lsn":12345678`) {
		t.Fatalf("position rollback = %q", pos.Rollback)
	}
}

func TestSourceOffsetResetPreviewSnapshotModes(t *testing.T) {
	tests := []struct {
		mode string
		want string
	}{
		{"", "initial (unset"},
		{"initial", "op r"},
		{"when_needed", "op r"},
		{"always", "every start"},
		{"initial_only", "does not stream"},
		{"schema_only", "no data snapshot"},
		{"never", "no snapshot"},
		{"exported", "exported snapshot"},
		{"custom", "custom snapshotter"},
		{"nonsense", "not one Debezium documents"},
	}
	for _, tc := range tests {
		t.Run(tc.mode, func(t *testing.T) {
			f := newLifecycleFake()
			if tc.mode == "" {
				delete(f.configs[lifecycleConnector], "snapshot.mode")
			} else {
				f.configs[lifecycleConnector]["snapshot.mode"] = tc.mode
			}
			a := resetAction(t, f, &lifecycleSource{rows: map[string]int64{"public.orders": 10}})
			res, err := a.Preview(scoped(), json.RawMessage(resetEmptyIn))
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(res.Expected, tc.want) {
				t.Fatalf("mode %q: Expected = %q, want %q", tc.mode, res.Expected, tc.want)
			}
		})
	}
}

func TestSourceOffsetResetPreviewWithoutEstimateOrOffsets(t *testing.T) {
	f := newLifecycleFake()
	a := resetAction(t, f, nil)
	res, err := a.Preview(scoped(), json.RawMessage(resetEmptyIn))
	if err != nil || !strings.Contains(res.Expected, "row estimate unavailable") {
		t.Fatalf("no source client: %+v %v", res, err)
	}
	if err := a.Validate(context.Background(), json.RawMessage(resetEmptyIn)); err != nil {
		t.Fatalf("unscoped Validate: %v", err)
	}
	// A table estimate needs the room's pipeline in ctx.
	f2 := newLifecycleFake()
	a2 := resetAction(t, f2, &lifecycleSource{rows: map[string]int64{"public.orders": 10}})
	res2, err := a2.Preview(context.Background(), json.RawMessage(resetEmptyIn))
	if err != nil || !strings.Contains(res2.Expected, "lists no source tables") {
		t.Fatalf("no pipeline: %+v %v", res2, err)
	}

	f3 := newLifecycleFake()
	f3.offsets[lifecycleConnector] = nil
	a3 := resetAction(t, f3, nil)
	res3, err := a3.Preview(scoped(), json.RawMessage(resetEmptyIn))
	if err != nil || !strings.Contains(res3.Expected, "no change") || res3.Reversible {
		t.Fatalf("no offsets: %+v %v", res3, err)
	}
}

func TestSourceOffsetResetPreviewWithoutOffsetReader(t *testing.T) {
	f := newLifecycleFake()
	a := &resetSourceOffsets{d: LifecycleDeps{Connect: noExtras{f}}}
	if err := a.Validate(scoped(), json.RawMessage(resetEmptyIn)); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("Validate without a reader: %v, want ErrNotSupported", err)
	}
}

func TestSourceOffsetResetExecute(t *testing.T) {
	f := newLifecycleFake()
	f.allowWrites = true
	a := resetAction(t, f, nil)
	out, err := a.Execute(scoped(), json.RawMessage(resetEmptyIn))
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(f.reset) != 1 || !strings.Contains(out, "cleared the stored offsets") || !strings.Contains(out, "no offsets") {
		t.Fatalf("reset=%v out=%q", f.reset, out)
	}

	// Position: the fake replaces its one stored offset.
	f2 := newLifecycleFake()
	f2.allowWrites = true
	a2 := resetAction(t, f2, nil)
	out, err = a2.Execute(scoped(), json.RawMessage(resetAtIn))
	if err != nil {
		t.Fatalf("Execute at a position: %v", err)
	}
	got := f2.altered[lifecycleConnector]
	if len(got) != 1 || !sameJSON(got[0].Partition, map[string]any{"server": "orders"}) {
		t.Fatalf("altered = %+v", got)
	}
	if !strings.Contains(out, "set the stored offset") || !strings.Contains(out, `"lsn":999`) {
		t.Fatalf("out = %q", out)
	}

	// The same position again writes nothing.
	f3 := newLifecycleFake()
	f3.allowWrites = true
	f3.offsets[lifecycleConnector][0].Offset["lsn"] = json.Number("999")
	a3 := resetAction(t, f3, nil)
	out, err = a3.Execute(scoped(), json.RawMessage(resetAtIn))
	if err != nil || !strings.Contains(out, "already at") || len(f3.altered) != 0 {
		t.Fatalf("no-op position: out=%q err=%v altered=%v", out, err, f3.altered)
	}

	// A Connect refusal for a running connector stays typed.
	f4 := newLifecycleFake()
	f4.allowWrites = true
	f4.alterErr = connect.ErrConnectorNotStopped
	a4 := resetAction(t, f4, nil)
	if _, err := a4.Execute(scoped(), json.RawMessage(resetAtIn)); !errors.Is(err, ErrStopStepRequired) || !errors.Is(err, connect.ErrConnectorNotStopped) {
		t.Fatalf("not-stopped write error: %v", err)
	}
}

func TestSourceOffsetResetAfterFullReset(t *testing.T) {
	f := newLifecycleFake()
	f.offsets[lifecycleConnector] = []connect.SourceOffset{{Partition: map[string]any{"server": "orders"}, Offset: nil}}
	a := resetAction(t, f, nil)

	// A full reset leaves a partition without a position: an empty reset is a
	// no-op there, and a position is checked against the config's partition.
	res, err := a.Preview(scoped(), json.RawMessage(resetEmptyIn))
	if err != nil || !strings.Contains(res.Expected, "no change") {
		t.Fatalf("empty reset after a full reset: %+v %v", res, err)
	}
	if err := a.Validate(scoped(), json.RawMessage(resetAtIn)); err != nil {
		t.Fatalf("position after a full reset: %v", err)
	}
	pos, err := a.Preview(scoped(), json.RawMessage(resetAtIn))
	if err != nil || !strings.Contains(pos.Expected, "stores no position") || pos.Reversible {
		t.Fatalf("position preview after a full reset: %+v %v", pos, err)
	}
	f.allowWrites = true
	if _, err := a.Execute(scoped(), json.RawMessage(resetAtIn)); err != nil {
		t.Fatalf("position Execute after a full reset: %v", err)
	}
	if got := f.altered[lifecycleConnector]; len(got) != 1 {
		t.Fatalf("altered = %+v", got)
	}

	// A non-Debezium connector with no stored shape is refused: fail closed.
	f2 := newLifecycleFake()
	f2.offsets[lifecycleConnector] = nil
	f2.configs[lifecycleConnector]["connector.class"] = "com.example.SourceConnector"
	if err := resetAction(t, f2, nil).Validate(scoped(), json.RawMessage(resetAtIn)); !errors.Is(err, ErrSourcePosition) {
		t.Fatalf("non-Debezium position: %v", err)
	}
}

func TestSourceOffsetResetExecuteReReadFailureIsNotAnError(t *testing.T) {
	f := newLifecycleFake()
	f.allowWrites = true
	a := &resetSourceOffsets{d: LifecycleDeps{Connect: &failsAfterWrite{f: f}}}
	out, err := a.Execute(scoped(), json.RawMessage(resetEmptyIn))
	if err != nil || !strings.Contains(out, "re-read of the stored offsets failed") {
		t.Fatalf("out=%q err=%v", out, err)
	}
}

// failsAfterWrite fails SourceOffsets once the reset write has happened, like
// a Connect that answers the DELETE and then stops answering.
type failsAfterWrite struct{ f *lifecycleFake }

func (w *failsAfterWrite) ListConnectors(ctx context.Context) ([]string, error) {
	return w.f.ListConnectors(ctx)
}
func (w *failsAfterWrite) Status(ctx context.Context, n string) (clients.ConnectorStatus, error) {
	return w.f.Status(ctx, n)
}
func (w *failsAfterWrite) Config(ctx context.Context, n string) (map[string]string, error) {
	return w.f.Config(ctx, n)
}
func (w *failsAfterWrite) RestartTask(ctx context.Context, n string, task int) error {
	return w.f.RestartTask(ctx, n, task)
}
func (w *failsAfterWrite) RestartConnector(ctx context.Context, n string) error {
	return w.f.RestartConnector(ctx, n)
}
func (w *failsAfterWrite) SourceOffsets(ctx context.Context, n string) ([]connect.SourceOffset, error) {
	if len(w.f.reset) > 0 {
		return nil, errors.New("connect 503")
	}
	return w.f.SourceOffsets(ctx, n)
}
func (w *failsAfterWrite) StopConnector(ctx context.Context, n string) error {
	return w.f.StopConnector(ctx, n)
}
func (w *failsAfterWrite) ResetSourceOffsets(ctx context.Context, n string) error {
	return w.f.ResetSourceOffsets(ctx, n)
}
func (w *failsAfterWrite) AlterSourceOffsets(ctx context.Context, n string, o []connect.SourceOffset) error {
	return w.f.AlterSourceOffsets(ctx, n, o)
}
func (w *failsAfterWrite) CreateConnector(ctx context.Context, n string, cfg map[string]string) error {
	return w.f.CreateConnector(ctx, n, cfg)
}
func (w *failsAfterWrite) DeleteConnector(ctx context.Context, n string) error {
	return w.f.DeleteConnector(ctx, n)
}

func TestRegisterLifecycle(t *testing.T) {
	f := newLifecycleFake()
	reg := NewRegistry()
	if err := RegisterLifecycle(reg, LifecycleDeps{Connect: f}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{StopConnectorName, ResetSourceOffsetsName} {
		a, ok := reg.Get(name)
		if !ok {
			t.Fatalf("%s not registered", name)
		}
		if _, ok := a.(Scoped); !ok {
			t.Fatalf("%s is not Scoped", name)
		}
		if _, ok := a.(Preview); !ok {
			t.Fatalf("%s has no Preview", name)
		}
		if a.Layer() != "kafka-connect" {
			t.Fatalf("%s layer %q", name, a.Layer())
		}
	}
	if len(reg.All()) != 2 {
		t.Fatalf("registered %d actions", len(reg.All()))
	}
	// The write-mode-off registration must not make them reachable.
	readOnly := NewRegistry()
	if err := Register(readOnly, Deps{Connect: f}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{StopConnectorName, ResetSourceOffsetsName} {
		if _, ok := readOnly.Get(name); ok {
			t.Fatalf("%s is registered by the read-only action set", name)
		}
	}
}
