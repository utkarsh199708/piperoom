package actions

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/tools"
)

// ---- Execute-time re-checks (mutation-review kill tests): each guard the
// proposal path applies must hold at Execute too, because the world can move
// between approval and execution. ----

// A connector whose type reads sink at execution (re-registered meanwhile)
// must be refused by Execute and Preview alike: an offsets write against a
// sink rewinds its consumer group.
func TestSourceOffsetResetRefusesSinkAtExecuteAndPreview(t *testing.T) {
	sinkFake := func() *lifecycleFake {
		f := newLifecycleFake()
		st := f.status[lifecycleConnector]
		st.Type = "sink"
		f.status[lifecycleConnector] = st
		f.allowWrites = true
		return f
	}
	f := sinkFake()
	if _, err := resetAction(t, f, nil).Execute(scoped(), json.RawMessage(resetEmptyIn)); err == nil || !strings.Contains(err.Error(), "only a source connector") {
		t.Fatalf("Execute on a sink: %v", err)
	}
	if len(f.reset) != 0 || len(f.altered) != 0 {
		t.Fatal("Execute wrote to a sink's offsets")
	}
	f2 := sinkFake()
	if _, err := resetAction(t, f2, nil).Preview(scoped(), json.RawMessage(resetEmptyIn)); err == nil || !strings.Contains(err.Error(), "only a source connector") {
		t.Fatalf("Preview on a sink: %v", err)
	}
}

// A second stored offset that appeared between approval and execution makes
// the approved full reset un-rollbackable: Execute must re-apply
// checkResetTarget and refuse.
func TestSourceOffsetResetExecuteRefusesGrownOffsets(t *testing.T) {
	f := newLifecycleFake()
	f.offsets[lifecycleConnector] = append(f.offsets[lifecycleConnector],
		connect.SourceOffset{Partition: map[string]any{"server": "orders2"}, Offset: map[string]any{"lsn": json.Number("1")}})
	f.allowWrites = true
	if _, err := resetAction(t, f, nil).Execute(scoped(), json.RawMessage(resetEmptyIn)); !errors.Is(err, ErrMultipleSourceOffsets) {
		t.Fatalf("Execute with two stored offsets: %v, want ErrMultipleSourceOffsets", err)
	}
	if len(f.reset) != 0 {
		t.Fatal("Execute cleared a multi-offset connector")
	}
}

// A misspelled param key must be refused, not silently ignored: "postion"
// would otherwise turn an intended one-partition move into a full reset.
func TestSourceOffsetResetRefusesUnknownParamKeys(t *testing.T) {
	f := newLifecycleFake()
	a := resetAction(t, f, nil)
	in := `{"connector":"pg-orders-connector","stop_first":true,"postion":{"partition":{"server":"orders"},"offset":{"lsn":1}}}`
	if err := a.Validate(scoped(), json.RawMessage(in)); err == nil || !strings.Contains(err.Error(), "bad params") {
		t.Fatalf("unknown key accepted: %v", err)
	}
}

// Position values are bounded at parse, where the no-stored-shape path (after
// a full reset) has no stored value to compare with: nested values, oversized
// strings and control characters are refused before any I/O.
func TestSourceOffsetResetParseBoundsValues(t *testing.T) {
	for name, in := range map[string]string{
		"nested object":   `{"connector":"c","position":{"partition":{"server":"s"},"offset":{"lsn":{"nested":1}}}}`,
		"nested array":    `{"connector":"c","position":{"partition":{"server":"s"},"offset":{"lsn":[1]}}}`,
		"oversized text":  `{"connector":"c","position":{"partition":{"server":"s"},"offset":{"gtids":"` + strings.Repeat("x", maxPositionStringBytes+1) + `"}}}`,
		"control chars":   `{"connector":"c","position":{"partition":{"server":"s"},"offset":{"file":"a\r\nb"}}}`,
		"oversized digit": `{"connector":"c","position":{"partition":{"server":"s"},"offset":{"lsn":` + strings.Repeat("1", 65) + `}}}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseResetParams(json.RawMessage(in)); err == nil {
				t.Fatal("parse accepted the position")
			}
		})
	}
}

// Writing back EXACTLY the stored value is accepted whatever its form: a
// source may store a negative sentinel (Confluent JDBC "incrementing": -1),
// and the advertised rollback must stay runnable. A different negative value
// is still refused.
func TestSourceOffsetResetAcceptsExactStoredValue(t *testing.T) {
	fake := func() *lifecycleFake {
		f := newLifecycleFake()
		f.offsets[lifecycleConnector] = []connect.SourceOffset{{
			Partition: map[string]any{"server": "orders"},
			Offset:    map[string]any{"incrementing": json.Number("-1")},
		}}
		return f
	}
	same := `{"connector":"pg-orders-connector","position":{"partition":{"server":"orders"},"offset":{"incrementing":-1}}}`
	if err := resetAction(t, fake(), nil).Validate(scoped(), json.RawMessage(same)); err != nil {
		t.Fatalf("writing back the stored sentinel refused: %v", err)
	}
	other := `{"connector":"pg-orders-connector","position":{"partition":{"server":"orders"},"offset":{"incrementing":-2}}}`
	if err := resetAction(t, fake(), nil).Validate(scoped(), json.RawMessage(other)); !errors.Is(err, ErrSourcePosition) {
		t.Fatalf("a different negative value accepted: %v", err)
	}
}

// settlingFake reports RUNNING until `after` status reads, then STOPPED: the
// stop step's write is applied asynchronously by Connect.
type settlingFake struct {
	*lifecycleFake
	after int
	reads int
}

func (f *settlingFake) Status(ctx context.Context, name string) (clients.ConnectorStatus, error) {
	st, err := f.lifecycleFake.Status(ctx, name)
	if err != nil {
		return st, err
	}
	f.reads++
	if f.reads <= f.after {
		st.State = "RUNNING"
	}
	return st, nil
}

// A reset right after an accepted stop polls briefly for STOPPED instead of
// failing the approved proposal while the stop settles; a connector that
// never stops is still refused, with an error that tells a settling stop
// apart from a missing stop step.
func TestSourceOffsetResetExecuteWaitsForSettlingStop(t *testing.T) {
	f := &settlingFake{lifecycleFake: newLifecycleFake(), after: 2}
	f.allowWrites = true
	a := &resetSourceOffsets{d: LifecycleDeps{Connect: f, PollInterval: time.Millisecond}}
	guardWrites(t, f.lifecycleFake)
	out, err := a.Execute(scoped(), json.RawMessage(resetEmptyIn))
	if err != nil {
		t.Fatalf("Execute during a settling stop: %v", err)
	}
	if len(f.reset) != 1 || !strings.Contains(out, "cleared the stored offsets") {
		t.Fatalf("reset=%v out=%q", f.reset, out)
	}

	f2 := &settlingFake{lifecycleFake: newLifecycleFake(), after: 1 << 30}
	f2.allowWrites = true
	a2 := &resetSourceOffsets{d: LifecycleDeps{Connect: f2, PollInterval: time.Millisecond}}
	guardWrites(t, f2.lifecycleFake)
	_, err = a2.Execute(scoped(), json.RawMessage(resetEmptyIn))
	if !errors.Is(err, ErrStopStepRequired) || !strings.Contains(err.Error(), "has not settled") {
		t.Fatalf("Execute on a never-stopping connector: %v", err)
	}
	if len(f2.reset) != 0 {
		t.Fatal("wrote to a non-STOPPED connector")
	}
}

// The full-reset preview advertises a rollback only when this action could
// really write the cleared position back afterwards: once cleared there is no
// stored shape, and only a Debezium {"server": topic.prefix} partition can be
// re-checked then.
func TestSourceOffsetResetPreviewReversibilityMatchesRollbackPath(t *testing.T) {
	// A partition the no-shape path would refuse (a second key, as Debezium
	// SQL Server stores): the preview must not claim reversibility.
	f := newLifecycleFake()
	f.offsets[lifecycleConnector] = []connect.SourceOffset{{
		Partition: map[string]any{"server": "orders", "database": "orders_db"},
		Offset:    map[string]any{"lsn": json.Number("12345678")},
	}}
	res, err := resetAction(t, f, nil).Preview(scoped(), json.RawMessage(resetEmptyIn))
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if res.Reversible || res.Rollback != "" {
		t.Fatalf("preview claims a rollback the action would refuse: reversible=%v rollback=%q", res.Reversible, res.Rollback)
	}
	if !strings.Contains(res.Expected, "not reversible") {
		t.Fatalf("Expected does not say the clear is irreversible: %q", res.Expected)
	}

	// The happy path still advertises the rollback, and its text round-trips
	// through the action's own parser against the post-clear state.
	f2 := newLifecycleFake()
	res2, err := resetAction(t, f2, nil).Preview(scoped(), json.RawMessage(resetEmptyIn))
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if !res2.Reversible || !strings.HasPrefix(res2.Rollback, ResetSourceOffsetsName+" ") {
		t.Fatalf("rollback = %q", res2.Rollback)
	}
	raw := strings.TrimPrefix(res2.Rollback, ResetSourceOffsetsName+" ")
	if _, err := parseResetParams(json.RawMessage(raw)); err != nil {
		t.Fatalf("the advertised rollback is not valid params: %v (%q)", err, raw)
	}
	f2.offsets[lifecycleConnector] = nil // the clear ran and removed the entry
	if err := resetAction(t, f2, nil).Validate(scoped(), json.RawMessage(raw)); err != nil {
		t.Fatalf("the advertised rollback is refused after the clear: %v", err)
	}
}

// A reset to the position already stored previews as a no-op, exactly as
// Execute treats it.
func TestSourceOffsetResetPreviewNoOpPosition(t *testing.T) {
	f := newLifecycleFake()
	in := `{"connector":"pg-orders-connector","position":{"partition":{"server":"orders"},"offset":{"lsn":12345678,"txId":42,"ts_usec":1700000000000000}}}`
	res, err := resetAction(t, f, nil).Preview(scoped(), json.RawMessage(in))
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if !strings.Contains(res.Expected, "no change") {
		t.Fatalf("Expected = %q, want a no-op", res.Expected)
	}
	if res.Reversible || res.Rollback != "" {
		t.Fatalf("a no-op must not advertise a rollback: %+v", res)
	}
}

// The undo of a stop of a PAUSED connector is a resume, which sets RUNNING:
// the preview must say so instead of implying the paused state comes back.
func TestStopConnectorPreviewPausedUndoNote(t *testing.T) {
	f := newLifecycleFake()
	f.setState("PAUSED")
	res, err := (&stopConnector{d: LifecycleDeps{Connect: f}}).Preview(scoped(), json.RawMessage(stopIn))
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if !strings.Contains(res.Expected, "not back to PAUSED") {
		t.Fatalf("Expected = %q, want the RUNNING-not-PAUSED note", res.Expected)
	}
}

// ---- create/delete ----

// A stored credential embedded in a URL blocks a delete exactly like one
// under a credential key: the only undo could not restore it.
func TestDeleteConnectorRefusesSecretValueInURL(t *testing.T) {
	f := newLifecycleFake()
	f.allowWrites = true
	f.configs[lifecycleConnector]["database.url"] = "jdbc:postgresql://db/x?password=hunter2"
	a := deleteAction(t, f)
	for _, step := range []func() error{
		func() error { return a.Validate(scoped(), json.RawMessage(deleteIn)) },
		func() error { _, err := a.Execute(scoped(), json.RawMessage(deleteIn)); return err },
	} {
		err := step()
		if !errors.Is(err, ErrPlaintextSecret) {
			t.Fatalf("err = %v, want ErrPlaintextSecret", err)
		}
		if strings.Contains(err.Error(), "hunter2") {
			t.Fatalf("err leaks the value: %v", err)
		}
	}
	if len(f.deleted) != 0 {
		t.Fatal("deleted a connector whose config holds a plaintext credential")
	}
}

// A connector that vanished between Execute's exists check and the DELETE
// leaves the approved end state in place: no error, no retry bait.
func TestDeleteConnectorForgivesVanishRace(t *testing.T) {
	f := &retryDeleteFake{lifecycleFake: newLifecycleFake(), errs: []error{clients.ErrNotFound}}
	f.allowWrites = true
	if _, err := deleteAction(t, f).Execute(scoped(), json.RawMessage(deleteIn)); err != nil {
		t.Fatalf("Execute over a vanished connector: %v", err)
	}
	if f.attempts != 1 {
		t.Fatalf("attempts=%d, want 1", f.attempts)
	}

	// And the symmetric timeout-then-clean-retry delete succeeds.
	f2 := &retryDeleteFake{lifecycleFake: newLifecycleFake(), errs: []error{context.DeadlineExceeded, nil}}
	f2.allowWrites = true
	if _, err := deleteAction(t, f2).Execute(scoped(), json.RawMessage(deleteIn)); err != nil {
		t.Fatalf("Execute with a timeout then retry: %v", err)
	}
	if f2.attempts != 2 || len(f2.deleted) != 1 {
		t.Fatalf("attempts=%d deleted=%v", f2.attempts, f2.deleted)
	}
}

// Create's out-of-scope refusal happens before any I/O.
func TestCreateConnectorValidateOutOfScopeBeforeIO(t *testing.T) {
	f := newLifecycleFake()
	f.names = nil
	a := createAction(t, f)
	if err := a.Validate(otherPipeline(), json.RawMessage(createIn)); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("Validate: %v, want ErrOutOfScope", err)
	}
	if f.listCalls != 0 || f.configCalls != 0 || f.validateCalls != 0 {
		t.Fatalf("reads before the scope refusal: list=%d config=%d validate=%d", f.listCalls, f.configCalls, f.validateCalls)
	}
}

// Every lifecycle Execute re-checks scope itself (defence in depth behind the
// executor and the write client).
func TestLifecycleExecuteOutOfScope(t *testing.T) {
	for name, run := range map[string]func(*lifecycleFake) error{
		"stop": func(f *lifecycleFake) error {
			_, err := (&stopConnector{d: LifecycleDeps{Connect: f}}).Execute(otherPipeline(), json.RawMessage(stopIn))
			return err
		},
		"reset": func(f *lifecycleFake) error {
			_, err := (&resetSourceOffsets{d: LifecycleDeps{Connect: f}}).Execute(otherPipeline(), json.RawMessage(resetEmptyIn))
			return err
		},
		"create": func(f *lifecycleFake) error {
			f.names = nil
			_, err := (&createConnector{d: LifecycleDeps{Connect: f}}).Execute(otherPipeline(), json.RawMessage(createIn))
			return err
		},
		"delete": func(f *lifecycleFake) error {
			_, err := (&deleteConnector{d: LifecycleDeps{Connect: f}}).Execute(otherPipeline(), json.RawMessage(deleteIn))
			return err
		},
	} {
		t.Run(name, func(t *testing.T) {
			f := newLifecycleFake()
			f.allowWrites = true
			if err := run(f); !errors.Is(err, tools.ErrOutOfScope) {
				t.Fatalf("Execute: %v, want ErrOutOfScope", err)
			}
			if len(f.created)+len(f.deleted)+len(f.reset)+len(f.altered)+len(f.stopped) != 0 {
				t.Fatal("an out-of-scope Execute wrote")
			}
		})
	}
}

// The plugin class travels into a URL path (the validate endpoint), so its
// shape is restricted; a path-traversal shape must be refused before any I/O.
func TestCreateConnectorRefusesMalformedClass(t *testing.T) {
	for _, class := range []string{"bad/../class", "a b", "a\nb", "io.x/../../admin"} {
		in, _ := json.Marshal(map[string]any{
			"connector": "pg-orders-connector",
			"config":    map[string]string{"connector.class": class},
		})
		f := newLifecycleFake()
		f.names = nil
		err := createAction(t, f).Validate(scoped(), json.RawMessage(in))
		if err == nil || !strings.Contains(err.Error(), "not a connector plugin class name") {
			t.Fatalf("class %q: %v", class, err)
		}
		if f.validateCalls != 0 {
			t.Fatalf("class %q reached the validate endpoint", class)
		}
	}
}

// The config's own "name" key must equal the connector param: Connect's POST
// carries it verbatim and refuses a mismatch only at Execute, after Preview
// passed.
func TestCreateConnectorRefusesNameMismatch(t *testing.T) {
	in := `{"connector":"pg-orders-connector","config":{"connector.class":"c","name":"other-name"}}`
	f := newLifecycleFake()
	f.names = nil
	if err := createAction(t, f).Validate(scoped(), json.RawMessage(in)); err == nil || !strings.Contains(err.Error(), `"name"`) {
		t.Fatalf("name mismatch accepted: %v", err)
	}
	same := `{"connector":"pg-orders-connector","config":{"connector.class":"c","name":"pg-orders-connector"}}`
	if err := createAction(t, f).Validate(scoped(), json.RawMessage(same)); err != nil {
		t.Fatalf("matching name refused: %v", err)
	}
}

// ---- the create host guard ----

// An in-scope connector NAME says nothing about where its config points. The
// guard refuses any host outside what the pipeline's connections and
// registered connectors already reach, before the validate round trip, and
// without echoing the allowed set.
func TestCreateConnectorHostGuard(t *testing.T) {
	newCreate := func() (*lifecycleFake, *createConnector) {
		f := newLifecycleFake()
		f.names = nil // not registered; the fake still serves its stored config for host collection
		return f, createAction(t, f)
	}
	mkIn := func(kv ...string) json.RawMessage {
		cfg := map[string]string{"connector.class": "io.debezium.connector.postgresql.PostgresConnector"}
		for i := 0; i+1 < len(kv); i += 2 {
			cfg[kv[i]] = kv[i+1]
		}
		in, _ := json.Marshal(map[string]any{"connector": "pg-orders-connector", "config": cfg})
		return in
	}

	// The pipeline's own host passes, at Validate and Preview.
	f, a := newCreate()
	if err := a.Validate(scoped(), json.RawMessage(mkIn("database.hostname", "postgres"))); err != nil {
		t.Fatalf("own host refused: %v", err)
	}
	if _, err := a.Preview(scoped(), json.RawMessage(mkIn("database.hostname", "postgres"))); err != nil {
		t.Fatalf("own host refused at Preview: %v", err)
	}

	// An attacker host is refused before Connect is asked to validate.
	f, a = newCreate()
	err := a.Validate(scoped(), json.RawMessage(mkIn("database.hostname", "evil.example.com")))
	if !errors.Is(err, tools.ErrOutOfScope) || !strings.Contains(err.Error(), "evil.example.com") {
		t.Fatalf("attacker host: %v", err)
	}
	if strings.Contains(err.Error(), "postgres") {
		t.Fatalf("refusal echoes the allowed set: %v", err)
	}
	if _, perr := a.Preview(scoped(), json.RawMessage(mkIn("database.hostname", "evil.example.com"))); !errors.Is(perr, tools.ErrOutOfScope) {
		t.Fatalf("attacker host at Preview: %v", perr)
	}
	if f.validateCalls != 0 {
		t.Fatal("the config reached the validate endpoint despite the host refusal")
	}

	// A second host cannot hide behind an allowed first one.
	_, a = newCreate()
	if err := a.Validate(scoped(), json.RawMessage(mkIn("database.hostname", "postgres,evil.example.com"))); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("multi-host value: %v", err)
	}
	// A userinfo/path trick must reduce to the real host.
	_, a = newCreate()
	if err := a.Validate(scoped(), json.RawMessage(mkIn("connection.url", "jdbc:postgresql://evil.example.com/x@postgres"))); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("path-@ trick: %v", err)
	}
	// A reference is not a literal host: checkCreateConfig refuses it first
	// (a host key is not a credential key), before any I/O.
	_, a = newCreate()
	if err := a.Validate(scoped(), json.RawMessage(mkIn("database.hostname", "${secret:host}"))); err == nil || !strings.Contains(err.Error(), "not a credential key") {
		t.Fatalf("reference on a host key: %v", err)
	}
	// The warehouse's bootstrap servers are hosts too.
	_, a = newCreate()
	if err := a.Validate(scoped(), json.RawMessage(mkIn("database.history.kafka.bootstrap.servers", "evil.example.com:9092"))); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("bootstrap servers: %v", err)
	}
}

// Debezium 2.x documents more snapshot modes; the preview must not call them
// undocumented.
func TestSnapshotRestartKnowsDebezium2Modes(t *testing.T) {
	cfg := func(mode string) map[string]string {
		return map[string]string{"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "snapshot.mode": mode}
	}
	for mode, want := range map[string]string{
		"no_data":              "no data snapshot",
		"recovery":             "schema history is rebuilt",
		"schema_only_recovery": "schema history is rebuilt",
		"configuration_based":  "snapshot.mode.configuration.based",
	} {
		got := snapshotRestart(cfg(mode), "", false)
		if !strings.Contains(got, want) {
			t.Errorf("mode %s (no offset): %q does not say %q", mode, got, want)
		}
		if strings.Contains(got, "not one Debezium documents") {
			t.Errorf("mode %s called undocumented: %q", mode, got)
		}
		withOffset := snapshotRestart(cfg(mode), "", true)
		if strings.Contains(withOffset, "not one Debezium documents") {
			t.Errorf("mode %s (with offset) called undocumented: %q", mode, withOffset)
		}
	}
}
