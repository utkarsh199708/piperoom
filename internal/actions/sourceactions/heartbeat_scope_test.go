package sourceactions

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/tools"
)

// TestHeartbeatNameRule: the table must be named as a heartbeat table, the
// rule connect_update_connector_config applies to heartbeat.action.query.
func TestHeartbeatNameRule(t *testing.T) {
	_, _, _, a := hbFixture()
	err := a.CheckScope(testPipeline(), json.RawMessage(`{"connector":"pg-orders","table":"public.audit_log"}`))
	if !errors.Is(err, tools.ErrOutOfScope) || !strings.Contains(err.Error(), "heartbeat") {
		t.Fatalf("CheckScope = %v, want ErrOutOfScope naming the heartbeat rule", err)
	}
}

// TestHeartbeatFollowUpQueryAdmitted proves the heartbeat.action.query this
// action proposes passes connect_update_connector_config's scope check, both
// before and after the table joins source_tables for a filtered publication,
// and that a captured data table is refused there.
func TestHeartbeatFollowUpQueryAdmitted(t *testing.T) {
	reg := actions.NewRegistry()
	if err := actions.Register(reg, actions.Deps{}); err != nil {
		t.Fatal(err)
	}
	ua, ok := reg.Get("connect_update_connector_config")
	if !ok {
		t.Fatal("connect_update_connector_config not registered")
	}
	sc, ok := ua.(actions.Scoped)
	if !ok {
		t.Fatal("connect_update_connector_config is not Scoped")
	}
	params := func(q string) json.RawMessage {
		raw, err := json.Marshal(map[string]any{
			"connector": "pg-orders",
			"changes":   map[string]string{"heartbeat.action.query": q, "heartbeat.interval.ms": defaultHeartbeatIntervalMS},
		})
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	pl := testPipeline()
	follow := params(heartbeatQuery("public.debezium_heartbeat"))
	if err := sc.CheckScope(pl, follow); err != nil {
		t.Fatalf("follow-up query refused: %v", err)
	}
	pl.SourceTables = append(pl.SourceTables, "public.debezium_heartbeat")
	if err := sc.CheckScope(pl, follow); err != nil {
		t.Fatalf("follow-up query refused once the table is in source_tables: %v", err)
	}
	if err := sc.CheckScope(pl, params("UPDATE public.orders SET ts = now()")); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("UPDATE public.orders: %v, want ErrOutOfScope", err)
	}
}
