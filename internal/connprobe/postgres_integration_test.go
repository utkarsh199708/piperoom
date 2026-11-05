//go:build integration

package connprobe

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"piperoom/internal/config"
	"piperoom/internal/owners"
)

// The dev stack (dev/docker-compose.yml). The role is a superuser there, so
// can_write must warn; that is the probe working, not a failure.
const (
	devSourceDSN    = "postgres://piperoom:piperoom-dev@localhost:5432/orders?sslmode=disable"
	devWarehouseDSN = "postgres://piperoom:piperoom-dev@localhost:5432/warehouse?sslmode=disable"
)

func devGuard() *Guard { return &Guard{AllowLoopback: true, AllowPrivate: true} }

func devPipelines(sourceTable string) []owners.Pipeline {
	return []owners.Pipeline{{
		ID:           "orders",
		SourceTables: []string{sourceTable},
		SinkTables:   []string{"analytics.orders"},
		SinkTSColumn: "updated_at",
	}}
}

func logChecks(t *testing.T, r Result) {
	t.Helper()
	b, _ := json.MarshalIndent(r, "", "  ")
	t.Logf("%s", b)
}

func devCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestIntegrationSourceDB(t *testing.T) {
	tg := Target{Kind: KindSourceDB, Config: config.Config{SourceDBURL: devSourceDSN}, Pipelines: devPipelines("public.orders")}
	r := Run(devCtx(t), tg, devGuard())
	logChecks(t, r)

	if !r.OK {
		t.Fatal("source_db probe failed on the dev stack")
	}
	st := statuses(r)
	for _, id := range []string{"config", "reach", "login", "read_slots", "read_publications", "select_tables"} {
		if st[id] != StatusOK {
			t.Errorf("%s = %s, want ok", id, st[id])
		}
	}
	// The dev role is a superuser: the probe must say so.
	if c := get(r, "can_write"); c.Status != StatusWarn || !strings.Contains(c.Problem, "superuser") {
		t.Errorf("can_write = %+v, want a superuser warning", c)
	}
	// read_only and statement_timeout depend on how the dev role was created.
	for _, id := range []string{"read_only", "statement_timeout"} {
		if st[id] != StatusOK && st[id] != StatusWarn {
			t.Errorf("%s = %s, want ok or warn", id, st[id])
		}
	}
	noLeaksIn(t, r, devSourceDSN, "piperoom-dev")
}

func TestIntegrationSourceDBUnknownTable(t *testing.T) {
	tg := Target{Kind: KindSourceDB, Config: config.Config{SourceDBURL: devSourceDSN}, Pipelines: devPipelines("public.it_no_such_table")}
	r := Run(devCtx(t), tg, devGuard())
	logChecks(t, r)
	if c := get(r, "select_tables"); c.Status != StatusFail || !strings.Contains(c.Problem, "public.it_no_such_table does not exist") {
		t.Errorf("select_tables = %+v", c)
	}
	if r.OK {
		t.Error("an unknown source table must fail the probe")
	}
	noLeaksIn(t, r, devSourceDSN, "piperoom-dev")
}

func TestIntegrationSourceDBWrongPassword(t *testing.T) {
	bad := strings.Replace(devSourceDSN, "piperoom-dev", "wrong-pw-it", 1)
	tg := Target{Kind: KindSourceDB, Config: config.Config{SourceDBURL: bad}, Pipelines: devPipelines("public.orders")}
	r := Run(devCtx(t), tg, devGuard())
	logChecks(t, r)
	st := statuses(r)
	if st["reach"] != StatusOK || st["login"] != StatusFail || st["read_only"] != StatusSkip || st["can_write"] != StatusSkip {
		t.Errorf("statuses = %v", st)
	}
	noLeaksIn(t, r, bad, "wrong-pw-it")
}

func TestIntegrationSourceDBGuardRefusesLoopback(t *testing.T) {
	tg := Target{Kind: KindSourceDB, Config: config.Config{SourceDBURL: devSourceDSN}, Pipelines: devPipelines("public.orders")}
	// A guard that does not allow loopback: the probe must not connect.
	r := Run(devCtx(t), tg, &Guard{})
	logChecks(t, r)
	st := statuses(r)
	if st["reach"] != StatusFail || st["login"] != StatusSkip {
		t.Errorf("statuses = %v", st)
	}
}

func TestIntegrationWarehouse(t *testing.T) {
	tg := Target{Kind: KindWarehouse, Config: config.Config{WarehouseDBURL: devWarehouseDSN, WarehouseKind: "postgres"}, Pipelines: devPipelines("public.orders")}
	r := Run(devCtx(t), tg, devGuard())
	logChecks(t, r)
	if !r.OK {
		t.Fatal("warehouse probe failed on the dev stack")
	}
	st := statuses(r)
	for _, id := range []string{"config", "reach", "login", "select_sink"} {
		if st[id] != StatusOK {
			t.Errorf("%s = %s, want ok", id, st[id])
		}
	}
	if st["read_only"] != StatusOK && st["read_only"] != StatusWarn {
		t.Errorf("read_only = %s", st["read_only"])
	}
	if d := get(r, "select_sink").Detail; d != "SELECT on 1 sink table and 1 timestamp column" {
		t.Errorf("select_sink detail = %q", d)
	}
	noLeaksIn(t, r, devWarehouseDSN, "piperoom-dev")
}

func TestIntegrationWarehouseUnknownColumn(t *testing.T) {
	ps := devPipelines("public.orders")
	ps[0].SinkTSColumn = "it_no_such_column"
	tg := Target{Kind: KindWarehouse, Config: config.Config{WarehouseDBURL: devWarehouseDSN}, Pipelines: ps}
	r := Run(devCtx(t), tg, devGuard())
	logChecks(t, r)
	if c := get(r, "select_sink"); c.Status != StatusFail || !strings.Contains(c.Problem, "no such column: it_no_such_column") {
		t.Errorf("select_sink = %+v", c)
	}
}

func noLeaksIn(t *testing.T, r Result, secrets ...string) {
	t.Helper()
	b, _ := json.Marshal(r)
	for _, s := range secrets {
		if strings.Contains(string(b), s) {
			t.Errorf("result contains %q", s)
		}
	}
}
