package dbtools

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/warehouse"
	"piperoom/internal/tools"
)

// loadWarehouse is a fakeWarehouse with load history.
type loadWarehouse struct {
	fakeWarehouse
	errs     []clients.LoadError
	err      error
	gotTable string
	gotSince time.Time
	gotLimit int
}

func (f *loadWarehouse) LoadErrors(_ context.Context, table string, since time.Time, limit int) ([]clients.LoadError, error) {
	f.gotTable, f.gotSince, f.gotLimit = table, since, limit
	return f.errs, f.err
}

// pgWarehouse is a warehouse without load history but with table statistics.
type pgWarehouse struct {
	fakeWarehouse
	act    warehouse.TableActivity
	actErr error
}

func (f *pgWarehouse) LoadErrors(context.Context, string, time.Time, int) ([]clients.LoadError, error) {
	return nil, clients.ErrNotSupported
}
func (f *pgWarehouse) TableActivity(context.Context, string) (warehouse.TableActivity, error) {
	return f.act, f.actErr
}

func loadTool(t *testing.T, wh clients.Warehouse) tools.Tool {
	t.Helper()
	d := testDeps()
	d.Wh = wh
	reg := tools.NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	tl, ok := reg.Get("warehouse_load_errors")
	if !ok {
		t.Fatal("warehouse_load_errors not registered")
	}
	return tl
}

func TestLoadErrorsToolRendersFailure(t *testing.T) {
	at := time.Date(2026, 10, 14, 3, 41, 0, 0, time.UTC)
	wh := &loadWarehouse{errs: []clients.LoadError{
		{Table: "analytics.orders", At: at, Status: "LOAD_FAILED", Rows: 0, Errors: 12000, FirstError: "SQL compilation error: invalid identifier 'RISK_SCORE'", File: "part-1.csv"},
		{Table: "analytics.orders", At: at.Add(-10 * time.Minute), Status: "LOAD_FAILED", Errors: 11000},
	}}
	res, err := loadTool(t, wh).Run(roomCtx(), json.RawMessage(`{"table":"analytics.orders","since":"2026-10-14T03:00:00Z"}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"2 failed or partial load(s)", "LOAD_FAILED 0 of 12000 rows", "invalid identifier 'RISK_SCORE'", "file=part-1.csv"} {
		if !strings.Contains(res.Summary, want) {
			t.Errorf("summary lacks %q:\n%s", want, res.Summary)
		}
	}
	if got, ok := res.Data.([]clients.LoadError); !ok || len(got) != 2 {
		t.Errorf("Data = %#v", res.Data)
	}
	if wh.gotTable != "analytics.orders" || !wh.gotSince.Equal(time.Date(2026, 10, 14, 3, 0, 0, 0, time.UTC)) || wh.gotLimit != loadErrorLimit {
		t.Errorf("client got %q %v %d", wh.gotTable, wh.gotSince, wh.gotLimit)
	}
}

func TestLoadErrorsToolStreamingNote(t *testing.T) {
	res, err := loadTool(t, &loadWarehouse{}).Run(roomCtx(), json.RawMessage(`{"table":"analytics.orders"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Summary, "Snowpipe Streaming loads are not listed here") || !strings.Contains(res.Summary, "no failed or partial loads") {
		t.Errorf("summary = %q", res.Summary)
	}
}

func TestLoadErrorsToolPostgresFallback(t *testing.T) {
	vac := time.Date(2026, 10, 14, 2, 0, 0, 0, time.UTC)
	wh := &pgWarehouse{act: warehouse.TableActivity{Table: "analytics.orders", Inserts: 5000, DeadTuples: 12, LastAutovacuum: &vac, LockWaiters: 2}}
	res, err := loadTool(t, wh).Run(roomCtx(), json.RawMessage(`{"table":"analytics.orders"}`))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"postgres warehouse has no load history", "5000 rows inserted", "12 dead tuples", "2 session(s) waiting on a lock"} {
		if !strings.Contains(res.Summary, want) {
			t.Errorf("summary lacks %q: %s", want, res.Summary)
		}
	}
	// A warehouse with neither interface still answers.
	res, err = loadTool(t, &fakeWarehouse{}).Run(roomCtx(), json.RawMessage(`{"table":"analytics.orders"}`))
	if err != nil || !strings.Contains(res.Summary, "no load history") {
		t.Errorf("plain warehouse: %q %v", res.Summary, err)
	}
}

func TestLoadErrorsToolErrorPaths(t *testing.T) {
	boom := errors.New("snowflake down")
	cases := []struct {
		name    string
		wh      clients.Warehouse
		ctx     context.Context
		in      string
		badIn   bool
		scope   bool
		wantErr string
	}{
		{name: "client error", wh: &loadWarehouse{err: boom}, ctx: roomCtx(), in: `{"table":"analytics.orders"}`, wantErr: "snowflake down"},
		{name: "fallback error", wh: &pgWarehouse{actErr: boom}, ctx: roomCtx(), in: `{"table":"analytics.orders"}`, wantErr: "snowflake down"},
		{name: "bad table", wh: &loadWarehouse{}, ctx: roomCtx(), in: `{"table":"orders; drop"}`, badIn: true},
		{name: "bad since", wh: &loadWarehouse{}, ctx: roomCtx(), in: `{"table":"analytics.orders","since":"yesterday"}`, badIn: true},
		{name: "no input", wh: &loadWarehouse{}, ctx: roomCtx(), in: ``, badIn: true},
		{name: "other pipeline's table", wh: &loadWarehouse{}, ctx: roomCtx(), in: `{"table":"analytics.payments"}`, scope: true},
		{name: "no room scope", wh: &loadWarehouse{}, ctx: context.Background(), in: `{"table":"analytics.orders"}`, scope: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := loadTool(t, tc.wh).Run(tc.ctx, json.RawMessage(tc.in))
			switch {
			case err == nil:
				t.Fatal("want error")
			case tc.badIn && !errors.Is(err, tools.ErrBadInput):
				t.Errorf("err = %v, want ErrBadInput", err)
			case tc.scope && !errors.Is(err, tools.ErrOutOfScope) && !errors.Is(err, tools.ErrNoScope):
				t.Errorf("err = %v, want a scope error", err)
			case tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr):
				t.Errorf("err = %v", err)
			}
		})
	}
}
