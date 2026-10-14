package dbtools

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// Tests for the QA3 findings (sweep 2026-10-14) on the db tools.

func toolOf(t *testing.T, d Deps, name string) tools.Tool {
	t.Helper()
	reg := tools.NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	tl, ok := reg.Get(name)
	if !ok {
		t.Fatalf("%s not registered", name)
	}
	return tl
}

// rowOnly hides PrimaryKeyColumn by exposing only the SourceDB methods.
type rowOnly struct{ clients.SourceDB }

func TestPgRowPKColumnMustBeThePrimaryKey(t *testing.T) {
	const in = `{"pipeline_id":"orders","table":"public.orders","pk_column":"%s","key":"7"}`
	tests := []struct {
		name    string
		pk      string
		pkErr   error
		noPK    bool // source cannot report primary keys
		col     string
		wantErr bool
		badIn   bool
		hidePK  bool
	}{
		{name: "real primary key", col: "id"},
		{name: "another column", col: "customer_id", wantErr: true, badIn: true},
		{name: "case differs", col: "ID", wantErr: true, badIn: true},
		{name: "composite key's first column is the key", pk: "tenant_id", col: "tenant_id"},
		{name: "table without primary key", pkErr: fmt.Errorf("postgres: primary key of public.orders: %w", pgx.ErrNoRows), col: "id", wantErr: true, badIn: true},
		{name: "catalog read fails", pkErr: errors.New("connection refused"), col: "id", wantErr: true},
		// Not the real client: the row is read, but the unverified key column
		// is redacted like any other.
		{name: "client cannot report primary keys", noPK: true, col: "id", hidePK: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := testDeps().Src.(*fakeSource)
			fs.pk, fs.pkErr = tt.pk, tt.pkErr
			if tt.pk != "" {
				fs.rows["public.orders|7"] = map[string]any{"tenant_id": "7", "ssn": "123-45-6789"}
			}
			d := testDeps()
			d.Src = fs
			if tt.noPK {
				d.Src = rowOnly{fs}
			}
			res, err := runTool(t, toolOf(t, d, "pg_row"), fmt.Sprintf(in, tt.col))
			if tt.wantErr {
				if err == nil {
					t.Fatalf("want an error, got %+v", res)
				}
				if errors.Is(err, tools.ErrBadInput) != tt.badIn {
					t.Fatalf("err = %v, ErrBadInput = %v, want %v", err, errors.Is(err, tools.ErrBadInput), tt.badIn)
				}
				if fs.rowCalls != 0 {
					t.Fatalf("Row was called %d time(s) for a refused pk_column", fs.rowCalls)
				}
				return
			}
			if err != nil {
				t.Fatalf("pg_row: %v", err)
			}
			if strings.Contains(res.Summary, "123-45-6789") {
				t.Fatalf("summary leaks a non-key value: %q", res.Summary)
			}
			data := res.Data.(map[string]any)
			if shown := data[tt.col] != nil; shown == tt.hidePK {
				t.Fatalf("%s value shown = %v, want %v (data %v)", tt.col, shown, !tt.hidePK, data)
			}
		})
	}
}

// typedSource reports column types like internal/clients/postgres.RowTyped.
type typedSource struct {
	*fakeSource
	types map[string]uint32
}

func (s *typedSource) RowTyped(ctx context.Context, table, pk, key string) (map[string]any, map[string]uint32, bool, error) {
	row, found, err := s.Row(ctx, table, pk, key)
	return row, s.types, found, err
}

func TestTimestampNamedColumnsNeedADateTimeType(t *testing.T) {
	row := map[string]any{
		"id":         "7",
		"updated_at": "2026-10-11T10:00:00Z",
		"secret_at":  "hunter2",              // text column with a timestamp name
		"token_at":   "2026-10-11T10:00:00Z", // text, but looks like a time
		"deleted_at": nil,
	}
	types := map[string]uint32{
		"id": pgtype.Int8OID, "updated_at": pgtype.TimestamptzOID,
		"secret_at": pgtype.TextOID, "token_at": pgtype.TextOID, "deleted_at": pgtype.TimestampOID,
	}
	tests := []struct {
		name   string
		types  map[string]uint32
		shown  []string
		hidden []string
	}{
		// With types: the type decides, even for a time-looking text value.
		{"typed source", types, []string{"id", "updated_at"}, []string{"secret_at", "token_at"}},
		// Without types: the value must be a date/time rendering.
		{"untyped source", nil, []string{"id", "updated_at", "token_at"}, []string{"secret_at"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := &fakeSource{rows: map[string]map[string]any{"public.orders|7": row}}
			d := testDeps()
			d.Src = fs
			if tt.types != nil {
				d.Src = &typedSource{fakeSource: fs, types: tt.types}
			}
			res, err := runTool(t, toolOf(t, d, "pg_row"), `{"pipeline_id":"orders","table":"public.orders","pk_column":"id","key":"7"}`)
			if err != nil {
				t.Fatalf("pg_row: %v", err)
			}
			data := res.Data.(map[string]any)
			for _, c := range tt.shown {
				if data[c] != row[c] || !strings.Contains(res.Summary, c+"=") {
					t.Errorf("%s should be shown: data=%v summary=%q", c, data[c], res.Summary)
				}
			}
			for _, c := range tt.hidden {
				if data[c] != nil || strings.Contains(res.Summary, c+"=") {
					t.Errorf("%s should be redacted: data=%v summary=%q", c, data[c], res.Summary)
				}
			}
			if strings.Contains(res.Summary, "hunter2") {
				t.Fatalf("summary leaks: %q", res.Summary)
			}
		})
	}
}

func TestWarehouseRowRedactsNonTimeValues(t *testing.T) {
	d := testDeps()
	d.Wh = &fakeWarehouse{rows: map[string]map[string]any{"analytics.orders|7": {
		"id": "7", "loaded_at": "2026-10-11 10:00:01", "note_at": "call me at 555-0100",
	}}}
	res, err := runTool(t, toolOf(t, d, "warehouse_row"), `{"pipeline_id":"orders","table":"analytics.orders","pk_column":"id","key":"7"}`)
	if err != nil {
		t.Fatalf("warehouse_row: %v", err)
	}
	data := res.Data.(map[string]any)
	if _, ok := data["note_at"]; ok || strings.Contains(res.Summary, "555-0100") {
		t.Fatalf("text note_at leaked: data=%v summary=%q", data, res.Summary)
	}
	if data["loaded_at"] != "2026-10-11 10:00:01" {
		t.Fatalf("loaded_at dropped: %v", data)
	}
}

func TestLooksLikeDateTime(t *testing.T) {
	tests := []struct {
		v    any
		want bool
	}{
		{"2026-10-11T10:00:00Z", true},
		{"2026-10-11T10:00:00.123456789Z", true},
		{"2026-10-11T10:00:00.5", true},
		{"2026-10-11 10:00:00+00", true},
		{"2026-10-11", true},
		{"12:34:56.5", true},
		{"infinity", true},
		{"-infinity", true},
		{nil, true},
		{time.Now(), true},
		{"hunter2", false},
		{"2026-10-11T10:00:00Z; secret", false},
		{"", false},
		{int64(5), false},
	}
	for _, tt := range tests {
		if got := looksLikeDateTime(tt.v); got != tt.want {
			t.Errorf("looksLikeDateTime(%#v) = %v, want %v", tt.v, got, tt.want)
		}
	}
}

func TestDecodeRejectsUnknownFields(t *testing.T) {
	tests := []struct {
		tool string
		in   string
	}{
		{"pg_row", `{"pipeline_id":"orders","table":"public.orders","pk_column":"id","key":"7","columns":"*"}`},
		{"pg_row", `{"pipeline_id":"orders","table":"public.orders","pk_colum":"id","key":"7"}`},
		{"pg_recent_ddl", `{"since":"2026-10-11T00:00:00Z","limit":5}`},
		{"pg_long_transactions", `{"older_than_minutes":5,"include_query":true}`},
		{"pg_replication_slots", `{"all":true}`},
		{"warehouse_freshness", `{"table":"analytics.orders","ts_column":"updated_at","x":1}`},
		{"pipeline_map", `{"pipeline_id":"orders"} {"pipeline_id":"payments"}`},
		{"pipeline_map", `{"pipeline_id":"orders"} trailing`},
	}
	for _, tt := range tests {
		t.Run(tt.tool+" "+tt.in, func(t *testing.T) {
			_, err := runTool(t, toolOf(t, testDeps(), tt.tool), tt.in)
			if !errors.Is(err, tools.ErrBadInput) {
				t.Fatalf("err = %v, want ErrBadInput", err)
			}
		})
	}
	// Known fields still work, and an empty object for the no-parameter tool.
	if _, err := runTool(t, toolOf(t, testDeps(), "pg_replication_slots"), `{}`); err != nil {
		t.Fatalf("pg_replication_slots {}: %v", err)
	}
}

func TestSlotLine(t *testing.T) {
	tests := []struct {
		name string
		slot clients.ReplicationSlot
		want []string
		not  []string
	}{
		{"healthy logical", clients.ReplicationSlot{Name: "s", Plugin: "pgoutput", Active: true, RestartLSN: "0/1", RetainedWALBytes: 2048, WALStatus: "reserved"},
			[]string{"slot s: active=true retained=2.0 KiB wal_status=reserved"}, []string{"physical", "invalidated"}},
		{"physical unreserved", clients.ReplicationSlot{Name: "p"},
			[]string{"slot p (physical):", "retained=n/a (no restart_lsn)", "wal_status=-"}, []string{"0 B"}},
		{"lost logical", clients.ReplicationSlot{Name: "l", Plugin: "pgoutput", WALStatus: "lost"},
			[]string{"retained=n/a", "wal_status=lost", "invalidated", "cannot resume"}, []string{"0 B"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := slotLine(tt.slot)
			for _, w := range tt.want {
				if !strings.Contains(got, w) {
					t.Errorf("slotLine = %q, missing %q", got, w)
				}
			}
			for _, n := range tt.not {
				if strings.Contains(got, n) {
					t.Errorf("slotLine = %q, must not contain %q", got, n)
				}
			}
		})
	}
}

func TestWarehouseFreshnessFutureIsFlagged(t *testing.T) {
	tests := []struct {
		name       string
		newest     time.Time
		wantFuture bool
	}{
		{"past", time.Now().Add(-5 * time.Minute), false},
		{"future", time.Now().Add(3 * time.Hour), true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := testDeps()
			d.Wh = &fakeWarehouse{fresh: tt.newest}
			res, err := runTool(t, toolOf(t, d, "warehouse_freshness"), `{"table":"analytics.orders","ts_column":"updated_at"}`)
			if err != nil {
				t.Fatal(err)
			}
			data := res.Data.(map[string]any)
			if tt.wantFuture {
				if !strings.Contains(res.Summary, "IN THE FUTURE") || data["in_future"] != true || data["stale_for"] != nil {
					t.Fatalf("future newest not flagged: %q %v", res.Summary, data)
				}
				return
			}
			if strings.Contains(res.Summary, "FUTURE") || data["stale_for"] != "5m" {
				t.Fatalf("past newest: %q %v", res.Summary, data)
			}
		})
	}
}

func TestDDLMatchesTable(t *testing.T) {
	tests := []struct {
		identity, table string
		want            bool
	}{
		{"public.orders", "public.orders", true},
		{"public.orders.amount", "public.orders", true},                 // column rename / comment
		{"orders_amount_check on public.orders", "public.orders", true}, // constraint
		{"orders_trg on public.orders", "public.orders", true},          // trigger
		{`public."MixedCase"`, "public.MixedCase", true},
		{`public."MixedCase"."Col"`, "public.MixedCase", true},
		{"public.mixedcase", "public.MixedCase", false},
		{"public.orders_archive", "public.orders", false},
		{"public.orders_archive.amount", "public.orders", false},
		{"x on public.orders_archive", "public.orders", false},
		{"public.payments", "public.orders", false},
		{"other.orders", "public.orders", false},
		{"public.orders", "bad", false},
	}
	for _, tt := range tests {
		if got := ddlMatchesTable(tt.identity, tt.table); got != tt.want {
			t.Errorf("ddlMatchesTable(%q, %q) = %v, want %v", tt.identity, tt.table, got, tt.want)
		}
	}
}

// ddlSource returns DDL on columns and attached objects, without a SQL table
// filter, so the in-memory fallback is used.
type ddlSource struct{ *fakeSource }

func (ddlSource) RecentDDL(context.Context, time.Time, int) ([]clients.DDLChange, error) {
	now := time.Now()
	return []clients.DDLChange{
		{At: now, CommandTag: "ALTER TABLE", ObjectIdentity: "public.orders.amount"},
		{At: now.Add(-time.Second), CommandTag: "ALTER TABLE", ObjectIdentity: "orders_chk on public.orders"},
		{At: now.Add(-2 * time.Second), CommandTag: "ALTER TABLE", ObjectIdentity: "public.payments.amount"},
	}, nil
}

func TestRecentDDLFallbackMatchesSubObjects(t *testing.T) {
	for _, ctx := range []context.Context{roomCtx(), tools.WithUnscoped(context.Background())} {
		d := testDeps()
		d.Src = ddlSource{&fakeSource{}}
		in := `{"since":"2000-01-01T00:00:00Z","table":"public.orders"}`
		res, err := toolOf(t, d, "pg_recent_ddl").Run(ctx, []byte(in))
		if err != nil {
			t.Fatal(err)
		}
		got := res.Data.([]clients.DDLChange)
		if len(got) != 2 || strings.Contains(res.Summary, "payments") {
			t.Fatalf("got %+v", got)
		}
	}
	// Scoped without a table: the pipeline's tables, sub-objects included.
	d := testDeps()
	d.Src = ddlSource{&fakeSource{}}
	res, err := toolOf(t, d, "pg_recent_ddl").Run(roomCtx(), []byte(`{"since":"2000-01-01T00:00:00Z"}`))
	if err != nil {
		t.Fatal(err)
	}
	if got := res.Data.([]clients.DDLChange); len(got) != 2 {
		t.Fatalf("scoped DDL = %+v", got)
	}
}
