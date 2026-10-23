package postgres

import (
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// ---- InsertSnapshotSignalWhere (X2a) ----

const whereCond = `"id" >= '100' AND "id" <= '1; drop'`

func TestInsertSnapshotSignalWhereSQL(t *testing.T) {
	tx := &fakeTx{}
	w, _ := testWriter(tx)
	ctx, rec := recordCtx()
	id, err := w.InsertSnapshotSignalWhere(tools.WithUnscoped(ctx), "debezium.signal", "pg-orders-connector", []string{"public.orders"}, whereCond)
	if err != nil {
		t.Fatal(err)
	}
	if !uuidV4.MatchString(id) {
		t.Fatalf("id %q is not a v4 UUID", id)
	}
	checkTx(t, tx, true)
	if got, want := tx.sqls(), []string{`INSERT INTO "debezium"."signal" (id, type, data) VALUES ($1, 'execute-snapshot', $2)`}; !reflect.DeepEqual(got, want) {
		t.Fatalf("statements:\n got %q\nwant %q", got, want)
	}
	// The condition is a bound value inside the JSON, never SQL text, and is
	// not HTML-escaped.
	wantData := `{"data-collections":["public.orders"],"type":"INCREMENTAL","additional-conditions":[{"data-collection":"public.orders","filter":"\"id\" >= '100' AND \"id\" <= '1; drop'"}]}`
	if got := tx.stmts[1].args; !reflect.DeepEqual(got, []any{id, wantData}) {
		t.Fatalf("insert args = %#v\nwant data %s", got, wantData)
	}
	var data snapshotSignal
	if err := json.Unmarshal([]byte(wantData), &data); err != nil || len(data.AdditionalConditions) != 1 || data.AdditionalConditions[0].Filter != whereCond {
		t.Fatalf("data does not parse back: %v %+v", err, data)
	}
	// The provenance receipt never carries the condition. The receipt does
	// carry the random signal id, whose hex can contain "100", so that one
	// bound param is skipped rather than substring-matched.
	for _, c := range rec.Calls() {
		for _, p := range c.ParamsRedacted {
			if strings.Contains(p, id) {
				continue
			}
			if strings.Contains(p, "drop") || strings.Contains(p, "100") {
				t.Fatalf("receipt shows the condition: %q", p)
			}
		}
	}
}

func TestInsertSnapshotSignalWhereRefusesBadConditionBeforeIO(t *testing.T) {
	for name, cond := range map[string]string{
		"empty":            "",
		"second statement": `"id" >= 1; drop table t`,
		"comment":          `"id" >= 1 -- x`,
		"backslash":        `"id" >= E'\x'`,
		"dollar quote":     `"id" >= $$1$$`,
		"unterminated":     `"id" >= '1`,
		"too long":         `"id" >= '` + strings.Repeat("a", clients.MaxSnapshotConditionBytes) + `'`,
	} {
		t.Run(name, func(t *testing.T) {
			w, b := testWriter(&fakeTx{})
			id, err := w.InsertSnapshotSignalWhere(unscopedCtx(), "public.sig", "c", []string{"public.t"}, cond)
			if !errors.Is(err, clients.ErrBadSnapshotCondition) || id != "" {
				t.Fatalf("= %q, %v; want ErrBadSnapshotCondition", id, err)
			}
			if b.begins != 0 {
				t.Fatal("a refused condition must not reach the database")
			}
			if cond != "" && strings.Contains(err.Error(), cond) {
				t.Fatalf("error echoes the condition: %v", err)
			}
		})
	}
}

func TestInsertSnapshotSignalWhereScope(t *testing.T) {
	for name, tc := range map[string]struct {
		connector string
		tables    []string
		want      error
	}{
		"other connector": {"pg-payroll-connector", []string{"public.orders"}, tools.ErrOutOfScope},
		"other table":     {"pg-orders-connector", []string{"public.payroll"}, tools.ErrOutOfScope},
	} {
		t.Run(name, func(t *testing.T) {
			w, b := testWriter(&fakeTx{})
			if _, err := w.InsertSnapshotSignalWhere(pipelineCtx(), "public.sig", tc.connector, tc.tables, whereCond); !errors.Is(err, tc.want) || b.begins != 0 {
				t.Fatalf("err = %v (begins %d), want %v before I/O", err, b.begins, tc.want)
			}
		})
	}
	w, b := testWriter(&fakeTx{})
	if _, err := w.InsertSnapshotSignalWhere(t.Context(), "public.sig", "pg-orders-connector", []string{"public.orders"}, whereCond); !errors.Is(err, tools.ErrNoScope) || b.begins != 0 {
		t.Fatalf("no scope: %v", err)
	}
	if _, err := (*Writer)(nil).InsertSnapshotSignalWhere(pipelineCtx(), "public.sig", "pg-orders-connector", []string{"public.orders"}, whereCond); !errors.Is(err, ErrNoWriter) {
		t.Fatalf("nil writer: %v", err)
	}
	tx := &fakeTx{}
	w, _ = testWriter(tx)
	if _, err := w.InsertSnapshotSignalWhere(pipelineCtx(), "public.sig", "pg-orders-connector", []string{"public.orders"}, whereCond); err != nil {
		t.Fatalf("in scope: %v", err)
	}
	checkTx(t, tx, true)
}

// A server error's detail can quote the inserted row, so with a condition
// only its SQLSTATE is reported; a missing signal table is still ErrNotFound.
func TestInsertSnapshotSignalWhereErrorsNeverEchoTheCondition(t *testing.T) {
	leak := &pgconn.PgError{Severity: "ERROR", Code: "23514", Message: "check failed", Detail: "Failing row contains (" + whereCond + ")"}
	tx := &fakeTx{execErr: failOn("INSERT", leak)}
	w, _ := testWriter(tx)
	_, err := w.InsertSnapshotSignalWhere(unscopedCtx(), "public.sig", "c", []string{"public.orders"}, whereCond)
	if err == nil || !strings.Contains(err.Error(), "SQLSTATE 23514") || strings.Contains(err.Error(), "drop") || strings.Contains(err.Error(), "check failed") {
		t.Fatalf("err = %v", err)
	}
	checkTx(t, tx, false)

	tx = &fakeTx{execErr: failOn("INSERT", pgErr("42P01"))}
	w, _ = testWriter(tx)
	if _, err := w.InsertSnapshotSignalWhere(unscopedCtx(), "public.sig", "c", []string{"public.orders"}, whereCond); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing signal table: %v", err)
	}
}

// The unconditional signal's data is unchanged by the condition support.
func TestSignalDataWithoutConditionUnchanged(t *testing.T) {
	b, err := signalData([]string{"public.orders"}, "")
	if err != nil || string(b) != `{"data-collections":["public.orders"],"type":"INCREMENTAL"}` {
		t.Fatalf("data = %s, %v", b, err)
	}
}
