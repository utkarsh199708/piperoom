package databricks

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

func TestIdentifierRefusal(t *testing.T) {
	d, f := newFake()
	ctx := context.Background()
	now := time.Now()
	for _, c := range []struct{ table, col string }{
		{"orders", "id"},                                // no schema
		{"main.sales.orders", "id"},                     // catalog prefix
		{"sales.orders; DROP TABLE x", "id"},            // statement
		{"sales.`orders`", "id"},                        // quote break-out
		{"sales.orders", "id`) FROM y --"},              // column break-out
		{"sales.orders", "updated at"},                  // space
		{"sales.or-ders", "id"},                         // hyphen
		{"1sales.orders", "id"},                         // leading digit
		{"sales.orders", ""},                            // empty
		{"sales.orders", "ïd"},                          // non-ASCII
		{"sales..orders", "id"},                         // empty part
		{"sales.orders", "id/*"},                        // comment
		{"sales.orders", strings.Repeat("a", 3) + "\n"}, // newline
	} {
		if _, err := d.Freshness(ctx, c.table, c.col); err == nil {
			t.Errorf("Freshness(%q, %q): want error", c.table, c.col)
		}
		if _, _, err := d.Row(ctx, c.table, c.col, "1"); err == nil {
			t.Errorf("Row(%q, %q): want error", c.table, c.col)
		}
		if _, err := d.CountByBucket(ctx, c.table, c.col, now, now.Add(time.Hour), time.Minute); err == nil {
			t.Errorf("CountByBucket(%q, %q): want error", c.table, c.col)
		}
	}
	for _, table := range []string{"orders", "a.b.c", "s.t`"} {
		if _, err := d.PrimaryKeyColumn(ctx, table); err == nil {
			t.Errorf("PrimaryKeyColumn(%q): want error", table)
		}
	}
	if n := len(f.calls()); n != 0 {
		t.Fatalf("ran %d queries for bad input", n)
	}
}

func TestRowKeyIsAParameter(t *testing.T) {
	evil := "1' OR '1'='1'; DROP TABLE main.sales.orders; --"
	d, f := newFake(ok([]field{{"ID", "BIGINT"}}))
	if _, found, err := d.Row(context.Background(), "sales.orders", "id", evil); err != nil || found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	q := f.calls()[0]
	want := "SELECT * FROM `main`.`sales`.`orders` WHERE CAST(`id` AS STRING) = :key LIMIT 1"
	if q.SQL != want {
		t.Fatalf("sql = %s\nwant  %s", q.SQL, want)
	}
	if strings.Contains(q.SQL, "DROP") || strings.Contains(q.SQL, "'1'") {
		t.Fatalf("key reached the SQL text: %s", q.SQL)
	}
	if len(q.Params) != 1 || q.Params[0].Name != "key" || q.Params[0].Value != evil {
		t.Fatalf("params = %#v", q.Params)
	}
}

func TestFreshness(t *testing.T) {
	berlin := time.FixedZone("CEST", 2*3600)
	ts := time.Date(2026, 10, 18, 12, 0, 0, 0, berlin)
	for _, c := range []struct {
		name string
		res  fakeResult
		want time.Time
		err  error
	}{
		{"timestamp to utc", ok([]field{{"newest", "TIMESTAMP"}}, []any{ts}), ts.UTC(), nil},
		{"ntz", ok([]field{{"newest", "TIMESTAMP_NTZ"}}, []any{ts.UTC()}), ts.UTC(), nil},
		{"empty table", ok([]field{{"newest", "TIMESTAMP"}}, []any{nil}), time.Time{}, nil},
		{"no rows", ok([]field{{"newest", "TIMESTAMP"}}), time.Time{}, nil},
		{"string column", ok([]field{{"newest", "STRING"}}, []any{"alice@example.com"}), time.Time{}, ErrNotTimestamp},
		{"date column", ok([]field{{"newest", "DATE"}}, []any{ts}), time.Time{}, ErrNotTimestamp},
		{"decoded oddly", ok([]field{{"newest", "TIMESTAMP"}}, []any{"2026-10-18"}), time.Time{}, ErrNotTimestamp},
	} {
		t.Run(c.name, func(t *testing.T) {
			d, f := newFake(c.res)
			got, err := d.Freshness(context.Background(), "sales.orders", "updated_at")
			if c.err != nil {
				if !errors.Is(err, c.err) {
					t.Fatalf("err = %v, want %v", err, c.err)
				}
				if strings.Contains(err.Error(), "alice") {
					t.Fatalf("value in error: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if !got.Equal(c.want) || (!got.IsZero() && got.Location() != time.UTC) {
				t.Fatalf("got %v want %v", got, c.want)
			}
			if q := f.calls()[0].SQL; q != "SELECT MAX(`updated_at`) AS newest FROM `main`.`sales`.`orders`" {
				t.Fatalf("sql = %s", q)
			}
		})
	}
}

func TestRowMapping(t *testing.T) {
	ts := time.Date(2026, 10, 18, 12, 0, 0, 123000000, time.FixedZone("X", 3600))
	fields := []field{
		{"ID", "BIGINT"}, {"Amount", "DECIMAL"}, {"price", "DOUBLE"}, {"qty", "INT"}, {"small", "SMALLINT"},
		{"tiny", "TINYINT"}, {"ratio", "FLOAT"}, {"paid", "BOOLEAN"}, {"name", "STRING"}, {"updated_at", "TIMESTAMP"},
		{"day", "DATE"}, {"blob", "BINARY"}, {"tags", "ARRAY"}, {"note", "STRING"},
	}
	row := []any{
		int64(9007199254740993), "12345678901234567890.12", 1.5, int32(7), int16(-2),
		int8(3), float32(0.25), true, "Alice", ts,
		time.Date(2026, 10, 18, 0, 0, 0, 0, time.UTC), []byte{1, 2, 3}, []byte(`["a","b"]`), nil,
	}
	d, _ := newFake(ok(fields, row))
	got, found, err := d.Row(context.Background(), "sales.orders", "id", "42")
	if err != nil || !found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	want := map[string]any{
		"id": "9007199254740993", "amount": "12345678901234567890.12", "price": "1.5", "qty": "7", "small": "-2",
		"tiny": "3", "ratio": "0.25", "paid": true, "name": "Alice", "updated_at": "2026-10-18T11:00:00.123Z",
		"day": "2026-10-18", "blob": "<3 bytes>", "tags": `["a","b"]`, "note": nil,
	}
	if len(got) != len(want) {
		t.Fatalf("got %d columns: %#v", len(got), got)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("%s = %#v, want %#v", k, got[k], v)
		}
	}
}

func TestRowNotFoundAndShape(t *testing.T) {
	d, _ := newFake(ok([]field{{"id", "BIGINT"}}))
	if _, found, err := d.Row(context.Background(), "sales.orders", "id", "1"); err != nil || found {
		t.Fatalf("found=%v err=%v", found, err)
	}
	d, _ = newFake(ok([]field{{"id", "BIGINT"}, {"x", "STRING"}}, []any{int64(1)}))
	if _, _, err := d.Row(context.Background(), "sales.orders", "id", "1"); err == nil {
		t.Fatal("want error on a short row")
	}
}

func TestPrimaryKeyColumn(t *testing.T) {
	cols := []field{{"column_name", "STRING"}}
	d, f := newFake(
		ok(cols, []any{"ID"}),
		ok(cols),
		ok(cols, []any{"tenant_id"}, []any{"id"}),
	)
	ctx := context.Background()
	got, err := d.PrimaryKeyColumn(ctx, "Sales.Orders")
	if err != nil || got != "id" {
		t.Fatalf("got %q err %v", got, err)
	}
	q := f.calls()[0]
	for _, want := range []string{"`main`.information_schema.table_constraints", "`main`.information_schema.key_column_usage", "lower(:schema)", "lower(:table)", "'PRIMARY KEY'", "ORDER BY k.ordinal_position"} {
		if !strings.Contains(q.SQL, want) {
			t.Errorf("sql lacks %s: %s", want, q.SQL)
		}
	}
	if strings.Contains(q.SQL, "Sales") || strings.Contains(q.SQL, "Orders") {
		t.Errorf("names reached the SQL text: %s", q.SQL)
	}
	if len(q.Params) != 2 || q.Params[0].Value != "Sales" || q.Params[1].Value != "Orders" {
		t.Errorf("params = %#v", q.Params)
	}
	if _, err := d.PrimaryKeyColumn(ctx, "sales.orders"); !errors.Is(err, clients.ErrNotFound) {
		t.Errorf("no key: %v", err)
	}
	if _, err := d.PrimaryKeyColumn(ctx, "sales.orders"); !errors.Is(err, clients.ErrNotSupported) {
		t.Errorf("composite: %v", err)
	}
}

func TestErrorMapping(t *testing.T) {
	long := strings.Repeat("x", 1000)
	for _, c := range []struct {
		name string
		res  fakeResult
		want error
	}{
		{"table missing", fail(execErr{"[TABLE_OR_VIEW_NOT_FOUND] The table or view `main`.`sales`.`nope` cannot be found.", "42P01"}), clients.ErrNotFound},
		{"schema missing", fail(execErr{"[SCHEMA_NOT_FOUND]", "42704"}), clients.ErrNotFound},
		{"catalog missing", fail(execErr{"[CATALOG_NOT_FOUND]", "42P08"}), clients.ErrNotFound},
		{"column missing", fail(execErr{"[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column `nope` cannot be resolved", "42703"}), clients.ErrNotFound},
		{"no select", fail(execErr{"[INSUFFICIENT_PERMISSIONS] User does not have SELECT on Table", "42501"}), ErrPermissionDenied},
		{"http 403", fakeResult{err: errors.New("unexpected HTTP status 403 Forbidden"), httpCode: http.StatusForbidden}, ErrPermissionDenied},
		{"http 401", fakeResult{err: errors.New("unexpected HTTP status 401"), httpCode: http.StatusUnauthorized}, ErrPermissionDenied},
		{"http 404", fakeResult{err: errors.New("unexpected HTTP status 404"), httpCode: http.StatusNotFound}, ErrEndpointNotFound},
		{"oversize", fakeResult{err: errors.New("short read"), oversize: true}, ErrResponseTooLarge},
		// Text alone never classifies: a message that says "not found" or
		// "permission" without a SQLSTATE stays a plain error.
		{"text only", fail(errors.New("TABLE_OR_VIEW_NOT_FOUND permission denied")), nil},
		{"syntax", fail(execErr{"[PARSE_SYNTAX_ERROR] " + long, "42601"}), nil},
	} {
		t.Run(c.name, func(t *testing.T) {
			d, _ := newFake(c.res)
			_, err := d.Freshness(context.Background(), "sales.orders", "updated_at")
			if err == nil {
				t.Fatal("want error")
			}
			for _, s := range []error{clients.ErrNotFound, ErrPermissionDenied, ErrEndpointNotFound, ErrResponseTooLarge} {
				if errors.Is(err, s) != (s == c.want) {
					t.Errorf("errors.Is(%v) = %v; err %v", s, !(s == c.want), err)
				}
			}
			if c.res.err != nil && !errors.Is(err, c.res.err) {
				t.Errorf("driver error not wrapped: %v", err)
			}
			if n := len([]rune(err.Error())); n > maxErrRunes+100 {
				t.Errorf("message is %d runes", n)
			}
			var se sqlStater
			if _, isExec := c.res.err.(execErr); isExec && !errors.As(err, &se) {
				t.Errorf("errors.As lost the driver error")
			}
		})
	}
}

// A data exception can quote a value; only its SQLSTATE stays (A3's
// contract case for the Postgres warehouse, #564).
func TestDataExceptionDropsServerText(t *testing.T) {
	leak := execErr{"[CAST_INVALID_INPUT] The value 'zoe.secret@example.com' of the type STRING cannot be cast to TIMESTAMP", "22018"}
	d, _ := newFake(fail(leak))
	_, err := d.Freshness(context.Background(), "sales.orders", "email")
	if err == nil || strings.Contains(err.Error(), "zoe.secret") || !strings.Contains(err.Error(), "22018") {
		t.Fatalf("class 22 error = %v, want a fixed phrase with the SQLSTATE", err)
	}
	if !errors.Is(err, error(leak)) {
		t.Fatalf("driver error not wrapped: %v", err)
	}
}

func TestServerTextIsRedactedAndFolded(t *testing.T) {
	text := "failed for token " + testToken + "\nAuthorization: Bearer abcdefghijklmnopqrstuvwxyz0123456789\r\nline3\x00"
	d, _ := newFake(fail(execErr{text, "XX000"}))
	_, err := d.Freshness(context.Background(), "sales.orders", "updated_at")
	msg := err.Error()
	if strings.Contains(msg, testToken) || strings.Contains(msg, "abcdefghijklmnopqrstuvwxyz0123456789") {
		t.Fatalf("secret survives: %s", msg)
	}
	if strings.ContainsAny(msg, "\n\r\x00") {
		t.Fatalf("not folded: %q", msg)
	}
}

func TestTimeoutAndCancel(t *testing.T) {
	d, f := newFake()
	f.block = true

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := d.Freshness(ctx, "sales.orders", "updated_at"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("deadline: %v", err)
	}

	ctx, cancel = context.WithCancel(context.Background())
	time.AfterFunc(20*time.Millisecond, cancel)
	if _, _, err := d.Row(ctx, "sales.orders", "id", "1"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
}

func TestEveryCallHasADeadline(t *testing.T) {
	var deadlines []time.Duration
	r := runnerFunc(func(ctx context.Context, q query) (result, error) {
		dl, ok := ctx.Deadline()
		if !ok {
			t.Errorf("no deadline for %s", q.SQL)
		}
		deadlines = append(deadlines, time.Until(dl))
		return result{Fields: []field{{"x", "TIMESTAMP"}}}, nil
	})
	d := newWithRunner(testCfg, r)
	ctx := context.Background()
	_, _ = d.Freshness(ctx, "s.t", "c")
	_, _, _ = d.Row(ctx, "s.t", "c", "1")
	_, _ = d.PrimaryKeyColumn(ctx, "s.t")
	_, _ = d.CountByBucket(ctx, "s.t", "c", time.Now(), time.Now().Add(time.Hour), time.Minute)
	if len(deadlines) != 4 {
		t.Fatalf("%d calls", len(deadlines))
	}
	for _, dl := range deadlines {
		if dl > defaultTimeout || dl < defaultTimeout-time.Second {
			t.Errorf("deadline %s", dl)
		}
	}
}

type runnerFunc func(ctx context.Context, q query) (result, error)

func (f runnerFunc) run(ctx context.Context, q query) (result, error) { return f(ctx, q) }

func TestProvenance(t *testing.T) {
	rec := tools.NewRecorder()
	ctx := tools.WithRecorder(context.Background(), rec)
	d, _ := newFake(ok([]field{{"id", "BIGINT"}}), fail(execErr{"boom", "42P01"}))
	_, _, _ = d.Row(ctx, "sales.orders", "id", "42")
	_, _ = d.Freshness(ctx, "sales.orders", "updated_at")
	calls := rec.Calls()
	if len(calls) != 2 {
		t.Fatalf("%d calls recorded", len(calls))
	}
	c := calls[0]
	if c.Kind != tools.CallSQL || c.Target != "databricks dbc-1234abcd-5678.cloud.databricks.com/main" {
		t.Errorf("call = %+v", c)
	}
	if !strings.Contains(c.Text, ":key") || strings.Contains(c.Text, "42") {
		t.Errorf("text = %s", c.Text)
	}
	if len(c.ParamsRedacted) != 1 || c.ParamsRedacted[0] != ":key=42" {
		t.Errorf("params = %v", c.ParamsRedacted)
	}
	for _, c := range calls {
		if strings.Contains(c.Target+c.Text+strings.Join(c.ParamsRedacted, ""), testToken) {
			t.Errorf("token in receipt: %+v", c)
		}
	}
}

func TestRenderParams(t *testing.T) {
	got := renderParams([]sql.NamedArg{
		sql.Named("s", strings.Repeat("é", 50)), sql.Named("n", int64(5)), sql.Named("b", []byte("xyz")),
		sql.Named("t", time.Date(2026, 1, 2, 3, 4, 5, 0, time.FixedZone("X", 3600))), sql.Named("z", nil),
	})
	if !strings.HasSuffix(got[0], "…") || len(got[0]) > len(":s=")+maxParamBytes+len("…") {
		t.Errorf("long string not cut: %s", got[0])
	}
	for i, want := range []string{":n=5", ":b=<3 bytes>", ":t=2026-01-02T02:04:05Z", ":z=NULL"} {
		if got[i+1] != want {
			t.Errorf("got %s want %s", got[i+1], want)
		}
	}
}
