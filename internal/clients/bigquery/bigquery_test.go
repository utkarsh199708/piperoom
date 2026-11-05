package bigquery

import (
	"context"
	"errors"
	"math/big"
	"net/http"
	"strings"
	"testing"
	"time"

	bq "cloud.google.com/go/bigquery"
	"cloud.google.com/go/civil"
	"google.golang.org/api/googleapi"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

var tsField = []field{{Name: "newest", Type: bq.TimestampFieldType}}

func TestFreshnessQueryIsQuotedAndParameterless(t *testing.T) {
	q, err := freshnessQuery("acme-data", "analytics.orders", "loaded_at")
	if err != nil {
		t.Fatal(err)
	}
	want := "SELECT MAX(`loaded_at`) AS newest FROM `acme-data`.`analytics`.`orders`"
	if q.SQL != want || len(q.Params) != 0 || q.MaxRows != 1 {
		t.Fatalf("query = %+v, want SQL %q", q, want)
	}
}

func TestRowQueryUsesParameterForKey(t *testing.T) {
	key := "o-1' OR '1'='1"
	q, err := rowQuery("acme-data", "analytics.orders", "id", key)
	if err != nil {
		t.Fatal(err)
	}
	want := "SELECT * FROM `acme-data`.`analytics`.`orders` WHERE CAST(`id` AS STRING) = @key LIMIT 1"
	if q.SQL != want {
		t.Fatalf("SQL = %q, want %q", q.SQL, want)
	}
	if strings.Contains(q.SQL, key) {
		t.Fatal("key value interpolated into SQL")
	}
	if len(q.Params) != 1 || q.Params[0].Name != "key" || q.Params[0].Value != key {
		t.Fatalf("params = %+v", q.Params)
	}
}

func TestPKQueryUsesParameterForTable(t *testing.T) {
	q, err := pkQuery("acme-data", "analytics.orders")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(q.SQL, "FROM `acme-data`.`analytics`.INFORMATION_SCHEMA.TABLE_CONSTRAINTS") ||
		!strings.Contains(q.SQL, "c.table_name = @table") || strings.Contains(q.SQL, "'orders'") {
		t.Fatalf("SQL = %q", q.SQL)
	}
	if len(q.Params) != 1 || q.Params[0].Value != "orders" {
		t.Fatalf("params = %+v", q.Params)
	}
}

func TestIdentifierRefusal(t *testing.T) {
	bad := []struct{ table, col string }{
		{"analytics.orders; DROP TABLE x", "id"},
		{"analytics.orders", "id; DROP TABLE x"},
		{"analytics.orders`", "id"},
		{"analytics.orders", "id`"},
		{"orders", "id"},
		{"acme-data.analytics.orders", "id"}, // no project prefix
		{"other.project.analytics.orders", "id"},
		{"analytics.orders", ""},
		{"analytics.orders", "1=1"},
		{"analytics.or-ders", "id"},
		{".orders", "id"},
		{"analytics.", "id"},
		{"analytics.orders", "a.b"},
		{"analytics.orders", "naïve"},
	}
	for _, tc := range bad {
		ctx := context.Background()
		db, f := newFake()
		if _, err := db.Freshness(ctx, tc.table, tc.col); err == nil || !strings.HasPrefix(err.Error(), "bigquery: ") {
			t.Errorf("Freshness(%q,%q) err = %v", tc.table, tc.col, err)
		}
		if _, _, err := db.Row(ctx, tc.table, tc.col, "1"); err == nil || !strings.HasPrefix(err.Error(), "bigquery: ") {
			t.Errorf("Row(%q,%q) err = %v", tc.table, tc.col, err)
		}
		if n := len(f.calls()); n != 0 {
			t.Errorf("%d queries ran for table=%q col=%q", n, tc.table, tc.col)
		}
	}
	// Nothing at all runs for an invalid table.
	db, f := newFake()
	_, _ = db.Freshness(context.Background(), "analytics.orders`; --", "id")
	_, _ = db.PrimaryKeyColumn(context.Background(), "x")
	_, _ = db.LoadErrors(context.Background(), "a.b.c", time.Time{}, 1)
	if n := len(f.calls()); n != 0 {
		t.Fatalf("%d queries ran for invalid input", n)
	}
}

func TestFreshness(t *testing.T) {
	ts := time.Date(2026, 10, 18, 9, 30, 0, 0, time.FixedZone("x", 2*3600))
	cases := []struct {
		name string
		res  fakeResult
		want time.Time
	}{
		{"timestamp to UTC", ok(tsField, []bq.Value{ts}), ts.UTC()},
		{"datetime as UTC wall time", ok([]field{{Name: "newest", Type: bq.DateTimeFieldType}},
			[]bq.Value{civil.DateTime{Date: civil.Date{Year: 2026, Month: 10, Day: 18}, Time: civil.Time{Hour: 7}}}),
			time.Date(2026, 10, 18, 7, 0, 0, 0, time.UTC)},
		{"empty table (NULL)", ok(tsField, []bq.Value{nil}), time.Time{}},
		{"no rows", ok(nil), time.Time{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, _ := newFake(tc.res)
			got, err := db.Freshness(context.Background(), "analytics.orders", "loaded_at")
			if err != nil {
				t.Fatal(err)
			}
			if !got.Equal(tc.want) || got.Location() != time.UTC {
				t.Fatalf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestFreshnessRefusesNonTimestampWithoutValue(t *testing.T) {
	const secret = "alice@example.com"
	for _, ft := range []bq.FieldType{bq.StringFieldType, bq.IntegerFieldType, bq.DateFieldType} {
		db, _ := newFake(ok([]field{{Name: "newest", Type: ft}}, []bq.Value{secret}))
		_, err := db.Freshness(context.Background(), "analytics.users", "email")
		if !errors.Is(err, ErrNotTimestamp) {
			t.Fatalf("%s: err = %v, want ErrNotTimestamp", ft, err)
		}
		if strings.Contains(err.Error(), secret) {
			t.Fatalf("error carries the column value: %v", err)
		}
	}
}

func TestRow(t *testing.T) {
	at := time.Date(2026, 10, 18, 9, 0, 0, 0, time.UTC)
	fields := []field{
		{Name: "ID", Type: bq.IntegerFieldType},
		{Name: "Amount", Type: bq.NumericFieldType},
		{Name: "updated_at", Type: bq.TimestampFieldType},
		{Name: "day", Type: bq.DateFieldType},
		{Name: "tags", Type: bq.StringFieldType},
		{Name: "blob", Type: bq.BytesFieldType},
		{Name: "gone", Type: bq.StringFieldType},
	}
	db, f := newFake(ok(fields, []bq.Value{
		int64(9007199254740993), big.NewRat(12345, 100), at, civil.Date{Year: 2026, Month: 10, Day: 18},
		[]bq.Value{"a", "b"}, []byte("xyz"), nil,
	}))
	row, found, err := db.Row(context.Background(), "analytics.orders", "id", "9007199254740993")
	if err != nil || !found {
		t.Fatalf("Row: found=%v err=%v", found, err)
	}
	want := map[string]any{
		"id": "9007199254740993", "amount": "123.450000000", "updated_at": "2026-10-18T09:00:00Z",
		"day": "2026-10-18", "tags": `["a","b"]`, "blob": "<3 bytes>", "gone": nil,
	}
	for k, v := range want {
		if row[k] != v {
			t.Errorf("row[%q] = %#v, want %#v", k, row[k], v)
		}
	}
	if len(row) != len(want) {
		t.Errorf("row = %v", row)
	}
	if q := f.calls(); len(q) != 1 || q[0].Params[0].Value != "9007199254740993" {
		t.Fatalf("queries = %+v", q)
	}
}

func TestRowNotFound(t *testing.T) {
	db, _ := newFake(ok([]field{{Name: "id", Type: bq.StringFieldType}}))
	row, found, err := db.Row(context.Background(), "analytics.orders", "id", "nope")
	if err != nil || found || row != nil {
		t.Fatalf("got row=%v found=%v err=%v", row, found, err)
	}
}

func TestPrimaryKeyColumn(t *testing.T) {
	col := []field{{Name: "column_name", Type: bq.StringFieldType}}
	db, _ := newFake(ok(col, []bq.Value{"order_id"}))
	pk, err := db.PrimaryKeyColumn(context.Background(), "analytics.orders")
	if err != nil || pk != "order_id" {
		t.Fatalf("pk=%q err=%v", pk, err)
	}

	db, _ = newFake(ok(col))
	if _, err := db.PrimaryKeyColumn(context.Background(), "analytics.orders"); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("no key: err = %v", err)
	}

	db, _ = newFake(ok(col, []bq.Value{"tenant_id"}, []bq.Value{"id"}))
	if _, err := db.PrimaryKeyColumn(context.Background(), "analytics.orders"); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("composite: err = %v", err)
	}
}

func TestErrorPaths(t *testing.T) {
	notFound := &googleapi.Error{Code: http.StatusNotFound, Message: "Not found: Table acme-data:analytics.orders",
		Errors: []googleapi.ErrorItem{{Reason: "notFound", Message: "Not found: Table acme-data:analytics.orders"}}}
	denied := &googleapi.Error{Code: http.StatusForbidden, Message: "Access Denied: Table acme-data:analytics.orders: User does not have permission",
		Errors: []googleapi.ErrorItem{{Reason: "accessDenied"}}}
	bare404 := &googleapi.Error{Code: http.StatusNotFound}
	bare403 := &googleapi.Error{Code: http.StatusForbidden}
	quota := &googleapi.Error{Code: http.StatusForbidden, Message: "Quota exceeded",
		Errors: []googleapi.ErrorItem{{Reason: "quotaExceeded"}}}
	jobNotFound := &bq.Error{Reason: "notFound", Message: "Not found: Dataset acme-data:analytics"}
	multiline := errors.New("Syntax error:\n\tline 1 password=hunter2-very-secret\n" + strings.Repeat("x", 1000))

	cases := []struct {
		name   string
		err    error
		is     error
		isNot  error
		noText string
	}{
		{"404 notFound", notFound, clients.ErrNotFound, ErrPermissionDenied, ""},
		{"404 bare", bare404, clients.ErrNotFound, nil, ""},
		{"job notFound", jobNotFound, clients.ErrNotFound, nil, ""},
		{"403 accessDenied", denied, ErrPermissionDenied, clients.ErrNotFound, ""},
		{"403 bare", bare403, ErrPermissionDenied, nil, ""},
		{"403 quota is not permission", quota, nil, ErrPermissionDenied, ""},
		{"untrusted text cleaned", multiline, nil, clients.ErrNotFound, "\n"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			db, _ := newFake(fail(tc.err), fail(tc.err), fail(tc.err), fail(tc.err))
			ctx := context.Background()
			_, e1 := db.Freshness(ctx, "analytics.orders", "loaded_at")
			_, _, e2 := db.Row(ctx, "analytics.orders", "id", "1")
			_, e3 := db.PrimaryKeyColumn(ctx, "analytics.orders")
			_, e4 := db.LoadErrors(ctx, "analytics.orders", time.Now().Add(-time.Hour), 5)
			for i, err := range []error{e1, e2, e3, e4} {
				if err == nil {
					t.Fatalf("call %d: no error", i)
				}
				if tc.is != nil && !errors.Is(err, tc.is) {
					t.Errorf("call %d: %v is not %v", i, err, tc.is)
				}
				if tc.isNot != nil && errors.Is(err, tc.isNot) {
					t.Errorf("call %d: %v is %v", i, err, tc.isNot)
				}
				if !errors.Is(err, tc.err) {
					t.Errorf("call %d: original error lost: %v", i, err)
				}
				if tc.noText != "" && strings.Contains(err.Error(), tc.noText) {
					t.Errorf("call %d: message keeps %q: %q", i, tc.noText, err)
				}
				if strings.Contains(err.Error(), "hunter2") {
					t.Errorf("call %d: secret not redacted: %q", i, err)
				}
				if len([]rune(err.Error())) > maxErrRunes+100 {
					t.Errorf("call %d: message not cut (%d runes)", i, len([]rune(err.Error())))
				}
			}
		})
	}
}

func TestTimeoutAndCancel(t *testing.T) {
	t.Run("deadline", func(t *testing.T) {
		db := newWithRunner(testCfg, &fakeRunner{block: true})
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		_, err := db.Freshness(ctx, "analytics.orders", "loaded_at")
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("cancelled", func(t *testing.T) {
		db := newWithRunner(testCfg, &fakeRunner{block: true})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, _, err := db.Row(ctx, "analytics.orders", "id", "1")
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("library error after deadline reports deadline", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		err := wrapQueryErr(ctx, "freshness of", "analytics.orders", errors.New("Post \"https://bigquery.googleapis.com/...\": net/http: request canceled"))
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("default deadline is set", func(t *testing.T) {
		var dl time.Time
		var had bool
		r := runnerFunc(func(ctx context.Context, _ query) (result, error) {
			dl, had = ctx.Deadline()
			return result{}, nil
		})
		db := newWithRunner(testCfg, r)
		if _, err := db.Freshness(context.Background(), "analytics.orders", "loaded_at"); err != nil {
			t.Fatal(err)
		}
		if !had || time.Until(dl) > defaultTimeout {
			t.Fatalf("deadline = %v (set=%v)", dl, had)
		}
	})
}

type runnerFunc func(context.Context, query) (result, error)

func (f runnerFunc) run(ctx context.Context, q query) (result, error) { return f(ctx, q) }

func TestRecordsProvenance(t *testing.T) {
	db, _ := newFake(ok([]field{{Name: "id", Type: bq.StringFieldType}}))
	rec := tools.NewRecorder()
	ctx := tools.WithRecorder(context.Background(), rec)
	if _, _, err := db.Row(ctx, "analytics.orders", "id", "o-1001"); err != nil {
		t.Fatal(err)
	}
	calls := rec.Calls()
	if len(calls) != 1 {
		t.Fatalf("calls = %+v", calls)
	}
	c := calls[0]
	if c.Kind != tools.CallSQL || c.Target != "bigquery acme-data/us" ||
		c.Text != "SELECT * FROM `acme-data`.`analytics`.`orders` WHERE CAST(`id` AS STRING) = @key LIMIT 1" {
		t.Fatalf("call = %+v", c)
	}
	if len(c.ParamsRedacted) != 1 || c.ParamsRedacted[0] != "@key=o-1001" {
		t.Fatalf("params = %v", c.ParamsRedacted)
	}
}

func TestRenderValue(t *testing.T) {
	long := strings.Repeat("é", 100)
	cases := []struct {
		in   any
		want string
	}{
		{nil, "NULL"},
		{"abc", "abc"},
		{int64(5), "5"},
		{time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC), "2026-01-02T03:04:05Z"},
		{[]byte("secret"), "<6 bytes>"},
	}
	for _, tc := range cases {
		if got := renderValue(tc.in); got != tc.want {
			t.Errorf("renderValue(%#v) = %q, want %q", tc.in, got, tc.want)
		}
	}
	if got := renderValue(long); len(got) > maxParamBytes+len("…") || !strings.HasSuffix(got, "…") {
		t.Errorf("long value not cut: %q", got)
	}
}

func TestCloseNil(t *testing.T) {
	var d *DB
	if err := d.Close(); err != nil {
		t.Fatal(err)
	}
	if err := newWithRunner(testCfg, &fakeRunner{}).Close(); err != nil {
		t.Fatal(err)
	}
}
