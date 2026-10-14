package warehouse

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"

	"piperoom/internal/clients"
)

func TestValidateTableColumn(t *testing.T) {
	schema, tbl, col, err := validateTableColumn("analytics.orders", "updated_at")
	if err != nil {
		t.Fatalf("validateTableColumn: %v", err)
	}
	if schema != "analytics" || tbl != "orders" || col != "updated_at" {
		t.Fatalf("got (%q,%q,%q)", schema, tbl, col)
	}
}

func TestWrapErrNotFound(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{&pgconn.PgError{Code: "42P01"}, true},
		{&pgconn.PgError{Code: "42703"}, true},
		{&pgconn.PgError{Code: "3F000"}, true},
		{fmt.Errorf("q: %w", &pgconn.PgError{Code: "42P01"}), true},
		{&pgconn.PgError{Code: "57014"}, false}, // query_canceled
		{errors.New("dial tcp: connection refused"), false},
		{errors.New(`relation "x" does not exist`), false}, // never string matching
	}
	for _, tc := range cases {
		err := wrapErr("freshness of", "analytics.orders", tc.err)
		if got := errors.Is(err, clients.ErrNotFound); got != tc.want {
			t.Errorf("wrapErr(%v) is ErrNotFound = %v, want %v", tc.err, got, tc.want)
		}
		if !errors.Is(err, tc.err) {
			t.Errorf("wrapErr(%v) dropped the cause", tc.err)
		}
	}
}

func TestEndOfDay(t *testing.T) {
	now := time.Date(2026, 10, 14, 15, 0, 0, 0, time.UTC)
	day := func(d int) time.Time { return time.Date(2026, 10, d, 0, 0, 0, 0, time.UTC) }
	cases := []struct {
		day, want time.Time
	}{
		{day(14), now},     // today: fresh, clamped to now
		{day(13), day(14)}, // yesterday: end of yesterday
		{day(16), day(16)}, // future date stays ahead of now
	}
	for _, tc := range cases {
		if got := endOfDay(tc.day, now); !got.Equal(tc.want) {
			t.Errorf("endOfDay(%v) = %v, want %v", tc.day, got, tc.want)
		}
	}
}

func TestValidateTableColumnRejectsInjection(t *testing.T) {
	cases := []struct{ table, col string }{
		{"analytics.orders; DROP TABLE x", "id"},
		{"analytics.orders", "id; DROP TABLE x"},
		{"orders", "id"},
		{"analytics.orders", ""},
		{"analytics.orders", "1=1"},
	}
	for _, tc := range cases {
		if _, _, _, err := validateTableColumn(tc.table, tc.col); err == nil {
			t.Fatalf("expected error for table=%q col=%q", tc.table, tc.col)
		} else if !strings.Contains(err.Error(), "warehouse") {
			t.Fatalf("error not wrapped: %v", err)
		}
	}
}
