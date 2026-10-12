package snowflake

import (
	"context"
	"strings"
	"testing"
)

func TestFreshnessSQL(t *testing.T) {
	cases := []struct {
		table, col, want string
	}{
		{"analytics.orders", "updated_at", `SELECT MAX("UPDATED_AT") FROM "ANALYTICS"."ORDERS"`},
		{"Analytics.Orders", "Updated_At", `SELECT MAX("UPDATED_AT") FROM "ANALYTICS"."ORDERS"`},
		{"dwh.analytics.orders", "_loaded$at", `SELECT MAX("_LOADED$AT") FROM "DWH"."ANALYTICS"."ORDERS"`},
		{"analytics.order", "timestamp", `SELECT MAX("TIMESTAMP") FROM "ANALYTICS"."ORDER"`},
	}
	for _, tc := range cases {
		got, err := freshnessSQL(tc.table, tc.col)
		if err != nil || got != tc.want {
			t.Errorf("freshnessSQL(%q, %q) = %q, %v; want %q", tc.table, tc.col, got, err, tc.want)
		}
	}
}

func TestRowSQL(t *testing.T) {
	got, err := rowSQL("analytics.orders", "id")
	want := `SELECT * FROM "ANALYTICS"."ORDERS" WHERE "ID" = ? LIMIT 1`
	if err != nil || got != want {
		t.Fatalf("rowSQL = %q, %v; want %q", got, err, want)
	}
}

// injections are table, column or key strings an LLM or a user could supply.
var injections = []string{
	`orders; DROP TABLE x`,
	`orders" --`,
	`"orders"`,
	"orders\n--",
	`orders) UNION SELECT 1 --`,
	`o'rders`,
	`a b`,
	`a-b`,
	``,
}

func TestSQLBuildersRefuseInjection(t *testing.T) {
	for _, bad := range injections {
		if q, err := freshnessSQL("analytics."+bad, "updated_at"); err == nil {
			t.Errorf("freshnessSQL(table part %q) built %q, want error", bad, q)
		}
		if q, err := freshnessSQL("analytics.orders", bad); err == nil {
			t.Errorf("freshnessSQL(column %q) built %q, want error", bad, q)
		}
		if q, err := rowSQL("analytics."+bad, "id"); err == nil {
			t.Errorf("rowSQL(table part %q) built %q, want error", bad, q)
		}
		if q, err := rowSQL("analytics.orders", bad); err == nil {
			t.Errorf("rowSQL(column %q) built %q, want error", bad, q)
		}
	}
}

// TestRefusedIdentifierNeverReachesDriver checks the public methods stop
// before running a query when an identifier is bad.
func TestRefusedIdentifierNeverReachesDriver(t *testing.T) {
	fq := &fakeQuerier{}
	db := &DB{q: fq}
	if _, err := db.Freshness(context.Background(), `analytics.orders" --`, "updated_at"); err == nil {
		t.Fatal("Freshness: want error")
	}
	if _, _, err := db.Row(context.Background(), "analytics.orders", `id" OR "1"="1`, "1"); err == nil {
		t.Fatal("Row: want error")
	}
	if fq.lastQuery != "" {
		t.Fatalf("a query reached the driver: %q", fq.lastQuery)
	}
}

// TestRowKeyIsBoundNotInterpolated: the key is attacker-shaped text, and must
// only ever travel as a bind argument.
func TestRowKeyIsBoundNotInterpolated(t *testing.T) {
	fq := &fakeQuerier{rows: &fakeRows{cols: []string{"ID"}, data: nil}}
	db := &DB{q: fq}
	key := `1' OR '1'='1`
	if _, _, err := db.Row(context.Background(), "analytics.orders", "id", key); err != nil {
		t.Fatalf("Row: %v", err)
	}
	if strings.Contains(fq.lastQuery, "'") || strings.Count(fq.lastQuery, "?") != 1 {
		t.Fatalf("key leaked into query text: %q", fq.lastQuery)
	}
	if len(fq.lastArgs) != 1 || fq.lastArgs[0] != key {
		t.Fatalf("args = %v, want the key as the only bind argument", fq.lastArgs)
	}
}
