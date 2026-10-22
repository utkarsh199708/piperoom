package main

import (
	"maps"
	"strings"
	"testing"
)

func TestParseSkipWhere(t *testing.T) {
	tests := []struct {
		raw     string
		want    skipWhere
		wantErr bool
	}{
		{"", skipWhere{}, false},
		{"  ", skipWhere{}, false},
		{"status=backfill", skipWhere{"status", "backfill"}, false},
		{" status = backfill ", skipWhere{"status", "backfill"}, false},
		{"customer_id=42", skipWhere{"customer_id", "42"}, false},
		{"id=7", skipWhere{"id", "7"}, false},
		{"amount=5", skipWhere{}, true},           // not a testable column
		{"status", skipWhere{}, true},             // no value
		{"status=", skipWhere{}, true},            // empty value
		{"customer_id=abc", skipWhere{}, true},    // numeric column
		{"id=1; DROP TABLE x", skipWhere{}, true}, // never reaches SQL, still refused
	}
	for _, tt := range tests {
		got, err := parseSkipWhere(tt.raw)
		if (err != nil) != tt.wantErr {
			t.Fatalf("parseSkipWhere(%q) error = %v, wantErr %v", tt.raw, err, tt.wantErr)
		}
		if !tt.wantErr && got != tt.want {
			t.Errorf("parseSkipWhere(%q) = %+v, want %+v", tt.raw, got, tt.want)
		}
	}
}

func TestSkipWhereMatches(t *testing.T) {
	row := func(id, cust int64, status string) change {
		return change{Op: "c", ID: id, After: &orderRow{ID: id, CustomerID: cust, Status: status}}
	}
	tests := []struct {
		name string
		w    string
		c    change
		want bool
	}{
		{"status match", "status=backfill", row(1, 5, "backfill"), true},
		{"status other", "status=backfill", row(1, 5, "placed"), false},
		{"customer match", "customer_id=5", row(1, 5, "placed"), true},
		{"customer other", "customer_id=5", row(1, 6, "placed"), false},
		{"id match", "id=1", row(1, 5, "placed"), true},
		{"update matches too", "status=backfill", change{Op: "u", ID: 2, After: &orderRow{ID: 2, Status: "backfill"}}, true},
		{"delete passes", "status=backfill", change{Op: "d", ID: 3}, false},
		{"off", "", row(1, 5, "backfill"), false},
	}
	for _, tt := range tests {
		w, err := parseSkipWhere(tt.w)
		if err != nil {
			t.Fatal(err)
		}
		if got := w.matches(tt.c); got != tt.want {
			t.Errorf("%s: matches = %v, want %v", tt.name, got, tt.want)
		}
	}
	if s := (skipWhere{"status", "x"}).String(); s != "status=x" {
		t.Errorf("String = %q", s)
	}
	if s := (skipWhere{}).String(); s != "" {
		t.Errorf("zero String = %q", s)
	}
}

func TestParseDropColumns(t *testing.T) {
	tests := []struct {
		raw     string
		want    map[string]bool
		joined  string
		wantErr bool
	}{
		{"", map[string]bool{}, "", false},
		{"updated_at", map[string]bool{"updated_at": true}, "updated_at", false},
		{" status , amount ,", map[string]bool{"amount": true, "status": true}, "amount,status", false},
		{"loaded_at", nil, "", true}, // the sink's own column is never dropped
		{"id", nil, "", true},        // the key is never dropped
		{"updated_at,nope", nil, "", true},
	}
	for _, tt := range tests {
		got, err := parseDropColumns(tt.raw)
		if (err != nil) != tt.wantErr {
			t.Fatalf("parseDropColumns(%q) error = %v, wantErr %v", tt.raw, err, tt.wantErr)
		}
		if tt.wantErr {
			continue
		}
		if !maps.Equal(got, tt.want) {
			t.Errorf("parseDropColumns(%q) = %v, want %v", tt.raw, got, tt.want)
		}
		if j := joinColumns(got); j != tt.joined {
			t.Errorf("joinColumns(%v) = %q, want %q", got, j, tt.joined)
		}
	}
}

// The default upsert is the original statement: every column in the INSERT and
// in the update list, loaded_at refreshed.
func TestUpsertSQLDefault(t *testing.T) {
	q, cols := upsertSQL(nil)
	want := "INSERT INTO analytics.orders (id, customer_id, amount, status, updated_at, loaded_at)\n" +
		"VALUES ($1, $2, $3, $4, $5, now())\n" +
		"ON CONFLICT (id) DO UPDATE SET customer_id = EXCLUDED.customer_id, amount = EXCLUDED.amount, " +
		"status = EXCLUDED.status, updated_at = EXCLUDED.updated_at, loaded_at = now()"
	if q != want {
		t.Errorf("default upsert =\n%s\nwant\n%s", q, want)
	}
	if strings.Join(cols, ",") != "customer_id,amount,status,updated_at" {
		t.Errorf("cols = %v", cols)
	}
}

// A dropped column is in neither list and the placeholders stay dense.
func TestUpsertSQLDropped(t *testing.T) {
	q, cols := upsertSQL(map[string]bool{"updated_at": true})
	if strings.Contains(q, "updated_at") {
		t.Errorf("dropped column still in the statement:\n%s", q)
	}
	if !strings.Contains(q, "VALUES ($1, $2, $3, $4, now())") || !strings.Contains(q, "loaded_at = now()") {
		t.Errorf("unexpected statement:\n%s", q)
	}
	if strings.Join(cols, ",") != "customer_id,amount,status" {
		t.Errorf("cols = %v", cols)
	}

	q, cols = upsertSQL(map[string]bool{"customer_id": true, "status": true})
	if !strings.Contains(q, "(id, amount, updated_at, loaded_at)") || !strings.Contains(q, "VALUES ($1, $2, $3, now())") {
		t.Errorf("two dropped:\n%s", q)
	}
	if strings.Join(cols, ",") != "amount,updated_at" {
		t.Errorf("cols = %v", cols)
	}
}
