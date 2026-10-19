package clickhouse

import (
	"strings"
	"testing"
)

func TestIdentifierValidationAndQuoting(t *testing.T) {
	tick := string(rune(96))
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{name: "column", input: "updated_at", want: tick + "updated_at" + tick},
		{name: "database and table", input: "analytics.orders", want: tick + "analytics" + tick + "." + tick + "orders" + tick},
		{name: "reserved word", input: "order", want: tick + "order" + tick},
		{name: "empty", input: "", wantErr: true},
		{name: "leading digit", input: "1orders", wantErr: true},
		{name: "injection", input: "orders; DROP TABLE users", wantErr: true},
		{name: "three parts", input: "db.schema.orders", wantErr: true},
		{name: "dash", input: "order-items", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got string
			var err error
			if strings.Contains(tt.input, ".") || tt.input == "" || tt.input == "db.schema.orders" {
				got, err = quoteTable(tt.input)
			} else {
				got, err = quoteIdent(tt.input)
			}
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %t", err, tt.wantErr)
			}
			if err == nil && got != tt.want {
				t.Fatalf("quoted identifier = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSQLBuildersValidateIdentifiersAndBindValues(t *testing.T) {
	tick := string(rune(96))
	got, err := freshnessSQL("analytics.orders", "updated_at")
	if err != nil {
		t.Fatal(err)
	}
	if want := "SELECT max(" + tick + "updated_at" + tick + ") FROM " + tick + "analytics" + tick + "." + tick + "orders" + tick; got != want {
		t.Fatalf("freshness SQL = %q, want %q", got, want)
	}

	got, err = rowSQL("analytics.orders", "id")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got, " FINAL WHERE "+tick+"id"+tick+" = ? LIMIT 1") {
		t.Fatalf("row SQL does not use FINAL and a bind placeholder: %q", got)
	}

	got, err = countSQL("analytics.orders", "updated_at")
	if err != nil {
		t.Fatal(err)
	}
	for _, fragment := range []string{"toStartOfInterval(", "INTERVAL ? SECOND", ">= ?", "< ?", "count()"} {
		if !strings.Contains(got, fragment) {
			t.Errorf("count SQL %q is missing %q", got, fragment)
		}
	}

	if _, err := freshnessSQL("analytics.orders", "updated_at; DROP TABLE x"); err == nil {
		t.Fatal("freshnessSQL accepted an unsafe timestamp column")
	}
	if _, err := rowSQL("analytics.orders", "id OR 1=1"); err == nil {
		t.Fatal("rowSQL accepted an unsafe key column")
	}
}
