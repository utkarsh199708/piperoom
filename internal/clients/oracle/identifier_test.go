package oracle

import "testing"

func TestQuoteIdent(t *testing.T) {
	ok := []struct{ in, want string }{
		{"ORDERS", `"ORDERS"`},
		{"orders", `"orders"`},
		{"C##LOGMINER", `"C##LOGMINER"`},
		{"A$B", `"A$B"`},
		{"A#B", `"A#B"`},
		{"_leading", `"_leading"`},
		{"A9_b", `"A9_b"`},
	}
	for _, tc := range ok {
		got, err := quoteIdent(tc.in)
		if err != nil {
			t.Errorf("quoteIdent(%q) error: %v", tc.in, err)
			continue
		}
		if got != tc.want {
			t.Errorf("quoteIdent(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}

	bad := []string{
		"", "9leading", "with space", "a.b", "semi;colon", "quote\"x", "a-b", "a@b",
	}
	for _, in := range bad {
		if _, err := quoteIdent(in); err == nil {
			t.Errorf("quoteIdent(%q) succeeded, want error", in)
		}
	}
}

func TestQuoteTable(t *testing.T) {
	got, err := quoteTable("APP.ORDERS")
	if err != nil {
		t.Fatalf("quoteTable: %v", err)
	}
	if want := `"APP"."ORDERS"`; got != want {
		t.Fatalf("quoteTable = %q, want %q", got, want)
	}

	for _, in := range []string{"ORDERS", "A.B.C", "APP.", ".ORDERS", "APP..ORDERS"} {
		if _, err := quoteTable(in); err == nil {
			t.Errorf("quoteTable(%q) succeeded, want error", in)
		}
	}
}

func TestQuoteTableParts(t *testing.T) {
	owner, name, err := quoteTableParts("APP.ORDERS")
	if err != nil {
		t.Fatalf("quoteTableParts: %v", err)
	}
	if owner != `"APP"` || name != `"ORDERS"` {
		t.Fatalf("got %q %q, want quoted parts", owner, name)
	}
}
