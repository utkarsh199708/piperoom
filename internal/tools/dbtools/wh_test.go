package dbtools

import "testing"

func TestValidWarehouseTable(t *testing.T) {
	cases := map[string]bool{
		"analytics.orders":     true,
		"dwh.analytics.orders": true,
		"orders":               false,
		"a.b.c.d":              false,
		"analytics.orders;":    false,
		`analytics."orders"`:   false,
		"a..b":                 false,
		"":                     false,
	}
	for name, want := range cases {
		if got := validWarehouseTable(name); got != want {
			t.Errorf("validWarehouseTable(%q) = %v, want %v", name, got, want)
		}
	}
}
