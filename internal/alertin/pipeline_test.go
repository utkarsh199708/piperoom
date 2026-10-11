package alertin

import "testing"

func TestMatchOwners(t *testing.T) {
	o := testOwners()
	tests := []struct {
		name   string
		values []string
		text   []string
		want   string
	}{
		{"connector value", []string{"pg-orders-connector"}, nil, "orders"},
		{"case-insensitive", []string{"PG-Orders-Connector"}, nil, "orders"},
		{"consumer group value", []string{"payment-sync"}, nil, "payments"},
		{"full table in text", nil, []string{"rows missing in analytics.orders."}, "orders"},
		{"bare table as value", []string{"payments"}, nil, "payments"},
		{"bare table in text is not enough", nil, []string{"payments are slow"}, ""},
		{"connector in text with punctuation", nil, []string{"Task (pg-orders-connector/0) FAILED"}, "orders"},
		{"substring does not match", nil, []string{"pg-orders-connector-v2 failed"}, ""},
		{"two pipelines is ambiguous", []string{"order-sync", "payment-sync"}, nil, ""},
		{"same pipeline twice is fine", []string{"order-sync", "pg-orders-connector"}, []string{"public.orders"}, "orders"},
		{"nothing", []string{"prod"}, []string{"disk full"}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := matchOwners(o, tt.values, tt.text); got != tt.want {
				t.Fatalf("matchOwners = %q, want %q", got, tt.want)
			}
		})
	}
	if got := matchOwners(nil, []string{"pg-orders-connector"}, nil); got != "" {
		t.Fatalf("nil owners matched %q", got)
	}
}
