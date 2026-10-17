package owners

import (
	"errors"
	"testing"
)

func TestConnectorSlot(t *testing.T) {
	const pg = "io.debezium.connector.postgresql.PostgresConnector"
	cases := []struct {
		name    string
		cfg     map[string]string
		want    string
		wantErr bool
	}{
		{"postgres default", map[string]string{"connector.class": pg}, DefaultSlot, false},
		{"explicit", map[string]string{"connector.class": pg, "slot.name": "custom_slot"}, "custom_slot", false},
		{"blank is invalid", map[string]string{"connector.class": pg, "slot.name": ""}, "", true},
		{"config provider is invalid", map[string]string{"connector.class": pg, "slot.name": "${file:/s:n}"}, "", true},
		{"upper case is invalid", map[string]string{"slot.name": "Orders"}, "", true},
		{"jdbc source has none", map[string]string{"connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector"}, "", false},
		{"nil config has none", nil, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ConnectorSlot(tc.cfg)
			if (err != nil) != tc.wantErr || (err != nil && !errors.Is(err, ErrInvalidSlotName)) {
				t.Fatalf("err = %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Fatalf("slot = %q, want %q", got, tc.want)
			}
		})
	}
}
