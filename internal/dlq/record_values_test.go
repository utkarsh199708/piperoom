package dlq

import (
	"strings"
	"testing"
)

// TestRedactTraceRecordValues: Connect prints a failing record with
// Struct.toString and Java Map.toString, unquoted. The values must go, the
// field names (evidence of the schema) must stay.
func TestRedactTraceRecordValues(t *testing.T) {
	tests := []struct {
		name   string
		in     string
		gone   []string
		intact []string
	}{
		{"struct", `DataException: bad value in Struct{id=7,name=Jane Doe,iban=DE89370400440532013000}`,
			[]string{"Jane", "Doe", "DE89", "=7"}, []string{"Struct{id=", ",name=", ",iban=", "DataException"}},
		{"nested struct", `value=Struct{id=7,addr=Struct{street=1 Main St, city=Springfield},email=a@b.co}`,
			[]string{"Main", "Springfield", "a@b"}, []string{"Struct{id=", "addr=", "email="}},
		{"java map", `failed record {id=7, name=Jane Doe}`,
			[]string{"Jane"}, []string{"id=", "name="}},
		{"ssn undashed", `invalid ssn 987654321 for row`, []string{"987654321"}, []string{"ssn", "for row"}},
		{"name kv", `customer_name=Jane Doe at offset 5`, []string{"Jane", "Doe"}, []string{"customer_name=", "at offset 5"}},
		{"iban colon", `iban: DE89370400440532013000; retrying`, []string{"DE89"}, []string{"iban:", "retrying"}},
		{"phone kv", `phone=5551234567 bad`, []string{"5551234567"}, []string{"phone=", "bad"}},
		{"dob kv", `dob=1990-01-02 bad`, []string{"1990-01-02"}, []string{"dob=", "bad"}},
		{"card kv", `card_number=4111111111111112 bad`, []string{"4111111111111112"}, []string{"card_number="}},
		{"address kv", `address=12 Main St, ok`, []string{"12 Main", "Main St"}, []string{"address=", "ok"}},
		{"evidence kept", `txId=987654321 offset=123456789 lsn=0/16B3748 ts=1697040000123 partition 3`, nil,
			[]string{"987654321", "123456789", "0/16B3748", "1697040000123", "partition 3"}},
		{"schema structure kept", `details: [{errorType:'MISSING_UNION_BRANCH'}, {oldSchemaVersion: 1}]`, nil,
			[]string{"MISSING_UNION_BRANCH", "oldSchemaVersion: 1"}},
		{"named non-record group kept", `SinkRecord{kafkaOffset=5, timestampType=CreateTime}`, nil,
			[]string{"kafkaOffset=5", "timestampType=CreateTime"}},
		{"identifier names kept", `topic_name=orders table_name=users`, nil, []string{"orders", "users"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := RedactTrace(tt.in)
			for _, g := range tt.gone {
				if strings.Contains(got, g) {
					t.Errorf("RedactTrace(%q) = %q, still has %q", tt.in, got, g)
				}
			}
			for _, k := range tt.intact {
				if !strings.Contains(got, k) {
					t.Errorf("RedactTrace(%q) = %q, lost %q", tt.in, got, k)
				}
			}
			if again := RedactTrace(got); again != got {
				t.Errorf("not idempotent: %q -> %q", got, again)
			}
		})
	}
}
