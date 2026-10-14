package redact

import (
	"strings"
	"testing"
)

// Test values only: 4111 1111 1111 1111 and 5555 5555 5555 4444 are the
// well-known test card numbers, 555-01xx phone numbers are reserved for
// fiction, and 123-45-6789 is the textbook SSN.

func TestPIIMasked(t *testing.T) {
	tests := []struct {
		name, in string
		leaks    []string
		kind     Kind
	}{
		{"pg unique ssn", `ERROR: duplicate key value violates unique constraint "customers_ssn_key"` + "\n" + `DETAIL: Key (ssn)=(123-45-6789) already exists.`, []string{"123-45-6789"}, KindPIIValue},
		{"pg unformatted ssn", `DETAIL: Key (customer_id, ssn)=(42, 123456789) already exists.`, []string{"123456789"}, KindPIIValue},
		{"pg fk phone", `DETAIL: Key (phone)=(+1 415 555 0100) is not present in table "contacts".`, []string{"415 555 0100"}, KindPIIValue},
		{"failing row", `DETAIL: Failing row contains (17, Alice, 123-45-6789, 4111 1111 1111 1111, (415) 555-0123, 2026-10-14 09:41:07).`, []string{"123-45-6789", "4111 1111 1111 1111", "555-0123"}, KindSSN},
		{"debezium struct", `org.apache.kafka.connect.errors.DataException: Struct{id=17,ssn=123456789,phone=4155550100,card_number=5555555555554444}`, []string{"123456789", "4155550100", "5555555555554444"}, KindPIIValue},
		{"jdbc sink batch", `java.sql.BatchUpdateException: Batch entry 0 INSERT INTO "customers" ("id","phone","card") VALUES (17,'+14155550100','4111-1111-1111-1111') was aborted`, []string{"14155550100", "4111-1111-1111-1111"}, KindCard},
		{"json after image", `{"before":null,"after":{"id":17,"ssn":"123-45-6789","phone_number":"+44 20 7946 0958"},"source":{"lsn":24023409728}}`, []string{"123-45-6789", "7946 0958"}, KindPIIValue},
		{"amex grouped", `card 3782 822463 10005 declined`, []string{"822463"}, KindCard},
		{"bare visa", `payment for 4111111111111111 failed`, []string{"4111111111111111"}, KindCard},
		{"nanp dashes", `customer callback 415-555-0199`, []string{"415-555-0199"}, KindPhone},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			out, st := Text(tc.in)
			for _, l := range tc.leaks {
				if strings.Contains(out, l) {
					t.Errorf("leaked %q:\n in:  %s\n out: %s", l, tc.in, out)
				}
			}
			if st.ByKind[tc.kind] == 0 {
				t.Errorf("stats %+v lack %s; out: %s", st, tc.kind, out)
			}
			if again, _ := Text(out); again != out {
				t.Errorf("not idempotent:\n 1: %s\n 2: %s", out, again)
			}
		})
	}
}

// TestPIIKeepsEvidence: positions, times, ids and sizes that look like card,
// SSN or phone numbers stay byte for byte.
func TestPIIKeepsEvidence(t *testing.T) {
	lines := []string{
		// Debezium source block: LSNs as decimals, ts in ms/us/ns, txId.
		`"source":{"version":"2.7.0.Final","connector":"postgresql","ts_ms":1760435267118,"ts_us":1760435267118204,"ts_ns":1760435267118204000,"txId":4111111,"lsn":4111111111111111,"sequence":"[\"24023409728\",\"24023409784\"]"}`,
		`lsn=5555555555554444 offset=4111111111111111`,
		`confirmed_flush_lsn=1A/3F0021C8 restart_lsn=16/B374D848`,
		`pg.public.orders/1@4111111111111111`,
		`record id 4111111111111111 not found`,
		`ratio=0.4111111111111111 lag 13908`,
		`snowflake query id 01b7c3a2-0000-4111-1111-111111111111`,
		`uuid 123e4567-e89b-12d3-a456-426614174000`,
		`at 2026-10-14T09:41:07.118204+00:00 (2026-10-14 09:41:07)`,
		`kafka-consumer-groups --reset-offsets --to-offset 25377155 --group order-sync`,
		`order ORD-415-555-0100 shipped`,
		`version 1.2.3-415-555-0100`,
		`partition key: 4111111111111111`,
		`committed 25377154, end 25391062, lag 13908`,
		`+25377154 records since 09:00`,
		`FATAL: password authentication failed for user "debezium" (SQLSTATE 28P01)`,
		`phone_type: mobile`,
		`DETAIL: Key (order_id)=(4111111111111111) already exists.`,
		`build 20261014-0941 sha 9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08`,
		`timeout after 10000 ms, 3 retries, 4111 bytes`,
	}
	for _, l := range lines {
		out, st := Text(l)
		if out != l || st.Total != 0 {
			t.Errorf("evidence changed (%+v):\n in:  %s\n out: %s", st, l, out)
		}
	}
}

func TestLuhnAndIssuer(t *testing.T) {
	for d, want := range map[string]bool{
		"4111111111111111": true, "5555555555554444": true, "378282246310005": true, "6011111111111117": true,
		"4111111111111112": false, // Luhn fails
		"1111111111111111": false, // no issuer
		"4222222222222":    false, // 13-digit Visa not accepted
		"9111111111111111": false,
	} {
		if got := isCard(d); got != want {
			t.Errorf("isCard(%s) = %v, want %v", d, got, want)
		}
	}
}

func TestPIIOptionOff(t *testing.T) {
	in := "Key (ssn)=(123-45-6789) card 4111111111111111"
	if out, st := New(Options{}).Text(in); out != in || st.Total != 0 {
		t.Errorf("PII masked with the option off: %s", out)
	}
}

func TestPIIJSON(t *testing.T) {
	in := map[string]any{"after": map[string]any{"id": 17, "ssn": "123456789", "phone": 4155550100, "phone_type": "mobile", "lsn": 4111111111111111}}
	out, st := JSONStats(in)
	m := out.(map[string]any)["after"].(map[string]any)
	if strings.Contains(m["ssn"].(string), "1234") || st.ByKind[KindPIIValue] != 2 {
		t.Errorf("got %v %+v", m, st)
	}
	if m["phone_type"] != "mobile" || m["lsn"] == nil {
		t.Errorf("non-PII changed: %v", m)
	}
}

// TestSecretGapsFromQA covers QA7's misses: a password after whitespace, a
// fullwidth key and pw-style keys.
func TestSecretGapsFromQA(t *testing.T) {
	tests := []struct{ in, leak string }{
		{"login with passwd Sup3rS3cret now", "Sup3rS3cret"},
		{"ALTER ROLE app PASSWORD 'hunter2pass';", "hunter2pass"},
		{"ｐａｓｓｗｏｒｄ＝Sup3rS3cret", "Sup3rS3cret"},
		{"ｐａｓｓｗｏｒｄ: hunter2", "hunter2"},
		{"db_pw=hunter2 user=app", "hunter2"},
		{"PGPW=hunter2", "hunter2"},
		{`{"pw":"hunter2"}`, "hunter2"},
	}
	for _, tc := range tests {
		out, st := Text(tc.in)
		if strings.Contains(out, tc.leak) || st.Total == 0 {
			t.Errorf("Text(%q) = %q, leaks %q", tc.in, out, tc.leak)
		}
		if again, _ := Text(out); again != out {
			t.Errorf("not idempotent: %q -> %q", out, again)
		}
	}
	for _, keep := range []string{
		"password authentication failed for user app",
		"pwd /opt/kafka/config",
		"reset pw link sent",
		"key ｏｒｄｅｒｓ differs from orders by a fullwidth form",
		"the password is wrong",
		"passwd file missing",
	} {
		if out, st := Text(keep); out != keep || st.Total != 0 {
			t.Errorf("Text(%q) = %q, want unchanged", keep, out)
		}
	}
}
