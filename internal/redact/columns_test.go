package redact

import (
	"slices"
	"strings"
	"testing"
	"unicode/utf8"
)

// X7c TC1: the classifier table, names and catalog types only.
func TestClassifyColumn(t *testing.T) {
	for _, tc := range []struct {
		name, typ, want string // want "" = not PII
	}{
		// TC1, as the ticket lists them.
		{"email", "text", ColumnEmail},
		{"customer_email", "character varying", ColumnEmail},
		{"e_mail", "text", ColumnEmail},
		{"phone_number", "text", ColumnPhone},
		{"msisdn", "bigint", ColumnPhone},
		{"ssn", "character(11)", ColumnNationalID},
		{"pan", "text", ColumnCard},
		{"card_last4", "smallint", ColumnCard},
		{"dob", "date", ColumnDOB},
		{"emailed_at", "timestamp with time zone", ""},
		{"phone_verified", "boolean", ""},
		{"ip_count", "integer", ""},
		// Neighbours of TC1: the same words with an unknown type and as
		// facts about the thing.
		{"emailed_at", "", ""},
		{"phone_verified", "", ""},
		{"ip_count", "", ""},
		{"email_verified_at", "timestamp with time zone", ""},
		{"email_verified_at", "", ""},
		{"card_id", "bigint", ""},
		{"is_email", "", ""},
		{"num_phones", "", ""},
		{"email_count", "integer", ""},
		{"email", "uuid", ""},
		{"email", "integer", ""},
		{"Email", "VARCHAR", ColumnEmail},
		{"customerEmail", "STRING", ColumnEmail},
		{"EMAIL_ADDRESS", "VARCHAR(16777216)", ColumnEmail},
		{"billing_mail", "text", ColumnEmail},
		{"mail_server", "text", ""},
		// Other classes.
		{"first_name", "text", ColumnName},
		{"lastName", "text", ColumnName},
		{"surname", "text", ColumnName},
		{"name", "text", ColumnName},
		{"customer_name", "text", ColumnName},
		{"card_holder_name", "text", ColumnName},
		{"product_name", "text", ""},
		{"table_name", "text", ""},
		{"username", "citext", ColumnName},
		{"name", "integer", ""},
		{"address", "text", ColumnAddress},
		{"shipping_address_line1", "text", ColumnAddress},
		{"zip", "integer", ColumnAddress},
		{"postal_code", "text", ColumnAddress},
		{"zip_file", "bytea", ""},
		{"address_id", "bigint", ""},
		{"national_id", "text", ColumnNationalID},
		{"tax_id", "text", ColumnNationalID},
		{"passport_number", "text", ColumnNationalID},
		{"passport_issued_at", "date", ""},
		{"credit_card_number", "numeric", ColumnCard},
		{"card_type", "text", ""},
		{"cvv", "text", ColumnCard},
		{"ip", "inet", ColumnIP},
		{"last_login_ip", "inet", ColumnIP},
		{"ip_address", "text", ColumnIP},
		{"IPAddress", "String", ColumnIP},
		{"client_ipv4", "IPv4", ColumnIP},
		{"remote_addr", "text", ColumnIP},
		{"ip", "bigint", ""},
		{"birth_date", "date", ColumnDOB},
		{"dateOfBirth", "timestamp without time zone", ColumnDOB},
		{"birthday", "text", ColumnDOB},
		{"birthday", "integer", ""},
		{"mobile", "text", ColumnPhone},
		{"mobile_os", "text", ""},
		{"telephone", "Nullable(String)", ColumnPhone},
		{"fax", "text", ColumnPhone},
		{"phone", "inet", ""},
		// Plain columns of a CDC table.
		{"id", "bigint", ""},
		{"customer_id", "bigint", ""},
		{"amount", "numeric(12,2)", ""},
		{"updated_at", "timestamp with time zone", ""},
		{"status", "text", ""},
		{"notes", "text", ""},
		{"", "text", ""},
		{"___", "", ""},
		// Postgres format_type spellings (X7a's ColumnReader reads
		// format_type(atttypid, NULL), not information_schema): arrays as
		// "<type>[]", domains and user types by their (maybe qualified or
		// quoted) name.
		{"emails", "text[]", ColumnEmail},
		{"phone_numbers", "bigint[]", ColumnPhone},
		{"login_ips", "inet[]", ColumnIP},
		{"email", "timestamp with time zone[]", ""},
		{"customer_email", "public.email_address", ColumnEmail},
		{"contact_email", "citext", ColumnEmail},
		{"dob", "public.birth_date_t", ColumnDOB},
		{"first_name", "\"PersonName\"", ColumnName},
		{"status", "public.order_status", ""},
		{"email", "ARRAY", ColumnEmail},
		{"email", "USER-DEFINED", ColumnEmail},
	} {
		got, ok := ClassifyColumn(tc.name, tc.typ)
		if ok != (tc.want != "") || got != tc.want {
			t.Errorf("ClassifyColumn(%q, %q) = %q, %v; want %q", tc.name, tc.typ, got, ok, tc.want)
		}
	}
}

func TestColumnWords(t *testing.T) {
	for in, want := range map[string][]string{
		"customer_email":   {"customer", "email"},
		"customerEmail":    {"customer", "email"},
		"IPAddress":        {"ip", "address"},
		"card_last4":       {"card", "last", "4"},
		"Address-Line.2 x": {"address", "line", "2", "x"},
		"ÉmailÄddress":     {"émail", "äddress"},
		"":                 nil,
	} {
		if got := columnWords(in); !slices.Equal(got, want) {
			t.Errorf("columnWords(%q) = %q, want %q", in, got, want)
		}
	}
	if n := len(columnWords(strings.Repeat("a_", 1000))); n != maxColumnWords {
		t.Fatalf("%d words kept, want %d", n, maxColumnWords)
	}
}

func TestColumnTypeKind(t *testing.T) {
	for typ, want := range map[string]typeKind{
		"boolean": kindBool, "BOOL": kindBool, "uuid": kindUUID, "UUID": kindUUID,
		"timestamp with time zone": kindTime, "TIMESTAMP_NTZ": kindTime, "date": kindTime, "interval": kindTime,
		"Nullable(DateTime64(3))": kindTime, "time without time zone": kindTime,
		"integer": kindNumber, "bigint": kindNumber, "numeric(12,2)": kindNumber, "NUMBER(38,0)": kindNumber,
		"INT64": kindNumber, "FLOAT64": kindNumber, "double precision": kindNumber, "Decimal(10, 2)": kindNumber,
		"UInt32": kindNumber, "integer[]": kindNumber,
		// format_type spellings: arrays, a typmod before the brackets,
		// domains and user types by name (never ruled out).
		"timestamp with time zone[]": kindTime, "date[]": kindTime, "interval[]": kindTime, "inet[]": kindNet,
		"boolean[]": kindBool, "uuid[]": kindUUID, "numeric(12,2)[]": kindNumber, "character varying(64)[]": kindOther,
		"text[]": kindOther, "public.email_address": kindOther, `"OrderStatus"`: kindOther, "citext": kindOther,
		"inet": kindNet, "cidr": kindNet, "macaddr8": kindNet, "IPv6": kindNet,
		"text": kindOther, "character varying": kindOther, "VARCHAR(255)": kindOther, "String": kindOther,
		"LowCardinality(String)": kindOther, "jsonb": kindOther, "bytea": kindOther, "USER-DEFINED": kindOther,
		"": kindOther,
	} {
		if got := columnTypeKind(typ); got != want {
			t.Errorf("columnTypeKind(%q) = %d, want %d", typ, got, want)
		}
	}
}

// Every class the classifier answers is listed, once.
func TestColumnClassesComplete(t *testing.T) {
	seen := map[string]bool{}
	for _, r := range columnRules {
		if !slices.Contains(ColumnClasses(), r.class) {
			t.Errorf("rule class %q not in ColumnClasses", r.class)
		}
		seen[r.class] = true
	}
	if len(seen) != len(ColumnClasses()) {
		t.Fatalf("rules cover %d classes, ColumnClasses lists %d", len(seen), len(ColumnClasses()))
	}
}

// PersonalColumn is at least as strict as ClassifyColumn without a type,
// and keeps the watcher's former name list (X1b).
func TestPersonalColumn(t *testing.T) {
	for _, tc := range []struct {
		name string
		pii  bool
	}{
		{"updated_at", false}, {"created_at", false}, {"shipped_at", false}, {"ts", false}, {"op_ts", false},
		{"email_verified_at", true}, {"birth_date", true}, {"dateofbirth", true}, {"dob", true},
		{"phone_confirmed_at", true}, {"last_login_ip", true}, {"card_expires_at", true}, {"name", true},
		{"PassportIssuedAt", true}, {"tax_filed_at", true}, {"timestamp", false}, {"pipeline_at", false},
		{"msisdn", true}, {"CArd", true}, {"customerName", true}, {"emailed_at", true}, {"id", false},
	} {
		if got := PersonalColumn(tc.name); got != tc.pii {
			t.Errorf("PersonalColumn(%q) = %v, want %v", tc.name, got, tc.pii)
		}
	}
}

// TC4: the redact fuzz harness pattern on column names and types: never a
// panic, the same answer twice, only a known class, and PersonalColumn
// covers every ClassifyColumn hit.
func FuzzClassifyColumn(f *testing.F) {
	for _, s := range []string{"customer_email", "e_mail", "card_last4", "IPAddress", "dateOfBirth", "ÉmailÄddress",
		"a\x00b", "\xff\xfe", strings.Repeat("Ab1_", 300), "zip$", "mobile$"} {
		f.Add(s, "text")
		f.Add(s, "Nullable(DateTime64(3))")
	}
	f.Add("x", "((((")
	f.Fuzz(func(t *testing.T, name, typ string) {
		c1, ok1 := ClassifyColumn(name, typ)
		c2, ok2 := ClassifyColumn(name, typ)
		if c1 != c2 || ok1 != ok2 {
			t.Fatalf("not deterministic: %q/%v vs %q/%v", c1, ok1, c2, ok2)
		}
		if ok1 != (c1 != "") || ok1 && !slices.Contains(ColumnClasses(), c1) {
			t.Fatalf("ClassifyColumn(%q, %q) = %q, %v", name, typ, c1, ok1)
		}
		if _, ok := ClassifyColumn(name, ""); ok && !PersonalColumn(name) {
			t.Fatalf("PersonalColumn(%q) = false for a classified name", name)
		}
		for _, w := range columnWords(name) {
			if w == "" || utf8.ValidString(name) && !utf8.ValidString(w) {
				t.Fatalf("columnWords(%q) gave %q", name, w)
			}
		}
	})
}
