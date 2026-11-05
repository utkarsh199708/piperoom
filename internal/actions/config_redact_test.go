package actions

import (
	"maps"
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"
)

// RedactConfig and ConfigHash (X5-0) are the one rule the watcher (X3b) and
// the config-version store (X5c) share.
func TestRedactConfig(t *testing.T) {
	long := strings.Repeat("a", MaxConfigValueBytes-2) + "éé" // a 2-byte rune straddles the cut
	for _, tc := range []struct {
		name, key, value string
		want             func(string) bool
		absent           string
	}{
		{"secret key", "database.password", "hunter2-secret", func(v string) bool { return v == RedactedValue }, "hunter2"},
		{"secret jaas", "sasl.jaas.config", `org.apache.kafka.common.security.plain.PlainLoginModule required username="u" password="p4ss";`,
			func(v string) bool { return v == RedactedValue }, "p4ss"},
		{"password in a JDBC URL", "connection.url", "postgres://app:Sup3rS3cretPw@db:5432/orders",
			func(v string) bool {
				return strings.HasPrefix(v, "postgres://app:") && strings.HasSuffix(v, "@db:5432/orders")
			}, "Sup3rS3cretPw"},
		{"password in a JDBC query", "connection.url", "jdbc:postgresql://db:5432/orders?user=app&password=Jd8cS3cretPw",
			func(v string) bool {
				return strings.HasPrefix(v, "jdbc:postgresql://db:5432/orders?user=app&password=")
			}, "Jd8cS3cretPw"},
		{"plain value", "decimal.handling.mode", "precise", func(v string) bool { return v == "precise" }, ""},
		{"empty value", "topic.prefix", "", func(v string) bool { return v == "" }, ""},
		{"value at the cap", "transforms", strings.Repeat("x", MaxConfigValueBytes), func(v string) bool { return len(v) == MaxConfigValueBytes }, ""},
		{"value over the cap", "transforms", long, func(v string) bool {
			return len(v) <= MaxConfigValueBytes && utf8.ValidString(v) && strings.HasSuffix(v, "…")
		}, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := map[string]string{tc.key: tc.value}
			orig := maps.Clone(in)
			got := RedactConfig(in)[tc.key]
			if !tc.want(got) {
				t.Fatalf("RedactConfig(%q=%q) = %q", tc.key, tc.value, got)
			}
			if tc.absent != "" && strings.Contains(got, tc.absent) {
				t.Fatalf("secret %q survived: %q", tc.absent, got)
			}
			if !maps.Equal(in, orig) {
				t.Fatalf("input modified: %v", in)
			}
		})
	}
	if got := RedactConfig(nil); got == nil || len(got) != 0 {
		t.Fatalf("RedactConfig(nil) = %#v, want an empty map", got)
	}
}

func TestConfigHash(t *testing.T) {
	hex64 := regexp.MustCompile(`^[0-9a-f]{64}$`)
	base := map[string]string{"connector.class": "io.debezium.connector.postgresql.PostgresConnector",
		"decimal.handling.mode": "precise", "database.password": "one"}
	h := ConfigHash(RedactConfig(base))
	if !hex64.MatchString(h) {
		t.Fatalf("hash %q is not 64 lowercase hex", h)
	}
	if ConfigHash(nil) != ConfigHash(map[string]string{}) {
		t.Fatal("nil and empty configs hash differently")
	}
	// Key order cannot matter for a map, but the encoding must not depend on
	// insertion order either: build the same content in reverse.
	rev := map[string]string{}
	for _, k := range []string{"database.password", "decimal.handling.mode", "connector.class"} {
		rev[k] = base[k]
	}
	if ConfigHash(RedactConfig(rev)) != h {
		t.Fatal("same content, different hash")
	}
	secretOnly := maps.Clone(base)
	secretOnly["database.password"] = "two"
	if ConfigHash(RedactConfig(secretOnly)) != h {
		t.Fatal("a secret-only change moved the hash: the hash would be a guessing oracle")
	}
	changed := maps.Clone(base)
	changed["decimal.handling.mode"] = "double"
	if ConfigHash(RedactConfig(changed)) == h {
		t.Fatal("a plain value change kept the hash")
	}
	added := maps.Clone(base)
	added["tasks.max"] = "1"
	if ConfigHash(RedactConfig(added)) == h {
		t.Fatal("an added key kept the hash")
	}
}
