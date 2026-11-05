package actions

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/events"
)

// The config_drift digest (X3b) and the config version (X5c) share one id:
// the hash of the redacted config. No raw value and no fingerprint can leave
// a digest through JSON, a format verb or a log line.
func TestDigestConfig(t *testing.T) {
	base := map[string]string{"connector.class": "io.debezium.connector.postgresql.PostgresConnector",
		"decimal.handling.mode": "precise", "database.password": "hunter2-secret",
		"connection.url": "postgres://app:Sup3rS3cretPw@db:5432/orders"}
	d := DigestConfig(base)
	if d.Hash != ConfigHash(RedactConfig(base)) {
		t.Fatalf("Hash %q is not ConfigHash(RedactConfig(cfg))", d.Hash)
	}
	if !maps.Equal(d.Redacted, RedactConfig(base)) {
		t.Fatalf("Redacted = %v; want RedactConfig(cfg)", d.Redacted)
	}

	// Same keys and values built in another order: same hash (X3b TC1).
	same := map[string]string{}
	for _, k := range []string{"database.password", "connection.url", "decimal.handling.mode", "connector.class"} {
		same[k] = base[k]
	}
	if DigestConfig(same).Hash != d.Hash || len(ChangedConfigKeys(d, DigestConfig(same))) != 0 {
		t.Error("key insertion order changed the hash or the changed keys")
	}
	if DigestConfig(nil).Hash != DigestConfig(map[string]string{}).Hash {
		t.Error("nil and empty configs hash differently")
	}
	if DigestConfig(map[string]string{"k": ""}).Hash == DigestConfig(nil).Hash {
		t.Error("an empty value is not a missing key")
	}
	// Length prefixes keep a split between key and value from colliding in
	// the fingerprints.
	ab, abc := DigestConfig(map[string]string{"a": "bc"}), DigestConfig(map[string]string{"ab": "c"})
	if ab.keys["a"] == abc.keys["ab"] {
		t.Error(`fingerprints of {"a":"bc"} and {"ab":"c"} collide`)
	}

	// Nothing a digest can be marshalled, printed or logged into carries a
	// raw value or a fingerprint.
	raw, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	var logged bytes.Buffer
	slog.New(slog.NewJSONHandler(&logged, nil)).Info("drift", "digest", d)
	outs := []string{string(raw), fmt.Sprintf("%v", d), fmt.Sprintf("%+v", d), fmt.Sprintf("%#v", d), fmt.Sprint(d), logged.String()}
	for _, out := range outs {
		for _, v := range []string{"hunter2", "Sup3rS3cretPw", "precise", "connection.url"} {
			if strings.Contains(out, v) {
				t.Errorf("digest output %q contains %q", out, v)
			}
		}
		for _, fp := range d.keys {
			if strings.Contains(out, fmt.Sprintf("%x", fp)) {
				t.Errorf("digest output %q contains a fingerprint", out)
			}
		}
		if !strings.Contains(out, d.Hash) {
			t.Errorf("digest output %q does not show the hash", out)
		}
	}
}

func TestChangedConfigKeys(t *testing.T) {
	base := map[string]string{"decimal.handling.mode": "precise", "database.password": "a", "tasks.max": "1",
		"database.user": "pr_reader", "connection.url": "postgres://app:Sup3rS3cretPw@db:5432/orders"}
	with := func(edit func(m map[string]string)) ConfigDigest {
		m := maps.Clone(base)
		edit(m)
		return DigestConfig(m)
	}
	old := DigestConfig(base)
	tests := []struct {
		name     string
		cur      ConfigDigest
		want     []string
		sameHash bool
	}{
		{"unchanged", with(func(map[string]string) {}), []string{}, true},
		{"one key (X3b TC2)", with(func(m map[string]string) { m["decimal.handling.mode"] = "string" }), []string{"decimal.handling.mode"}, false},
		// A secret-only change keeps the public hash (no oracle) but is still
		// reported, by placeholder (X3b TC3).
		{"secret key (X3b TC3)", with(func(m map[string]string) { m["database.password"] = "b" }), []string{events.SecretKeyChanged}, true},
		// redact.Text masks the password in the URL, so the redacted form and
		// the hash stay; the key name is still reported, never the value.
		{"password inside a URL", with(func(m map[string]string) {
			m["connection.url"] = "postgres://app:An0therS3cret@db:5432/orders"
		}), []string{"connection.url"}, true},
		{"added and removed", with(func(m map[string]string) { delete(m, "tasks.max"); m["heartbeat.interval.ms"] = "10000" }),
			[]string{"heartbeat.interval.ms", "tasks.max"}, false},
		{"secret removed", with(func(m map[string]string) { delete(m, "database.password") }), []string{events.SecretKeyChanged}, false},
		{"two secrets report once", with(func(m map[string]string) {
			m["database.password"] = "b"
			m["sasl.jaas.config"] = "x"
			m["ssl.key.password"] = "y"
			m["tasks.max"] = "2"
		}), []string{events.SecretKeyChanged, "tasks.max"}, false},
	}
	for _, tt := range tests {
		got := ChangedConfigKeys(old, tt.cur)
		if got == nil || !slices.Equal(got, tt.want) {
			t.Errorf("%s: ChangedConfigKeys = %#v; want %#v", tt.name, got, tt.want)
		}
		if (old.Hash == tt.cur.Hash) != tt.sameHash {
			t.Errorf("%s: hash kept = %v; want %v", tt.name, old.Hash == tt.cur.Hash, tt.sameHash)
		}
	}
	if got := ChangedConfigKeys(ConfigDigest{}, ConfigDigest{}); got == nil || len(got) != 0 {
		t.Errorf("zero digests: %#v; want []", got)
	}
}

// E4: ChangedConfigKeys drops nothing; the caps belong to its two consumers
// (events.MaxChangedKeys for the signal, clients.MaxConfigKeys for a version).
func TestChangedConfigKeysKeepsEveryName(t *testing.T) {
	for _, n := range []int{events.MaxChangedKeys - 1, events.MaxChangedKeys, events.MaxChangedKeys + 1, 3 * events.MaxChangedKeys} {
		cur := map[string]string{}
		for i := range n {
			cur[fmt.Sprintf("k%04d", i)] = "v"
		}
		got := ChangedConfigKeys(DigestConfig(nil), DigestConfig(cur))
		if len(got) != n || !slices.IsSorted(got) || got[0] != "k0000" {
			t.Errorf("n=%d: %d keys, sorted %v, first %q", n, len(got), slices.IsSorted(got), got[0])
		}
	}
}
