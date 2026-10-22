package actions

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/events"
)

func TestDigestConfig(t *testing.T) {
	base := map[string]string{"connector.class": "io.debezium.connector.postgresql.PostgresConnector",
		"decimal.handling.mode": "precise", "database.password": "hunter2-secret"}
	d := DigestConfig(base)
	if len(d.Hash) != 64 || strings.ToLower(d.Hash) != d.Hash {
		t.Fatalf("Hash = %q; want 64 lowercase hex characters", d.Hash)
	}

	// Same keys and values built in another order: same hash (X3b TC1).
	same := map[string]string{}
	for _, k := range []string{"database.password", "decimal.handling.mode", "connector.class"} {
		same[k] = base[k]
	}
	if DigestConfig(same).Hash != d.Hash {
		t.Error("key insertion order changed the hash")
	}
	if DigestConfig(nil).Hash != DigestConfig(map[string]string{}).Hash {
		t.Error("nil and empty configs hash differently")
	}
	// Length prefixes keep a split between key and value from colliding.
	if DigestConfig(map[string]string{"a": "bc"}).Hash == DigestConfig(map[string]string{"ab": "c"}).Hash {
		t.Error(`{"a":"bc"} and {"ab":"c"} collide`)
	}
	if DigestConfig(map[string]string{"k": ""}).Hash == DigestConfig(nil).Hash {
		t.Error("an empty value is not a missing key")
	}

	// Nothing a digest can be marshalled or printed into carries a value.
	raw, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	for _, out := range []string{string(raw), fmt.Sprintf("%+v", d)} {
		for _, v := range base {
			if strings.Contains(out, v) {
				t.Errorf("digest output %q contains the value %q", out, v)
			}
		}
	}
	if strings.Contains(string(raw), "database.password") {
		t.Errorf("digest JSON %s carries key hashes; want the hash only", raw)
	}
}

func TestChangedConfigKeys(t *testing.T) {
	base := map[string]string{"decimal.handling.mode": "precise", "database.password": "a", "tasks.max": "1",
		"database.user": "pr_reader"}
	with := func(edit func(m map[string]string)) ConfigDigest {
		m := map[string]string{}
		for k, v := range base {
			m[k] = v
		}
		edit(m)
		return DigestConfig(m)
	}
	old := DigestConfig(base)
	tests := []struct {
		name string
		cur  ConfigDigest
		want []string
	}{
		{"unchanged", with(func(map[string]string) {}), []string{}},
		{"one key (X3b TC2)", with(func(m map[string]string) { m["decimal.handling.mode"] = "string" }), []string{"decimal.handling.mode"}},
		{"secret key (X3b TC3)", with(func(m map[string]string) { m["database.password"] = "b" }), []string{events.SecretKeyChanged}},
		{"added and removed", with(func(m map[string]string) { delete(m, "tasks.max"); m["heartbeat.interval.ms"] = "10000" }),
			[]string{"heartbeat.interval.ms", "tasks.max"}},
		{"two secrets report once", with(func(m map[string]string) {
			m["database.password"] = "b"
			m["sasl.jaas.config"] = "x"
			m["tasks.max"] = "2"
		}), []string{events.SecretKeyChanged, "tasks.max"}},
	}
	for _, tt := range tests {
		got, omitted := ChangedConfigKeys(old, tt.cur)
		if got == nil || !slices.Equal(got, tt.want) || omitted != 0 {
			t.Errorf("%s: ChangedConfigKeys = %#v, %d; want %#v, 0", tt.name, got, omitted, tt.want)
		}
	}
}

// E4: the cap holds at the boundary and the rest is counted, not dropped silently.
func TestChangedConfigKeysCap(t *testing.T) {
	for _, n := range []int{events.MaxChangedKeys - 1, events.MaxChangedKeys, events.MaxChangedKeys + 1, 3 * events.MaxChangedKeys} {
		cur := map[string]string{}
		for i := range n {
			cur[fmt.Sprintf("k%04d", i)] = "v"
		}
		got, omitted := ChangedConfigKeys(DigestConfig(nil), DigestConfig(cur))
		wantLen := min(n, events.MaxChangedKeys)
		if len(got) != wantLen || omitted != n-wantLen {
			t.Errorf("n=%d: %d keys, %d omitted; want %d, %d", n, len(got), omitted, wantLen, n-wantLen)
		}
		if !slices.IsSorted(got) || (len(got) > 0 && got[0] != "k0000") {
			t.Errorf("n=%d: keys not the sorted first ones: %v", n, got[:min(3, len(got))])
		}
	}
}

func TestMaskChangedKeys(t *testing.T) {
	tests := []struct {
		in, want []string
	}{
		{nil, []string{}},
		{[]string{"b", "a", "b"}, []string{"a", "b"}},
		{[]string{"database.password"}, []string{events.SecretKeyChanged}},
		{[]string{"topic.prefix", "ssl.key.password", "api.key", "slot.name"},
			[]string{events.SecretKeyChanged, "slot.name", "topic.prefix"}},
	}
	for _, tt := range tests {
		in := slices.Clone(tt.in)
		got := MaskChangedKeys(tt.in)
		if got == nil || !slices.Equal(got, tt.want) {
			t.Errorf("MaskChangedKeys(%v) = %#v; want %#v", tt.in, got, tt.want)
		}
		if !slices.Equal(in, tt.in) {
			t.Errorf("MaskChangedKeys changed its input: %v -> %v", in, tt.in)
		}
	}
}
