package kafkatools

import (
	"context"
	"strings"
	"testing"

	"piperoom/internal/tools"
)

// The signal.* and publication.* keys the agent needs for pg_snapshot_signal
// pass through; every other rule (credential-looking keys, unlisted keys)
// still holds, also inside those prefixes.
func TestConnectConfigSignalAndPublicationKeys(t *testing.T) {
	cases := []struct {
		key, value string
		want       string // line expected in the summary; "" means the key is absent
	}{
		{"signal.data.collection", "public.debezium_signal", "signal.data.collection: public.debezium_signal"},
		{"signal.enabled.channels", "source,kafka", "signal.enabled.channels: source,kafka"},
		{"publication.name", "dbz_orders", "publication.name: dbz_orders"},
		{"publication.autocreate.mode", "filtered", "publication.autocreate.mode: filtered"},
		// Secret-looking keys under the same prefixes stay redacted.
		{"signal.kafka.consumer.sasl.password", "hunter2", "signal.kafka.consumer.sasl.password: ***"},
		{"signal.consumer.ssl.key.password", "kpw-value", "signal.consumer.ssl.key.password: ***"},
		{"publication.token", "tok-123", "publication.token: ***"},
		// Unlisted keys under the same prefixes stay hidden.
		{"signal.kafka.topic", "hidden-signal-topic", ""},
		{"publication.other", "hidden-pub", ""},
	}
	cfg := map[string]string{"connector.class": "io.debezium.connector.postgresql.PostgresConnector"}
	for _, c := range cases {
		cfg[c.key] = c.value
	}
	fc := &fakeConnect{
		configFn: func(ctx context.Context, n string) (map[string]string, error) { return cfg, nil },
	}
	guardWrites(t, fc)
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Connect: fc}); err != nil {
		t.Fatal(err)
	}
	tool, _ := reg.Get("connect_config")
	r := runTool(t, tool, `{"connector":"pg-orders-connector"}`)
	lines := map[string]bool{}
	for _, l := range strings.Split(r.Summary, "\n") {
		lines[l] = true
	}
	for _, c := range cases {
		t.Run(c.key, func(t *testing.T) {
			if c.want != "" && !lines[c.want] {
				t.Errorf("summary lacks %q:\n%s", c.want, r.Summary)
			}
			if c.want == "" && strings.Contains(r.Summary, c.key) {
				t.Errorf("unlisted key %q shown:\n%s", c.key, r.Summary)
			}
			if (c.want == "" || strings.HasSuffix(c.want, "***")) && strings.Contains(r.Summary, c.value) {
				t.Errorf("value of %q leaked:\n%s", c.key, r.Summary)
			}
		})
	}
}
