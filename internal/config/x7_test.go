package config

import (
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"
)

// X7: the OpenLineage emitter settings reach their fields, empty is off, and
// a key is never sent in clear text to a remote host.
func TestX7ParseOpenLineage(t *testing.T) {
	t.Setenv("OPENLINEAGE_URL", "")
	t.Setenv("OPENLINEAGE_NAMESPACE", "")
	t.Setenv("OPENLINEAGE_API_KEY", "")
	o, err := ParseOpenLineage(Load())
	if err != nil || o.URL != "" || o.Namespace != DefaultOpenLineageNamespace || o.APIKey != "" {
		t.Fatalf("unset = %+v, %v; want off with the default namespace", o, err)
	}

	const key = "k3y-Sup3rS3cret"
	for _, tt := range []struct {
		url, ns, key string
		ok           bool
	}{
		{"https://datahub.example.com/openapi/openlineage/api/v1/lineage", "piperoom-prod", key, true},
		{"http://marquez:5000/api/v1/lineage", "", "", true},
		{"http://localhost:5000/api/v1/lineage", "", key, true},
		{"http://127.0.0.1:5000/api/v1/lineage", "", key, true},
		{"http://[::1]:5000/api/v1/lineage", "", key, true},
		{"http://marquez:5000/api/v1/lineage", "", key, false}, // key in clear text to a remote host
		{"ftp://marquez/api", "", "", false},
		{"marquez:5000", "", "", false},
		{"https://user:pw@marquez/api/v1/lineage", "", "", false},
		{"https://marquez/api/v1/lineage", "team/data:cdc.v1", "", true},
		{"https://marquez/api/v1/lineage", "-lead", "", false},
		{"https://marquez/api/v1/lineage", "has space", "", false},
		{"https://marquez/api/v1/lineage", strings.Repeat("n", 129), "", false},
		{"", "bad ns", "", false}, // the namespace is checked even when off
	} {
		o, err := ParseOpenLineage(Config{OpenLineageURL: tt.url, OpenLineageNamespace: tt.ns, OpenLineageAPIKey: tt.key})
		if (err == nil) != tt.ok {
			t.Errorf("url %q ns %q key %v: err %v, want ok=%v", tt.url, tt.ns, tt.key != "", err, tt.ok)
		}
		if err != nil && strings.Contains(err.Error(), key) {
			t.Errorf("error quotes the API key: %v", err)
		}
		if err == nil && tt.ns == "" && o.Namespace != DefaultOpenLineageNamespace {
			t.Errorf("namespace = %q, want the default", o.Namespace)
		}
	}

	// LogValue never prints the key.
	o, err = ParseOpenLineage(Config{OpenLineageURL: "https://m/api/v1/lineage", OpenLineageAPIKey: key})
	if err != nil {
		t.Fatal(err)
	}
	var sb strings.Builder
	slog.New(slog.NewTextHandler(&sb, nil)).Info("emitter", "openlineage", o)
	if strings.Contains(sb.String(), key) || !strings.Contains(sb.String(), "api_key_set=true") {
		t.Fatalf("log line = %s", sb.String())
	}
	if strings.Contains(fmt.Sprint(Load().LogValue()), key) {
		t.Fatal("Config.LogValue leaks")
	}

	if OpenLineageEmitInterval != 15*time.Minute || OpenLineageTimeout != 10*time.Second || OpenLineageMaxRetries != 3 ||
		OpenLineageMaxEventBytes != 1<<20 || OpenLineageMaxResponseBytes != 4<<20 {
		t.Fatal("X7 emitter limits changed; docs/design/lineage.md states them")
	}
}

func TestX7OpenLineageEnv(t *testing.T) {
	t.Setenv("OPENLINEAGE_URL", "https://marquez.internal/api/v1/lineage")
	t.Setenv("OPENLINEAGE_NAMESPACE", "acme")
	t.Setenv("OPENLINEAGE_API_KEY", "abc")
	c := Load()
	if c.OpenLineageURL != "https://marquez.internal/api/v1/lineage" || c.OpenLineageNamespace != "acme" || c.OpenLineageAPIKey != "abc" {
		t.Fatalf("Load = %q %q %q", c.OpenLineageURL, c.OpenLineageNamespace, c.OpenLineageAPIKey)
	}
}
