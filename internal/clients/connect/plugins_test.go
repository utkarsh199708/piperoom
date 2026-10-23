package connect

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
)

// modernPlugins is a Kafka 3.2+ answer: connectors, both converter kinds,
// transformations and predicates, each with a version.
const modernPlugins = `[
  {"class":"org.apache.kafka.connect.file.FileStreamSourceConnector","type":"source","version":"3.6.1"},
  {"class":"org.apache.kafka.connect.file.FileStreamSinkConnector","type":"sink","version":"3.6.1"},
  {"class":"io.confluent.connect.avro.AvroConverter","type":"converter","version":"7.6.1"},
  {"class":"org.apache.kafka.connect.storage.StringConverter","type":"header_converter","version":"3.6.1"},
  {"class":"io.debezium.transforms.ExtractNewRecordState","type":"transformation","version":"3.0.0.Final"},
  {"class":"io.debezium.transforms.Filter","type":"predicate","version":"3.0.0.Final"}
]`

func TestPlugins(t *testing.T) {
	var gotPath, gotQuery string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotQuery = r.URL.Path, r.URL.RawQuery
		w.Write([]byte(modernPlugins))
	})
	plugins, err := c.Plugins(context.Background())
	if err != nil {
		t.Fatalf("Plugins: %v", err)
	}
	if gotPath != "/connector-plugins" {
		t.Fatalf("path %q, want /connector-plugins", gotPath)
	}
	// Kafka 3.2+ lists converters, transforms and predicates only when asked.
	if gotQuery != "connectorsOnly=false" {
		t.Fatalf("query %q, want connectorsOnly=false", gotQuery)
	}
	if len(plugins) != 6 {
		t.Fatalf("got %d plugins, want 6: %+v", len(plugins), plugins)
	}
	types := map[string]string{}
	for _, p := range plugins {
		types[p.Type] = p.Version
	}
	for _, want := range []string{"source", "sink", "converter", "header_converter", "transformation", "predicate"} {
		if types[want] == "" {
			t.Fatalf("plugin type %q missing or has no version: %+v", want, plugins)
		}
	}
	if LegacyPlugins(plugins) {
		t.Fatal("a 3.2+ answer was marked legacy")
	}
}

// A pre-3.2 worker ignores connectorsOnly=false, lists connectors only and
// reports no versions. The entries still decode; LegacyPlugins marks them.
func TestPluginsLegacyResponse(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`[{"class":"io.debezium.connector.postgresql.PostgresConnector","type":"source"}]`))
	})
	plugins, err := c.Plugins(context.Background())
	if err != nil {
		t.Fatalf("Plugins: %v", err)
	}
	if len(plugins) != 1 || plugins[0].Class != "io.debezium.connector.postgresql.PostgresConnector" || plugins[0].Version != "" {
		t.Fatalf("plugins %+v", plugins)
	}
	if !LegacyPlugins(plugins) {
		t.Fatal("a versionless list was not marked legacy")
	}
	// An empty inventory says nothing about the worker's age.
	if LegacyPlugins(nil) {
		t.Fatal("empty list marked legacy")
	}
}

func TestPluginsNotFound(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	_, err := c.Plugins(context.Background())
	if !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("err %v, want ErrNotFound", err)
	}
}

func TestPluginsServerError(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"message":"boom"}`))
	})
	_, err := c.Plugins(context.Background())
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("err %v, want status 500", err)
	}
	if errors.Is(err, clients.ErrNotFound) || errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("err %v misclassified a server error", err)
	}
}

func TestPluginsTimeout(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.Write([]byte(`[]`))
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := c.Plugins(ctx); err == nil {
		t.Fatal("timed-out call returned no error")
	}
}

// The 4 MB response cap is the client's, not a per-endpoint one: an oversized
// plugin list is ErrResponseTooLarge, not a JSON syntax error from a cut body.
func TestPluginsOversized(t *testing.T) {
	big := `[{"class":"c","type":"source","version":"` + strings.Repeat("x", 5<<20) + `"}]`
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(big))
	})
	_, err := c.Plugins(context.Background())
	if !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("err %v, want ErrResponseTooLarge", err)
	}
}

func TestPluginsBadJSON(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`[{"class":`))
	})
	_, err := c.Plugins(context.Background())
	if err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("bad JSON err %v", err)
	}
}
