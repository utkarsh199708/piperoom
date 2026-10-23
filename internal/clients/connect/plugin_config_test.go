package connect

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"piperoom/internal/clients"
)

func defsClient(t *testing.T, status int, body string, gotPath *string) *Client {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if gotPath != nil {
			*gotPath = r.Method + " " + r.URL.Path
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return New(srv.URL, srv.Client())
}

func TestPluginConfigDef(t *testing.T) {
	var path string
	// KIP-769: the endpoint answers a top-level JSON array of ConfigKeyInfo
	// (apache/kafka ConnectorPluginsResource.getConnectorConfigDef returns
	// List<ConfigKeyInfo>). default_value is a Java String on the wire, but a
	// scalar is tolerated.
	body := `[
		{"name":"database.hostname","type":"STRING","required":true,"default_value":null},
		{"name":"database.port","type":"INT","required":false,"default_value":5432},
		{"name":"topic.prefix","type":"STRING","required":true,"default_value":"pg"},
		{"name":"decimal.handling.mode","type":"STRING","required":false,"default_value":"precise"},
		{"name":"tombstones.on.delete","type":"BOOLEAN","required":false,"default_value":true},
		{"name":"","type":"STRING"}
	]`
	c := defsClient(t, http.StatusOK, body, &path)
	defs, err := c.PluginConfigDef(context.Background(), "io.debezium.connector.postgresql.PostgresConnector")
	if err != nil {
		t.Fatal(err)
	}
	if path != "GET /connector-plugins/io.debezium.connector.postgresql.PostgresConnector/config" {
		t.Fatalf("path = %q", path)
	}
	want := []ConfigKeyDef{
		{Name: "database.hostname", Type: "STRING", Required: true},
		{Name: "database.port", Type: "INT", Default: "5432"},
		{Name: "topic.prefix", Type: "STRING", Required: true, Default: "pg"},
		{Name: "decimal.handling.mode", Type: "STRING", Default: "precise"},
		{Name: "tombstones.on.delete", Type: "BOOLEAN", Default: "true"},
	}
	if len(defs) != len(want) {
		t.Fatalf("got %d defs, want %d: %+v", len(defs), len(want), defs)
	}
	for i := range want {
		if defs[i] != want[i] {
			t.Errorf("defs[%d] = %+v, want %+v", i, defs[i], want[i])
		}
	}
}

func TestPluginConfigDefErrors(t *testing.T) {
	tests := []struct {
		name     string
		status   int
		body     string
		wantIs   error
		wantText string
	}{
		{"not found on an older Connect", http.StatusNotFound, "", clients.ErrNotFound, ""},
		{"method not allowed is ErrNotSupported", http.StatusMethodNotAllowed, "nope", clients.ErrNotSupported, ""},
		{"bad JSON", http.StatusOK, `[`, nil, "decode"},
		{"envelope object instead of the KIP-769 array", http.StatusOK, `{"configs":[]}`, nil, "decode"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := defsClient(t, tc.status, tc.body, nil).PluginConfigDef(context.Background(), "c")
			if err == nil {
				t.Fatal("want an error")
			}
			if tc.wantIs != nil && !errors.Is(err, tc.wantIs) {
				t.Fatalf("err = %v, want %v", err, tc.wantIs)
			}
			if tc.wantText != "" && !strings.Contains(err.Error(), tc.wantText) {
				t.Fatalf("err = %v, want %q", err, tc.wantText)
			}
		})
	}
}

func TestPluginConfigDefEmptyClass(t *testing.T) {
	if _, err := New("http://127.0.0.1:1", nil).PluginConfigDef(context.Background(), ""); err == nil {
		t.Fatal("want an error for an empty class")
	}
}
