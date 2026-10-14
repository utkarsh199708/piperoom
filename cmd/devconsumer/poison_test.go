package main

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/hamba/avro/v2"
)

// stringRegistry serves schema id 7 as a plain Avro string.
func stringRegistry(t *testing.T) (*httptest.Server, avro.Schema) {
	t.Helper()
	const schema = `"string"`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/schemas/ids/7" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]string{"schema": schema})
	}))
	t.Cleanup(srv.Close)
	return srv, avro.MustParse(schema)
}

func wire(t *testing.T, s avro.Schema, v any) []byte {
	t.Helper()
	body, err := avro.Marshal(s, v)
	if err != nil {
		t.Fatal(err)
	}
	return append([]byte{0, 0, 0, 0, 7}, body...)
}

// TestDecodeLargeValue: a 2 MiB string is a legal record; hamba/avro's 1 MiB
// default made devconsumer crash-loop on it.
func TestDecodeLargeValue(t *testing.T) {
	srv, s := stringRegistry(t)
	big := strings.Repeat("x", 2<<20)
	v, err := newSchemaCache(srv.URL).decode(context.Background(), wire(t, s, big))
	if err != nil {
		t.Fatalf("decode 2 MiB string: %v", err)
	}
	if got, _ := v.(string); len(got) != len(big) {
		t.Fatalf("decoded %d bytes, want %d", len(got), len(big))
	}
}

func TestDecodeRejectsBadPayloads(t *testing.T) {
	srv, s := stringRegistry(t)
	good := wire(t, s, "hello")
	tests := []struct {
		name string
		b    []byte
	}{
		{"trailing bytes", append(append([]byte{}, good...), 0x01, 0x02)},
		{"truncated", good[:len(good)-2]},
		{"no magic byte", []byte{1, 0, 0, 0, 7, 2, 'x'}},
		{"too short", []byte{0, 0}},
		{"negative length", []byte{0, 0, 0, 0, 7, 0x01}},
	}
	cache := newSchemaCache(srv.URL)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := cache.decode(context.Background(), tc.b)
			if !errors.Is(err, errUndecodable) {
				t.Fatalf("decode = %v, want errUndecodable", err)
			}
		})
	}
	if _, err := cache.decode(context.Background(), good); err != nil {
		t.Fatalf("good payload: %v", err)
	}
}

// TestRegistryErrorIsNotUndecodable: a Registry outage must not make the
// consumer skip (and so lose) records.
func TestRegistryErrorIsNotUndecodable(t *testing.T) {
	srv, s := stringRegistry(t)
	b := wire(t, s, "x")
	srv.Close()
	_, err := newSchemaCache(srv.URL).decode(context.Background(), b)
	if err == nil || errors.Is(err, errUndecodable) {
		t.Fatalf("decode with registry down = %v, want a transient error", err)
	}
}
