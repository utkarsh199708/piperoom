package kafka

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hamba/avro/v2"

	"piperoom/internal/clients/schemareg"
)

func testClientWithRegistry(t *testing.T, handler http.HandlerFunc) *Client {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	sr := schemareg.New(srv.URL, srv.Client())
	return &Client{sr: sr, srCache: map[int]avro.Schema{}}
}

// TestDecodeKeyConfluentSingleField decodes a Confluent-framed Avro key that is
// a single-field record, and checks the schema is fetched once (cached) per id.
func TestDecodeKeyConfluentSingleField(t *testing.T) {
	schemaStr := `{"type":"record","name":"Key","fields":[{"name":"id","type":"long"}]}`
	var fetches int
	c := testClientWithRegistry(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/schemas/ids/1" {
			http.NotFound(w, r)
			return
		}
		fetches++
		body, _ := json.Marshal(map[string]string{"schema": schemaStr})
		w.Write(body)
	})

	schema, err := avro.Parse(schemaStr)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := avro.Marshal(schema, map[string]any{"id": int64(42)})
	if err != nil {
		t.Fatal(err)
	}
	key := make([]byte, 5+len(payload))
	key[0] = 0x00
	binary.BigEndian.PutUint32(key[1:5], 1)
	copy(key[5:], payload)

	got, err := c.decodeKey(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	if got != "42" {
		t.Fatalf("decoded %q, want 42", got)
	}

	// Second decode with the same id must hit the cache, not the registry.
	if _, err := c.decodeKey(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if fetches != 1 {
		t.Fatalf("schema fetched %d times, want 1 (cached)", fetches)
	}
}

func TestDecodeKeyRaw(t *testing.T) {
	c := testClientWithRegistry(t, func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	got, err := c.decodeKey(context.Background(), []byte("plain-key"))
	if err != nil {
		t.Fatal(err)
	}
	if got != "plain-key" {
		t.Fatalf("decoded %q, want plain-key", got)
	}
}
