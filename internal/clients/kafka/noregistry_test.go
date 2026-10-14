package kafka

import (
	"context"
	"testing"

	"github.com/hamba/avro/v2"
)

// With no Schema Registry configured (SCHEMA_REGISTRY_URL empty or off) the
// wiring passes a nil registry: a key that looks Confluent-framed is compared
// raw, never dereferencing nil.
func TestDecodeKeyWithoutRegistry(t *testing.T) {
	c := &Client{srCache: map[int]avro.Schema{}}
	key := []byte{0x00, 0, 0, 0, 1, 'x'}
	got, err := c.decodeKey(context.Background(), key)
	if err != nil || got != string(key) {
		t.Fatalf("decodeKey = %q, %v; want the raw key", got, err)
	}
}
