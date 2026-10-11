package kafka

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/twmb/franz-go/pkg/kadm"

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

// TestTopicConfigFrom pins the mapping from kadm's DescribeConfigs entries and
// metadata partitions onto the TopicConfig DTO, including the keys it must
// ignore.
func TestTopicConfigFrom(t *testing.T) {
	rc := kadm.ResourceConfig{Name: "pg.public.orders", Configs: []kadm.Config{
		{Key: "cleanup.policy", Value: kadm.StringPtr("compact,delete")},
		{Key: "retention.ms", Value: kadm.StringPtr("259200000")},
		{Key: "retention.bytes", Value: kadm.StringPtr("-1")},
		{Key: "min.insync.replicas", Value: kadm.StringPtr("2")},
		{Key: "segment.bytes", Value: kadm.StringPtr("1073741824")},
	}}
	td := kadm.TopicDetail{Topic: "pg.public.orders", Partitions: kadm.PartitionDetails{
		0: {Topic: "pg.public.orders", Partition: 0, Replicas: []int32{1, 2, 3}},
		1: {Topic: "pg.public.orders", Partition: 1, Replicas: []int32{2, 3, 1}},
		2: {Topic: "pg.public.orders", Partition: 2, Replicas: []int32{3, 1, 2}},
	}}
	want := TopicConfig{
		Topic: "pg.public.orders", Partitions: 3, ReplicationFactor: 3,
		RetentionMS: "259200000", RetentionBytes: "-1",
		CleanupPolicy: "compact,delete", MinInsyncReplicas: "2",
	}
	if got := topicConfigFrom("pg.public.orders", rc, td); got != want {
		t.Fatalf("topicConfigFrom = %+v, want %+v", got, want)
	}
}

func TestTopicConfigFromEdges(t *testing.T) {
	tests := []struct {
		name string
		rc   kadm.ResourceConfig
		td   kadm.TopicDetail
		want TopicConfig
	}{
		{
			name: "missing config keys leave empty strings",
			rc:   kadm.ResourceConfig{Name: "t", Configs: []kadm.Config{{Key: "segment.ms", Value: kadm.StringPtr("1")}}},
			td:   kadm.TopicDetail{Partitions: kadm.PartitionDetails{0: {Replicas: []int32{1, 2}}}},
			want: TopicConfig{Topic: "t", Partitions: 1, ReplicationFactor: 2},
		},
		{
			name: "nil value is empty, other keys still map",
			rc: kadm.ResourceConfig{Name: "t", Configs: []kadm.Config{
				{Key: "retention.ms"},
				{Key: "cleanup.policy", Value: kadm.StringPtr("compact")},
			}},
			td:   kadm.TopicDetail{Partitions: kadm.PartitionDetails{0: {Replicas: []int32{1}}}},
			want: TopicConfig{Topic: "t", Partitions: 1, ReplicationFactor: 1, CleanupPolicy: "compact"},
		},
		{
			name: "no partitions",
			rc:   kadm.ResourceConfig{Name: "t"},
			td:   kadm.TopicDetail{},
			want: TopicConfig{Topic: "t"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := topicConfigFrom("t", tt.rc, tt.td); got != tt.want {
				t.Fatalf("topicConfigFrom = %+v, want %+v", got, tt.want)
			}
		})
	}
}
