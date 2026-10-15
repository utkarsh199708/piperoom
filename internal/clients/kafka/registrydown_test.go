package kafka

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/clients"
	"piperoom/internal/clients/schemareg"
)

func avroKeyRec(p int32, off int64) *kgo.Record {
	r := data(p, off, "")
	r.Key = []byte{0, 0, 0, 0, 1, 2, 'k'}
	return r
}

func registryClient(t *testing.T, h http.HandlerFunc, recs ...*kgo.Record) *Client {
	t.Helper()
	c := testClientWithRegistry(t, h)
	l := &fakeLister{
		starts: merge(listed("t", map[int32]int64{0: 0})),
		ends:   merge(listed("t", map[int32]int64{0: int64(len(recs))})),
	}
	cons := &fakeConsumer{polls: []kgo.Fetches{fetchOf("t", recs...)}}
	c.offs = l
	c.consumerFn = func([]kgo.Opt) (pollConsumer, error) { return cons, nil }
	return c
}

// TestFindByKeyRegistryDownIsNotNotFound: #386. Keys that cannot be decoded
// because the registry fails must not read as a complete, empty scan.
func TestFindByKeyRegistryDownIsNotNotFound(t *testing.T) {
	c := registryClient(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error_code":50301,"message":"down"}`, http.StatusServiceUnavailable)
	}, avroKeyRec(0, 0))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	scan, err := c.FindByKeyScan(ctx, "t", "k", epoch, 0)
	if err == nil {
		t.Fatalf("got %+v, nil error; want an incomplete-scan error", scan)
	}
	var serr *schemareg.Error
	if !errors.Is(err, errRegistryUnavailable) || !errors.As(err, &serr) {
		t.Fatalf("err = %v, want it to wrap the registry failure", err)
	}
}

// TestFindByKeyRegistryDownWithMatch: a raw-key match is kept and the
// partition with undecodable keys is reported truncated.
func TestFindByKeyRegistryDownWithMatch(t *testing.T) {
	c := registryClient(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error_code":50301,"message":"down"}`, http.StatusServiceUnavailable)
	}, avroKeyRec(0, 0), data(0, 1, "k"))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	scan, err := c.FindByKeyScan(ctx, "t", "k", epoch, 0)
	if err != nil || len(scan.Refs) != 1 || len(scan.Truncated) != 1 {
		t.Fatalf("got %+v, %v; want one ref and partition 0 truncated", scan, err)
	}
}

// TestFindByKeyRegistryDeadline: a registry that blocks until the scan
// context ends is an error, not an empty scan.
func TestFindByKeyRegistryBlocked(t *testing.T) {
	c := registryClient(t, func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}, avroKeyRec(0, 0))
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	scan, err := c.FindByKeyScan(ctx, "t", "k", epoch, 0)
	if err == nil {
		t.Fatalf("got %+v, nil error; want an error", scan)
	}
}

// TestFindByKeyUnknownSchemaIsNonMatch: a 404 for the schema id is a
// definite answer about that key (it cannot be the one we look for).
func TestFindByKeyUnknownSchemaIsNonMatch(t *testing.T) {
	c := registryClient(t, func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, `{"error_code":40403,"message":"Schema not found"}`, http.StatusNotFound)
	}, avroKeyRec(0, 0))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	scan, err := c.FindByKeyScan(ctx, "t", "k", epoch, 0)
	if err != nil || len(scan.Refs) != 0 || len(scan.Truncated) != 0 {
		t.Fatalf("got %+v, %v; want an empty complete scan", scan, err)
	}
}

var _ = avro.Schema(nil)
var _ clients.KeyScan
