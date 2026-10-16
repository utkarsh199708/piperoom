package kafka

import (
	"context"
	"errors"
	"net/http"
	"slices"
	"strings"
	"sync/atomic"
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

// TestFindByKeyBare404IsUnavailable: #520. A bare 404 from the registry,
// with no Confluent error body (a proxy, a wrong URL path), says nothing
// about the key: only error_code 40403 is "schema not found". The scan is
// incomplete, never a complete "not found".
func TestFindByKeyBare404IsUnavailable(t *testing.T) {
	for name, h := range map[string]http.HandlerFunc{
		"bare": func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) },
		"other code": func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, `{"error_code":40401,"message":"x"}`, http.StatusNotFound)
		},
		"html proxy": func(w http.ResponseWriter, r *http.Request) { http.Error(w, `<html>nginx</html>`, http.StatusNotFound) },
		"zero code": func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, `{"message":"no route"}`, http.StatusNotFound)
		},
	} {
		t.Run(name, func(t *testing.T) {
			c := registryClient(t, h, avroKeyRec(0, 0))
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			scan, err := c.FindByKeyScan(ctx, "t", "k", epoch, 0)
			if err == nil || !errors.Is(err, errRegistryUnavailable) {
				t.Fatalf("got %+v, %v; want an incomplete-scan error", scan, err)
			}
		})
	}
}

// TestFindByKeyRegistryDownAsksOnce: #538. After the first
// registry-unavailable answer the scan sends the registry nothing more,
// whatever the schema ids of later keys, and still reports them undecoded.
func TestFindByKeyRegistryDownAsksOnce(t *testing.T) {
	var hits atomic.Int32
	recs := make([]*kgo.Record, 0, 200)
	for i := range 200 {
		r := avroKeyRec(0, int64(i))
		r.Key[4] = byte(1 + i%3) // three schema ids
		recs = append(recs, r)
	}
	c := registryClient(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.Error(w, `{"error_code":50301,"message":"down"}`, http.StatusServiceUnavailable)
	}, recs...)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	scan, err := c.FindByKeyScan(ctx, "t", "k", epoch, 0)
	if err == nil || !errors.Is(err, errRegistryUnavailable) {
		t.Fatalf("got %+v, %v; want an incomplete-scan error", scan, err)
	}
	if !strings.Contains(err.Error(), "200 keys could not be decoded") {
		t.Errorf("err = %v, want all 200 keys counted undecoded", err)
	}
	if n := hits.Load(); n != 1 {
		t.Errorf("registry got %d requests, want 1", n)
	}
}

// TestFindByKeyRegistryDownWithMatchAsksOnce: #538 with a raw match. The
// match is kept, the partition reported truncated as in #516, and the
// registry asked once.
func TestFindByKeyRegistryDownWithMatchAsksOnce(t *testing.T) {
	var hits atomic.Int32
	c := registryClient(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		http.Error(w, `{"error_code":50301,"message":"down"}`, http.StatusServiceUnavailable)
	}, data(0, 0, "k"), avroKeyRec(0, 1), avroKeyRec(0, 2), data(0, 3, "k"))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	scan, err := c.FindByKeyScan(ctx, "t", "k", epoch, 0)
	if err != nil || len(scan.Refs) != 2 || !slices.Equal(scan.Truncated, []int32{0}) {
		t.Fatalf("got %+v, %v; want two refs and partition 0 truncated", scan, err)
	}
	if n := hits.Load(); n != 1 {
		t.Errorf("registry got %d requests, want 1", n)
	}
}

// TestFindByKeyRegistryDownIsPerScan: the "registry down" state belongs to
// one scan. The next scan asks again, and decodes once the registry is back.
func TestFindByKeyRegistryDownIsPerScan(t *testing.T) {
	var up atomic.Bool
	var hits atomic.Int32
	c := registryClient(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		if !up.Load() {
			http.Error(w, `{"error_code":50301,"message":"down"}`, http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"schema":"{\"type\":\"record\",\"name\":\"K\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}"}`))
	}, avroKeyRec(0, 0))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := c.FindByKeyScan(ctx, "t", "k", epoch, 0); err == nil {
		t.Fatal("first scan: want an incomplete-scan error")
	}
	up.Store(true)
	cons := &fakeConsumer{polls: []kgo.Fetches{fetchOf("t", avroKeyRec(0, 0))}}
	c.consumerFn = func([]kgo.Opt) (pollConsumer, error) { return cons, nil }
	scan, err := c.FindByKeyScan(ctx, "t", "k", epoch, 0)
	if err != nil || len(scan.Refs) != 1 {
		t.Fatalf("second scan: got %+v, %v; want the key decoded and matched", scan, err)
	}
	if n := hits.Load(); n != 2 {
		t.Errorf("registry got %d requests over two scans, want 2", n)
	}
}
