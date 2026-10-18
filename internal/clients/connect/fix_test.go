package connect

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

func fixPipeline() owners.Pipeline {
	return owners.Pipeline{ID: "pg-orders", TopicPrefix: "pg", Connectors: []string{"pg-orders-connector"}, SourceTables: []string{"public.orders"}}
}

func scopedCtx() context.Context {
	return tools.WithPipeline(context.Background(), fixPipeline())
}

// The two P3 writes, called the same way, so the status, scope and transport
// cases run against both.
var fixWrites = []struct {
	name string
	call func(ctx context.Context, c *Client, connector string) error
}{
	{"PauseConnector", func(ctx context.Context, c *Client, n string) error { return c.PauseConnector(ctx, n) }},
	{"UpdateConnectorConfigKeys", func(ctx context.Context, c *Client, n string) error {
		return c.UpdateConnectorConfigKeys(ctx, n, map[string]string{"tombstones.on.delete": "false"})
	}},
}

func TestPauseConnectorPuts(t *testing.T) {
	var method, path string
	var bodyLen int
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		method, path = r.Method, r.URL.EscapedPath()
		b, _ := io.ReadAll(r.Body)
		bodyLen = len(b)
		w.WriteHeader(http.StatusAccepted)
	})
	if err := c.PauseConnector(scopedCtx(), "pg-orders-connector"); err != nil {
		t.Fatal(err)
	}
	if method != http.MethodPut || path != "/connectors/pg-orders-connector/pause" || bodyLen != 0 {
		t.Fatalf("%s %s body %d", method, path, bodyLen)
	}
}

func TestUpdateConnectorConfigKeysPatchesAllKeysOnce(t *testing.T) {
	var reqs []string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		reqs = append(reqs, r.Method+" "+r.URL.Path+" "+r.Header.Get("Content-Type")+" "+string(b))
		w.Write([]byte(`{"name":"pg-orders-connector"}`))
	})
	err := c.UpdateConnectorConfigKeys(scopedCtx(), "pg-orders-connector", map[string]string{
		"decimal.handling.mode": "string", "tombstones.on.delete": "false",
	})
	if err != nil {
		t.Fatal(err)
	}
	// One PATCH, no GET: the server merges, so no other key is read or sent.
	want := `PATCH /connectors/pg-orders-connector/config application/json {"decimal.handling.mode":"string","tombstones.on.delete":"false"}`
	if len(reqs) != 1 || reqs[0] != want {
		t.Fatalf("requests %q", reqs)
	}
}

func TestUpdateConnectorConfigKeysMethodNotAllowed(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusMethodNotAllowed)
	})
	err := c.UpdateConnectorConfigKeys(scopedCtx(), "pg-orders-connector", map[string]string{"tombstones.on.delete": "false"})
	if err == nil || !strings.Contains(err.Error(), "needs Kafka Connect 3.5+") {
		t.Fatalf("err %v", err)
	}
}

func TestFixWritesStatusErrors(t *testing.T) {
	for _, w := range fixWrites {
		for _, tc := range []struct {
			code    int
			isFound bool
		}{{http.StatusNotFound, true}, {http.StatusConflict, false}, {http.StatusInternalServerError, false}} {
			c := newTestClient(t, func(rw http.ResponseWriter, r *http.Request) {
				rw.WriteHeader(tc.code)
				rw.Write([]byte(`{"error_code":0,"message":"rebalance in progress"}`))
			})
			err := w.call(scopedCtx(), c, "pg-orders-connector")
			if err == nil {
				t.Fatalf("%s %d: want error", w.name, tc.code)
			}
			if errors.Is(err, clients.ErrNotFound) != tc.isFound {
				t.Fatalf("%s %d: err %v", w.name, tc.code, err)
			}
			var se *statusError
			if !tc.isFound && (!errors.As(err, &se) || se.Code != tc.code) {
				t.Fatalf("%s %d: err %v, want statusError", w.name, tc.code, err)
			}
		}
	}
}

// A huge error body is cut to 4 KiB; a huge success body is never read.
func TestFixWritesOversizeBody(t *testing.T) {
	big := strings.Repeat("x", 10<<20)
	for _, w := range fixWrites {
		c := newTestClient(t, func(rw http.ResponseWriter, r *http.Request) {
			rw.WriteHeader(http.StatusInternalServerError)
			rw.Write([]byte(big))
		})
		err := w.call(scopedCtx(), c, "pg-orders-connector")
		if err == nil || len(err.Error()) > 4096+200 {
			t.Fatalf("%s: error of %d bytes", w.name, len(err.Error()))
		}
		c = newTestClient(t, func(rw http.ResponseWriter, r *http.Request) {
			rw.Write([]byte(big))
		})
		if err := w.call(scopedCtx(), c, "pg-orders-connector"); err != nil {
			t.Fatalf("%s: oversize success body: %v", w.name, err)
		}
	}
}

func TestFixWritesTimeout(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	for _, w := range fixWrites {
		c := newTestClient(t, func(rw http.ResponseWriter, r *http.Request) {
			select {
			case <-release:
			case <-r.Context().Done():
			}
		})
		ctx, cancel := context.WithTimeout(scopedCtx(), 50*time.Millisecond)
		start := time.Now()
		err := w.call(ctx, c, "pg-orders-connector")
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 5*time.Second {
			t.Fatalf("%s: err %v after %v", w.name, err, time.Since(start))
		}
	}
}

// Fail closed: no scope, a bare pipeline id, or another pipeline's connector
// is refused before any request; the explicit opt-out passes.
func TestFixWritesScope(t *testing.T) {
	for _, w := range fixWrites {
		var reqs atomic.Int32
		c := newTestClient(t, func(rw http.ResponseWriter, r *http.Request) {
			reqs.Add(1)
			rw.WriteHeader(http.StatusAccepted)
		})
		refusals := []struct {
			name      string
			ctx       context.Context
			connector string
			want      error
		}{
			{"no scope", context.Background(), "pg-orders-connector", tools.ErrNoScope},
			{"pipeline id only", tools.WithPipelineID(context.Background(), "pg-orders"), "pg-orders-connector", tools.ErrNoScope},
			{"other pipeline's connector", scopedCtx(), "pg-inventory-connector", tools.ErrOutOfScope},
			{"pipeline beats opt-out", tools.WithPipeline(tools.WithUnscoped(context.Background()), fixPipeline()), "pg-inventory-connector", tools.ErrOutOfScope},
		}
		for _, tc := range refusals {
			if err := w.call(tc.ctx, c, tc.connector); !errors.Is(err, tc.want) {
				t.Fatalf("%s %s: err %v, want %v", w.name, tc.name, err, tc.want)
			}
		}
		if n := reqs.Load(); n != 0 {
			t.Fatalf("%s: %d refused writes reached Connect", w.name, n)
		}
		if err := w.call(tools.WithUnscoped(context.Background()), c, "anything"); err != nil {
			t.Fatalf("%s unscoped opt-out: %v", w.name, err)
		}
		if err := w.call(scopedCtx(), c, "pg-orders-connector"); err != nil || reqs.Load() != 2 {
			t.Fatalf("%s in scope: %v (requests %d)", w.name, err, reqs.Load())
		}
	}
}

func TestUpdateConnectorConfigKeysBadInput(t *testing.T) {
	c := newTestClient(t, func(rw http.ResponseWriter, r *http.Request) {
		t.Error("no request expected")
	})
	many := map[string]string{}
	for i := 0; i < maxChangeKeys+1; i++ {
		many[strings.Repeat("k", i+1)] = "v"
	}
	tests := []struct {
		name      string
		connector string
		changes   map[string]string
	}{
		{"no connector", "", map[string]string{"a": "b"}},
		{"nil changes", "c", nil},
		{"empty changes", "c", map[string]string{}},
		{"empty key", "c", map[string]string{"": "x"}},
		{"key with space", "c", map[string]string{"a b": "x"}},
		{"key with newline", "c", map[string]string{"a\nb": "x"}},
		{"key not ascii", "c", map[string]string{"dеcimal": "x"}},
		{"key too long", "c", map[string]string{strings.Repeat("a", maxChangeKeyLen+1): "x"}},
		{"value too long", "c", map[string]string{"a": "s3cret" + strings.Repeat("v", maxChangeValLen)}},
		{"too many keys", "c", many},
	}
	for _, tc := range tests {
		err := c.UpdateConnectorConfigKeys(tools.WithUnscoped(context.Background()), tc.connector, tc.changes)
		if err == nil {
			t.Fatalf("%s: want error", tc.name)
		}
		if strings.Contains(err.Error(), "s3cret") {
			t.Fatalf("%s: error echoes a value: %v", tc.name, err)
		}
	}
	if err := c.PauseConnector(tools.WithUnscoped(context.Background()), ""); err == nil {
		t.Fatal("pause with empty name: want error")
	}
}
