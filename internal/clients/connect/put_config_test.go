package connect

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// The whole config goes out in one PUT, unchanged, to the escaped path.
func TestPutConnectorConfigPuts(t *testing.T) {
	cfg := map[string]string{
		"name":                  "pg-orders-connector",
		"connector.class":       "io.debezium.connector.postgresql.PostgresConnector",
		"database.password":     "${file:/secrets/pg.properties:password}",
		"decimal.handling.mode": "precise",
	}
	var method, path, ct string
	var got map[string]string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		method, path, ct = r.Method, r.URL.EscapedPath(), r.Header.Get("Content-Type")
		b, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(b, &got); err != nil {
			t.Errorf("body: %v", err)
		}
		w.Write([]byte(`{"name":"pg-orders-connector","config":{},"tasks":[]}`))
	})
	if err := c.PutConnectorConfig(scopedCtx(), "pg-orders-connector", cfg); err != nil {
		t.Fatalf("PutConnectorConfig: %v", err)
	}
	if method != http.MethodPut || path != "/connectors/pg-orders-connector/config" || ct != "application/json" {
		t.Fatalf("%s %s (%s)", method, path, ct)
	}
	if !maps.Equal(got, cfg) {
		t.Fatalf("body %v, want %v", got, cfg)
	}
}

// Connect's refusal is reported by status only: its message can quote a value.
func TestPutConnectorConfigErrorsCarryNoValue(t *testing.T) {
	const secret = "s3cr3t-Pa55word"
	for _, code := range []int{http.StatusBadRequest, http.StatusConflict, http.StatusInternalServerError} {
		c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(code)
			w.Write([]byte(`{"error_code":400,"message":"Invalid value ` + secret + ` for configuration database.password"}`))
		})
		err := c.PutConnectorConfig(scopedCtx(), "pg-orders-connector", map[string]string{"database.password": secret})
		if err == nil || strings.Contains(err.Error(), secret) || !strings.Contains(err.Error(), "status") {
			t.Fatalf("status %d: err %v", code, err)
		}
	}
	// A transport failure names neither the config nor its values.
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {})
	ctx, cancel := context.WithCancel(scopedCtx())
	cancel()
	err := c.PutConnectorConfig(ctx, "pg-orders-connector", map[string]string{"database.password": secret})
	if !errors.Is(err, context.Canceled) || strings.Contains(err.Error(), secret) {
		t.Fatalf("cancelled: %v", err)
	}
}

// Fail closed before any request: bad input, no scope, another pipeline's
// connector. The opt-out passes.
func TestPutConnectorConfigRefusals(t *testing.T) {
	var reqs atomic.Int32
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		reqs.Add(1)
		w.Write([]byte(`{}`))
	})
	ok := map[string]string{"connector.class": "x"}
	many := map[string]string{}
	for i := 0; len(many) <= maxPutKeys; i++ {
		many["k"+strings.Repeat("x", i%100)+string(rune('a'+i/100))] = "v"
	}
	for _, tc := range []struct {
		name      string
		ctx       context.Context
		connector string
		cfg       map[string]string
		want      error
	}{
		{"empty name", scopedCtx(), "", ok, nil},
		{"empty config", scopedCtx(), "pg-orders-connector", map[string]string{}, nil},
		{"nil config", scopedCtx(), "pg-orders-connector", nil, nil},
		{"too many keys", scopedCtx(), "pg-orders-connector", many, nil},
		{"name of another connector", scopedCtx(), "pg-orders-connector", map[string]string{"name": "other-secret-name"}, nil},
		{"empty key", scopedCtx(), "pg-orders-connector", map[string]string{"": "v"}, nil},
		{"control byte in key", scopedCtx(), "pg-orders-connector", map[string]string{"a\nb": "v"}, nil},
		{"huge value", scopedCtx(), "pg-orders-connector", map[string]string{"k": strings.Repeat("v", maxPutValLen+1)}, nil},
		{"no scope", context.Background(), "pg-orders-connector", ok, tools.ErrNoScope},
		{"pipeline id only", tools.WithPipelineID(context.Background(), "pg-orders"), "pg-orders-connector", ok, tools.ErrNoScope},
		{"other pipeline's connector", scopedCtx(), "pg-inventory-connector", ok, tools.ErrOutOfScope},
	} {
		err := c.PutConnectorConfig(tc.ctx, tc.connector, tc.cfg)
		if err == nil || (tc.want != nil && !errors.Is(err, tc.want)) {
			t.Fatalf("%s: err %v, want %v", tc.name, err, tc.want)
		}
		if strings.Contains(err.Error(), "other-secret-name") || strings.Contains(err.Error(), "vvvv") {
			t.Fatalf("%s: error echoes a value: %v", tc.name, err)
		}
	}
	if n := reqs.Load(); n != 0 {
		t.Fatalf("%d refused PUTs reached Connect", n)
	}
	if err := c.PutConnectorConfig(tools.WithUnscoped(context.Background()), "any", ok); err != nil || reqs.Load() != 1 {
		t.Fatalf("opt-out: %v (requests %d)", err, reqs.Load())
	}
}

// 404 is ErrNotFound; a hung Connect ends at ctx's deadline.
func TestPutConnectorConfigNotFoundAndDeadline(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) { http.NotFound(w, r) })
	if err := c.PutConnectorConfig(scopedCtx(), "pg-orders-connector", map[string]string{"k": "v"}); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("404: %v", err)
	}
	hang := make(chan struct{})
	c = newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-hang:
		case <-r.Context().Done():
		}
	})
	t.Cleanup(func() { close(hang) })
	ctx, cancel := context.WithTimeout(scopedCtx(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := c.PutConnectorConfig(ctx, "pg-orders-connector", map[string]string{"k": "v"}); !errors.Is(err, context.DeadlineExceeded) || time.Since(start) > 5*time.Second {
		t.Fatalf("hung Connect: %v after %v", err, time.Since(start))
	}
}
