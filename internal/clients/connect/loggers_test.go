package connect

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"piperoom/internal/clients"
)

func TestLoggerLevels(t *testing.T) {
	var gotMethod, gotPath string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		w.Write([]byte(`{
			"org.apache.kafka.connect.runtime.Worker":{"level":"INFO"},
			"org.apache.kafka.connect.runtime.distributed.DistributedHerder":{"level":"DEBUG"},
			"io.debezium.connector.postgresql":{"level":"TRACE"}
		}`))
	})
	levels, err := c.LoggerLevels(context.Background())
	if err != nil {
		t.Fatalf("LoggerLevels: %v", err)
	}
	if gotMethod != http.MethodGet || gotPath != "/admin/loggers" {
		t.Fatalf("%s %s", gotMethod, gotPath)
	}
	if len(levels) != 3 || levels["io.debezium.connector.postgresql"] != "TRACE" {
		t.Fatalf("levels %+v", levels)
	}
}

// The endpoint is a read: a worker that lists its loggers with no level at all
// yields no entry rather than a made-up level.
func TestLoggerLevelsEmptyLevelDropped(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"a":{"level":"INFO"},"":{"level":"DEBUG"},"b":{}}`))
	})
	levels, err := c.LoggerLevels(context.Background())
	if err != nil {
		t.Fatalf("LoggerLevels: %v", err)
	}
	if len(levels) != 2 || levels["b"] != "" || levels["a"] != "INFO" {
		t.Fatalf("levels %+v", levels)
	}
	if _, ok := levels[""]; ok {
		t.Fatalf("empty logger name kept: %+v", levels)
	}
}

func TestLoggerLevelsErrors(t *testing.T) {
	t.Run("not found", func(t *testing.T) {
		c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			http.NotFound(w, r)
		})
		if _, err := c.LoggerLevels(context.Background()); !errors.Is(err, clients.ErrNotFound) {
			t.Fatalf("err %v, want ErrNotFound", err)
		}
	})
	t.Run("server error body sanitized", func(t *testing.T) {
		c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("boom\nsecond line\x1b[31m"))
		})
		_, err := c.LoggerLevels(context.Background())
		if err == nil {
			t.Fatal("expected an error")
		}
		if strings.ContainsAny(err.Error(), "\n\x1b") {
			t.Fatalf("error body not sanitized: %q", err)
		}
	})
}
