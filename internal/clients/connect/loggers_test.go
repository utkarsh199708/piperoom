package connect

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"piperoom/internal/clients"
)

func TestLoggers(t *testing.T) {
	var gotMethod, gotPath string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		w.Write([]byte(`{
  "org.apache.kafka.connect": {"level": "INFO", "last_modified": null},
  "root": {"level": "INFO"},
  "io.debezium.connector.postgresql": {"level": "debug", "last_modified": 1729000000000}
}`))
	})
	ls, err := c.Loggers(context.Background())
	if err != nil {
		t.Fatalf("Loggers: %v", err)
	}
	if gotMethod != http.MethodGet || gotPath != "/admin/loggers" {
		t.Fatalf("request %s %s", gotMethod, gotPath)
	}
	if len(ls) != 3 || ls[0].Logger != "root" || ls[1].Logger != "io.debezium.connector.postgresql" {
		t.Fatalf("loggers = %+v (root first, then by name)", ls)
	}
	if ls[1].Level != "DEBUG" || ls[1].LastModified != 1729000000000 || ls[2].LastModified != 0 {
		t.Errorf("loggers = %+v", ls)
	}
}

func TestLoggersNotSupported(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	_, err := c.Loggers(context.Background())
	if !errors.Is(err, clients.ErrNotSupported) || !strings.Contains(err.Error(), "2.4") {
		t.Fatalf("err = %v, want ErrNotSupported naming 2.4", err)
	}
}

func TestLoggersErrors(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("boom password=hunter2\x1b[31m"))
	})
	_, err := c.Loggers(context.Background())
	if err == nil || strings.Contains(err.Error(), "hunter2") || strings.Contains(err.Error(), "\x1b") {
		t.Fatalf("err = %v, want a redacted, folded status error", err)
	}

	c = newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`["not","an","object"]`))
	})
	if _, err := c.Loggers(context.Background()); err == nil {
		t.Fatal("a non-object answer must fail to decode")
	}
}

func TestLoggersCap(t *testing.T) {
	var sb strings.Builder
	sb.WriteString(`{"root":{"level":"WARN"}`)
	for i := range maxLoggers + 20 {
		sb.WriteString(`,"l` + strings.Repeat("x", 3) + strconv.Itoa(i) + `":{"level":"INFO"}`)
	}
	sb.WriteString("}")
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(sb.String()))
	})
	ls, err := c.Loggers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(ls) != maxLoggers || ls[0].Logger != "root" {
		t.Fatalf("got %d loggers, first %q", len(ls), ls[0].Logger)
	}
}
