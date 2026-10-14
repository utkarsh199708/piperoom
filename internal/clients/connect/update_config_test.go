package connect

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"piperoom/internal/clients"
)

func TestUpdateConfigPatchesOneKey(t *testing.T) {
	var gotMethod, gotPath, gotCT, gotBody string
	requests := 0
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		requests++
		gotMethod, gotPath, gotCT = r.Method, r.URL.Path, r.Header.Get("Content-Type")
		b, _ := io.ReadAll(r.Body)
		gotBody = string(b)
		w.Write([]byte(`{"name":"pg-orders-connector"}`))
	})
	if err := c.UpdateConfig(context.Background(), "pg-orders-connector", "decimal.handling.mode", "double"); err != nil {
		t.Fatalf("UpdateConfig: %v", err)
	}
	// Exactly one request: no GET of the config, so no other key is read.
	if requests != 1 {
		t.Fatalf("%d requests, want 1", requests)
	}
	if gotMethod != http.MethodPatch || gotPath != "/connectors/pg-orders-connector/config" {
		t.Fatalf("%s %s", gotMethod, gotPath)
	}
	if gotCT != "application/json" {
		t.Errorf("content-type %q", gotCT)
	}
	if gotBody != `{"decimal.handling.mode":"double"}` {
		t.Fatalf("body %q", gotBody)
	}
}

func TestUpdateConfigPathEscapesName(t *testing.T) {
	var gotRaw string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotRaw = r.URL.EscapedPath()
		w.Write([]byte(`{}`))
	})
	if err := c.UpdateConfig(context.Background(), "a/b c", "tombstones.on.delete", "false"); err != nil {
		t.Fatal(err)
	}
	if gotRaw != "/connectors/a%2Fb%20c/config" {
		t.Fatalf("escaped path %q", gotRaw)
	}
}

// A connector deleted meanwhile answers 404: PATCH never re-creates it.
func TestUpdateConfigNotFound(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	err := c.UpdateConfig(context.Background(), "nope", "tombstones.on.delete", "true")
	if !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

// Connect older than 3.5 has no PATCH. There is no PUT fallback.
func TestUpdateConfigMethodNotAllowed(t *testing.T) {
	var methods []string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		methods = append(methods, r.Method)
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte(`{"error_code":405,"message":"HTTP 405 Method Not Allowed"}`))
	})
	err := c.UpdateConfig(context.Background(), "pg-orders-connector", "tombstones.on.delete", "true")
	if err == nil || !strings.Contains(err.Error(), "does not support PATCH config; needs Kafka Connect 3.5+") {
		t.Fatalf("err %v", err)
	}
	if len(methods) != 1 || methods[0] != http.MethodPatch {
		t.Fatalf("methods %v, want one PATCH and no fallback", methods)
	}
}

func TestUpdateConfigOtherStatusErrors(t *testing.T) {
	for _, code := range []int{http.StatusConflict, http.StatusInternalServerError} {
		c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(code)
			w.Write([]byte(`{"error_code":409,"message":"rebalance in progress"}`))
		})
		err := c.UpdateConfig(context.Background(), "pg-orders-connector", "tombstones.on.delete", "true")
		if err == nil || !strings.Contains(err.Error(), "status ") || strings.Contains(err.Error(), "PATCH config; needs") {
			t.Fatalf("code %d: err %v", code, err)
		}
	}
}

func TestUpdateConfigEmptyKey(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("no request expected")
	})
	if err := c.UpdateConfig(context.Background(), "x", "", "v"); err == nil {
		t.Fatal("want error for empty key")
	}
}
