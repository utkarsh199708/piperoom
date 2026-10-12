package connect

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"piperoom/internal/clients"
)

const updateCfgBody = `{"name":"pg-orders-connector","connector.class":"io.debezium.connector.postgresql.PostgresConnector","decimal.handling.mode":"precise","slot.name":"orders_slot"}`

func TestUpdateConfigPutsFullMapWithOneKeyChanged(t *testing.T) {
	var gotMethod, gotPath, gotCT string
	var gotBody map[string]string
	var gets int
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			gets++
			w.Write([]byte(updateCfgBody))
		case http.MethodPut:
			gotMethod, gotPath, gotCT = r.Method, r.URL.Path, r.Header.Get("Content-Type")
			b, _ := io.ReadAll(r.Body)
			if err := json.Unmarshal(b, &gotBody); err != nil {
				t.Errorf("body not JSON: %v", err)
			}
			w.Write([]byte(`{"name":"pg-orders-connector"}`))
		}
	})
	if err := c.UpdateConfig(context.Background(), "pg-orders-connector", "decimal.handling.mode", "double"); err != nil {
		t.Fatalf("UpdateConfig: %v", err)
	}
	if gets != 1 || gotMethod != http.MethodPut || gotPath != "/connectors/pg-orders-connector/config" {
		t.Fatalf("gets=%d %s %s", gets, gotMethod, gotPath)
	}
	if gotCT != "application/json" {
		t.Errorf("content-type %q", gotCT)
	}
	var want map[string]string
	if err := json.Unmarshal([]byte(updateCfgBody), &want); err != nil {
		t.Fatal(err)
	}
	want["decimal.handling.mode"] = "double"
	if len(gotBody) != len(want) {
		t.Fatalf("body %v, want %v", gotBody, want)
	}
	for k, v := range want {
		if gotBody[k] != v {
			t.Errorf("body[%q] = %q, want %q", k, gotBody[k], v)
		}
	}
}

func TestUpdateConfigAddsAbsentKey(t *testing.T) {
	var gotBody map[string]string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			w.Write([]byte(`{"name":"x"}`))
			return
		}
		json.NewDecoder(r.Body).Decode(&gotBody)
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{}`))
	})
	if err := c.UpdateConfig(context.Background(), "x", "errors.tolerance", "all"); err != nil {
		t.Fatal(err)
	}
	if gotBody["errors.tolerance"] != "all" || gotBody["name"] != "x" {
		t.Fatalf("body %v", gotBody)
	}
}

func TestUpdateConfigPathEscapesName(t *testing.T) {
	var gotRaw string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			gotRaw = r.URL.EscapedPath()
		}
		w.Write([]byte(`{}`))
	})
	if err := c.UpdateConfig(context.Background(), "a/b c", "errors.tolerance", "none"); err != nil {
		t.Fatal(err)
	}
	if gotRaw != "/connectors/a%2Fb%20c/config" {
		t.Fatalf("escaped path %q", gotRaw)
	}
}

func TestUpdateConfigPutStatusErrors(t *testing.T) {
	for _, code := range []int{http.StatusConflict, http.StatusInternalServerError} {
		c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			if r.Method == http.MethodGet {
				w.Write([]byte(updateCfgBody))
				return
			}
			w.WriteHeader(code)
			w.Write([]byte(`{"error_code":409,"message":"Cannot complete request momentarily due to stale configuration"}`))
		})
		err := c.UpdateConfig(context.Background(), "pg-orders-connector", "decimal.handling.mode", "double")
		if err == nil || !strings.Contains(err.Error(), "status ") {
			t.Fatalf("code %d: want status error, got %v", code, err)
		}
	}
}

func TestUpdateConfigNotFoundOnRead(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			t.Error("PUT sent although the read failed")
		}
		http.NotFound(w, r)
	})
	err := c.UpdateConfig(context.Background(), "nope", "errors.tolerance", "all")
	if !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("want ErrNotFound, got %v", err)
	}
}

// Writing a masked value back would replace the real secret with "[hidden]".
func TestUpdateConfigRefusesMaskedConfig(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			t.Error("PUT sent for a masked config")
		}
		w.Write([]byte(`{"name":"x","database.password":"[hidden]"}`))
	})
	err := c.UpdateConfig(context.Background(), "x", "errors.tolerance", "all")
	if err == nil || !strings.Contains(err.Error(), "masked") {
		t.Fatalf("want masked error, got %v", err)
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
