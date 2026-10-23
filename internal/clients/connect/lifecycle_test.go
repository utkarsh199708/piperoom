package connect

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// unscoped is the CLI/integration escape hatch: lifecycle writes need a room
// pipeline or this explicit opt-out.
func unscoped() context.Context { return tools.WithUnscoped(context.Background()) }

// roomCtx scopes the call to the room's pipeline with the given connectors.
func roomCtx(connectors ...string) context.Context {
	return tools.WithPipeline(context.Background(), owners.Pipeline{ID: "orders", Connectors: connectors})
}

func TestServerInfo(t *testing.T) {
	var gotPath string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Write([]byte(`{"version":"3.6.1","commit":"abc123","kafka_cluster_id":"c-1"}`))
	})
	info, err := c.ServerInfo(context.Background())
	if err != nil {
		t.Fatalf("ServerInfo: %v", err)
	}
	if gotPath != "/" {
		t.Fatalf("path %q, want /", gotPath)
	}
	if info.Version != "3.6.1" || info.Commit != "abc123" || info.KafkaClusterID != "c-1" {
		t.Fatalf("info %+v", info)
	}
}

func TestSourceOffsets(t *testing.T) {
	var gotMethod, gotPath string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		w.Write([]byte(`{"offsets":[{"partition":{"server":"pg"},"offset":{"lsn":9223372036854775807,"txId":42}}]}`))
	})
	offs, err := c.SourceOffsets(context.Background(), "pg-orders-connector")
	if err != nil {
		t.Fatalf("SourceOffsets: %v", err)
	}
	if gotMethod != http.MethodGet || gotPath != "/connectors/pg-orders-connector/offsets" {
		t.Fatalf("%s %s", gotMethod, gotPath)
	}
	if len(offs) != 1 || offs[0].Partition["server"] != "pg" {
		t.Fatalf("offsets %+v", offs)
	}
	// UseNumber: a 64-bit position keeps every digit in a map[string]any.
	if got := offs[0].Offset["lsn"]; got == nil || got.(json.Number).String() != "9223372036854775807" {
		t.Fatalf("lsn %#v, want exact json.Number", got)
	}
}

// A Connect older than the endpoint answers 404 while the connector itself
// exists: ErrNotSupported, naming the Kafka version that adds it.
func TestSourceOffsetsUnsupported404(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/connectors/pg-orders-connector" {
			w.Write([]byte(`{"name":"pg-orders-connector"}`))
			return
		}
		http.NotFound(w, r)
	})
	_, err := c.SourceOffsets(context.Background(), "pg-orders-connector")
	if !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("err %v, want ErrNotSupported", err)
	}
	if !strings.Contains(err.Error(), "3.5") {
		t.Fatalf("err %v does not name Kafka 3.5", err)
	}
}

func TestSourceOffsetsUnsupported405(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusMethodNotAllowed)
	})
	_, err := c.SourceOffsets(context.Background(), "pg-orders-connector")
	if !errors.Is(err, clients.ErrNotSupported) || !strings.Contains(err.Error(), "3.5") {
		t.Fatalf("err %v, want ErrNotSupported naming 3.5", err)
	}
}

// A connector that does not exist answers ErrNotFound on both paths; it is not
// an old-Connect endpoint.
func TestSourceOffsetsConnectorMissing(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	_, err := c.SourceOffsets(context.Background(), "pg-orders-connector")
	if !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("err %v, want ErrNotFound", err)
	}
	if errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("err %v, a missing connector is not an unsupported endpoint", err)
	}
}

// When the 404-disambiguation probe itself fails (a 500 mid-rebalance), the
// original ErrNotFound stands: a failed probe must not turn a missing
// connector into "endpoint not supported".
func TestSourceOffsetsProbeFailureKeepsNotFound(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/connectors/pg-orders-connector" {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(`{"message":"boom"}`))
			return
		}
		http.NotFound(w, r)
	})
	_, err := c.SourceOffsets(context.Background(), "pg-orders-connector")
	if !errors.Is(err, clients.ErrNotFound) || errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("err %v, want ErrNotFound and not ErrNotSupported", err)
	}
}

// Every lifecycle error path sanitizes a Connect error body: credentials are
// redacted and control characters folded before the body can reach a log or a
// prompt, like createError always did.
func TestLifecycleErrorsSanitizeBody(t *testing.T) {
	const hostile = `{"message":"password=\"hunter2SecretQ\" \u001b[31mboom\nline"}`
	serve := func() *Client {
		return newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(hostile))
		})
	}
	calls := map[string]func(c *Client) error{
		"delete": func(c *Client) error { return c.DeleteConnector(unscoped(), "pg-orders-connector") },
		"stop":   func(c *Client) error { return c.StopConnector(unscoped(), "pg-orders-connector") },
		"reset":  func(c *Client) error { return c.ResetSourceOffsets(unscoped(), "pg-orders-connector") },
		"alter": func(c *Client) error {
			return c.AlterSourceOffsets(unscoped(), "pg-orders-connector", []SourceOffset{{Partition: map[string]any{"server": "s"}}})
		},
		"offsets": func(c *Client) error {
			_, err := c.SourceOffsets(context.Background(), "pg-orders-connector")
			return err
		},
		"info": func(c *Client) error { _, err := c.ServerInfo(context.Background()); return err },
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			err := call(serve())
			if err == nil {
				t.Fatal("no error")
			}
			msg := err.Error()
			if strings.Contains(msg, "hunter2SecretQ") {
				t.Fatalf("credential reached the error: %q", msg)
			}
			if strings.ContainsAny(msg, "\x1b\n\r") {
				t.Fatalf("control characters reached the error: %q", msg)
			}
		})
	}
}

// A create 409 is "already exists" only when Connect says so; the same status
// during a worker rebalance is the transient ErrRebalancing, and treating it
// as a duplicate would silently skip the create.
func TestCreateConnectorConflictDisambiguation(t *testing.T) {
	for _, tc := range []struct {
		name, body string
		want       error
	}{
		{"exists", `{"error_code":409,"message":"Connector pg-orders-connector already exists"}`, ErrConnectorExists},
		{"rebalance", `{"error_code":409,"message":"Cannot complete request momentarily due to stale configuration (typically caused by a worker rebalance)"}`, ErrRebalancing},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusConflict)
				w.Write([]byte(tc.body))
			})
			err := c.CreateConnector(unscoped(), "pg-orders-connector", map[string]string{"connector.class": "x"})
			if !errors.Is(err, tc.want) {
				t.Fatalf("err %v, want %v", err, tc.want)
			}
		})
	}
}

func TestSourceOffsetsCapAndBadJSON(t *testing.T) {
	big := `{"offsets":[{"partition":{"server":"` + strings.Repeat("x", 5<<20) + `"},"offset":{}}]}`
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(big))
	})
	if _, err := c.SourceOffsets(context.Background(), "pg-orders-connector"); err == nil {
		t.Fatal("oversized response decoded without an error")
	}
	c = newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"offsets":`))
	})
	if _, err := c.SourceOffsets(context.Background(), "pg-orders-connector"); err == nil || !strings.Contains(err.Error(), "decode") {
		t.Fatalf("bad JSON err %v", err)
	}
}

func TestSourceOffsetsServerError(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"message":"worker is rebalancing"}`))
	})
	_, err := c.SourceOffsets(context.Background(), "pg-orders-connector")
	if err == nil || !strings.Contains(err.Error(), "500") {
		t.Fatalf("err %v, want status 500", err)
	}
	if errors.Is(err, clients.ErrNotSupported) || errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("err %v misclassified a server error", err)
	}
}

// Every lifecycle write propagates Connect's 500 as a target error, not as a
// missing endpoint or a state conflict.
func TestLifecycleServerError(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"message":"boom"}`))
	})
	calls := map[string]func() error{
		"create": func() error {
			return c.CreateConnector(unscoped(), "pg-orders-connector", map[string]string{"a": "b"})
		},
		"delete": func() error { return c.DeleteConnector(unscoped(), "pg-orders-connector") },
		"stop":   func() error { return c.StopConnector(unscoped(), "pg-orders-connector") },
		"reset":  func() error { return c.ResetSourceOffsets(unscoped(), "pg-orders-connector") },
		"alter": func() error {
			return c.AlterSourceOffsets(unscoped(), "pg-orders-connector", []SourceOffset{{Partition: map[string]any{"server": "pg"}}})
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			err := call()
			if err == nil || !strings.Contains(err.Error(), "500") {
				t.Fatalf("err %v, want status 500", err)
			}
			if errors.Is(err, clients.ErrNotSupported) || errors.Is(err, ErrConnectorExists) || errors.Is(err, ErrConnectorNotStopped) {
				t.Fatalf("err %v misclassified a server error", err)
			}
		})
	}
}

func TestSourceOffsetsTimeout(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		w.Write([]byte(`{"offsets":[]}`))
	})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := c.SourceOffsets(ctx, "pg-orders-connector"); err == nil {
		t.Fatal("timed-out call returned no error")
	}
}

func TestCreateConnector(t *testing.T) {
	var gotMethod, gotPath string
	var got struct {
		Name   string            `json:"name"`
		Config map[string]string `json:"config"`
	}
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		b, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(b, &got); err != nil {
			t.Errorf("body: %v", err)
		}
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte(`{"name":"pg-orders-connector"}`))
	})
	cfg := map[string]string{"connector.class": "io.debezium.connector.postgresql.PostgresConnector"}
	if err := c.CreateConnector(unscoped(), "pg-orders-connector", cfg); err != nil {
		t.Fatalf("CreateConnector: %v", err)
	}
	if gotMethod != http.MethodPost || gotPath != "/connectors" {
		t.Fatalf("%s %s", gotMethod, gotPath)
	}
	if got.Name != "pg-orders-connector" || got.Config["connector.class"] != cfg["connector.class"] {
		t.Fatalf("body %+v", got)
	}
}

func TestCreateConnectorConflict(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
		w.Write([]byte(`{"error_code":409,"message":"Connector pg-orders-connector already exists"}`))
	})
	err := c.CreateConnector(unscoped(), "pg-orders-connector", map[string]string{"a": "b"})
	if !errors.Is(err, ErrConnectorExists) {
		t.Fatalf("err %v, want ErrConnectorExists", err)
	}
}

// A create validation error can echo config values, so the body is redacted
// before it reaches the error.
func TestCreateConnectorErrorRedactsConfig(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"message":"Invalid value database.password=sup3rSecret for configuration database.password"}`))
	})
	err := c.CreateConnector(unscoped(), "pg-orders-connector", map[string]string{"database.password": "sup3rSecret"})
	if err == nil {
		t.Fatal("no error")
	}
	if strings.Contains(err.Error(), "sup3rSecret") {
		t.Fatalf("error leaks the config value: %v", err)
	}
	if !strings.Contains(err.Error(), "400") {
		t.Fatalf("error lost the status: %v", err)
	}
}

func TestCreateConnectorValidatesInput(t *testing.T) {
	c := New("http://127.0.0.1:1", nil)
	if err := c.CreateConnector(unscoped(), "", map[string]string{"a": "b"}); err == nil {
		t.Fatal("empty name accepted")
	}
	if err := c.CreateConnector(unscoped(), "pg-orders-connector", nil); err == nil {
		t.Fatal("empty config accepted")
	}
}

func TestCreateConnectorScope(t *testing.T) {
	c := New("http://127.0.0.1:1", nil)
	cfg := map[string]string{"connector.class": "x"}
	if err := c.CreateConnector(roomCtx("pg-orders-connector"), "bill-connector", cfg); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("err %v, want ErrOutOfScope", err)
	}
	if err := c.CreateConnector(context.Background(), "pg-orders-connector", cfg); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("err %v, want ErrNoScope", err)
	}
}

func TestDeleteConnector(t *testing.T) {
	var gotMethod, gotPath string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	})
	if err := c.DeleteConnector(unscoped(), "pg-orders-connector"); err != nil {
		t.Fatalf("DeleteConnector: %v", err)
	}
	if gotMethod != http.MethodDelete || gotPath != "/connectors/pg-orders-connector" {
		t.Fatalf("%s %s", gotMethod, gotPath)
	}
}

func TestDeleteConnectorNotFound(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	})
	err := c.DeleteConnector(unscoped(), "pg-orders-connector")
	if !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("err %v, want ErrNotFound", err)
	}
}

func TestStopConnector(t *testing.T) {
	var gotMethod, gotPath string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		w.WriteHeader(http.StatusAccepted)
	})
	if err := c.StopConnector(unscoped(), "pg-orders-connector"); err != nil {
		t.Fatalf("StopConnector: %v", err)
	}
	if gotMethod != http.MethodPut || gotPath != "/connectors/pg-orders-connector/stop" {
		t.Fatalf("%s %s", gotMethod, gotPath)
	}
}

func TestStopConnectorUnsupported(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/connectors/pg-orders-connector" {
			w.Write([]byte(`{"name":"pg-orders-connector"}`))
			return
		}
		http.NotFound(w, r)
	})
	err := c.StopConnector(unscoped(), "pg-orders-connector")
	if !errors.Is(err, clients.ErrNotSupported) || !strings.Contains(err.Error(), "3.5") {
		t.Fatalf("err %v, want ErrNotSupported naming 3.5", err)
	}
}

func TestResetSourceOffsets(t *testing.T) {
	var gotMethod, gotPath string
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		w.Write([]byte(`{"message":"Request to reset the offsets for connector pg-orders-connector has been submitted"}`))
	})
	if err := c.ResetSourceOffsets(unscoped(), "pg-orders-connector"); err != nil {
		t.Fatalf("ResetSourceOffsets: %v", err)
	}
	if gotMethod != http.MethodDelete || gotPath != "/connectors/pg-orders-connector/offsets" {
		t.Fatalf("%s %s", gotMethod, gotPath)
	}
}

// A running connector refuses offset changes. Kafka answers 409 for a state
// conflict and 400 with the documented STOPPED message for this one; a 409
// whose message names a rebalance is the transient ErrRebalancing instead
// (the connector may already be stopped), and a 400 that merely echoes the
// STOPPED phrase inside a request value is neither.
func TestResetSourceOffsetsNotStopped(t *testing.T) {
	for _, tc := range []struct {
		name string
		code int
		body string
		want error
	}{
		{"409 state conflict", http.StatusConflict, `{"message":"Connector pg-orders-connector is not in the STOPPED state"}`, ErrConnectorNotStopped},
		{"409 rebalance", http.StatusConflict, `{"message":"Request cannot be completed because a rebalance is expected"}`, ErrRebalancing},
		{"400 stopped message", http.StatusBadRequest, `{"message":"Connectors must be in the STOPPED state before their offsets can be modified."}`, ErrConnectorNotStopped},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tc.code)
				w.Write([]byte(tc.body))
			})
			err := c.ResetSourceOffsets(unscoped(), "pg-orders-connector")
			if !errors.Is(err, tc.want) {
				t.Fatalf("err %v, want %v", err, tc.want)
			}
		})
	}
	// A 400 that echoes the phrase inside a request value (Connect quotes the
	// offending input in deserialization errors) is not a state statement.
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(`{"message":"Cannot deserialize value 'x: Connectors must be in the STOPPED state y'"}`))
	})
	err := c.ResetSourceOffsets(unscoped(), "pg-orders-connector")
	if err == nil || errors.Is(err, ErrConnectorNotStopped) || errors.Is(err, ErrRebalancing) {
		t.Fatalf("echoed phrase was misread as a state conflict: %v", err)
	}
}

func TestResetSourceOffsetsUnsupported(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/connectors/pg-orders-connector" {
			w.Write([]byte(`{"name":"pg-orders-connector"}`))
			return
		}
		http.NotFound(w, r)
	})
	err := c.ResetSourceOffsets(unscoped(), "pg-orders-connector")
	if !errors.Is(err, clients.ErrNotSupported) || !strings.Contains(err.Error(), "3.6") {
		t.Fatalf("err %v, want ErrNotSupported naming 3.6", err)
	}
}

func TestAlterSourceOffsets(t *testing.T) {
	var gotMethod, gotPath string
	var got struct {
		Offsets []SourceOffset `json:"offsets"`
	}
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		gotMethod, gotPath = r.Method, r.URL.Path
		b, _ := io.ReadAll(r.Body)
		if err := json.Unmarshal(b, &got); err != nil {
			t.Errorf("body: %v", err)
		}
		w.Write([]byte(`{"message":"submitted"}`))
	})
	offsets := []SourceOffset{{
		Partition: map[string]any{"server": "pg"},
		Offset:    map[string]any{"lsn": json.Number("1234567890123")},
	}}
	if err := c.AlterSourceOffsets(unscoped(), "pg-orders-connector", offsets); err != nil {
		t.Fatalf("AlterSourceOffsets: %v", err)
	}
	if gotMethod != http.MethodPatch || gotPath != "/connectors/pg-orders-connector/offsets" {
		t.Fatalf("%s %s", gotMethod, gotPath)
	}
	if len(got.Offsets) != 1 || got.Offsets[0].Partition["server"] != "pg" {
		t.Fatalf("body %+v", got)
	}
}

func TestAlterSourceOffsetsNotStopped(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusConflict)
	})
	err := c.AlterSourceOffsets(unscoped(), "pg-orders-connector", []SourceOffset{{Partition: map[string]any{"server": "pg"}}})
	if !errors.Is(err, ErrConnectorNotStopped) {
		t.Fatalf("err %v, want ErrConnectorNotStopped", err)
	}
}

func TestAlterSourceOffsetsUnsupported(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/connectors/pg-orders-connector" {
			w.Write([]byte(`{"name":"pg-orders-connector"}`))
			return
		}
		http.NotFound(w, r)
	})
	err := c.AlterSourceOffsets(unscoped(), "pg-orders-connector", []SourceOffset{{Partition: map[string]any{"server": "pg"}}})
	if !errors.Is(err, clients.ErrNotSupported) || !strings.Contains(err.Error(), "3.6") {
		t.Fatalf("err %v, want ErrNotSupported naming 3.6", err)
	}
}

func TestAlterSourceOffsetsValidates(t *testing.T) {
	c := New("http://127.0.0.1:1", nil)
	if err := c.AlterSourceOffsets(unscoped(), "pg-orders-connector", nil); err == nil {
		t.Fatal("empty offsets accepted")
	}
	if err := c.AlterSourceOffsets(unscoped(), "pg-orders-connector", []SourceOffset{{Offset: map[string]any{"lsn": 1}}}); err == nil {
		t.Fatal("entry without a partition accepted")
	}
	tooMany := make([]SourceOffset, maxOffsetEntries+1)
	for i := range tooMany {
		tooMany[i] = SourceOffset{Partition: map[string]any{"p": i}}
	}
	if err := c.AlterSourceOffsets(unscoped(), "pg-orders-connector", tooMany); err == nil {
		t.Fatal("oversized alter accepted")
	}
}

func TestLifecycleWritesScope(t *testing.T) {
	c := New("http://127.0.0.1:1", nil)
	offsets := []SourceOffset{{Partition: map[string]any{"server": "pg"}}}
	calls := map[string]func(context.Context) error{
		"delete": func(ctx context.Context) error { return c.DeleteConnector(ctx, "bill-connector") },
		"stop":   func(ctx context.Context) error { return c.StopConnector(ctx, "bill-connector") },
		"reset":  func(ctx context.Context) error { return c.ResetSourceOffsets(ctx, "bill-connector") },
		"alter": func(ctx context.Context) error {
			return c.AlterSourceOffsets(ctx, "bill-connector", offsets)
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			if err := call(roomCtx("pg-orders-connector")); !errors.Is(err, tools.ErrOutOfScope) {
				t.Fatalf("room ctx: err %v, want ErrOutOfScope", err)
			}
			if err := call(context.Background()); !errors.Is(err, tools.ErrNoScope) {
				t.Fatalf("no ctx: err %v, want ErrNoScope", err)
			}
		})
	}
}

// A connector in the room's pipeline passes the scope check and reaches the
// client; the handler answers 204 so no state is changed.
func TestLifecycleWritesAcceptRoomPipeline(t *testing.T) {
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	ctx := roomCtx("pg-orders-connector")
	if err := c.DeleteConnector(ctx, "pg-orders-connector"); err != nil {
		t.Fatalf("DeleteConnector: %v", err)
	}
	if err := c.StopConnector(ctx, "pg-orders-connector"); err != nil {
		t.Fatalf("StopConnector: %v", err)
	}
	if err := c.ResetSourceOffsets(ctx, "pg-orders-connector"); err != nil {
		t.Fatalf("ResetSourceOffsets: %v", err)
	}
	if err := c.AlterSourceOffsets(ctx, "pg-orders-connector", []SourceOffset{{Partition: map[string]any{"server": "pg"}}}); err != nil {
		t.Fatalf("AlterSourceOffsets: %v", err)
	}
}

// One alter entry can carry an arbitrarily large map; the encoded body is
// capped before anything is sent.
func TestAlterSourceOffsetsBodyCap(t *testing.T) {
	c := New("http://127.0.0.1:1", nil)
	offsets := []SourceOffset{{
		Partition: map[string]any{"server": "pg"},
		Offset:    map[string]any{"blob": strings.Repeat("x", maxAlterBodyBytes+1)},
	}}
	err := c.AlterSourceOffsets(unscoped(), "pg-orders-connector", offsets)
	if err == nil || !strings.Contains(err.Error(), "at most") {
		t.Fatalf("oversized alter body accepted: %v", err)
	}
	if strings.Contains(err.Error(), "xxxx") {
		t.Fatalf("error quotes the offset value: %.120v", err)
	}
}

// The capped create-error body is cut on a rune boundary, so the error text
// stays valid UTF-8.
func TestCreateConnectorErrorBodyRuneSafe(t *testing.T) {
	body := strings.Repeat("€", 300) // 900 bytes of 3-byte runes; 512 splits one
	c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte(body))
	})
	err := c.CreateConnector(unscoped(), "pg-orders-connector", map[string]string{"a": "b"})
	if err == nil {
		t.Fatal("no error")
	}
	if !utf8.ValidString(err.Error()) {
		t.Fatalf("error is not valid UTF-8: %q", err.Error())
	}
	if !strings.Contains(err.Error(), "…") {
		t.Fatalf("oversized body not truncated: %v", err)
	}
}

// IsRebalancing tells Connect's transient rebalance answers (a 409 or a 500
// whose envelope message names a rebalance) from real failures, also when the
// error is wrapped.
func TestIsRebalancing(t *testing.T) {
	tests := []struct {
		name string
		code int
		body string
		want bool
	}{
		{"500 rebalance expected", 500, `{"error_code":500,"message":"Request cannot be completed because a rebalance is expected"}`, true},
		{"409 stale config", 409, `{"error_code":409,"message":"Cannot complete request momentarily due to stale configuration (typically caused by a worker rebalance)"}`, true},
		{"500 other", 500, `{"error_code":500,"message":"Failed to read offsets"}`, false},
		{"409 exists", 409, `{"error_code":409,"message":"Connector x already exists"}`, false},
		{"400 echoing rebalance", 400, `{"error_code":400,"message":"bad value","rebalance":"x"}`, false},
		{"500 not json", 500, `rebalance`, false},
		{"503 rebalance", 503, `{"error_code":503,"message":"rebalance is expected"}`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.code)
				_, _ = io.WriteString(w, tt.body)
			})
			_, err := c.Config(unscoped(), "x")
			if err == nil {
				t.Fatal("no error")
			}
			if got := IsRebalancing(fmt.Errorf("wrapped: %w", err)); got != tt.want {
				t.Fatalf("IsRebalancing(%v) = %v, want %v", err, got, tt.want)
			}
		})
	}
	if !IsRebalancing(fmt.Errorf("x: %w", ErrRebalancing)) {
		t.Fatal("ErrRebalancing not recognised")
	}
	if IsRebalancing(nil) || IsRebalancing(errors.New("rebalance")) {
		t.Fatal("a nil or untyped error recognised")
	}
}
