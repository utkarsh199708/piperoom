package mongodb

import (
	"context"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/clients"
)

// recWriter records the calls that reach the executing writer.
type recWriter struct {
	calls  []string
	secret string
	err    error
}

func (w *recWriter) CreatePublication(context.Context, string, []string) error {
	w.calls = append(w.calls, "publication")
	return w.err
}

func (w *recWriter) CreateCanaryTable(_ context.Context, table string) error {
	w.calls = append(w.calls, "canary "+table)
	return w.err
}

func (w *recWriter) InsertCanary(_ context.Context, table, nonce string) error {
	w.calls = append(w.calls, "insert "+table+" "+nonce)
	return w.err
}

func (w *recWriter) CreateReadRole(_ context.Context, role string, tables []string) (string, error) {
	w.calls = append(w.calls, "user "+role+" "+strings.Join(tables, ","))
	return w.secret, w.err
}

func (w *recWriter) AlterSystem(context.Context, string, string) (bool, error) {
	w.calls = append(w.calls, "alter")
	return false, w.err
}

const nonce = "0123456789abcdef0123456789abcdef"

func TestWriterDelegatesValidCalls(t *testing.T) {
	in := &recWriter{secret: "source_read_password"}
	w, err := NewWriter(in)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := w.CreateCanaryTable(ctx, "inventory.piperoom_canary"); err != nil {
		t.Fatal(err)
	}
	if err := w.InsertCanary(ctx, "inventory.piperoom_canary", nonce); err != nil {
		t.Fatal(err)
	}
	name, err := w.CreateReadRole(ctx, "piperoom_cdc", []string{"inventory.orders"})
	if err != nil || name != "source_read_password" {
		t.Fatalf("CreateReadRole = %q, %v", name, err)
	}
	want := []string{"canary inventory.piperoom_canary", "insert inventory.piperoom_canary " + nonce, "user piperoom_cdc inventory.orders"}
	if strings.Join(in.calls, "|") != strings.Join(want, "|") {
		t.Fatalf("calls = %q, want %q", in.calls, want)
	}
}

func TestWriterRefusesBeforeIO(t *testing.T) {
	in := &recWriter{secret: "s"}
	w, _ := NewWriter(in)
	ctx := context.Background()
	for name, call := range map[string]func() error{
		"canary in a system db": func() error { return w.CreateCanaryTable(ctx, "local.piperoom_canary") },
		"canary not namespaced": func() error { return w.CreateCanaryTable(ctx, "piperoom_canary") },
		"bad nonce":             func() error { return w.InsertCanary(ctx, "inventory.piperoom_canary", "NOT-A-NONCE") },
		"reserved user": func() error {
			_, err := w.CreateReadRole(ctx, "root", []string{"inventory.orders"})
			return err
		},
		"no collections": func() error {
			_, err := w.CreateReadRole(ctx, "piperoom_cdc", nil)
			return err
		},
		"publication": func() error { return w.CreatePublication(ctx, "x", []string{"inventory.orders"}) },
		"alter system": func() error {
			_, err := w.AlterSystem(ctx, "oplogSizeMB", "1024")
			return err
		},
	} {
		if err := call(); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if len(in.calls) != 0 {
		t.Fatalf("refused calls reached the writer: %q", in.calls)
	}
	if err := w.CreatePublication(ctx, "x", nil); !errors.Is(err, clients.ErrNotSupported) {
		t.Errorf("CreatePublication = %v, want ErrNotSupported", err)
	}
	if _, err := w.AlterSystem(ctx, "x", "y"); !errors.Is(err, clients.ErrNotSupported) {
		t.Errorf("AlterSystem = %v, want ErrNotSupported", err)
	}
}

func TestWriterErrors(t *testing.T) {
	if _, err := NewWriter(nil); !errors.Is(err, ErrNoWriter) {
		t.Fatalf("NewWriter(nil) = %v", err)
	}
	var nilW *Writer
	if err := nilW.CreateCanaryTable(context.Background(), "inventory.piperoom_canary"); !errors.Is(err, ErrNoWriter) {
		t.Fatalf("nil writer = %v", err)
	}
	boom := errors.New("not primary")
	w, _ := NewWriter(&recWriter{err: boom})
	if err := w.CreateCanaryTable(context.Background(), "inventory.piperoom_canary"); !errors.Is(err, boom) {
		t.Fatalf("wrapped error lost: %v", err)
	}
	w, _ = NewWriter(&recWriter{secret: "bad name with spaces"})
	if _, err := w.CreateReadRole(context.Background(), "piperoom_cdc", []string{"inventory.orders"}); !errors.Is(err, ErrParams) {
		t.Fatalf("an unusable secret name was accepted: %v", err)
	}
}

func TestConnectorConfig(t *testing.T) {
	cfg, err := ConnectorConfig(ConnectorOptions{
		ConnectionStringSecret: "mongo_connection_string",
		TopicPrefix:            "mongo",
		Collections:            []string{"inventory.orders", "inventory.piperoom_canary"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"connector.class":           ConnectorClass,
		"mongodb.connection.string": "${secret:mongo_connection_string}",
		"topic.prefix":              "mongo",
		"collection.include.list":   "inventory.orders,inventory.piperoom_canary",
	}
	if len(cfg) != len(want) {
		t.Fatalf("config = %v, want %v", cfg, want)
	}
	for k, v := range want {
		if cfg[k] != v {
			t.Errorf("%s = %q, want %q", k, cfg[k], v)
		}
	}
	for k, v := range cfg {
		if strings.Contains(v, "mongodb://") || strings.Contains(v, "mongodb+srv://") {
			t.Errorf("%s carries a connection string literal: %q", k, v)
		}
	}

	for name, o := range map[string]ConnectorOptions{
		"no secret":         {TopicPrefix: "mongo", Collections: []string{"inventory.orders"}},
		"secret is a URL":   {ConnectionStringSecret: "mongodb://u:p@h", TopicPrefix: "mongo", Collections: []string{"inventory.orders"}},
		"bad topic prefix":  {ConnectionStringSecret: "s", TopicPrefix: "mo ngo", Collections: []string{"inventory.orders"}},
		"no collections":    {ConnectionStringSecret: "s", TopicPrefix: "mongo"},
		"system collection": {ConnectionStringSecret: "s", TopicPrefix: "mongo", Collections: []string{"config.system"}},
	} {
		if _, err := ConnectorConfig(o); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
