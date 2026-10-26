package sqlserver

import (
	"context"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/clients"
	sqlclient "piperoom/internal/clients/sqlserver"
)

// fakeWriter records the setup writer's calls; it never sees a credential.
type fakeWriter struct {
	calls  []string
	secret string
	err    error
}

func (f *fakeWriter) CreatePublication(context.Context, string, []string) error {
	f.calls = append(f.calls, "create_publication")
	return f.err
}

func (f *fakeWriter) CreateCanaryTable(context.Context, string) error {
	f.calls = append(f.calls, "create_canary_table")
	return f.err
}

func (f *fakeWriter) InsertCanary(context.Context, string, string) error {
	f.calls = append(f.calls, "insert_canary")
	return f.err
}

func (f *fakeWriter) CreateReadRole(context.Context, string, []string) (string, error) {
	f.calls = append(f.calls, "create_read_role")
	return f.secret, f.err
}

func (f *fakeWriter) AlterSystem(context.Context, string, string) (bool, error) {
	f.calls = append(f.calls, "alter_system")
	return false, f.err
}

// The executing U4a writer is what Writer wraps on the runner.
var _ clients.SetupSourceWriter = (*sqlclient.SetupWriter)(nil)

const nonce = "4f2a9c71e0b85d36a1c7e2f09b4d8a65"

func TestWriterPassesValidCalls(t *testing.T) {
	f := &fakeWriter{secret: "mssql_piperoom_cdc"}
	w, err := NewWriter(f)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	ctx := context.Background()
	if err := w.CreatePublication(ctx, "orders_mssql", []string{"dbo.orders"}); err != nil {
		t.Fatalf("CreatePublication: %v", err)
	}
	if err := w.CreateCanaryTable(ctx, DefaultCanaryTable); err != nil {
		t.Fatalf("CreateCanaryTable: %v", err)
	}
	if err := w.InsertCanary(ctx, DefaultCanaryTable, nonce); err != nil {
		t.Fatalf("InsertCanary: %v", err)
	}
	if name, err := w.CreateReadRole(ctx, "piperoom_cdc", []string{"dbo.orders"}); err != nil || name != "mssql_piperoom_cdc" {
		t.Fatalf("CreateReadRole = %q, %v", name, err)
	}
	want := "create_publication,create_canary_table,insert_canary,create_read_role"
	if got := strings.Join(f.calls, ","); got != want {
		t.Fatalf("calls = %s, want %s", got, want)
	}
}

func TestWriterRefusesBeforeAnyIO(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		call func(w *Writer) error
	}{
		{"publication name injection", func(w *Writer) error { return w.CreatePublication(ctx, "x]; DROP", []string{"dbo.orders"}) }},
		{"publication no tables", func(w *Writer) error { return w.CreatePublication(ctx, "p", nil) }},
		{"capture instance too long", func(w *Writer) error {
			return w.CreatePublication(ctx, strings.Repeat("p", 80), []string{"dbo.orders", "dbo." + strings.Repeat("t", 20)})
		}},
		{"canary three parts", func(w *Writer) error { return w.CreateCanaryTable(ctx, "orders.dbo.c") }},
		{"canary bad nonce", func(w *Writer) error { return w.InsertCanary(ctx, DefaultCanaryTable, "ABC") }},
		{"canary bad table", func(w *Writer) error { return w.InsertCanary(ctx, "c", nonce) }},
		{"sa login", func(w *Writer) error { _, err := w.CreateReadRole(ctx, "sa", []string{"dbo.orders"}); return err }},
		{"guest login", func(w *Writer) error { _, err := w.CreateReadRole(ctx, "guest", []string{"dbo.orders"}); return err }},
		{"cdc login", func(w *Writer) error { _, err := w.CreateReadRole(ctx, "cdc", []string{"dbo.orders"}); return err }},
		{"fixed role login", func(w *Writer) error {
			_, err := w.CreateReadRole(ctx, "db_datareader", []string{"dbo.orders"})
			return err
		}},
		{"login no tables", func(w *Writer) error { _, err := w.CreateReadRole(ctx, "ro", nil); return err }},
		{"alter system", func(w *Writer) error { _, err := w.AlterSystem(ctx, "wal_level", "logical"); return err }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeWriter{secret: "s"}
			w, _ := NewWriter(f)
			if err := tc.call(w); err == nil {
				t.Fatal("accepted")
			}
			if len(f.calls) != 0 {
				t.Fatalf("the wrapped writer was called: %v", f.calls)
			}
		})
	}
	w, _ := NewWriter(&fakeWriter{})
	if _, err := w.AlterSystem(ctx, "x", "y"); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("AlterSystem: %v, want clients.ErrNotSupported", err)
	}
}

func TestWriterErrors(t *testing.T) {
	if _, err := NewWriter(nil); !errors.Is(err, ErrNoWriter) {
		t.Fatalf("NewWriter(nil): %v", err)
	}
	var nilW *Writer
	if err := nilW.CreateCanaryTable(context.Background(), DefaultCanaryTable); !errors.Is(err, ErrNoWriter) {
		t.Fatalf("nil writer: %v", err)
	}
	down := errors.New("connection refused")
	w, _ := NewWriter(&fakeWriter{err: down})
	if err := w.CreatePublication(context.Background(), "p", []string{"dbo.orders"}); !errors.Is(err, down) {
		t.Fatalf("CreatePublication did not wrap the writer's error: %v", err)
	}
	if _, err := w.CreateReadRole(context.Background(), "ro", []string{"dbo.orders"}); !errors.Is(err, down) {
		t.Fatalf("CreateReadRole did not wrap the writer's error: %v", err)
	}
	w, _ = NewWriter(&fakeWriter{secret: "../escape"})
	if _, err := w.CreateReadRole(context.Background(), "ro", []string{"dbo.orders"}); !errors.Is(err, ErrParams) {
		t.Fatalf("an unusable secret name was accepted: %v", err)
	}
}
