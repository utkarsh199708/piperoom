package oracle

import (
	"context"
	"errors"
	"testing"

	"piperoom/internal/clients"
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
	if f.err != nil {
		return "", f.err
	}
	if f.secret == "" {
		return "source_read_password", nil
	}
	return f.secret, nil
}

func (f *fakeWriter) AlterSystem(context.Context, string, string) (bool, error) {
	f.calls = append(f.calls, "alter_system")
	return false, f.err
}

const nonce = "0123456789abcdef0123456789abcdef"

func TestNewWriterRefusesNil(t *testing.T) {
	if _, err := NewWriter(nil); !errors.Is(err, ErrNoWriter) {
		t.Fatalf("NewWriter(nil) = %v", err)
	}
}

func TestWriterDelegates(t *testing.T) {
	f := &fakeWriter{}
	w, err := NewWriter(f)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if err := w.CreatePublication(ctx, "ignored", []string{"PIPEROOM.ORDERS"}); err != nil {
		t.Fatal(err)
	}
	if err := w.CreateCanaryTable(ctx, "PIPEROOM.PIPEROOM_CANARY"); err != nil {
		t.Fatal(err)
	}
	if err := w.InsertCanary(ctx, "PIPEROOM.PIPEROOM_CANARY", nonce); err != nil {
		t.Fatal(err)
	}
	name, err := w.CreateReadRole(ctx, "DBZUSER", []string{"PIPEROOM.ORDERS"})
	if err != nil || name != "source_read_password" {
		t.Fatalf("CreateReadRole = %q, %v", name, err)
	}
	want := []string{"create_publication", "create_canary_table", "insert_canary", "create_read_role"}
	if len(f.calls) != len(want) {
		t.Fatalf("calls = %v, want %v", f.calls, want)
	}
	for i := range want {
		if f.calls[i] != want[i] {
			t.Fatalf("calls = %v, want %v", f.calls, want)
		}
	}
}

func TestWriterWrapsErrors(t *testing.T) {
	boom := errors.New("ORA-01031: insufficient privileges")
	w, _ := NewWriter(&fakeWriter{err: boom})
	if err := w.CreatePublication(context.Background(), "", []string{"PIPEROOM.ORDERS"}); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if _, err := w.CreateReadRole(context.Background(), "DBZUSER", []string{"PIPEROOM.ORDERS"}); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
}

func TestWriterRefusesBeforeIO(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		call func(w *Writer) error
	}{
		{"publication without tables", func(w *Writer) error { return w.CreatePublication(ctx, "", nil) }},
		{"publication lower-case table", func(w *Writer) error { return w.CreatePublication(ctx, "", []string{"piperoom.orders"}) }},
		{"canary injection", func(w *Writer) error { return w.CreateCanaryTable(ctx, `PIPEROOM.X"; DROP TABLE Y`) }},
		{"canary without owner", func(w *Writer) error { return w.CreateCanaryTable(ctx, "CANARY") }},
		{"insert bad nonce", func(w *Writer) error { return w.InsertCanary(ctx, "PIPEROOM.C", "' OR 1=1 --") }},
		{"insert upper-case nonce", func(w *Writer) error { return w.InsertCanary(ctx, "PIPEROOM.C", "0123456789ABCDEF0123456789ABCDEF") }},
		{"read role SYS", func(w *Writer) error { _, err := w.CreateReadRole(ctx, "SYS", []string{"PIPEROOM.ORDERS"}); return err }},
		{"read role injection", func(w *Writer) error {
			_, err := w.CreateReadRole(ctx, `X" IDENTIFIED BY "Y`, []string{"PIPEROOM.ORDERS"})
			return err
		}},
		{"read role no tables", func(w *Writer) error { _, err := w.CreateReadRole(ctx, "DBZUSER", nil); return err }},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakeWriter{}
			w, _ := NewWriter(f)
			if err := tc.call(w); err == nil {
				t.Fatal("accepted")
			}
			if len(f.calls) != 0 {
				t.Fatalf("the wrapped writer was called: %v", f.calls)
			}
		})
	}
}

func TestWriterRefusesAnUnusableSecretName(t *testing.T) {
	w, _ := NewWriter(&fakeWriter{secret: "../x"})
	if _, err := w.CreateReadRole(context.Background(), "DBZUSER", []string{"PIPEROOM.ORDERS"}); !errors.Is(err, ErrParams) {
		t.Fatalf("err = %v", err)
	}
}

// TestAlterSystemNeverReachesTheWriter: archive log mode is a DBA's restart
// (ErrNotSupported with the steps); every other name is ErrParams.
func TestAlterSystemNeverReachesTheWriter(t *testing.T) {
	f := &fakeWriter{}
	w, _ := NewWriter(f)
	if _, err := w.AlterSystem(context.Background(), "archive_log", "on"); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("archive_log: %v", err)
	}
	if _, err := w.AlterSystem(context.Background(), "wal_level", "logical"); !errors.Is(err, ErrParams) {
		t.Fatalf("wal_level: %v", err)
	}
	if len(f.calls) != 0 {
		t.Fatalf("the wrapped writer was called: %v", f.calls)
	}
}

func TestNilWriter(t *testing.T) {
	var w *Writer
	if err := w.CreateCanaryTable(context.Background(), "PIPEROOM.C"); !errors.Is(err, ErrNoWriter) {
		t.Fatalf("err = %v", err)
	}
}
