package oracle

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

type fakeSetupWriter struct {
	calls      []string
	secretName string
	pending    bool
	err        error
}

func (f *fakeSetupWriter) CreatePublication(context.Context, string, []string) error {
	f.calls = append(f.calls, "publication")
	return f.err
}
func (f *fakeSetupWriter) CreateCanaryTable(context.Context, string) error {
	f.calls = append(f.calls, "canary")
	return f.err
}
func (f *fakeSetupWriter) InsertCanary(context.Context, string, string) error {
	f.calls = append(f.calls, "insert")
	return f.err
}
func (f *fakeSetupWriter) CreateReadRole(context.Context, string, []string) (string, error) {
	f.calls = append(f.calls, "role")
	return f.secretName, f.err
}
func (f *fakeSetupWriter) AlterSystem(context.Context, string, string) (bool, error) {
	f.calls = append(f.calls, "alter")
	return f.pending, f.err
}

func TestWriterValidatesAndDelegates(t *testing.T) {
	cause := errors.New("writer failed")
	tests := []struct {
		name     string
		run      func(*Writer, *fakeSetupWriter) error
		wantCall string
	}{
		{
			name: "publication",
			run: func(w *Writer, _ *fakeSetupWriter) error {
				return w.CreatePublication(context.Background(), "", []string{"PIPEROOM.ORDERS"})
			},
			wantCall: "publication",
		},
		{
			name: "canary",
			run: func(w *Writer, _ *fakeSetupWriter) error {
				return w.CreateCanaryTable(context.Background(), "PIPEROOM.CANARY")
			},
			wantCall: "canary",
		},
		{
			name: "insert",
			run: func(w *Writer, _ *fakeSetupWriter) error {
				return w.InsertCanary(context.Background(), "PIPEROOM.CANARY", "0123456789abcdef0123456789abcdef")
			},
			wantCall: "insert",
		},
		{
			name: "read role",
			run: func(w *Writer, _ *fakeSetupWriter) error {
				_, err := w.CreateReadRole(context.Background(), "C##DBZUSER", []string{"PIPEROOM.ORDERS"})
				return err
			},
			wantCall: "role",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inner := &fakeSetupWriter{secretName: "source_read_password", err: cause}
			w, err := NewWriter(inner)
			if err != nil {
				t.Fatalf("NewWriter: %v", err)
			}
			if err := tt.run(w, inner); !errors.Is(err, cause) {
				t.Fatalf("delegated error = %v, want underlying error", err)
			}
			if !reflect.DeepEqual(inner.calls, []string{tt.wantCall}) {
				t.Fatalf("delegated calls = %v, want [%s]", inner.calls, tt.wantCall)
			}
		})
	}
}

func TestWriterRejectsBadInputBeforeDelegation(t *testing.T) {
	tests := []struct {
		name string
		run  func(*Writer) error
	}{
		{"bad publication table", func(w *Writer) error { return w.CreatePublication(context.Background(), "", []string{"X.Y;DROP"}) }},
		{"bad canary table", func(w *Writer) error { return w.CreateCanaryTable(context.Background(), "X.Y.Z") }},
		{"bad canary nonce", func(w *Writer) error { return w.InsertCanary(context.Background(), "X.CANARY", "password") }},
		{"bad LogMiner user", func(w *Writer) error {
			_, err := w.CreateReadRole(context.Background(), "bad;user", []string{"X.T"})
			return err
		}},
		{"bad table for role", func(w *Writer) error {
			_, err := w.CreateReadRole(context.Background(), "DBZ", []string{"X.T;DROP"})
			return err
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inner := &fakeSetupWriter{}
			w, err := NewWriter(inner)
			if err != nil {
				t.Fatalf("NewWriter: %v", err)
			}
			if err := tt.run(w); !errors.Is(err, ErrIdentifier) && !errors.Is(err, ErrParams) {
				t.Fatalf("bad input error = %v", err)
			}
			if len(inner.calls) != 0 {
				t.Fatalf("bad input reached writer: %v", inner.calls)
			}
		})
	}
}

func TestWriterReadRoleReturnsOnlyValidSecretName(t *testing.T) {
	tests := []struct {
		name    string
		secret  string
		wantErr bool
	}{
		{"valid", "source_read_password", false},
		{"empty", "", true},
		{"spaces", "password secret", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			inner := &fakeSetupWriter{secretName: tt.secret}
			w, err := NewWriter(inner)
			if err != nil {
				t.Fatalf("NewWriter: %v", err)
			}
			got, err := w.CreateReadRole(context.Background(), "DBZ", []string{"X.T"})
			if tt.wantErr {
				if !errors.Is(err, ErrParams) {
					t.Fatalf("CreateReadRole error = %v, want ErrParams", err)
				}
				if got != "" {
					t.Fatalf("secret result = %q on error", got)
				}
			} else if got != tt.secret || err != nil {
				t.Fatalf("CreateReadRole = %q, %v", got, err)
			}
		})
	}
}

func TestWriterAlterSystemOnlyAllowsArchiveLog(t *testing.T) {
	inner := &fakeSetupWriter{pending: true}
	w, err := NewWriter(inner)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}
	if _, err := w.AlterSystem(context.Background(), "processes", "100"); !errors.Is(err, ErrParams) {
		t.Fatalf("AlterSystem bad setting error = %v, want ErrParams", err)
	}
	if len(inner.calls) != 0 {
		t.Fatalf("bad setting reached writer: %v", inner.calls)
	}
	pending, err := w.AlterSystem(context.Background(), "archive_log", "ARCHIVELOG")
	if err != nil || !pending {
		t.Fatalf("AlterSystem archive_log = %v, %v", pending, err)
	}
	if !reflect.DeepEqual(inner.calls, []string{"alter"}) {
		t.Fatalf("delegated calls = %v", inner.calls)
	}
}

func TestWriterRejectsMissingWriter(t *testing.T) {
	if _, err := NewWriter(nil); !errors.Is(err, ErrNoWriter) {
		t.Fatalf("NewWriter(nil) error = %v", err)
	}
	var w *Writer
	if err := w.CreateCanaryTable(context.Background(), "X.CANARY"); !errors.Is(err, ErrNoWriter) {
		t.Fatalf("nil Writer call = %v", err)
	}
}
