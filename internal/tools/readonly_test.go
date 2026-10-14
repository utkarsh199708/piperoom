package tools

import (
	"errors"
	"testing"
)

// kindTool is stubTool with a chosen Kind.
type kindTool struct {
	stubTool
	kind Kind
}

func (k kindTool) Kind() Kind { return k.kind }

func TestRegisterKinds(t *testing.T) {
	tests := []struct {
		name         string
		readOnly     bool
		kind         Kind
		wantErr      bool
		wantReadOnly bool // error wraps ErrNotReadOnly
	}{
		{name: "plain takes read", kind: KindRead},
		{name: "plain takes room", kind: KindRoom},
		{name: "plain refuses unknown", kind: "write", wantErr: true},
		{name: "plain refuses empty", kind: "", wantErr: true},
		{name: "read-only takes read", readOnly: true, kind: KindRead},
		{name: "read-only refuses room", readOnly: true, kind: KindRoom, wantErr: true, wantReadOnly: true},
		{name: "read-only refuses unknown", readOnly: true, kind: "write", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewRegistry()
			if tt.readOnly {
				r = NewReadOnlyRegistry()
			}
			if r.ReadOnly() != tt.readOnly {
				t.Fatalf("ReadOnly() = %v, want %v", r.ReadOnly(), tt.readOnly)
			}
			err := r.Register(kindTool{stubTool{name: "t"}, tt.kind})
			if (err != nil) != tt.wantErr {
				t.Fatalf("Register err = %v, wantErr %v", err, tt.wantErr)
			}
			if got := errors.Is(err, ErrNotReadOnly); got != tt.wantReadOnly {
				t.Fatalf("errors.Is(err, ErrNotReadOnly) = %v, want %v (err %v)", got, tt.wantReadOnly, err)
			}
			if _, ok := r.Get("t"); ok == tt.wantErr {
				t.Fatalf("Get after Register: ok = %v, want %v", ok, !tt.wantErr)
			}
		})
	}
}
