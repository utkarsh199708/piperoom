package main

import (
	"context"
	"errors"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// registry serves testdata/schema-<id>.json, captured from the dev stack.
func registry(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/schemas/ids/")
		b, err := os.ReadFile(filepath.Join("testdata", "schema-"+id+".json"))
		if err != nil {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(b)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func readBin(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// Real records captured from pg.public.orders: schema 2 is before the
// amount type change (Avro int), schema 3 is after it (bytes/decimal).
func TestDecodeRealRecords(t *testing.T) {
	cache := newSchemaCache(registry(t).URL)
	tests := []struct {
		file       string
		wantOp     string
		wantID     int64
		wantAmount string
	}{
		{"value-schema2", "r", 1, ""},    // snapshot row; amount is a whole number
		{"value-schema3", "c", 103, "*"}, // an insert after the ALTER; amount is a decimal
	}
	for _, tt := range tests {
		t.Run(tt.file, func(t *testing.T) {
			ctx := context.Background()
			key, err := cache.decode(ctx, readBin(t, tt.file+".key.bin"))
			if err != nil {
				t.Fatalf("decode key: %v", err)
			}
			val, err := cache.decode(ctx, readBin(t, tt.file+".value.bin"))
			if err != nil {
				t.Fatalf("decode value: %v", err)
			}
			c, err := toChange(key, val)
			if err != nil {
				t.Fatalf("toChange: %v", err)
			}
			if c.Op != tt.wantOp || c.ID != tt.wantID {
				t.Fatalf("got op=%q id=%d, want op=%q id=%d", c.Op, c.ID, tt.wantOp, tt.wantID)
			}
			if c.After == nil || c.After.ID != tt.wantID {
				t.Fatalf("after = %+v", c.After)
			}
			if tt.wantAmount == "*" {
				if _, ok := new(big.Rat).SetString(c.After.Amount); !ok {
					t.Fatalf("amount = %q, want a decimal", c.After.Amount)
				}
			}
			if tt.wantAmount == "" && strings.Contains(c.After.Amount, ".") {
				t.Fatalf("amount = %q, want a whole number", c.After.Amount)
			}
			if c.After.UpdatedAt.IsZero() {
				t.Fatal("updated_at not parsed")
			}
		})
	}
}

func TestDecodeUnknownSchemaID(t *testing.T) {
	cache := newSchemaCache(registry(t).URL)
	_, err := cache.decode(context.Background(), []byte{0, 0, 0, 0, 99, 1})
	if err == nil || !strings.Contains(err.Error(), "HTTP 404") {
		t.Fatalf("err = %v, want HTTP 404", err)
	}
}

func TestSplitWire(t *testing.T) {
	tests := []struct {
		name    string
		in      []byte
		wantID  int
		wantErr bool
	}{
		{"ok", []byte{0, 0, 0, 1, 2, 7, 8}, 258, false},
		{"bad magic", []byte{1, 0, 0, 0, 1, 7}, 0, true},
		{"too short", []byte{0, 0, 1}, 0, true},
		{"nil", nil, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			id, body, err := splitWire(tt.in)
			if tt.wantErr {
				if !errors.Is(err, errNoMagic) {
					t.Fatalf("err = %v, want errNoMagic", err)
				}
				return
			}
			if err != nil || id != tt.wantID || len(body) != len(tt.in)-5 {
				t.Fatalf("got id=%d body=%v err=%v", id, body, err)
			}
		})
	}
}

func TestAsDecimal(t *testing.T) {
	tests := []struct {
		in      any
		want    string
		wantErr bool
	}{
		{int32(500), "500", false},
		{int64(-3), "-3", false},
		{big.NewRat(124950, 100), "1249.5", false},
		{big.NewRat(9999, 100), "99.99", false},
		{big.NewRat(100, 1), "100", false},
		{"1.0", "", true},
	}
	for _, tt := range tests {
		got, err := asDecimal(tt.in)
		if (err != nil) != tt.wantErr || got != tt.want {
			t.Errorf("asDecimal(%v) = %q, %v; want %q (err %v)", tt.in, got, err, tt.want, tt.wantErr)
		}
	}
}

func TestUnwrap(t *testing.T) {
	rec := map[string]any{"id": int64(1)}
	tests := []struct {
		name string
		in   any
		want any
	}{
		{"named union", map[string]any{"pg.public.orders.Value": rec}, rec},
		{"primitive union", map[string]any{"string": "x"}, "x"},
		{"single-field record stays", rec, rec},
		{"plain value", "x", "x"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := unwrap(tt.in)
			if m, ok := tt.want.(map[string]any); ok {
				gm, ok := got.(map[string]any)
				if !ok || gm["id"] != m["id"] {
					t.Fatalf("got %v, want %v", got, tt.want)
				}
				return
			}
			if got != tt.want {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}
