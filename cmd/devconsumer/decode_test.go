package main

import (
	"context"
	"errors"
	"math"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
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
		// Captured from the dev stack: 4969 is the snapshot row's amount as an
		// Avro int; 4651 is the exact decimal amount of the post-ALTER insert
		// (Avro bytes + decimal(12,2), scale 2 -> "4651" after trailing zeros).
		{"value-schema2", "r", 1, "4969"},
		{"value-schema3", "c", 103, "4651"},
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
			if c.After.Amount != tt.wantAmount {
				t.Fatalf("amount = %q, want %q", c.After.Amount, tt.wantAmount)
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
		name    string
		in      any
		want    string
		wantErr bool
	}{
		// int: amount before the schema change
		{"int32", int32(500), "500", false},
		{"int64", int64(-3), "-3", false},
		// decimal bytes: amount after the schema change (*big.Rat)
		{"decimal bytes", big.NewRat(124950, 100), "1249.5", false},
		{"decimal bytes 2dp", big.NewRat(9999, 100), "99.99", false},
		{"decimal bytes whole", big.NewRat(100, 1), "100", false},
		{"decimal bytes value", *big.NewRat(5, 2), "2.5", false},
		// float64: decimal.handling.mode=double
		{"float64", float64(1249.5), "1249.5", false},
		{"float64 whole", float64(100), "100", false},
		{"float64 small", float64(0.1), "0.1", false},
		{"float64 nan", math.NaN(), "", true},
		{"float64 inf", math.Inf(1), "", true},
		// string: decimal.handling.mode=string
		{"string", "1249.50", "1249.5", false},
		{"string whole", "100", "100", false},
		{"string padded", " 7.25 ", "7.25", false},
		{"string exponent", "1e3", "1000", false},
		{"string not a number", "oops", "", true},
		{"string fraction", "1/3", "", true},
		{"string empty", "", "", true},
		// other types
		{"bool", true, "", true},
		{"nil", nil, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := asDecimal(tt.in)
			if (err != nil) != tt.wantErr || got != tt.want {
				t.Errorf("asDecimal(%v) = %q, %v; want %q (err %v)", tt.in, got, err, tt.want, tt.wantErr)
			}
		})
	}
}

func TestToChange(t *testing.T) {
	key := func(id any) any { return map[string]any{"id": id} }
	env := func(op string, after any) any { return map[string]any{"op": op, "after": after} }
	tests := []struct {
		name         string
		key          any
		value        any
		wantOp       string
		wantID       int64
		wantErr      string // substring; empty means success
		wantNilAfter bool
	}{
		{
			name:         "delete",
			key:          key(int64(5)),
			value:        env("d", nil),
			wantOp:       "d",
			wantID:       5,
			wantNilAfter: true,
		},
		{
			name:    "key not a record",
			key:     "not a record",
			value:   env("c", map[string]any{"id": int64(1)}),
			wantErr: "key is string, want record",
		},
		{
			name:    "key id not an integer",
			key:     key("abc"),
			value:   env("c", map[string]any{"id": int64(1)}),
			wantErr: "key id",
		},
		{
			name:    "create without after",
			key:     key(int64(1)),
			value:   env("c", nil),
			wantErr: `op "c" without after`,
		},
		{
			name: "amount of the wrong type",
			key:  key(int64(1)),
			value: env("u", map[string]any{
				"id":          int64(1),
				"customer_id": int64(2),
				"amount":      "oops",
			}),
			wantErr: "amount",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c, err := toChange(tt.key, tt.value)
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("toChange = %+v, err = %v; want error containing %q", c, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("toChange err = %v", err)
			}
			if c.Op != tt.wantOp || c.ID != tt.wantID {
				t.Fatalf("got op=%q id=%d, want op=%q id=%d", c.Op, c.ID, tt.wantOp, tt.wantID)
			}
			if tt.wantNilAfter {
				if c.After != nil {
					t.Fatalf("After = %+v, want nil", c.After)
				}
			} else if c.After == nil {
				t.Fatal("After = nil, want non-nil")
			}
		})
	}
}

func TestSchemaCacheGet(t *testing.T) {
	t.Run("registry down", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		srv.Close()
		c := newSchemaCache(srv.URL)
		if _, err := c.get(context.Background(), 1); err == nil {
			t.Fatal("get = nil error, want connection error")
		}
	})

	t.Run("http 500", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
		}))
		defer srv.Close()
		c := newSchemaCache(srv.URL)
		_, err := c.get(context.Background(), 1)
		if err == nil || !strings.Contains(err.Error(), "HTTP 500") {
			t.Fatalf("err = %v, want HTTP 500", err)
		}
	})

	t.Run("invalid json body", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("not json"))
		}))
		defer srv.Close()
		c := newSchemaCache(srv.URL)
		_, err := c.get(context.Background(), 1)
		if err == nil || !strings.Contains(err.Error(), "decode schema 1 response") {
			t.Fatalf("err = %v, want decode schema response", err)
		}
	})

	t.Run("invalid avro schema", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"schema":"not avro"}`))
		}))
		defer srv.Close()
		c := newSchemaCache(srv.URL)
		_, err := c.get(context.Background(), 1)
		if err == nil || !strings.Contains(err.Error(), "parse schema 1") {
			t.Fatalf("err = %v, want parse schema error", err)
		}
	})

	t.Run("caches by id", func(t *testing.T) {
		var mu sync.Mutex
		var requests int
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			mu.Lock()
			requests++
			mu.Unlock()
			b, err := os.ReadFile(filepath.Join("testdata", "schema-2.json"))
			if err != nil {
				http.NotFound(w, r)
				return
			}
			_, _ = w.Write(b)
		}))
		defer srv.Close()
		c := newSchemaCache(srv.URL)
		if _, err := c.get(context.Background(), 2); err != nil {
			t.Fatalf("first get: %v", err)
		}
		if _, err := c.get(context.Background(), 2); err != nil {
			t.Fatalf("second get: %v", err)
		}
		mu.Lock()
		defer mu.Unlock()
		if requests != 1 {
			t.Fatalf("requests = %d, want 1 (second get must hit the cache)", requests)
		}
	})
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
