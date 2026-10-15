package kafka

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"math"
	"net/http"
	"strings"
	"testing"
	"time"
)

// registryWith serves schema as id 1 and counts fetches.
func registryWith(t *testing.T, schema string, fetches *int) *Client {
	t.Helper()
	return testClientWithRegistry(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/schemas/ids/1" {
			http.NotFound(w, r)
			return
		}
		*fetches++
		b, _ := json.Marshal(map[string]string{"schema": schema})
		w.Write(b)
	})
}

func raw(bs ...byte) []byte { return bs }

// framed builds a Confluent-framed key (magic byte, schema id 1) from parts.
func framed(parts ...[]byte) []byte {
	k := []byte{0x00, 0, 0, 0, 1}
	for _, p := range parts {
		k = append(k, p...)
	}
	return k
}

func zigzag(n int64) []byte {
	return binary.AppendUvarint(nil, uint64(n<<1)^uint64(n>>63))
}

func TestDecodeKeyGuard(t *testing.T) {
	const (
		arr       = `{"type":"array","items":"long"}`
		nestedMap = `{"type":"record","name":"K","fields":[{"name":"a","type":"long"},{"name":"m","type":{"type":"map","values":"string"}}]}`
		unionArr  = `{"type":"record","name":"K","fields":[{"name":"u","type":["null",{"type":"array","items":"string"}]}]}`
		recursive = `{"type":"record","name":"Node","fields":[{"name":"id","type":"long"},{"name":"kids","type":{"type":"array","items":"Node"}}]}`
		recNoArr  = `{"type":"record","name":"Node","fields":[{"name":"id","type":"long"},{"name":"next","type":["null","Node"]}]}`
	)
	huge := append(zigzag(math.MaxInt64), 0) // block count near MaxInt64, then EOF
	tests := []struct {
		name    string
		schema  string
		key     []byte
		wantErr string
	}{
		{"top-level array", arr, framed(zigzag(1), zigzag(5), raw(0)), "array/map"},
		{"map in record", nestedMap, framed(zigzag(1), raw(0)), "array/map"},
		{"array in union", unionArr, framed(zigzag(1), zigzag(1), raw(2, 'x', 0)), "array/map"},
		{"recursive record with array", recursive, framed(zigzag(1), raw(0)), "array/map"},
		{"GO-2026-5046 shape", arr, framed(huge), "array/map"},
		{"key over 64 KiB", `"string"`, framed(make([]byte, maxKeyBytes)), "too large"},
		{"huge string length", `"string"`, framed(zigzag(1 << 20)), ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var n int
			c := registryWith(t, tt.schema, &n)
			start := time.Now()
			_, err := c.decodeKey(context.Background(), tt.key)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err = %v, want it to contain %q", err, tt.wantErr)
			}
			if d := time.Since(start); d > 500*time.Millisecond {
				t.Fatalf("took %v", d)
			}
		})
	}

	t.Run("refusal verdict cached per id", func(t *testing.T) {
		var n int
		c := registryWith(t, arr, &n)
		for i := 0; i < 3; i++ {
			if _, err := c.decodeKey(context.Background(), framed(zigzag(0))); err == nil {
				t.Fatal("want refusal")
			}
		}
		if n != 1 {
			t.Fatalf("registry fetched %d times, want 1", n)
		}
	})

	good := []struct {
		name, schema string
		key          []byte
		want         string
	}{
		{"record string+long", `{"type":"record","name":"K","fields":[{"name":"s","type":"string"},{"name":"n","type":"long"}]}`,
			framed(zigzag(2), raw('a', 'b'), zigzag(7)), `{"n":7,"s":"ab"}`},
		{"single long field", `{"type":"record","name":"K","fields":[{"name":"id","type":"long"}]}`, framed(zigzag(42)), "42"},
		{"recursive without array", recNoArr, framed(zigzag(3), raw(0)), `{"id":3,"next":null}`},
	}
	for _, tt := range good {
		t.Run("ok "+tt.name, func(t *testing.T) {
			var n int
			c := registryWith(t, tt.schema, &n)
			got, err := c.decodeKey(context.Background(), tt.key)
			if err != nil || got != tt.want {
				t.Fatalf("decodeKey = %q, %v; want %q", got, err, tt.want)
			}
		})
	}

	t.Run("plain string key unchanged", func(t *testing.T) {
		var n int
		c := registryWith(t, `"string"`, &n)
		got, err := c.decodeKey(context.Background(), []byte("order-17"))
		if err != nil || got != "order-17" {
			t.Fatalf("decodeKey = %q, %v", got, err)
		}
		if n != 0 {
			t.Fatal("registry consulted for a raw key")
		}
	})

	t.Run("decoder panic becomes error", func(t *testing.T) {
		var n int
		c := registryWith(t, `"string"`, &n)
		c.decodeHook = func() { panic("boom") }
		_, err := c.decodeKey(context.Background(), framed(zigzag(0)))
		if err == nil || !strings.Contains(err.Error(), "panicked: boom") {
			t.Fatalf("err = %v", err)
		}
	})
}
