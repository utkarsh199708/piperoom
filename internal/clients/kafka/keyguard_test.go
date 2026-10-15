package kafka

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"math"
	"net/http"
	"strconv"
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

// dagSchema builds D0{f0: D1{...}, f1: "D1"} down to a leaf of two null
// fields: a small text that decodes to 2^depth nodes.
func dagSchema(depth, fan int) string {
	cur := `{"type":"record","name":"D` + strconv.Itoa(depth) + `","fields":[{"name":"f0","type":"null"},{"name":"f1","type":"null"}]}`
	for i := depth - 1; i >= 0; i-- {
		name := "D" + strconv.Itoa(i)
		child := "D" + strconv.Itoa(i+1)
		fields := `{"name":"f0","type":` + cur + `}`
		for j := 1; j < fan; j++ {
			fields += `,{"name":"f` + strconv.Itoa(j) + `","type":"` + child + `"}`
		}
		cur = `{"type":"record","name":"` + name + `","fields":[` + fields + `]}`
	}
	return cur
}

func nested(depth int) string { return nestedOf(depth, `"null"`) }

func nestedOf(depth int, leaf string) string {
	cur := leaf
	for i := 0; i < depth; i++ {
		cur = `{"type":"record","name":"N` + strconv.Itoa(i) + `","fields":[{"name":"x","type":` + cur + `}]}`
	}
	return cur
}

func TestDecodeKeyGuard(t *testing.T) {
	const (
		arr       = `{"type":"array","items":"long"}`
		nestedMap = `{"type":"record","name":"K","fields":[{"name":"a","type":"long"},{"name":"m","type":{"type":"map","values":"string"}}]}`
		unionArr  = `{"type":"record","name":"K","fields":[{"name":"u","type":["null",{"type":"array","items":"string"}]}]}`
		recursive = `{"type":"record","name":"Node","fields":[{"name":"id","type":"long"},{"name":"kids","type":{"type":"array","items":"Node"}}]}`
		recNoArr  = `{"type":"record","name":"Node","fields":[{"name":"id","type":"long"},{"name":"next","type":["null","Node"]}]}`
	)
	manyFields := func(n int) string {
		var sb strings.Builder
		sb.WriteString(`{"type":"record","name":"W","fields":[`)
		for i := 0; i < n; i++ {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(`{"name":"f` + strconv.Itoa(i) + `","type":"null"}`)
		}
		sb.WriteString(`]}`)
		return sb.String()
	}
	manySymbols := func(n int) string {
		var sb strings.Builder
		sb.WriteString(`{"type":"enum","name":"E","symbols":[`)
		for i := 0; i < n; i++ {
			if i > 0 {
				sb.WriteByte(',')
			}
			sb.WriteString(`"S` + strconv.Itoa(i) + `"`)
		}
		sb.WriteString(`]}`)
		return sb.String()
	}
	huge := append(zigzag(math.MaxInt64), 0) // block count near MaxInt64, then EOF
	null1 := framed(raw(0))
	// atDecoder names refusals that only the decoder itself can raise.
	atDecoder := map[string]bool{"huge string length": true}
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
		{"huge string length", `"string"`, framed(zigzag(1 << 20)), "MaxByteSliceSize"},
		{"direct recursion", `{"type":"record","name":"Loop","fields":[{"name":"next","type":"Loop"}]}`, null1, "recursive"},
		{"mutual recursion", `{"type":"record","name":"A","fields":[{"name":"b","type":{"type":"record","name":"B","fields":[{"name":"a","type":"A"}]}}]}`, null1, "recursive"},
		{"recursion through union", recNoArr, framed(zigzag(3), raw(0)), "recursive"},
		{"fixed 65537", `{"type":"fixed","name":"F","size":65537}`, null1, "fixed size"},
		{"fixed 65537 in union", `["null",{"type":"fixed","name":"F","size":65537}]`, null1, "fixed size"},
		{"fixed 40000 referenced twice", `{"type":"record","name":"R","fields":[{"name":"a","type":{"type":"fixed","name":"F","size":40000}},{"name":"b","type":"F"}]}`, null1, "fixed size"},
		{"decimal bytes precision 77", `{"type":"bytes","logicalType":"decimal","precision":77,"scale":2}`, framed(raw(2, 1)), "decimal"},
		{"decimal bytes scale 77", `{"type":"bytes","logicalType":"decimal","precision":77,"scale":77}`, framed(raw(2, 1)), "decimal"},
		{"decimal fixed precision 90", `{"type":"fixed","name":"D","size":40,"logicalType":"decimal","precision":90,"scale":2}`, null1, "decimal"},
		{"DAG 20x2", dagSchema(20, 2), null1, "node count"},
		{"DAG 6x10", dagSchema(6, 10), null1, "node count"},
		{"nested 33 deep", nested(33), null1, "too deeply"},
		{"1000 fields", manyFields(1000), null1, "node count"},
		{"1000 enum symbols", manySymbols(1000), framed(zigzag(0)), "node count"},
		{"schema text over 64 KiB", `"null"` + strings.Repeat(" ", maxKeySchemaBytes), null1, "text too large"},
		{"unknown type", `{"type":"record","name":"R","fields":[{"name":"a","type":"Nope"}]}`, null1, "does not parse"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var n int
			c := registryWith(t, tt.schema, &n)
			if !atDecoder[tt.name] {
				c.decodeHook = func() { t.Error("refused key reached the decoder") }
			}
			start := time.Now()
			_, err := c.decodeKey(context.Background(), tt.key)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("err = %v, want it to contain %q", err, tt.wantErr)
			}
			if d := time.Since(start); d > 500*time.Millisecond {
				t.Fatalf("took %v", d)
			}
			// The refusal is cached: a second key for the same id costs no fetch.
			fetched := n
			if _, err := c.decodeKey(context.Background(), tt.key); err == nil {
				t.Fatal("second key not refused")
			}
			if n != fetched {
				t.Fatalf("second key fetched the schema again (%d -> %d)", fetched, n)
			}
		})
	}

	t.Run("redefined name cannot shadow across schema ids", func(t *testing.T) {
		const safe = `{"type":"record","name":"ShadowX","fields":[{"name":"v","type":"long"}]}`
		const hostile = `{"type":"record","name":"Top","fields":[{"name":"a","type":["null","ShadowX"]},{"name":"b","type":{"type":"record","name":"ShadowX","fields":[{"name":"arr","type":{"type":"array","items":"long"}}]}}]}`
		c := testClientWithRegistry(t, func(w http.ResponseWriter, r *http.Request) {
			schemas := map[string]string{"/schemas/ids/1": safe, "/schemas/ids/2": hostile}
			sc, ok := schemas[r.URL.Path]
			if !ok {
				http.NotFound(w, r)
				return
			}
			b, _ := json.Marshal(map[string]string{"schema": sc})
			w.Write(b)
		})
		// id 1 first, so a process-wide name cache would hold ShadowX{v}.
		if got, err := c.decodeKey(context.Background(), framed(zigzag(5))); err != nil || got != "5" {
			t.Fatalf("id 1: %q, %v", got, err)
		}
		key := []byte{0x00, 0, 0, 0, 2}
		key = append(key, framed(raw(0))[5:]...)
		key = append(key, zigzag(3)...)
		key = append(key, zigzag(1)...)
		key = append(key, zigzag(2)...)
		key = append(key, 0)
		// With a private parse cache the unresolved ShadowX reference fails the
		// parse; with the shared cache it parsed and the array was decoded.
		if got, err := c.decodeKey(context.Background(), key); err == nil {
			t.Fatalf("decoded %q, want an error", got)
		}
	})

	t.Run("parse failure cached per id", func(t *testing.T) {
		var n int
		c := registryWith(t, `{"type":"record","name":"R","fields":[{"name":"a","type":"Nope"}]}`, &n)
		for i := 0; i < 3; i++ {
			if _, err := c.decodeKey(context.Background(), framed(raw(0))); err == nil {
				t.Fatal("want refusal")
			}
		}
		if n != 1 {
			t.Fatalf("registry fetched %d times, want 1", n)
		}
	})

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
		{"five primitives", `{"type":"record","name":"P","fields":[{"name":"b","type":"boolean"},{"name":"i","type":"int"},{"name":"l","type":"long"},{"name":"f","type":"float"},{"name":"s","type":"string"}]}`,
			framed(raw(1), zigzag(3), zigzag(4), raw(0, 0, 0xc0, 0x3f), zigzag(2), raw('h', 'i')), `{"b":true,"f":1.5,"i":3,"l":4,"s":"hi"}`},
		{"nullable union", `{"type":"record","name":"U","fields":[{"name":"a","type":"long"},{"name":"u","type":["null","long"]}]}`,
			framed(zigzag(1), zigzag(1), zigzag(9)), `{"a":1,"u":9}`},
		{"enum with 10 symbols", `{"type":"enum","name":"E","symbols":["S0","S1","S2","S3","S4","S5","S6","S7","S8","S9"]}`, framed(zigzag(3)), `"S3"`},
		{"record nested 3 deep", `{"type":"record","name":"L1","fields":[{"name":"a","type":"long"},{"name":"b","type":{"type":"record","name":"L2","fields":[{"name":"c","type":{"type":"record","name":"L3","fields":[{"name":"d","type":"long"},{"name":"e","type":"long"}]}},{"name":"f","type":"long"}]}}]}`,
			framed(zigzag(1), zigzag(2), zigzag(3), zigzag(4)), `{"a":1,"b":{"c":{"d":2,"e":3},"f":4}}`},
		{"two-level record reused twice", `{"type":"record","name":"O","fields":[{"name":"a","type":{"type":"record","name":"Pair","fields":[{"name":"x","type":"long"},{"name":"y","type":"long"}]}},{"name":"b","type":"Pair"}]}`,
			framed(zigzag(1), zigzag(2), zigzag(3), zigzag(4)), `{"a":{"x":1,"y":2},"b":{"x":3,"y":4}}`},
		{"depth 16", `{"type":"record","name":"T","fields":[{"name":"a","type":"long"},{"name":"n","type":` + nestedOf(16, `"long"`) + `}]}`,
			framed(zigzag(1), zigzag(5)), `{"a":1,"n":` + strings.Repeat(`{"x":`, 16) + `5` + strings.Repeat(`}`, 16) + `}`},
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
