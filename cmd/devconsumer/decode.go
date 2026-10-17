package main

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hamba/avro/v2"
)

// errNoMagic means the bytes are not in the Confluent wire format.
var errNoMagic = errors.New("not confluent wire format")

// splitWire splits Confluent wire-format bytes into schema ID and Avro body:
// byte 0 is magic 0, bytes 1-4 are the big-endian schema ID.
func splitWire(b []byte) (int, []byte, error) {
	if len(b) < 5 || b[0] != 0 {
		return 0, nil, errNoMagic
	}
	return int(binary.BigEndian.Uint32(b[1:5])), b[5:], nil
}

// schemaCache fetches writer schemas from Schema Registry by ID and keeps them.
// Schema IDs are immutable, so entries never expire.
type schemaCache struct {
	baseURL string
	http    *http.Client
	mu      sync.Mutex
	byID    map[int]avro.Schema
}

func newSchemaCache(baseURL string) *schemaCache {
	return &schemaCache{
		baseURL: strings.TrimRight(baseURL, "/"),
		http:    &http.Client{Timeout: 10 * time.Second},
		byID:    map[int]avro.Schema{},
	}
}

func (c *schemaCache) get(ctx context.Context, id int) (avro.Schema, error) {
	c.mu.Lock()
	s, ok := c.byID[id]
	c.mu.Unlock()
	if ok {
		return s, nil
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/schemas/ids/"+strconv.Itoa(id), nil)
	if err != nil {
		return nil, fmt.Errorf("devconsumer: schema request: %w", err)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("devconsumer: fetch schema %d: %w", id, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, fmt.Errorf("devconsumer: read schema %d: %w", id, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("devconsumer: fetch schema %d: HTTP %d", id, resp.StatusCode)
	}
	var out struct {
		Schema string `json:"schema"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("devconsumer: decode schema %d response: %w", id, err)
	}
	s, err = avro.Parse(out.Schema)
	if err != nil {
		return nil, fmt.Errorf("devconsumer: parse schema %d: %w", id, err)
	}
	c.mu.Lock()
	c.byID[id] = s
	c.mu.Unlock()
	return s, nil
}

// decode turns one wire-format message into a generic Avro value.
func (c *schemaCache) decode(ctx context.Context, b []byte) (any, error) {
	id, body, err := splitWire(b)
	if err != nil {
		return nil, err
	}
	s, err := c.get(ctx, id)
	if err != nil {
		return nil, err
	}
	var v any
	if err := avro.Unmarshal(s, body, &v); err != nil {
		return nil, fmt.Errorf("devconsumer: avro decode (schema %d): %w", id, err)
	}
	return v, nil
}

// orderRow is one row of analytics.orders.
type orderRow struct {
	ID         int64
	CustomerID int64
	Amount     string // decimal text, e.g. "1249.50"
	Status     string
	UpdatedAt  time.Time
}

// change is one Debezium change event for public.orders.
type change struct {
	Op    string    // c, u, d, r
	ID    int64     // primary key
	After *orderRow // nil for deletes
}

// toChange reads a decoded Debezium key and envelope value.
func toChange(key, value any) (change, error) {
	k, ok := unwrap(key).(map[string]any)
	if !ok {
		return change{}, fmt.Errorf("devconsumer: key is %T, want record", key)
	}
	id, err := asInt64(k["id"])
	if err != nil {
		return change{}, fmt.Errorf("devconsumer: key id: %w", err)
	}
	env, ok := unwrap(value).(map[string]any)
	if !ok {
		return change{}, fmt.Errorf("devconsumer: value is %T, want envelope record", value)
	}
	op, _ := unwrap(env["op"]).(string)
	c := change{Op: op, ID: id}
	if op == "d" {
		return c, nil
	}
	after, ok := unwrap(env["after"]).(map[string]any)
	if !ok {
		return change{}, fmt.Errorf("devconsumer: op %q without after", op)
	}
	row, err := toRow(after)
	if err != nil {
		return change{}, err
	}
	c.After = &row
	return c, nil
}

func toRow(m map[string]any) (orderRow, error) {
	var r orderRow
	var err error
	if r.ID, err = asInt64(unwrap(m["id"])); err != nil {
		return r, fmt.Errorf("devconsumer: id: %w", err)
	}
	if r.CustomerID, err = asInt64(unwrap(m["customer_id"])); err != nil {
		return r, fmt.Errorf("devconsumer: customer_id: %w", err)
	}
	if r.Amount, err = asDecimal(unwrap(m["amount"])); err != nil {
		return r, fmt.Errorf("devconsumer: amount: %w", err)
	}
	r.Status, _ = unwrap(m["status"]).(string)
	// Debezium sends timestamptz as io.debezium.time.ZonedTimestamp (ISO-8601 text).
	if ts, ok := unwrap(m["updated_at"]).(string); ok && ts != "" {
		if r.UpdatedAt, err = time.Parse(time.RFC3339Nano, ts); err != nil {
			return r, fmt.Errorf("devconsumer: updated_at %q: %w", ts, err)
		}
	}
	return r, nil
}

// unwrap removes the single-key map that generic Avro decoding uses for
// union values, e.g. {"pg.public.orders.Value": {...}} or {"string": "x"}.
func unwrap(v any) any {
	m, ok := v.(map[string]any)
	if !ok || len(m) != 1 {
		return v
	}
	for name, inner := range m {
		if strings.Contains(name, ".") || isPrimitive(name) {
			return inner
		}
	}
	return v
}

func isPrimitive(name string) bool {
	switch name {
	case "null", "boolean", "int", "long", "float", "double", "bytes", "string":
		return true
	}
	return false
}

func asInt64(v any) (int64, error) {
	switch n := v.(type) {
	case int:
		return int64(n), nil
	case int32:
		return int64(n), nil
	case int64:
		return n, nil
	}
	return 0, fmt.Errorf("want integer, got %T", v)
}

// asDecimal turns the amount into decimal text. It accepts the four shapes the
// connector can send: an Avro int (before the schema change), Avro bytes with
// the decimal logical type (decoded as *big.Rat), a float64 (connector setting
// decimal.handling.mode=double) and a string (decimal.handling.mode=string).
func asDecimal(v any) (string, error) {
	switch n := v.(type) {
	case int, int32, int64:
		i, _ := asInt64(n)
		return strconv.FormatInt(i, 10), nil
	case *big.Rat:
		return ratText(n), nil
	case big.Rat:
		return ratText(&n), nil
	case float64:
		// Shortest text that reads back as the same float64; NaN and Inf are not amounts.
		if math.IsNaN(n) || math.IsInf(n, 0) {
			return "", fmt.Errorf("amount %v is not finite", n)
		}
		return strconv.FormatFloat(n, 'f', -1, 64), nil
	case string:
		// big.Rat also accepts "a/b"; an amount is plain decimal text only.
		t := strings.TrimSpace(n)
		if strings.Contains(t, "/") {
			return "", fmt.Errorf("amount %q is not a decimal", n)
		}
		r, ok := new(big.Rat).SetString(t)
		if !ok {
			return "", fmt.Errorf("amount %q is not a decimal", n)
		}
		return ratText(r), nil
	}
	return "", fmt.Errorf("want int, decimal, float or string, got %T", v)
}

// ratText writes r with up to 10 fractional digits and no trailing zeros, so
// 1249.50 becomes "1249.5" and 100 becomes "100".
func ratText(r *big.Rat) string {
	s := r.FloatString(10)
	s = strings.TrimRight(s, "0")
	return strings.TrimSuffix(s, ".")
}
