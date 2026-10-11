package snowflake

import (
	"encoding/json"
	"fmt"
	"math/big"
	"time"
)

// toJSON converts a value scanned from a Snowflake row into a JSON-friendly
// one: numbers (including the *big.Int/*big.Float gosnowflake returns for
// values that don't fit a native Go type under WithHigherPrecision) become
// strings, so large or high-scale NUMBER columns never go through a lossy
// float64. Times become RFC 3339 strings.
func toJSON(v any) any {
	switch t := v.(type) {
	case nil:
		return nil
	case bool:
		return t
	case string:
		return t
	case []byte:
		return string(t)
	case time.Time:
		return t.UTC().Format(time.RFC3339Nano)
	case int64:
		return fmt.Sprintf("%d", t)
	case int:
		return fmt.Sprintf("%d", t)
	case float64:
		return formatFloat(t)
	case float32:
		return formatFloat(float64(t))
	case *big.Int:
		if t == nil {
			return nil
		}
		return t.String()
	case *big.Float:
		if t == nil {
			return nil
		}
		return t.Text('f', -1)
	default:
		if b, err := json.Marshal(t); err == nil {
			return string(b)
		}
		return fmt.Sprintf("%v", t)
	}
}

// formatFloat renders a float64 in plain decimal form (no exponent) so a
// freshly-converted NUMBER value reads the way it would in Snowflake.
func formatFloat(f float64) string {
	return big.NewFloat(f).Text('f', -1)
}
