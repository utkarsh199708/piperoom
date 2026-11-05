package clickhouse

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"time"
)

func timestamp(value any) (time.Time, bool, error) {
	switch v := value.(type) {
	case nil:
		return time.Time{}, false, nil
	case time.Time:
		return v.UTC(), true, nil
	case *time.Time:
		if v == nil {
			return time.Time{}, false, nil
		}
		return v.UTC(), true, nil
	case string:
		return parseTimestampString(v)
	case []byte:
		return parseTimestampString(string(v))
	case int:
		return time.Unix(int64(v), 0).UTC(), true, nil
	case int32:
		return time.Unix(int64(v), 0).UTC(), true, nil
	case int64:
		return time.Unix(v, 0).UTC(), true, nil
	case uint32:
		return time.Unix(int64(v), 0).UTC(), true, nil
	case uint64:
		if v > math.MaxInt64 {
			return time.Time{}, false, fmt.Errorf("clickhouse: timestamp %d exceeds int64 range", v)
		}
		return time.Unix(int64(v), 0).UTC(), true, nil
	default:
		return time.Time{}, false, fmt.Errorf("clickhouse: timestamp is %T, want DateTime or DateTime64", value)
	}
}

func parseTimestampString(value string) (time.Time, bool, error) {
	layouts := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999 -07:00",
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
	}
	for _, layout := range layouts {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed.UTC(), true, nil
		}
	}
	return time.Time{}, false, fmt.Errorf("clickhouse: cannot parse timestamp %q", value)
}

func asInt64(value any) int64 {
	switch v := value.(type) {
	case int:
		return int64(v)
	case int8:
		return int64(v)
	case int16:
		return int64(v)
	case int32:
		return int64(v)
	case int64:
		return v
	case uint:
		return int64(v)
	case uint8:
		return int64(v)
	case uint16:
		return int64(v)
	case uint32:
		return int64(v)
	case uint64:
		if v <= math.MaxInt64 {
			return int64(v)
		}
	case float32:
		return int64(v)
	case float64:
		return int64(v)
	case []byte:
		n, _ := strconv.ParseInt(string(v), 10, 64)
		return n
	case string:
		n, _ := strconv.ParseInt(v, 10, 64)
		return n
	}
	return 0
}

// toJSON converts driver values to JSON-friendly values without losing
// precision in integer or decimal columns.
func toJSON(value any) any {
	switch v := value.(type) {
	case nil:
		return nil
	case bool:
		return v
	case string:
		return v
	case []byte:
		return string(v)
	case time.Time:
		return v.UTC().Format(time.RFC3339Nano)
	case int:
		return strconv.FormatInt(int64(v), 10)
	case int8:
		return strconv.FormatInt(int64(v), 10)
	case int16:
		return strconv.FormatInt(int64(v), 10)
	case int32:
		return strconv.FormatInt(int64(v), 10)
	case int64:
		return strconv.FormatInt(v, 10)
	case uint:
		return strconv.FormatUint(uint64(v), 10)
	case uint8:
		return strconv.FormatUint(uint64(v), 10)
	case uint16:
		return strconv.FormatUint(uint64(v), 10)
	case uint32:
		return strconv.FormatUint(uint64(v), 10)
	case uint64:
		return strconv.FormatUint(v, 10)
	case float32:
		return strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		return strconv.FormatFloat(v, 'f', -1, 64)
	case fmt.Stringer:
		return v.String()
	default:
		if encoded, err := json.Marshal(v); err == nil {
			return string(encoded)
		}
		return fmt.Sprint(v)
	}
}
