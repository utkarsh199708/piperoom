package mongodb

import "go.mongodb.org/mongo-driver/bson"

// Helpers to read values out of a decoded command reply (bson.M, whose nested
// documents are bson.M and arrays are bson.A).

func asMap(v any) (bson.M, bool) {
	m, ok := v.(bson.M)
	return m, ok
}

func asSlice(v any) ([]any, bool) {
	s, ok := v.(bson.A)
	return s, ok
}

func asBool(v any) bool {
	b, _ := v.(bool)
	return b
}

func stringOf(v any) string {
	s, _ := v.(string)
	return s
}

func intOf(v any) int {
	switch n := v.(type) {
	case int:
		return n
	case int32:
		return int(n)
	case int64:
		return int(n)
	case float64:
		return int(n)
	}
	return 0
}
