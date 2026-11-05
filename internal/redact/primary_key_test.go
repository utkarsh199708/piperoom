package redact

import (
	"strings"
	"testing"
)

// has_primary_key is a table property the replication tools report; a bare
// primary_key stays a secret name (Azure storage and Cosmos DB keys).
func TestPrimaryKeyPredicateIsNotASecret(t *testing.T) {
	for k, want := range map[string]keyStrength{
		"has_primary_key":             keyNone,
		"without_primary_key":         keyNone,
		"hasPrimaryKey":               keyNone,
		"primary_key":                 keyStrong,
		"primaryKey":                  keyStrong,
		"PrimaryKey":                  keyStrong,
		"azure_primary_key":           keyStrong,
		"storage_account_primary_key": keyStrong,
		"has_primary_key_secret":      keyStrong,
		"has_api_key":                 keyStrong,
	} {
		if got := secretKey(k); got != want {
			t.Errorf("secretKey(%q) = %v, want %v", k, got, want)
		}
	}

	line := "public.orders: has_primary_key=yes replica_identity=nothing"
	if got, _ := Text(line); got != line {
		t.Fatalf("Text changed the replication line: %q", got)
	}
	data := map[string]any{"table": "public.orders", "has_primary_key": true}
	out, _ := JSON(data).(map[string]any)
	if out["has_primary_key"] != true {
		t.Fatalf("JSON masked has_primary_key: %v", out)
	}

	const azure = "Zm9vYmFyYmF6cXV4MTIzNDU2Nzg5MGFiY2RlZmdoaWprbG1ub3A="
	for _, s := range []string{"primary_key=" + azure, `{"primaryKey":"` + azure + `"}`} {
		if got, _ := Text(s); strings.Contains(got, azure) {
			t.Fatalf("Azure primary key leaked: %q", got)
		}
	}
}
