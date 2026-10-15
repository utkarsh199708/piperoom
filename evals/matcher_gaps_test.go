package evals

import (
	"reflect"
	"testing"
)

// #334: a plural "s" after an acronym stays on it.
func TestTokenizeAcronymPlural(t *testing.T) {
	tests := []struct {
		in   string
		want []string
	}{
		{"ACLs acls ACL", []string{"acls", "acls", "acl"}},
		{"the DBAs", []string{"the", "dbas"}},
		{"ACLsAnd", []string{"acls", "and"}},
		{"HTTPServer", []string{"http", "server"}},
		{"PostgreSQL", []string{"postgre", "sql"}},
		{"KafkaConnect", []string{"kafka", "connect"}},
		{"IDsLeft", []string{"ids", "left"}},
	}
	for _, tt := range tests {
		if got := tokenize(tt.in); !reflect.DeepEqual(got, tt.want) {
			t.Errorf("tokenize(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
	if !matchGroups([][]string{{"acl*"}}, "The connector's ACLs were removed.") {
		t.Error(`acl* does not match "ACLs"`)
	}
}
