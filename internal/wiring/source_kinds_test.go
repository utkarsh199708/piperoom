package wiring

import (
	"strings"
	"testing"

	"piperoom/internal/owners"
)

// A set whose CDC pipelines have a source kind the wiring has no client for
// (sqlserver, mongodb, oracle: KAN-529) is refused with a clear error, with
// or without a source URL, instead of being wired as Postgres (KAN-541).
func TestCheckSourceURLsBySourceKind(t *testing.T) {
	tests := []struct {
		name     string
		kind     string
		url      string
		wantKind string // "" = want an error
	}{
		{"default, postgres URL", "", readerDSN, "postgres"},
		{"postgres, postgres URL", owners.SourceKindPostgres, readerDSN, "postgres"},
		{"postgres, no URL", owners.SourceKindPostgres, "", "postgres"},
		{"mysql, mysql URL", owners.SourceKindMySQL, mysqlReadURL, "mysql"},
		{"mysql, no URL", owners.SourceKindMySQL, "", "mysql"},
		{"sqlserver, no URL", owners.SourceKindSQLServer, "", ""},
		{"sqlserver, postgres URL", owners.SourceKindSQLServer, readerDSN, ""},
		{"mongodb, no URL", owners.SourceKindMongoDB, "", ""},
		{"mongodb, postgres URL", owners.SourceKindMongoDB, readerDSN, ""},
		{"oracle, no URL", owners.SourceKindOracle, "", ""},
		{"oracle, postgres URL", owners.SourceKindOracle, readerDSN, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := cdcPipe("orders")
			p.SourceKind = tt.kind
			cfg := devA2Config()
			cfg.SourceDBURL, cfg.SourceDBWriteURL = tt.url, ""
			got, err := checkSourceURLs(cfg, ownersOf(p))
			if tt.wantKind != "" {
				if err != nil || got != tt.wantKind {
					t.Fatalf("checkSourceURLs = %q, %v; want %q", got, err, tt.wantKind)
				}
				return
			}
			if err == nil {
				t.Fatalf("checkSourceURLs = %q, nil; want an error", got)
			}
			if !strings.Contains(err.Error(), "pipeline orders") {
				t.Errorf("err %q does not name the pipeline", err)
			}
			if tt.url == "" && !strings.Contains(err.Error(), "source_kind "+tt.kind+" has no source client") {
				t.Errorf("err %q does not name the kind", err)
			}
		})
	}
}
