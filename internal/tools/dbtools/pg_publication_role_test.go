package dbtools

import (
	"errors"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"

	"piperoom/internal/clients/postgres"
)

// A dropped connector role makes Postgres refuse the privilege functions with
// 42704. The tool must answer with the "role does not exist" finding, not the
// raw SQL error (Q2 F15).
func TestPgPublicationTablesMissingRoleIsAFinding(t *testing.T) {
	undefined := &pgconn.PgError{Code: "42704", Message: `role "debezium" does not exist`}
	src := &infoSource{
		fakeSource: &fakeSource{pubErr: wrapUndefined(undefined)},
		info:       postgres.PublicationInfo{Publication: "dbz_publication", Owner: "dba", Role: "debezium"},
	}
	res := runPublication(t, src, "filtered")
	if !strings.Contains(res.Summary, "PROBLEM connector user debezium does not exist as a role") {
		t.Fatalf("summary lacks the finding:\n%s", res.Summary)
	}
	if strings.Contains(res.Summary, "SQLSTATE") || strings.Contains(res.Summary, "ERROR:") {
		t.Fatalf("raw SQL error in the summary:\n%s", res.Summary)
	}
}

// When the role does exist, a 42704 is something else: it stays an error.
func TestPgPublicationTablesUndefinedObjectStaysAnErrorWhenRoleExists(t *testing.T) {
	undefined := &pgconn.PgError{Code: "42704", Message: `type "x" does not exist`}
	src := &infoSource{
		fakeSource: &fakeSource{pubErr: wrapUndefined(undefined)},
		info:       postgres.PublicationInfo{Publication: "dbz_publication", Owner: "dba", Role: "debezium", RoleExists: true},
	}
	_, err := runTool(t, publicationTool(t, src, authConnect("filtered")), `{"pipeline_id":"orders"}`)
	if err == nil {
		t.Fatal("want an error")
	}
}

func wrapUndefined(err error) error {
	return errors.Join(errors.New("postgres: check publication"), err)
}
