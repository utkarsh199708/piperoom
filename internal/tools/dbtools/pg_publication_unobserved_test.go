package dbtools

import (
	"strings"
	"testing"

	"piperoom/internal/clients/postgres"
)

// Q4 R13: a fact the catalogs could not show is never a PROBLEM.
func TestPublicationFindingsNeverProblemForUnobservableRoleFacts(t *testing.T) {
	tests := []struct {
		name      string
		role      string
		info      postgres.PublicationInfo
		wantWord  string // first word of the one expected finding; "" for none
		wantInMsg string
	}{
		{
			name: "rds_replication member",
			role: "debezium",
			info: postgres.PublicationInfo{Role: "debezium", RoleExists: true, RoleManagedReplication: true},
		},
		{
			name:      "unresolved provider reference, no such role",
			role:      "${secrets:db:user}",
			info:      postgres.PublicationInfo{Role: "${secrets:db:user}"},
			wantWord:  "UNKNOWN",
			wantInMsg: "could not check",
		},
		{
			name:      "unresolved provider reference, no REPLICATION",
			role:      "${file:/opt/user.txt:u}",
			info:      postgres.PublicationInfo{Role: "${file:/opt/user.txt:u}", RoleExists: true},
			wantWord:  "UNKNOWN",
			wantInMsg: "could not check",
		},
		{
			name:     "plain role without REPLICATION is still a problem",
			role:     "debezium",
			info:     postgres.PublicationInfo{Role: "debezium", RoleExists: true},
			wantWord: "PROBLEM",
		},
		{
			name:     "missing plain role is still a problem",
			role:     "debezium",
			info:     postgres.PublicationInfo{Role: "debezium"},
			wantWord: "PROBLEM",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := connectorConfig{role: tc.role, publication: "p"}
			got := publicationFindings(c, nil, nil, roleFacts{info: &tc.info})
			if tc.wantWord == "" {
				if len(got) != 0 {
					t.Fatalf("findings = %q, want none", got)
				}
				return
			}
			if len(got) != 1 || !strings.HasPrefix(got[0], tc.wantWord+" ") {
				t.Fatalf("findings = %q, want one %s", got, tc.wantWord)
			}
			if !strings.Contains(got[0], tc.wantInMsg) {
				t.Errorf("finding lacks %q: %s", tc.wantInMsg, got[0])
			}
		})
	}
}
