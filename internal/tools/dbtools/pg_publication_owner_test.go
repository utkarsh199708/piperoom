package dbtools

import (
	"context"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/tools"
)

// infoSource adds the optional PublicationInfo read to the shared fake source.
type infoSource struct {
	*fakeSource
	info  postgres.PublicationInfo
	err   error
	calls [][2]string // publication, role
}

func (s *infoSource) PublicationInfo(_ context.Context, publication, role string) (postgres.PublicationInfo, error) {
	s.calls = append(s.calls, [2]string{publication, role})
	return s.info, s.err
}

// authConnect is the connector of the connector-auth scenario: the least
// privilege role debezium, and filtered publication mode.
func authConnect(mode string) *fakeConnect {
	cfg := map[string]string{
		"connector.class":      "io.debezium.connector.postgresql.PostgresConnector",
		"database.user":        "debezium",
		"database.server.name": "pg",
		"publication.name":     "dbz_publication",
	}
	if mode != "" {
		cfg["publication.autocreate.mode"] = mode
	}
	return &fakeConnect{configs: map[string]map[string]string{"pg-orders-connector": cfg}}
}

func ordersTable(owner string, member bool) postgres.PublicationTable {
	return postgres.PublicationTable{
		Schema: "public", Table: "orders", InPublication: true, Selectable: true,
		Owner: owner, RoleIsOwner: owner == "debezium", RoleMemberOfOwner: member,
	}
}

func debeziumInfo(mut func(*postgres.PublicationInfo)) postgres.PublicationInfo {
	i := postgres.PublicationInfo{
		Publication: "dbz_publication", Owner: "debezium", Role: "debezium",
		RoleExists: true, RoleReplication: true, RoleIsOwner: true, RoleMemberOfOwner: true,
	}
	if mut != nil {
		mut(&i)
	}
	return i
}

func runPublication(t *testing.T, src *infoSource, mode string) tools.Result {
	t.Helper()
	res, err := runTool(t, publicationTool(t, src, authConnect(mode)), `{"pipeline_id":"orders"}`)
	if err != nil {
		t.Fatalf("pg_publication_tables: %v", err)
	}
	return res
}

// The connector-auth break: debezium owns the publication but the table moved
// to orders_owner. The summary has to say so without the model inferring it.
func TestPgPublicationTablesConnectorAuthIsStatedPlainly(t *testing.T) {
	src := &infoSource{
		fakeSource: &fakeSource{pubTables: []postgres.PublicationTable{ordersTable("orders_owner", false)}},
		info:       debeziumInfo(nil),
	}
	res := runPublication(t, src, "filtered")

	for _, want := range []string{
		"publication dbz_publication (owner debezium; connector user debezium: REPLICATION=yes, SUPERUSER=no)",
		"public.orders: in_publication=yes, selectable=yes, owner=orders_owner, role_member_of_owner=no",
		"PROBLEM public.orders: connector user debezium is not the owner of this table (owner: orders_owner) and is not a member of that role.",
		`fails with "must be owner of table"`,
		"connect_worker_log",
	} {
		if !strings.Contains(res.Summary, want) {
			t.Errorf("summary lacks %q:\n%s", want, res.Summary)
		}
	}
	if strings.Count(res.Summary, "PROBLEM") != 1 {
		t.Errorf("want exactly one problem (the publication is owned by the role):\n%s", res.Summary)
	}
	if src.calls[0] != [2]string{"dbz_publication", "debezium"} {
		t.Errorf("PublicationInfo was asked for %v", src.calls)
	}

	data := res.Data.(map[string]any)
	if data["publication_owner"] != "debezium" || data["autocreate_mode"] != "filtered" {
		t.Errorf("data = %v", data)
	}
	if info, ok := data["publication_info"].(postgres.PublicationInfo); !ok || !info.RoleReplication || info.RoleSuperuser {
		t.Errorf("publication_info = %v", data["publication_info"])
	}
	if f, ok := data["findings"].([]string); !ok || len(f) != 1 || !strings.HasPrefix(f[0], "PROBLEM public.orders") {
		t.Errorf("findings = %v", data["findings"])
	}
	srcTables := data["source_tables"].([]postgres.PublicationTable)
	if srcTables[0].Owner != "orders_owner" || srcTables[0].RoleMemberOfOwner {
		t.Errorf("source_tables = %+v", srcTables)
	}
}

func TestPgPublicationTablesFindingTable(t *testing.T) {
	cases := []struct {
		name  string
		mode  string
		table postgres.PublicationTable
		info  postgres.PublicationInfo
		want  []string // substrings, each in some finding
		none  bool     // no finding at all
	}{
		{
			name: "not filtered: a note, not a problem", mode: "disabled",
			table: ordersTable("orders_owner", false), info: debeziumInfo(nil),
			want: []string{"NOTE public.orders", "matters only when the connector changes the publication"},
		},
		{
			name: "mode unset: a note", mode: "",
			table: ordersTable("orders_owner", false), info: debeziumInfo(nil),
			want: []string{"NOTE public.orders"},
		},
		{
			name: "member of the owner", mode: "filtered",
			table: ordersTable("orders_owner", true), info: debeziumInfo(nil), none: true,
		},
		{
			name: "role is the owner", mode: "filtered",
			table: ordersTable("debezium", true), info: debeziumInfo(nil), none: true,
		},
		{
			name: "owner unknown (client did not say)", mode: "filtered",
			table: ordersTable("", false), info: debeziumInfo(nil), none: true,
		},
		{
			name: "no REPLICATION attribute", mode: "filtered",
			table: ordersTable("debezium", true),
			info:  debeziumInfo(func(i *postgres.PublicationInfo) { i.RoleReplication = false }),
			want:  []string{"PROBLEM connector user debezium has no REPLICATION attribute and is not a superuser"},
		},
		{
			name: "superuser needs no REPLICATION", mode: "filtered",
			table: ordersTable("debezium", true),
			info: debeziumInfo(func(i *postgres.PublicationInfo) {
				i.RoleReplication, i.RoleSuperuser = false, true
			}),
			none: true,
		},
		{
			name: "publication owned by someone else, filtered", mode: "filtered",
			table: ordersTable("debezium", true),
			info: debeziumInfo(func(i *postgres.PublicationInfo) {
				i.Owner, i.RoleIsOwner, i.RoleMemberOfOwner = "dba", false, false
			}),
			want: []string{"PROBLEM publication dbz_publication is owned by dba", "only the owner may"},
		},
		{
			// The least-privilege setup: a ready-made publication owned by a DBA
			// and a connector that never alters it.
			name: "publication owned by someone else, disabled mode", mode: "disabled",
			table: ordersTable("debezium", true),
			info: debeziumInfo(func(i *postgres.PublicationInfo) {
				i.Owner, i.RoleIsOwner, i.RoleMemberOfOwner = "dba", false, false
			}),
			none: true,
		},
		{
			name: "role does not exist", mode: "filtered",
			table: ordersTable("", false),
			info:  postgres.PublicationInfo{Publication: "dbz_publication", Owner: "dba", Role: "debezium"},
			want:  []string{"PROBLEM connector user debezium does not exist as a role"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			src := &infoSource{fakeSource: &fakeSource{pubTables: []postgres.PublicationTable{tc.table}}, info: tc.info}
			res := runPublication(t, src, tc.mode)
			findings, _ := res.Data.(map[string]any)["findings"].([]string)
			if tc.none {
				if len(findings) != 0 || strings.Contains(res.Summary, "PROBLEM") || strings.Contains(res.Summary, "NOTE") {
					t.Fatalf("want no finding, got %v\n%s", findings, res.Summary)
				}
				return
			}
			all := strings.Join(findings, "\n")
			for _, want := range tc.want {
				if !strings.Contains(all, want) || !strings.Contains(res.Summary, want) {
					t.Errorf("finding lacks %q (in findings: %v, in summary: %v)\n%s", want, strings.Contains(all, want), strings.Contains(res.Summary, want), res.Summary)
				}
			}
		})
	}
}

// The role details are extra: when they cannot be read the table facts still
// come back, and the summary says what is missing.
func TestPgPublicationTablesRoleReadFails(t *testing.T) {
	src := &infoSource{
		fakeSource: &fakeSource{pubTables: []postgres.PublicationTable{ordersTable("orders_owner", false)}},
		err:        errors.New("postgres: read publication \"dbz_publication\" owner: connection reset"),
	}
	res := runPublication(t, src, "filtered")
	if !strings.Contains(res.Summary, "Publication owner and role attributes could not be read: postgres: read publication") {
		t.Errorf("summary = %s", res.Summary)
	}
	if !strings.Contains(res.Summary, "public.orders: in_publication=yes, selectable=yes, owner=orders_owner") {
		t.Errorf("the table facts were lost:\n%s", res.Summary)
	}
	data := res.Data.(map[string]any)
	if data["publication_info_error"] == nil || data["publication_info"] != nil {
		t.Errorf("data = %v", data)
	}
	// The table finding does not need the role read.
	if !strings.Contains(res.Summary, "PROBLEM public.orders") {
		t.Errorf("table finding missing:\n%s", res.Summary)
	}
}

// A source client with only the table read (the old contract) behaves as before.
func TestPgPublicationTablesWithoutInfoClient(t *testing.T) {
	src := &fakeSource{pubTables: []postgres.PublicationTable{
		{Schema: "public", Table: "orders", InPublication: true, Selectable: true},
	}}
	res, err := runTool(t, publicationTool(t, src, authConnect("filtered")), `{"pipeline_id":"orders"}`)
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"REPLICATION=", "PROBLEM", "NOTE", "could not be read", "owner="} {
		if strings.Contains(res.Summary, bad) {
			t.Errorf("summary mentions %q without the data for it:\n%s", bad, res.Summary)
		}
	}
	data := res.Data.(map[string]any)
	for _, k := range []string{"findings", "publication_info", "publication_info_error", "publication_owner"} {
		if _, ok := data[k]; ok {
			t.Errorf("data has %q", k)
		}
	}
}

// A missing publication is still ErrNotFound, and the role read is not tried.
func TestPgPublicationTablesMissingPublicationSkipsRoleRead(t *testing.T) {
	src := &infoSource{fakeSource: &fakeSource{pubErr: clients.ErrNotFound}}
	_, err := runTool(t, publicationTool(t, src, authConnect("filtered")), `{"pipeline_id":"orders"}`)
	if !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("err = %v", err)
	}
	if len(src.calls) != 0 {
		t.Errorf("PublicationInfo was called for a publication that does not exist: %v", src.calls)
	}
}

// Another pipeline's tables are not described: owners of tables outside the
// pipeline stay out of the result, the same as the rest of the tool.
func TestPgPublicationTablesOwnersOfOtherTablesStayOut(t *testing.T) {
	src := &infoSource{
		fakeSource: &fakeSource{pubTables: []postgres.PublicationTable{
			ordersTable("orders_owner", false),
			{Schema: "public", Table: "payments", InPublication: true, Selectable: true, Owner: "secret_payments_owner"},
		}},
		info: debeziumInfo(nil),
	}
	res := runPublication(t, src, "filtered")
	if strings.Contains(res.Summary, "secret_payments_owner") || strings.Contains(res.Summary, "payments") {
		t.Errorf("another pipeline's table reached the summary:\n%s", res.Summary)
	}
}

func TestPgPublicationTablesDescriptionNamesTheNewFacts(t *testing.T) {
	d := publicationTool(t, &fakeSource{}).Description()
	for _, want := range []string{"owner", "REPLICATION", "SUPERUSER", "SELECT", "member"} {
		if !strings.Contains(d, want) {
			t.Errorf("description lacks %q: %s", want, d)
		}
	}
}
