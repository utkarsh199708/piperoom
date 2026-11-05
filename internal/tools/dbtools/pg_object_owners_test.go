package dbtools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// Tests for the live gap of 2026-10-25 (devctl break connector-auth): the
// owner of public.orders and dbz_publication moved to orders_owner, the
// Debezium task failed with "must be owner of table orders", and no tool
// said who owned them.

// ownersSource adds the owner read to fakeSource and records how it was asked.
type ownersSource struct {
	*fakeSource
	oo  postgres.ObjectOwners
	err error

	calls             int
	role, publication string
	tables            []string
}

func (s *ownersSource) ObjectOwners(ctx context.Context, role, publication string, tables []string) (postgres.ObjectOwners, error) {
	s.calls++
	s.role, s.publication, s.tables = role, publication, tables
	if _, ok := ctx.Deadline(); !ok {
		return postgres.ObjectOwners{}, errors.New("object owners read without a deadline")
	}
	return s.oo, s.err
}

func ownersTool(t *testing.T, d Deps) tools.Tool {
	t.Helper()
	reg := tools.NewRegistry()
	if err := RegisterObjectOwners(reg, d); err != nil {
		t.Fatal(err)
	}
	tl, ok := reg.Get("pg_object_owners")
	if !ok {
		t.Fatal("pg_object_owners not registered")
	}
	return tl
}

// owned builds the client answer: the role piperoom (replication, not
// superuser), public.orders and the publication owned by owner.
func owned(owner string, isOwner, member bool) postgres.ObjectOwners {
	return postgres.ObjectOwners{
		Role: "piperoom", RoleExists: true, Replication: true,
		Publication: postgres.PublicationOwner{Name: "dbz_publication", Exists: true, Owner: owner, RoleIsOwner: isOwner, RoleHasOwnerPrivs: isOwner || member},
		Tables:      []postgres.TableOwner{{Table: "public.orders", Exists: true, Owner: owner, RoleIsOwner: isOwner, RoleHasOwnerPrivs: isOwner || member, InPublication: true}},
	}
}

func TestObjectOwnersVerdicts(t *testing.T) {
	const differs = "owner_differs: public.orders is owned by orders_owner, not by piperoom\n"
	cases := []struct {
		name        string
		oo          postgres.ObjectOwners
		tableV      string
		pubV        string
		differs     int
		want        []string
		wantNot     []string
		wantPubbed  []string
		wantMissing bool
	}{
		{
			name: "connector owns everything", oo: owned("piperoom", true, false),
			tableV: verdictOwner, pubV: verdictOwner, wantPubbed: []string{"public.orders"},
			want: []string{
				"connector pg-orders-connector role piperoom: exists=yes superuser=no replication=yes\n",
				"publication dbz_publication (Debezium default name): owner=piperoom all_tables=no publishes 1 of 1 pipeline tables, verdict=connector_is_owner\n",
				"public.orders: owner=piperoom in_publication=yes, verdict=connector_is_owner\n",
			},
			wantNot: []string{"owner_differs", "must be owner"},
		},
		{
			name: "owner changed to another role (connector-auth)", oo: owned("orders_owner", false, false),
			tableV: verdictDiffers, pubV: verdictDiffers, differs: 2, wantPubbed: []string{"public.orders"},
			want: []string{
				"owner_differs: publication dbz_publication is owned by orders_owner, not by piperoom\n",
				differs,
				"Debezium's role must own the publication and its tables",
				"public.orders: owner=orders_owner in_publication=yes, verdict=owner_differs\n",
			},
		},
		{
			name: "connector is a member of the owner role", oo: owned("orders_owner", false, true),
			tableV: verdictMember, pubV: verdictMember, wantPubbed: []string{"public.orders"},
			want:    []string{"public.orders: owner=orders_owner in_publication=yes, verdict=connector_is_member_of_owner\n"},
			wantNot: []string{"owner_differs:"},
		},
		{
			name: "superuser owns nothing but passes every check",
			oo: func() postgres.ObjectOwners {
				o := owned("orders_owner", false, false)
				o.Superuser = true
				return o
			}(),
			tableV: verdictSuperuser, pubV: verdictSuperuser, wantPubbed: []string{"public.orders"},
			want:    []string{"exists=yes superuser=yes replication=yes", "verdict=connector_is_superuser"},
			wantNot: []string{"owner_differs:"},
		},
		{
			name: "publication missing",
			oo: func() postgres.ObjectOwners {
				o := owned("piperoom", true, false)
				o.Publication = postgres.PublicationOwner{Name: "dbz_publication"}
				o.Tables[0].InPublication = false
				return o
			}(),
			tableV: verdictOwner, pubV: verdictMissing, wantPubbed: []string{},
			want:    []string{"publication dbz_publication (Debezium default name): does not exist, verdict=missing\n"},
			wantNot: []string{"owner_differs"},
		},
		{
			name: "table missing",
			oo: func() postgres.ObjectOwners {
				o := owned("piperoom", true, false)
				o.Tables = []postgres.TableOwner{{Table: "public.orders"}}
				return o
			}(),
			tableV: verdictMissing, pubV: verdictOwner, wantPubbed: []string{},
			want:    []string{"public.orders: does not exist, verdict=missing\n", "publishes 0 of 1 pipeline tables"},
			wantNot: []string{"owner_differs"},
		},
		{
			name: "connector role does not exist",
			oo: func() postgres.ObjectOwners {
				o := owned("orders_owner", false, false)
				o.RoleExists, o.Replication = false, false
				return o
			}(),
			tableV: verdictDiffers, pubV: verdictDiffers, differs: 2, wantPubbed: []string{"public.orders"},
			want: []string{"role piperoom: the role does not exist (connector config database.user)\n", differs},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			src := &ownersSource{fakeSource: &fakeSource{}, oo: c.oo}
			r, err := ownersTool(t, metaDeps(src)).Run(roomCtx(), json.RawMessage(`{"pipeline_id":"orders"}`))
			if err != nil {
				t.Fatal(err)
			}
			for _, w := range c.want {
				if !strings.Contains(r.Summary, w) {
					t.Errorf("summary lacks %q:\n%s", w, r.Summary)
				}
			}
			for _, w := range c.wantNot {
				if strings.Contains(r.Summary, w) {
					t.Errorf("summary has %q:\n%s", w, r.Summary)
				}
			}
			// The role and publication come from the connector config, the
			// tables from the room's pipeline.
			if src.calls != 1 || src.role != "piperoom" || src.publication != "dbz_publication" || !slices.Equal(src.tables, []string{"public.orders"}) {
				t.Errorf("asked role %q publication %q tables %v (%d calls)", src.role, src.publication, src.tables, src.calls)
			}
			d := r.Data.(objectOwnersData)
			if len(d.Tables) != 1 || d.Tables[0].Verdict != c.tableV || d.Publication.Verdict != c.pubV || d.OwnerDiffers != c.differs {
				t.Errorf("verdicts table=%+v publication=%+v differs=%d; want %s/%s/%d", d.Tables, d.Publication, d.OwnerDiffers, c.tableV, c.pubV, c.differs)
			}
			if !slices.Equal(d.Publication.PublishedTables, c.wantPubbed) {
				t.Errorf("published_tables = %v, want %v", d.Publication.PublishedTables, c.wantPubbed)
			}
			if d.Connector != "pg-orders-connector" || d.ConnectorUser != "piperoom" || d.Publication.NameSource != "default" {
				t.Errorf("data = %+v", d)
			}
		})
	}
}

// The owner_differs line leads the summary so truncation never cuts it.
func TestObjectOwnersDiffersLeads(t *testing.T) {
	src := &ownersSource{fakeSource: &fakeSource{}, oo: owned("orders_owner", false, false)}
	r, err := ownersTool(t, metaDeps(src)).Run(roomCtx(), json.RawMessage(`{"pipeline_id":"orders"}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(r.Summary, "owner_differs: publication dbz_publication is owned by orders_owner, not by piperoom\nowner_differs: public.orders is owned by orders_owner, not by piperoom\n") {
		t.Fatalf("summary does not lead with the finding:\n%s", r.Summary)
	}
	b := string(mustJSON(t, r.Data))
	for _, want := range []string{`"verdict":"owner_differs"`, `"owner":"orders_owner"`, `"owner_differs":2`, `"connector_user":"piperoom"`, `"published_tables":["public.orders"]`} {
		if !strings.Contains(b, want) {
			t.Errorf("data lacks %s: %s", want, b)
		}
	}
}

func TestObjectOwnersPublicationName(t *testing.T) {
	d := metaDeps(nil)
	d.Connect = &fakeConnect{configs: map[string]map[string]string{
		"pg-orders-connector": {"connector.class": pgClass, "database.user": "alice", "publication.name": "orders_pub"},
	}}
	src := &ownersSource{fakeSource: &fakeSource{}, oo: owned("alice", true, false)}
	d.Src = src
	r, err := ownersTool(t, d).Run(roomCtx(), json.RawMessage(`{"pipeline_id":"orders"}`))
	if err != nil {
		t.Fatal(err)
	}
	if src.publication != "orders_pub" || src.role != "alice" {
		t.Fatalf("asked publication %q role %q", src.publication, src.role)
	}
	data := r.Data.(objectOwnersData)
	if data.Publication.Name != "orders_pub" || data.Publication.NameSource != "connector pg-orders-connector config publication.name" {
		t.Fatalf("publication = %+v", data.Publication)
	}
	if strings.Contains(r.Summary, "default name") {
		t.Fatalf("summary calls a configured name the default:\n%s", r.Summary)
	}
}

// A client without the read answers not supported, never a crash or error.
func TestObjectOwnersNotSupported(t *testing.T) {
	for name, src := range map[string]clients.SourceDB{
		"no method":            &fakeSource{},
		"method says not sup.": &ownersSource{fakeSource: &fakeSource{}, err: fmt.Errorf("mysql: %w", clients.ErrNotSupported)},
	} {
		t.Run(name, func(t *testing.T) {
			r, err := ownersTool(t, metaDeps(src)).Run(roomCtx(), json.RawMessage(`{"pipeline_id":"orders"}`))
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(r.Summary, "not supported") || !strings.Contains(r.Summary, "not ruled out") {
				t.Errorf("summary = %q", r.Summary)
			}
			if d := r.Data.(objectOwnersData); !d.NotSupported || d.ConnectorUser != "piperoom" {
				t.Errorf("data = %+v", d)
			}
		})
	}
}

func TestObjectOwnersErrors(t *testing.T) {
	for _, e := range []error{errors.New("connection refused"), context.DeadlineExceeded} {
		src := &ownersSource{fakeSource: &fakeSource{}, err: e}
		r, err := ownersTool(t, metaDeps(src)).Run(roomCtx(), json.RawMessage(`{"pipeline_id":"orders"}`))
		if !errors.Is(err, e) || !strings.HasPrefix(err.Error(), "dbtools: pg_object_owners:") || r.Summary != "" {
			t.Errorf("err = %v, summary = %q; want the wrapped %v", err, r.Summary, e)
		}
	}
	// Connect down: the role is unknown, so nothing is read.
	d := metaDeps(nil)
	src := &ownersSource{fakeSource: &fakeSource{}}
	d.Src = src
	d.Connect = &fakeConnect{configErr: errors.New("connect: 503")}
	if _, err := ownersTool(t, d).Run(roomCtx(), json.RawMessage(`{"pipeline_id":"orders"}`)); err == nil || src.calls != 0 {
		t.Errorf("connect failure: err = %v, calls = %d", err, src.calls)
	}
}

func TestObjectOwnersInput(t *testing.T) {
	src := &ownersSource{fakeSource: &fakeSource{}}
	tl := ownersTool(t, metaDeps(src))
	for _, in := range []string{`{}`, `{"pipeline_id":"payments"}`, `not json`} {
		if _, err := tl.Run(roomCtx(), json.RawMessage(in)); err == nil {
			t.Errorf("input %s accepted", in)
		}
	}
	// No room and no explicit opt-out: refused.
	if _, err := tl.Run(context.Background(), json.RawMessage(`{"pipeline_id":"orders"}`)); err == nil {
		t.Error("call outside a room accepted")
	}
	if src.calls != 0 {
		t.Errorf("client read %d times on refused input", src.calls)
	}
	// A role in the input is not a parameter: the connector's role is asked.
	if _, err := tl.Run(roomCtx(), json.RawMessage(`{"pipeline_id":"orders","role":"postgres"}`)); err != nil {
		t.Fatal(err)
	}
	if src.role != "piperoom" {
		t.Errorf("asked role %q, want the connector's piperoom", src.role)
	}
}

// A hostile source table name in owners.yaml never reaches the client.
func TestObjectOwnersHostileTableName(t *testing.T) {
	for _, bad := range []string{`public.orders"; DROP TABLE orders; --`, "orders", "public.or-ders", "a.b.c"} {
		d := metaDeps(nil)
		src := &ownersSource{fakeSource: &fakeSource{}}
		d.Src = src
		d.Owners = &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders", SourceTables: []string{"public.orders", bad}, Connectors: []string{"pg-orders-connector"}}}}
		_, err := ownersTool(t, d).Run(roomCtx(), json.RawMessage(`{"pipeline_id":"orders"}`))
		if !errors.Is(err, tools.ErrBadInput) || src.calls != 0 {
			t.Errorf("%q: err = %v, calls = %d; want ErrBadInput before any read", bad, err, src.calls)
		}
	}
}

func TestObjectOwnersRegistration(t *testing.T) {
	d := metaDeps(&fakeSource{})
	tl := ownersTool(t, d)
	if tl.Kind() != tools.KindRead {
		t.Errorf("kind = %v, want read", tl.Kind())
	}
	for _, w := range []string{"must be owner of table", "publication", "owner_differs"} {
		if !strings.Contains(tl.Description(), w) {
			t.Errorf("description lacks %q", w)
		}
	}
	d.SourceKind = owners.SourceKindMySQL
	reg := tools.NewRegistry()
	if err := RegisterObjectOwners(reg, d); err != nil {
		t.Fatal(err)
	}
	if _, ok := reg.Get("pg_object_owners"); ok {
		t.Error("registered for a MySQL source")
	}
	if err := RegisterObjectOwners(tools.NewRegistry(), Deps{}); err == nil {
		t.Error("nil Owners accepted")
	}
	// Register (the existing list) leaves it to wiring.
	reg = tools.NewRegistry()
	if err := Register(reg, metaDeps(&fakeSource{})); err != nil {
		t.Fatal(err)
	}
	if _, ok := reg.Get("pg_object_owners"); ok {
		t.Error("Register added pg_object_owners; wiring registers it")
	}
}
