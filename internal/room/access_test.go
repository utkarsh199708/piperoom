package room

import (
	"context"
	"errors"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/auth"
	"piperoom/internal/store/memstore"
)

const accessOwnersYAML = `layers:
  postgres: priya
  kafka: [user:arjun, group:dataeng-oncall]
  consumer: group:consumers
auditors: [group:security]
`

func accessService(t *testing.T, ownersYAML, solo string, sessions auth.Store) *Service {
	t.Helper()
	own := ownersFromYAML(t, ownersYAML)
	svc := New(Deps{
		Store: memstore.New(), Users: mustUsers([]User{
			{ID: "arjun", Name: "Arjun", Layers: []string{"kafka"}},
			{ID: "priya", Name: "Priya", Layers: []string{"postgres"}},
		}),
		Owners: own, ViewerGroups: []string{"viewers"}, SoloUser: solo,
		Sessions: sessions, SessionIdle: time.Hour,
		Run: func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
			<-ctx.Done()
			return nil
		},
	})
	t.Cleanup(svc.Close)
	return svc
}

func TestRoleFor(t *testing.T) {
	svc := accessService(t, accessOwnersYAML, "", nil)
	cases := []struct {
		name   string
		id     string
		groups []string
		role   string
		ok     bool
	}{
		{"users.yaml user", "priya", nil, RoleMember, true},
		{"users.yaml user with groups", "priya", []string{"x"}, RoleMember, true},
		{"group owner", "u_1", []string{"dataeng-oncall"}, RoleMember, true},
		{"another group owner", "u_1", []string{"consumers"}, RoleMember, true},
		{"viewer group", "u_1", []string{"viewers"}, RoleViewer, true},
		{"auditors alone is a viewer, not a member", "u_1", []string{"security"}, RoleViewer, true},
		{"viewer group and owner group is a member", "u_1", []string{"viewers", "consumers"}, RoleMember, true},
		{"auditor and viewer group is a viewer", "u_1", []string{"viewers", "security"}, RoleViewer, true},
		{"stranger", "u_1", nil, "", false},
		{"unrelated group", "u_1", []string{"other"}, "", false},
		{"a user: principal in owners.yaml by derived id is not matched by a group name", "dataeng-oncall", nil, "", false},
	}
	for _, c := range cases {
		role, ok := svc.RoleFor(c.id, c.groups)
		if role != c.role || ok != c.ok {
			t.Errorf("%s: RoleFor = %q, %v; want %q, %v", c.name, role, ok, c.role, c.ok)
		}
	}
}

func TestAuthorizeLayerGrants(t *testing.T) {
	svc := accessService(t, accessOwnersYAML, "", nil)
	cases := []struct {
		name   string
		user   User
		layer  string
		grant  string
		denied string // error code, "" when allowed
	}{
		{"owners user", User{ID: "priya"}, "postgres", "user:priya", ""},
		{"owners user, groups irrelevant", User{ID: "arjun", Groups: []string{"dataeng-oncall"}}, "kafka", "user:arjun", ""},
		{"group only", User{ID: "u_1", Groups: []string{"dataeng-oncall"}}, "kafka", "group:dataeng-oncall", ""},
		{"users.yaml layers as a fallback", User{ID: "x", Layers: []string{"warehouse"}}, "warehouse", "user:x", ""},
		{"wrong layer", User{ID: "priya"}, "kafka", "", "not_layer_owner"},
		{"stranger", User{ID: "u_1"}, "postgres", "", "not_layer_owner"},
		{"no groups claim, group layer", User{ID: "u_1", GroupsMissing: true}, "kafka", "", "not_layer_owner"},
	}
	for _, c := range cases {
		grant, err := svc.authorizeLayer("orders", c.user, c.layer, "", false)
		var re *Error
		switch {
		case c.denied == "" && err != nil:
			t.Errorf("%s: denied: %v", c.name, err)
		case c.denied != "" && !errors.As(err, &re):
			t.Errorf("%s: allowed with grant %q, want %s", c.name, grant, c.denied)
		case c.denied != "" && re.Code != c.denied:
			t.Errorf("%s: code %q, want %q", c.name, re.Code, c.denied)
		case c.denied == "" && grant != c.grant:
			t.Errorf("%s: grant %q, want %q", c.name, grant, c.grant)
		}
	}
	// The overage message only when a group is what could have granted it.
	_, err := svc.authorizeLayer("orders", User{ID: "u_1", GroupsMissing: true}, "kafka", "", false)
	if err == nil || err.Error() != "no groups claim in token (IdP overage?)" {
		t.Errorf("overage: %v", err)
	}
	_, err = svc.authorizeLayer("orders", User{ID: "u_1", GroupsMissing: true}, "postgres", "", false)
	if err == nil || err.Error() == "no groups claim in token (IdP overage?)" {
		t.Errorf("a layer with no group blamed the claim: %v", err)
	}
	// An unknown pipeline fails closed for owners.yaml rights.
	if _, err := svc.authorizeLayer("nope", User{ID: "priya"}, "postgres", "", false); err == nil {
		t.Error("an unknown pipeline granted an owners.yaml right")
	}
}

func TestAuthorizeLayerSolo(t *testing.T) {
	// Solo mode: priya owns everything, nobody else is named.
	svc := accessService(t, "layers:\n  postgres: priya\n", "priya", nil)
	if _, err := svc.authorizeLayer("orders", User{ID: "priya"}, "kafka", "", true); err == nil {
		t.Fatal("solo approval without confirm was allowed")
	}
	if _, err := svc.authorizeLayer("orders", User{ID: "priya"}, "kafka", "orders/postgres", true); err == nil {
		t.Fatal("solo approval with the wrong confirm was allowed")
	}
	if _, err := svc.authorizeLayer("orders", User{ID: "priya"}, "kafka", "ORDERS/KAFKA", true); err == nil {
		t.Fatal("confirm is case sensitive")
	}
	grant, err := svc.authorizeLayer("orders", User{ID: "priya"}, "kafka", "orders/kafka", true)
	if err != nil || grant != "solo" {
		t.Fatalf("solo with confirm = %q, %v", grant, err)
	}
	// A rejection needs no confirm but is still recorded as solo.
	if grant, err := svc.authorizeLayer("orders", User{ID: "priya"}, "kafka", "", false); err != nil || grant != "solo" {
		t.Fatalf("solo rejection = %q, %v", grant, err)
	}
	// Everybody else keeps the normal rule, and their confirm is ignored.
	if _, err := svc.authorizeLayer("orders", User{ID: "arjun"}, "kafka", "orders/kafka", true); err == nil {
		t.Fatal("a non-solo user approved a layer they do not own")
	}
	// /api/me lists every layer for the solo user.
	if got := svc.LayersFor(User{ID: "priya"}); len(got) != 6 {
		t.Fatalf("solo layers = %v, want all six", got)
	}
}

func TestSoloStartCheckWithGroups(t *testing.T) {
	own := ownersFromYAML(t, "layers:\n  postgres: priya\n  kafka: group:oncall\n")
	users := mustUsers([]User{{ID: "priya", Name: "Priya"}})
	if err := CheckSoloUser("priya", users, own); err == nil {
		t.Fatal("solo mode started although owners.yaml names a group")
	}
	own = ownersFromYAML(t, "layers:\n  postgres: priya\n")
	if err := CheckSoloUser("priya", users, own); err != nil {
		t.Fatalf("solo start check: %v", err)
	}
}

func TestLayersFor(t *testing.T) {
	svc := accessService(t, accessOwnersYAML, "", nil)
	got := svc.LayersFor(User{ID: "u_1", Groups: []string{"dataeng-oncall", "consumers"}})
	if len(got) != 2 || got[0] != "kafka" || got[1] != "consumer" {
		t.Fatalf("layers = %v, want [kafka consumer] in layer order", got)
	}
	got = svc.LayersFor(User{ID: "priya", Layers: []string{"postgres"}})
	if len(got) != 1 || got[0] != "postgres" {
		t.Fatalf("layers = %v", got)
	}
	if got := svc.LayersFor(User{ID: "stranger"}); got == nil || len(got) != 0 {
		t.Fatalf("a stranger's layers = %#v, want an empty non-nil slice", got)
	}
}

func TestLookupPersonRules(t *testing.T) {
	sessions := auth.NewMemStore()
	svc := accessService(t, accessOwnersYAML, "", sessions)
	ctx := context.Background()
	now := time.Now().UTC()
	add := func(id, user string, groups []string, created time.Time, revoked bool) {
		t.Helper()
		_, hash, _ := auth.NewToken()
		s := auth.Session{ID: id, TokenHash: hash, UserID: user, Name: "name of " + id, Groups: groups, Method: auth.MethodOIDC,
			CreatedAt: created, LastSeenAt: now, ExpiresAt: now.Add(time.Hour)}
		if err := sessions.Create(ctx, s); err != nil {
			t.Fatal(err)
		}
		if revoked {
			_ = sessions.Revoke(ctx, id, now)
		}
	}
	add("s_old", "u_member", []string{"consumers"}, now.Add(-time.Hour), false)
	add("s_new", "u_member", []string{"dataeng-oncall"}, now.Add(-time.Minute), false)
	add("s_viewer", "u_viewer", []string{"viewers"}, now, false)
	add("s_audit", "u_audit", []string{"security"}, now, false)
	add("s_revoked", "u_revoked", []string{"consumers"}, now, true)
	add("s_downgraded", "u_down", []string{"nothing-any-more"}, now, false)

	u, err := svc.LookupPerson(ctx, "u_member")
	if err != nil || u.ID != "u_member" || u.Name != "name of s_new" || len(u.Groups) != 1 || u.Groups[0] != "dataeng-oncall" {
		t.Fatalf("member = %+v, %v (want the newest session's groups)", u, err)
	}
	if u, err := svc.LookupPerson(ctx, "priya"); err != nil || u.Name != "Priya" {
		t.Fatalf("users.yaml user = %+v, %v", u, err)
	}
	for _, id := range []string{"u_viewer", "u_audit", "u_revoked", "u_down", "u_never", "", "bad id"} {
		_, err := svc.LookupPerson(ctx, id)
		var re *Error
		if !errors.As(err, &re) || re.Code != "validation" || re.Status != 400 {
			t.Errorf("LookupPerson(%q) = %v, want 400 validation", id, err)
		}
	}
	// Idle expiry: the session counts only while it is within the idle limit.
	late := New(Deps{Store: memstore.New(), Users: mustUsers(nil), Owners: svc.owners, Sessions: sessions, SessionIdle: time.Minute,
		Now: func() time.Time { return now.Add(10 * time.Minute) }})
	t.Cleanup(late.Close)
	if _, err := late.LookupPerson(ctx, "u_member"); err == nil {
		t.Error("an idle session was accepted")
	}
}
