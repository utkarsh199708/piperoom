package room

import (
	"context"
	"testing"
	"time"

	"piperoom/internal/auth"
	"piperoom/internal/store/memstore"
)

// The users.yaml layers fallback fails closed like owners.CanApprove: a
// non-empty pipeline id that matches no pipeline grants nothing, a known one
// with no override still does.
func TestUserLayersFallbackFailsClosed(t *testing.T) {
	svc := accessService(t, accessOwnersYAML, "", nil)
	u := User{ID: "x", Layers: []string{"warehouse"}}
	if _, err := svc.authorizeLayer("renamed-pipeline", u, "warehouse", "", false); err == nil {
		t.Fatal("an unknown pipeline granted a users.yaml layer")
	}
	if g, err := svc.authorizeLayer("orders", u, "warehouse", "", false); err != nil || g != "user:x" {
		t.Fatalf("known pipeline, no override: %q, %v", g, err)
	}
	if g, err := svc.authorizeLayer("", u, "warehouse", "", false); err != nil || g != "user:x" {
		t.Fatalf("empty pipeline id: %q, %v", g, err)
	}
	// With no owners config at all (tests, --demo) the fallback is the only source.
	bare := New(Deps{Store: memstore.New(), Users: mustUsers(nil)})
	t.Cleanup(bare.Close)
	if g, err := bare.authorizeLayer("anything", u, "warehouse", "", false); err != nil || g != "user:x" {
		t.Fatalf("no owners config: %q, %v", g, err)
	}
}

func TestParseUsersEmailAndIDRules(t *testing.T) {
	const kelvin, longS = "K", "ſ"
	good := "users:\n  - id: ksenia\n    name: K\n    email: ksenia@example.com\n"
	us, err := ParseUsers([]byte(good))
	if err != nil {
		t.Fatal(err)
	}
	if u, ok := us.ByEmail("KSENIA@Example.COM"); !ok || u.ID != "ksenia" {
		t.Fatalf("ASCII case-insensitive match failed: %v %v", u, ok)
	}
	for name, email := range map[string]string{
		"kelvin sign": kelvin + "senia@example.com",
		"long s":      "k" + longS + "enia@example.com",
		"empty":       "",
		"no match":    "other@example.com",
	} {
		if u, ok := us.ByEmail(email); ok {
			t.Errorf("%s: ByEmail(%q) matched %q", name, email, u.ID)
		}
	}
	// users.yaml itself takes ASCII emails only, and two emails that differ only
	// in ASCII case are the same address.
	if _, err := ParseUsers([]byte("users:\n  - id: a\n    email: " + kelvin + "a@example.com\n")); err == nil {
		t.Error("a non-ASCII email in users.yaml was accepted")
	}
	if _, err := ParseUsers([]byte("users:\n  - id: a\n    email: A@x.com\n  - id: b\n    email: a@X.com\n")); err == nil {
		t.Error("two emails that differ only in ASCII case were accepted")
	}
	// The u_ prefix is the id space of IdP users without an entry.
	for _, id := range []string{"u_abcdef012345", "u_x"} {
		if _, err := ParseUsers([]byte("users:\n  - id: " + id + "\n")); err == nil {
			t.Errorf("users.yaml id %q was accepted", id)
		}
	}
	if _, err := ParseUsers([]byte("users:\n  - id: uu_ok\n  - id: user\n")); err != nil {
		t.Errorf("ids that merely contain u_: %v", err)
	}
}

func TestPeople(t *testing.T) {
	sessions := auth.NewMemStore()
	svc := accessService(t, accessOwnersYAML, "", sessions)
	ctx := context.Background()
	now := time.Now().UTC()
	add := func(user, name, email string, groups []string) {
		t.Helper()
		_, hash, _ := auth.NewToken()
		if err := sessions.Create(ctx, auth.Session{ID: "s_" + user, TokenHash: hash, UserID: user, Name: name, Email: email, Groups: groups,
			Method: auth.MethodOIDC, CreatedAt: now, LastSeenAt: now, ExpiresAt: now.Add(time.Hour)}); err != nil {
			t.Fatal(err)
		}
	}
	add("u_b", "Bea", "bea@example.com", []string{"consumers"})
	add("u_a", "Ann", "", []string{"dataeng-oncall"})
	add("u_v", "Vic", "vic@example.com", []string{"viewers"})
	add("u_s", "Sec", "sec@example.com", []string{"security"})
	add("priya", "Priya at IdP", "priya@idp.example", nil) // already a users.yaml entry: listed once, from the file
	got, err := svc.People(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var ids []string
	for _, p := range got {
		ids = append(ids, p.ID)
	}
	want := []string{"arjun", "priya", "u_a", "u_b"} // file order, then IdP members by name
	if len(ids) != len(want) {
		t.Fatalf("people = %v, want %v", ids, want)
	}
	for i := range want {
		if ids[i] != want[i] {
			t.Fatalf("people = %v, want %v", ids, want)
		}
	}
	if got[1].Name != "Priya" || got[1].Email != "" || got[3].Email != "bea@example.com" || got[2].Email != "" {
		t.Fatalf("names/emails = %+v", got)
	}
	// No session store: users.yaml only.
	bare := accessService(t, accessOwnersYAML, "", nil)
	if got, err := bare.People(ctx); err != nil || len(got) != 2 {
		t.Fatalf("no sessions: %v, %v", got, err)
	}
}
