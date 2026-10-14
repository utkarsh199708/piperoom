package room

import (
	"context"
	"reflect"
	"testing"
	"time"

	"piperoom/internal/auth"
)

// GET /api/people carries each person's approvable layers (F9): users.yaml
// users from LayersFor, IdP members through their session's groups, [] (never
// nil) for nobody.
func TestPeopleLayers(t *testing.T) {
	sessions := auth.NewMemStore()
	svc := accessService(t, accessOwnersYAML, "", sessions)
	ctx := context.Background()
	now := time.Now().UTC()
	add := func(user, name string, groups []string) {
		t.Helper()
		_, hash, _ := auth.NewToken()
		if err := sessions.Create(ctx, auth.Session{ID: "s_" + user, TokenHash: hash, UserID: user, Name: name, Groups: groups,
			Method: auth.MethodOIDC, CreatedAt: now, LastSeenAt: now, ExpiresAt: now.Add(time.Hour)}); err != nil {
			t.Fatal(err)
		}
	}
	add("u_a", "Ann", []string{"dataeng-oncall"})
	add("u_b", "Bea", []string{"consumers", "dataeng-oncall"})
	add("u_c", "Cy", []string{"consumers", "viewers"}) // member via consumers

	got, err := svc.People(ctx)
	if err != nil {
		t.Fatal(err)
	}
	layers := map[string][]string{}
	for _, p := range got {
		if p.Layers == nil {
			t.Errorf("%s: layers is nil, want []", p.ID)
		}
		layers[p.ID] = p.Layers
	}
	want := map[string][]string{
		"arjun": {"kafka"}, "priya": {"postgres"},
		"u_a": {"kafka"}, "u_b": {"kafka", "consumer"}, "u_c": {"consumer"},
	}
	for id, w := range want {
		if !reflect.DeepEqual(layers[id], w) {
			t.Errorf("%s layers = %v, want %v", id, layers[id], w)
		}
	}
}
