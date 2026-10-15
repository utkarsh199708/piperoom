package schemareg

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"piperoom/internal/clients/netsec"
)

// TestDotSegmentSubjectsRefused: PUT /config/. is PUT /config after dot
// segment removal, which sets the registry's GLOBAL compatibility level.
// "", "." and ".." are refused before any request.
func TestDotSegmentSubjectsRefused(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.Write([]byte(`{"compatibilityLevel":"NONE"}`))
	}))
	defer srv.Close()
	c := New(srv.URL, srv.Client())
	ctx := context.Background()
	for _, s := range []string{"", ".", ".."} {
		calls := map[string]error{}
		_, calls["Versions"] = c.Versions(ctx, s)
		_, calls["Schema"] = c.Schema(ctx, s, 1)
		_, _, calls["Compatibility"] = c.Compatibility(ctx, s)
		calls["SetCompatibility"] = c.SetCompatibility(ctx, s, "NONE")
		_, calls["Register"] = c.Register(ctx, s, `"string"`)
		for m, err := range calls {
			if !errors.Is(err, netsec.ErrBadPath) {
				t.Errorf("%s(%q) = %v, want netsec.ErrBadPath", m, s, err)
			}
		}
	}
	if hits != 0 {
		t.Fatalf("%d requests sent for refused subjects", hits)
	}
}
