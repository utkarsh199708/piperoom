package connect

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"piperoom/internal/clients/netsec"
)

// TestDotSegmentNamesRefused: url.PathEscape leaves "." and ".." alone, so a
// connector named ".." reached GET /connectors/../config, which a server
// that collapses dot segments routes to another resource. Such names (and
// "") must be refused before any request.
func TestDotSegmentNamesRefused(t *testing.T) {
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.Write([]byte(`{}`))
	}))
	defer srv.Close()
	c := New(srv.URL, srv.Client())
	ctx := context.Background()
	for _, name := range []string{"", ".", ".."} {
		calls := map[string]error{}
		_, calls["Status"] = c.Status(ctx, name)
		_, calls["Config"] = c.Config(ctx, name)
		calls["RestartTask"] = c.RestartTask(ctx, name, 0)
		calls["RestartConnector"] = c.RestartConnector(ctx, name)
		calls["Resume"] = c.Resume(ctx, name)
		calls["UpdateConfig"] = c.UpdateConfig(ctx, name, "k", "v")
		for m, err := range calls {
			if !errors.Is(err, netsec.ErrBadPath) {
				t.Errorf("%s(%q) = %v, want netsec.ErrBadPath", m, name, err)
			}
		}
	}
	if hits != 0 {
		t.Fatalf("%d requests sent for refused names", hits)
	}
	if _, err := c.Config(ctx, "a.b..c"); err != nil {
		t.Fatalf("a name with dots inside must pass: %v", err)
	}
}
