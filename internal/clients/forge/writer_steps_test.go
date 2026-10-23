package forge

import (
	"errors"
	"net/http"
	"strings"
	"testing"
)

func TestOpenPullRequestStepFailures(t *testing.T) {
	steps := map[string]string{"blob": "/git/blobs", "tree": "/git/trees", "commit": "/git/commits", "ref": "/git/refs"}
	for name, suffix := range steps {
		t.Run(name, func(t *testing.T) {
			g, base := newGH(t, baseFiles())
			w, _ := NewWriter(g.client(t))
			g.setHook(func(w http.ResponseWriter, r *http.Request) bool {
				if r.Method == "POST" && strings.HasSuffix(r.URL.Path, suffix) {
					ghErr(w, 500, "boom")
					return true
				}
				return false
			})
			if _, err := w.OpenPullRequest(ctx10(t), tRepo, fixReq(base, "README.md", "x\n")); err == nil {
				t.Fatal("no error")
			}
			if len(g.pulls) != 0 || g.branches[tBranch] != "" || g.count("DELETE", `.`) != 0 {
				t.Fatal("wrote past the failed step")
			}
		})
	}
	// The ref appears between the check and the create (a concurrent call).
	g, base := newGH(t, baseFiles())
	w, _ := NewWriter(g.client(t))
	g.setHook(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/git/refs") {
			ghErr(w, 422, "Reference already exists")
			return true
		}
		return false
	})
	if _, err := w.OpenPullRequest(ctx10(t), tRepo, fixReq(base, "README.md", "x\n")); !errors.Is(err, ErrBranchExists) {
		t.Fatalf("race: %v", err)
	}
	// A blob answer that is not the content sent is refused.
	g2, base2 := newGH(t, baseFiles())
	w2, _ := NewWriter(g2.client(t))
	g2.setHook(func(w http.ResponseWriter, r *http.Request) bool {
		if r.Method == "POST" && strings.HasSuffix(r.URL.Path, "/git/blobs") {
			writeJSON(w, 201, map[string]string{"sha": strings.Repeat("b", 40)})
			return true
		}
		return false
	})
	if _, err := w2.OpenPullRequest(ctx10(t), tRepo, fixReq(base2, "README.md", "x\n")); err == nil || !strings.Contains(err.Error(), "not the content sent") {
		t.Fatalf("blob mismatch: %v", err)
	}
}
