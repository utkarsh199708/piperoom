package httpapi_test

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
)

// Lead decision: for the solo user a stale params_hash (409) is reported before
// a missing or wrong confirm text (400).
func TestSoloStaleHashIsReportedBeforeTheConfirm(t *testing.T) {
	srv, svc, _, newProposal := devApprovalEnv(t, "priya")
	pid := newProposal("kafka")
	hash := hashOfOpen(t, svc, pid)
	post := func(body string) (int, string) {
		resp := doJSON(t, http.MethodPost, srv.URL+"/api/proposals/"+pid+"/approve", "priya", body)
		status := resp.StatusCode
		e := decode[struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}](t, resp)
		return status, e.Error.Code
	}
	stale := strings.Repeat("0", 64)

	if st, code := post(fmt.Sprintf(`{"layer":"kafka","params_hash":%q}`, stale)); st != http.StatusConflict || code != "params_mismatch" {
		t.Fatalf("stale hash, no confirm = %d %s, want 409 params_mismatch", st, code)
	}
	if st, code := post(fmt.Sprintf(`{"layer":"kafka","params_hash":%q,"confirm":"orders/kafka"}`, stale)); st != http.StatusConflict || code != "params_mismatch" {
		t.Fatalf("stale hash, right confirm = %d %s, want 409 params_mismatch", st, code)
	}
	// With the right hash the confirm is checked as before.
	if st, code := post(fmt.Sprintf(`{"layer":"kafka","params_hash":%q}`, hash)); st != http.StatusBadRequest || code != "validation" {
		t.Fatalf("right hash, no confirm = %d %s, want 400 validation", st, code)
	}
	if st, _ := post(fmt.Sprintf(`{"layer":"kafka","params_hash":%q,"confirm":"orders/kafka"}`, hash)); st != http.StatusOK {
		t.Fatalf("right hash and confirm = %d, want 200", st)
	}
}
