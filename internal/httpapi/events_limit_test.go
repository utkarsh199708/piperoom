package httpapi_test

import (
	"net/http"
	"testing"
)

// TestEventsLimitQuery: KAN-395. GET /api/rooms/{id}/events?limit=0 is an
// empty page (200, events [], the room's last_seq), not the 500-event default;
// an absent limit still means up to 500; a negative or non-numeric limit is
// 400 validation; a limit over 1000 is accepted and clamped by the service.
func TestEventsLimitQuery(t *testing.T) {
	srv, _ := newServer(t)
	resp := doJSON(t, http.MethodPost, srv.URL+"/api/rooms", "arjun", `{"title":"t","severity":"sev2","pipeline_id":"orders"}`)
	created := decode[map[string]any](t, resp)
	roomID, _ := created["id"].(string)
	head, ok := created["last_seq"].(float64)
	if roomID == "" || !ok || head == 0 {
		t.Fatalf("bad create response: %+v", created)
	}

	cases := []struct {
		name       string
		query      string
		wantStatus int
		wantCode   string // error code for a refused request
		wantEvents int
	}{
		{"limit zero is an empty page", "?limit=0", http.StatusOK, "", 0},
		{"limit zero with after", "?after=1&limit=0", http.StatusOK, "", 0},
		{"absent limit", "", http.StatusOK, "", 2},
		{"limit at the cap", "?limit=1000", http.StatusOK, "", 2},
		{"limit over the cap is accepted", "?limit=1001", http.StatusOK, "", 2},
		{"negative limit", "?limit=-1", http.StatusBadRequest, "validation", 0},
		{"limit is not a number", "?limit=abc", http.StatusBadRequest, "validation", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := doJSON(t, http.MethodGet, srv.URL+"/api/rooms/"+roomID+"/events"+tc.query, "arjun", "")
			if resp.StatusCode != tc.wantStatus {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.wantStatus)
			}
			body := decode[map[string]any](t, resp)
			if tc.wantCode != "" {
				e, _ := body["error"].(map[string]any)
				if e == nil || e["code"] != tc.wantCode {
					t.Fatalf("error = %v, want code %q", body["error"], tc.wantCode)
				}
				return
			}
			evs, ok := body["events"].([]any)
			if !ok {
				t.Fatalf("events must be a JSON array, got %#v", body["events"])
			}
			if len(evs) != tc.wantEvents {
				t.Fatalf("events = %d, want %d", len(evs), tc.wantEvents)
			}
			if body["last_seq"].(float64) != head {
				t.Fatalf("last_seq = %v, want the room's %v", body["last_seq"], head)
			}
		})
	}
}
