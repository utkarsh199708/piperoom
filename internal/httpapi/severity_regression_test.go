package httpapi_test

import (
	"net/http"
	"testing"
)

// TestCreateRoomRejectsMissingOrInvalidSeverity pins the 400 the service has
// always returned for a severity outside sev1..sev4. KAN-397 was filed against
// a build where a missing severity was accepted; these cases keep it refused.
func TestCreateRoomRejectsMissingOrInvalidSeverity(t *testing.T) {
	srv, _ := newServer(t)
	cases := map[string]string{
		"omitted": `{"title":"no-severity","pipeline_id":"orders"}`,
		"null":    `{"title":"null-severity","pipeline_id":"orders","severity":null}`,
		"empty":   `{"title":"empty-severity","pipeline_id":"orders","severity":""}`,
		"sev5":    `{"title":"sev5","pipeline_id":"orders","severity":"sev5"}`,
		"upper":   `{"title":"SEV1","pipeline_id":"orders","severity":"SEV1"}`,
		"space":   `{"title":"space","pipeline_id":"orders","severity":" sev1"}`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			resp := doJSON(t, http.MethodPost, srv.URL+"/api/rooms", "arjun", body)
			if resp.StatusCode != http.StatusBadRequest {
				resp.Body.Close()
				t.Fatalf("status = %d, want 400", resp.StatusCode)
			}
			got := decode[map[string]any](t, resp)
			errObj, ok := got["error"].(map[string]any)
			if !ok || errObj["code"] != "validation" {
				t.Fatalf("error = %+v, want code validation", got["error"])
			}
		})
	}

	// None of the refused requests may leave a room behind.
	resp := doJSON(t, http.MethodGet, srv.URL+"/api/rooms", "arjun", "")
	list := decode[[]map[string]any](t, resp)
	if len(list) != 0 {
		t.Fatalf("refused creates left %d rooms: %+v", len(list), list)
	}
}

// TestRoomListNeverHasEmptySeverity checks every creation path the HTTP API
// exposes: a manual room and an alert webhook that names no severity. The
// alert path defaults (sev3); the manual path refuses. No listed room may have
// an empty severity, which would render as a blank badge.
func TestRoomListNeverHasEmptySeverity(t *testing.T) {
	srv, _ := newServer(t)

	resp := doJSON(t, http.MethodPost, srv.URL+"/api/rooms", "arjun",
		`{"title":"manual","severity":"sev2","pipeline_id":"orders"}`)
	if resp.StatusCode != http.StatusCreated {
		resp.Body.Close()
		t.Fatalf("manual create status = %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp = doJSON(t, http.MethodPost, srv.URL+"/api/alerts/webhook", "arjun",
		`{"title":"lag on orders","pipeline_id":"orders"}`)
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		resp.Body.Close()
		t.Fatalf("alert create status = %d", resp.StatusCode)
	}
	resp.Body.Close()

	resp = doJSON(t, http.MethodGet, srv.URL+"/api/rooms", "arjun", "")
	list := decode[[]map[string]any](t, resp)
	if len(list) != 2 {
		t.Fatalf("want 2 rooms, got %d", len(list))
	}
	for _, r := range list {
		sev, _ := r["severity"].(string)
		switch sev {
		case "sev1", "sev2", "sev3", "sev4":
		default:
			t.Fatalf("room %v has severity %q, want sev1..sev4", r["id"], sev)
		}
	}
}
