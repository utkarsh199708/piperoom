package httpapi_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"strings"
	"testing"

	"piperoom/internal/room"
)

const exportHeader = "X-Piperoom-Export-SHA256"

// getExport fetches the export and returns status, header hash and body.
func getExport(t *testing.T, srvURL, roomID, user string) (*http.Response, []byte) {
	t.Helper()
	resp := doJSON(t, http.MethodGet, srvURL+"/api/rooms/"+roomID+"/export", user, "")
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read export body: %v", err)
	}
	return resp, body
}

func createRoomHTTP(t *testing.T, srvURL, user string) string {
	t.Helper()
	resp := doJSON(t, http.MethodPost, srvURL+"/api/rooms", user, `{"title":"Orders stale","severity":"sev2","pipeline_id":"orders"}`)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create room: %d", resp.StatusCode)
	}
	return decode[map[string]any](t, resp)["id"].(string)
}

func TestExportShapeHeadersAndHash(t *testing.T) {
	srv, _ := newServer(t)
	roomID := createRoomHTTP(t, srv.URL, "arjun")
	doJSON(t, http.MethodPost, srv.URL+"/api/rooms/"+roomID+"/comments", "kabir", `{"text":"hello @agent"}`).Body.Close()
	doJSON(t, http.MethodPost, srv.URL+"/api/rooms/"+roomID+"/handoffs", "arjun", `{"to":"priya"}`).Body.Close()

	// kabir is neither the driver nor a layer owner of anything relevant:
	// any user in users.yaml may export.
	resp, body := getExport(t, srv.URL, roomID, "kabir")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status %d: %s", resp.StatusCode, body)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("content type %q", ct)
	}
	disp, params, err := mime.ParseMediaType(resp.Header.Get("Content-Disposition"))
	if err != nil || disp != "attachment" || !strings.HasSuffix(params["filename"], ".json") || !strings.Contains(params["filename"], roomID) {
		t.Fatalf("bad Content-Disposition %q (%v)", resp.Header.Get("Content-Disposition"), err)
	}
	if resp.Header.Get("Cache-Control") != "no-store" {
		t.Fatalf("export must not be cacheable, got %q", resp.Header.Get("Cache-Control"))
	}
	sum := resp.Header.Get(exportHeader)
	if len(sum) != 64 {
		t.Fatalf("bad %s header %q", exportHeader, sum)
	}

	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var doc map[string]any
	if err := dec.Decode(&doc); err != nil {
		t.Fatalf("body is not JSON: %v", err)
	}
	for _, k := range []string{"schema", "room", "events", "event_count", "proposals", "approvals", "action_results", "handoffs", "integrity"} {
		if _, ok := doc[k]; !ok {
			t.Errorf("export lacks %q", k)
		}
	}
	if doc["integrity"].(map[string]any)["sha256"] != sum {
		t.Fatalf("integrity.sha256 != header")
	}
	if len(doc["handoffs"].([]any)) != 1 || len(doc["events"].([]any)) < 4 {
		t.Fatalf("missing content: %d handoffs, %d events", len(doc["handoffs"].([]any)), len(doc["events"].([]any)))
	}

	// Recompute the hash the way the contract says.
	delete(doc, "integrity")
	canon, err := room.CanonicalJSON(doc)
	if err != nil {
		t.Fatal(err)
	}
	h := sha256.Sum256(canon)
	if hex.EncodeToString(h[:]) != sum {
		t.Fatalf("hash of canonical document != header")
	}
}

func TestExportHashStableThenChanges(t *testing.T) {
	srv, _ := newServer(t)
	roomID := createRoomHTTP(t, srv.URL, "arjun")

	r1, b1 := getExport(t, srv.URL, roomID, "arjun")
	r2, b2 := getExport(t, srv.URL, roomID, "priya") // a different caller must not change the bytes
	if r1.Header.Get(exportHeader) != r2.Header.Get(exportHeader) || !bytes.Equal(b1, b2) {
		t.Fatalf("same room must export identically")
	}

	doJSON(t, http.MethodPost, srv.URL+"/api/rooms/"+roomID+"/comments", "priya", `{"text":"new evidence"}`).Body.Close()
	r3, _ := getExport(t, srv.URL, roomID, "arjun")
	if r3.Header.Get(exportHeader) == r1.Header.Get(exportHeader) {
		t.Fatalf("hash must change after a new event")
	}
}

func TestExportUnknownRoomIs404(t *testing.T) {
	srv, _ := newServer(t)
	resp, body := getExport(t, srv.URL, "r_missing", "arjun")
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("want 404, got %d", resp.StatusCode)
	}
	var e map[string]map[string]string
	if err := json.Unmarshal(body, &e); err != nil || e["error"]["code"] != "not_found" {
		t.Fatalf("want the JSON error envelope, got %s", body)
	}
	if resp.Header.Get(exportHeader) != "" || resp.Header.Get("Content-Disposition") != "" {
		t.Fatalf("an error must not look like a download")
	}
}

func TestExportRequiresKnownUser(t *testing.T) {
	srv, _ := newServer(t)
	roomID := createRoomHTTP(t, srv.URL, "arjun")
	for _, user := range []string{"", "nobody"} {
		resp, body := getExport(t, srv.URL, roomID, user)
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("user %q: want 401, got %d", user, resp.StatusCode)
		}
		if bytes.Contains(body, []byte(roomID)) || resp.Header.Get(exportHeader) != "" {
			t.Fatalf("user %q: an unauthenticated call must leak nothing: %s", user, body)
		}
	}
}

func TestExportUnknownRoomUnauthenticatedIs401NotLeak(t *testing.T) {
	srv, _ := newServer(t)
	resp, _ := getExport(t, srv.URL, "r_missing", "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("auth must be checked before the room lookup, got %d", resp.StatusCode)
	}
}
