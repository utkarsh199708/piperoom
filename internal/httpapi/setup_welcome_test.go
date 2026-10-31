package httpapi_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/httpapi"
	"piperoom/internal/setup/protocol"
)

// The Welcome a setup room's runner reads carries the room's targets, the
// public origin and the same fingerprint POST /api/setup returned, so the
// in-cluster runner can register its target driver and the person can
// compare the fingerprint (docs/api.md, docs/design/setup-strimzi.md §3.2).
func TestSetupWelcomeCarriesTargetsAndFingerprint(t *testing.T) {
	origin, err := httpapi.ParsePublicURL("https://piperoom.example.com")
	if err != nil {
		t.Fatal(err)
	}
	env := newSetupAPIEnv(t, true, httpapi.WithPublicOrigin(origin))
	resp := doJSON(t, http.MethodPost, env.srv.URL+"/api/setup", "alice", setupAPIBody)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var out struct {
		Code        string `json:"code"`
		Fingerprint string `json:"fingerprint"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if len(out.Fingerprint) != 12 {
		t.Fatalf("fingerprint = %q, want 12 hex characters", out.Fingerprint)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(env.srv.URL, "http")+"/api/runners/ws", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	if err := writeRunnerFrame(ctx, conn, protocol.Frame{Type: protocol.FramePair, Pair: &protocol.Pair{
		Code: out.Code, ProtocolVersion: protocol.CurrentMajor, ProtocolMinor: protocol.CurrentMinor,
	}}); err != nil {
		t.Fatal(err)
	}
	f, err := readRunnerFrame(ctx, conn)
	if err != nil || f.Welcome == nil {
		t.Fatalf("welcome: %+v, %v", f, err)
	}
	w := f.Welcome
	if w.PublicOrigin != "https://piperoom.example.com" {
		t.Errorf("public_origin = %q", w.PublicOrigin)
	}
	if w.Fingerprint != out.Fingerprint {
		t.Errorf("welcome fingerprint %q != POST /api/setup fingerprint %q", w.Fingerprint, out.Fingerprint)
	}
	if len(w.Targets) != 1 || w.Targets[0].Kind != "postgres" || w.Targets[0].Address != "db.acme:5432" || w.Targets[0].Class != "shared" {
		t.Errorf("targets = %+v", w.Targets)
	}
}

// Two rooms never share a fingerprint, even with the same pipeline name and
// creator: the nonce drawn with each code differs.
func TestSetupFingerprintDiffersPerCode(t *testing.T) {
	env := newSetupAPIEnv(t, true)
	fp := func(body string) string {
		resp := doJSON(t, http.MethodPost, env.srv.URL+"/api/setup", "alice", body)
		defer resp.Body.Close()
		var out struct {
			Fingerprint string `json:"fingerprint"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
			t.Fatal(err)
		}
		return out.Fingerprint
	}
	a := fp(setupAPIBody)
	b := fp(strings.Replace(setupAPIBody, "orders-setup", "orders-setup-2", 1))
	if a == "" || a == b {
		t.Fatalf("fingerprints %q and %q", a, b)
	}
}
