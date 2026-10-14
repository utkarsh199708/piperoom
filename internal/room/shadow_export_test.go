package room_test

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// Export of a shadow room: without the flag the document has no shadow member
// and is the plain export; with ?include=shadow the member (with data) is added
// after "schema", inside the hash, and the rest is unchanged.
func TestShadowExport(t *testing.T) {
	env := newShadowEnv(t, shadowCfg{})
	ctx := context.Background()
	snap := env.open(t, "export")
	env.waitFinished(t, snap.ID)

	if _, err := env.svc.PrepareExportShadow(ctx, snap.ID); err == nil {
		t.Fatal("include=shadow on an open room")
	} else {
		wantCode(t, err, "room_open")
	}
	env.close(t, snap.ID, "resolved")
	if _, err := env.svc.PrepareExportShadow(ctx, snap.ID); err == nil {
		t.Fatal("include=shadow before the label")
	} else {
		wantCode(t, err, "not_labelled")
	}
	if _, err := env.svc.ShadowLabel(ctx, env.user(t, "priya"), snap.ID, "kafka-connect", []string{"schema"}); err != nil {
		t.Fatal(err)
	}

	plainSum, plain := exportRoomFor(t, env.svc, snap.ID)
	verifyExport(t, plainSum, plain)
	if bytes.Contains(plain, []byte(`"shadow":{`)) {
		t.Fatal("plain export holds a shadow member")
	}
	for _, secret := range []string{secretHypothesis, secretMessage, secretData} {
		if bytes.Contains(plain, []byte(secret)) {
			t.Fatalf("plain export leaks %q", secret)
		}
	}
	plainAgain, plainBytes := exportRoomFor(t, env.svc, snap.ID)
	if plainAgain != plainSum || !bytes.Equal(plainBytes, plain) {
		t.Fatal("plain export is not stable")
	}

	ex, err := env.svc.PrepareExportShadow(ctx, snap.ID)
	if err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := ex.Write(ctx, &buf); err != nil {
		t.Fatal(err)
	}
	withShadow := buf.Bytes()
	doc := verifyExport(t, ex.SHA256(), withShadow)
	if ex.SHA256() == plainSum {
		t.Fatal("the hash must differ with the shadow member")
	}
	if i, j := bytes.Index(withShadow, []byte(`"schema":`)), bytes.Index(withShadow, []byte(`,"shadow":{"events":`)); i < 0 || j < i {
		t.Fatalf("shadow member must follow schema: %d %d", i, j)
	}
	sh, ok := doc["shadow"].(map[string]any)
	if !ok || sh["run"] == nil || sh["events"] == nil {
		t.Fatalf("shadow member = %v", doc["shadow"])
	}
	if !strings.Contains(string(withShadow), secretData) || !strings.Contains(string(withShadow), secretHypothesis) {
		t.Fatal("shadow member must carry the run with tool_result.data")
	}

	// Everything except integrity and shadow is identical to the plain export.
	var a, b map[string]json.RawMessage
	if err := json.Unmarshal(plain, &a); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(withShadow, &b); err != nil {
		t.Fatal(err)
	}
	delete(a, "integrity")
	delete(b, "integrity")
	delete(b, "shadow")
	if len(a) != len(b) {
		t.Fatalf("members differ: %d vs %d", len(a), len(b))
	}
	for k, v := range a {
		if !bytes.Equal(v, b[k]) {
			t.Fatalf("member %q differs with the flag", k)
		}
	}

	arjun := env.user(t, "arjun")
	live, err := env.svc.CreateRoom(ctx, arjun, "live", "sev2", "live")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := env.svc.PrepareExportShadow(ctx, live.ID); err == nil {
		t.Fatal("include=shadow on a live room")
	} else {
		wantCode(t, err, "not_found")
	}
}
