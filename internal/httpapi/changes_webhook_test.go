package httpapi_test

import (
	"net/http"
	"testing"
	"time"

	"piperoom/internal/alertin"
	"piperoom/internal/httpapi"
	"piperoom/internal/owners"
	"piperoom/internal/store/memstore"
)

// POST /api/changes/webhook (X4) is mounted outside the session group and,
// like the signed alert routes, skips the Host check: a signed request from an
// in-cluster name with no X-User is 202, an unsigned one reaches the verifier (401).
func TestChangesWebhookMounted(t *testing.T) {
	h := alertin.NewChangeWebhook(alertin.ChangeWebhookConfig{
		Secret: "change-secret",
		Owners: &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders"}}},
		Writer: memstore.New(),
	})
	e := newAuthEnv(t, httpapi.WithChangesWebhook(h))
	body := `{"pipeline_id":"orders","kind":"deploy","at":"` + time.Now().UTC().Add(-time.Minute).Format(time.RFC3339) +
		`","title":"checkout-api v2","service":"checkout-api"}`
	sig := alertin.SignGeneric("change-secret", time.Now(), []byte(body))
	wantStatus(t, e.do(t, http.MethodPost, alertin.ChangeWebhookPath, body, "Host: argocd.argocd.svc", alertin.GenericSignatureHeader+": "+sig), http.StatusAccepted)

	resp := e.do(t, http.MethodPost, alertin.ChangeWebhookPath, body, "Host: argocd.argocd.svc")
	wantStatus(t, resp, http.StatusUnauthorized)
	if b := readErr(t, resp); b.Error.Code != "unauthenticated" {
		t.Fatalf("code %q, want unauthenticated from the verifier", b.Error.Code)
	}
	// Only POST of that exact path is exempt.
	wantStatus(t, e.do(t, http.MethodGet, alertin.ChangeWebhookPath, "", "Host: evil.test"), http.StatusMisdirectedRequest)
}
