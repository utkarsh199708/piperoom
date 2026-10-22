package alertin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"
	"unicode"

	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/owners"
	"piperoom/internal/store/memstore"
)

const changeSecret = "change-secret"

var changeNow = time.Date(2026, 10, 20, 14, 5, 0, 0, time.UTC)

type changeEnv struct {
	h     *ChangeWebhook
	store *memstore.Store
	logs  *bytes.Buffer
	now   time.Time
}

func newChangeEnv(t *testing.T, secret string) *changeEnv {
	t.Helper()
	e := &changeEnv{store: memstore.New(), logs: &bytes.Buffer{}, now: changeNow}
	e.h = NewChangeWebhook(ChangeWebhookConfig{
		Secret: secret,
		Owners: &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders"}, {ID: "billing"}}},
		Writer: e.store,
		Log:    slog.New(slog.NewTextHandler(e.logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Now:    func() time.Time { return e.now },
	})
	return e
}

// post sends body signed with secret at signedAt (the zero time = e.now).
func (e *changeEnv) post(t *testing.T, body []byte, secret string, signedAt time.Time) *httptest.ResponseRecorder {
	t.Helper()
	if signedAt.IsZero() {
		signedAt = e.now
	}
	r := httptest.NewRequest(http.MethodPost, ChangeWebhookPath, bytes.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	if secret != "" {
		r.Header.Set(GenericSignatureHeader, SignGeneric(secret, signedAt, body))
	}
	w := httptest.NewRecorder()
	e.h.ServeHTTP(w, r)
	return w
}

func (e *changeEnv) rows(t *testing.T, pipeline string) []clients.PipelineChange {
	t.Helper()
	got, err := e.store.ListChanges(context.Background(), pipeline, time.Time{}, e.now.Add(time.Hour), nil, 100)
	if err != nil {
		t.Fatal(err)
	}
	return got
}

func changeBody(t *testing.T, mut func(m map[string]any)) []byte {
	t.Helper()
	m := map[string]any{
		"pipeline_id": "orders", "kind": "deploy", "at": "2026-10-20T14:03:00Z",
		"title": "checkout-api v1.42.0", "service": "checkout-api",
		"url": "https://ci.example.com/runs/1", "actor": "octocat", "id": "run-1",
	}
	if mut != nil {
		mut(m)
	}
	b, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func errorCode(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var env struct {
		Error struct{ Code string } `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &env); err != nil {
		t.Fatalf("body %q: %v", w.Body.String(), err)
	}
	return env.Error.Code
}

// TC1, E1: a valid signature stores one row mapped as docs/api.md says.
func TestChangeWebhookStoresSignedChange(t *testing.T) {
	e := newChangeEnv(t, changeSecret)
	w := e.post(t, changeBody(t, nil), changeSecret, time.Time{})
	if w.Code != http.StatusAccepted || strings.TrimSpace(w.Body.String()) != `{"stored":true}` {
		t.Fatalf("got %d %s", w.Code, w.Body)
	}
	rows := e.rows(t, "orders")
	if len(rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(rows))
	}
	c := rows[0]
	if c.Kind != clients.ChangeDeploy || c.Object != "checkout-api" || c.Source != clients.ChangeSourceWebhook ||
		c.Actor != "octocat" || c.Summary != "checkout-api v1.42.0" || !c.At.Equal(time.Date(2026, 10, 20, 14, 3, 0, 0, time.UTC)) ||
		len(c.Before) != 0 || c.Fingerprint == "" {
		t.Fatalf("row = %+v", c)
	}
	var ext clients.ExternalChange
	if err := json.Unmarshal(c.After, &ext); err != nil || ext != (clients.ExternalChange{URL: "https://ci.example.com/runs/1", Service: "checkout-api"}) {
		t.Fatalf("after = %s (%v)", c.After, err)
	}
	if !strings.Contains(e.logs.String(), "pipeline=orders kind=deploy stored=true") {
		t.Fatalf("log = %s", e.logs)
	}
}

// TC1, E2: bad, missing, stale signatures and a disabled route are 401 and store nothing.
func TestChangeWebhookRefusesUnsigned(t *testing.T) {
	body := changeBody(t, nil)
	for name, tc := range map[string]struct {
		secret, sign string
		at           time.Time
	}{
		"wrong secret":      {changeSecret, "other", time.Time{}},
		"no signature":      {changeSecret, "", time.Time{}},
		"replay too old":    {changeSecret, changeSecret, changeNow.Add(-6 * time.Minute)},
		"from the future":   {changeSecret, changeSecret, changeNow.Add(6 * time.Minute)},
		"no secret set":     {"", changeSecret, time.Time{}},
		"only blank secret": {" , ", changeSecret, time.Time{}},
	} {
		t.Run(name, func(t *testing.T) {
			e := newChangeEnv(t, tc.secret)
			w := e.post(t, body, tc.sign, tc.at)
			if w.Code != http.StatusUnauthorized || errorCode(t, w) != "unauthenticated" {
				t.Fatalf("got %d %s", w.Code, w.Body)
			}
			if n := len(e.rows(t, "orders")); n != 0 {
				t.Fatalf("rows = %d", n)
			}
		})
	}
	t.Run("tampered body", func(t *testing.T) {
		e := newChangeEnv(t, changeSecret)
		r := httptest.NewRequest(http.MethodPost, ChangeWebhookPath, bytes.NewReader(changeBody(t, func(m map[string]any) { m["title"] = "other" })))
		r.Header.Set(GenericSignatureHeader, SignGeneric(changeSecret, changeNow, body))
		w := httptest.NewRecorder()
		e.h.ServeHTTP(w, r)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("got %d", w.Code)
		}
	})
	t.Run("no writer", func(t *testing.T) {
		h := NewChangeWebhook(ChangeWebhookConfig{Secret: changeSecret, Owners: &owners.Owners{}})
		if h.Enabled() {
			t.Fatal("a webhook without a writer must be disabled")
		}
	})
}

// Rotation: either comma-separated secret verifies.
func TestChangeWebhookRotation(t *testing.T) {
	e := newChangeEnv(t, "new-secret, old-secret")
	for i, s := range []string{"new-secret", "old-secret"} {
		w := e.post(t, changeBody(t, func(m map[string]any) { m["id"] = fmt.Sprint("run-", i) }), s, time.Time{})
		if w.Code != http.StatusAccepted {
			t.Fatalf("%s: got %d %s", s, w.Code, w.Body)
		}
	}
	if w := e.post(t, changeBody(t, nil), "older-secret", time.Time{}); w.Code != http.StatusUnauthorized {
		t.Fatalf("a retired secret: got %d", w.Code)
	}
}

// TC1, E4: a body over 4 KB is 413 before anything is read past the cap,
// whether or not the sender declared its length.
func TestChangeWebhookBodyCap(t *testing.T) {
	e := newChangeEnv(t, changeSecret)
	big := changeBody(t, func(m map[string]any) { m["title"] = strings.Repeat("x", config.ChangesWebhookMaxBodyBytes) })
	if w := e.post(t, big, changeSecret, time.Time{}); w.Code != http.StatusRequestEntityTooLarge || errorCode(t, w) != "too_large" {
		t.Fatalf("declared length: got %d %s", w.Code, w.Body)
	}
	r := httptest.NewRequest(http.MethodPost, ChangeWebhookPath, struct{ *bytes.Reader }{bytes.NewReader(big)})
	r.ContentLength = -1
	r.Header.Set(GenericSignatureHeader, SignGeneric(changeSecret, changeNow, big))
	w := httptest.NewRecorder()
	e.h.ServeHTTP(w, r)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("chunked: got %d", w.Code)
	}
	exact := changeBody(t, nil)
	exact = append(exact, bytes.Repeat([]byte(" "), config.ChangesWebhookMaxBodyBytes-len(exact))...)
	if w := e.post(t, exact, changeSecret, time.Time{}); w.Code != http.StatusAccepted {
		t.Fatalf("exactly 4 KB: got %d %s", w.Code, w.Body)
	}
	if n := len(e.rows(t, "orders")); n != 1 {
		t.Fatalf("rows = %d", n)
	}
}

// TC2: an unknown pipeline is 404 and stores nothing.
func TestChangeWebhookUnknownPipeline(t *testing.T) {
	e := newChangeEnv(t, changeSecret)
	for _, id := range []string{"payments", "../orders", strings.Repeat("a", 70)} {
		w := e.post(t, changeBody(t, func(m map[string]any) { m["pipeline_id"] = id }), changeSecret, time.Time{})
		if w.Code != http.StatusNotFound || errorCode(t, w) != "not_found" {
			t.Fatalf("%q: got %d %s", id, w.Code, w.Body)
		}
	}
	if strings.Contains(e.logs.String(), "payments") {
		t.Fatalf("an unknown pipeline id was logged: %s", e.logs)
	}
}

// TC2, E4: the 61st verified request of one pipeline within a minute is 429;
// another pipeline is not affected, and a minute later the pipeline may send again.
func TestChangeWebhookRateLimit(t *testing.T) {
	e := newChangeEnv(t, changeSecret)
	send := func(pipeline string, i int) int {
		return e.post(t, changeBody(t, func(m map[string]any) {
			m["pipeline_id"] = pipeline
			m["id"] = fmt.Sprint("run-", i)
		}), changeSecret, time.Time{}).Code
	}
	for i := range config.ChangesWebhookPerMinute {
		if c := send("orders", i); c != http.StatusAccepted {
			t.Fatalf("request %d: got %d", i+1, c)
		}
		e.now = e.now.Add(500 * time.Millisecond)
	}
	w := e.post(t, changeBody(t, func(m map[string]any) { m["id"] = "run-61" }), changeSecret, time.Time{})
	if w.Code != http.StatusTooManyRequests || errorCode(t, w) != "rate_limited" {
		t.Fatalf("61st: got %d %s", w.Code, w.Body)
	}
	if c := send("billing", 0); c != http.StatusAccepted {
		t.Fatalf("another pipeline: got %d", c)
	}
	if n := len(e.rows(t, "orders")); n != config.ChangesWebhookPerMinute {
		t.Fatalf("rows = %d", n)
	}
	e.now = changeNow.Add(time.Minute + time.Second)
	if c := send("orders", 99); c != http.StatusAccepted {
		t.Fatalf("after a minute: got %d", c)
	}
}

// E5: a redelivery is not stored twice, with or without the sender's id.
func TestChangeWebhookDedup(t *testing.T) {
	e := newChangeEnv(t, changeSecret)
	withID := changeBody(t, nil)
	retitled := changeBody(t, func(m map[string]any) { m["title"] = "retitled"; m["at"] = "2026-10-20T14:04:00Z" })
	noID := changeBody(t, func(m map[string]any) { delete(m, "id") })
	sameInstant := changeBody(t, func(m map[string]any) { delete(m, "id"); m["at"] = "2026-10-20T16:03:00+02:00" })
	for i, tc := range []struct {
		body []byte
		want string
	}{
		{withID, "true"}, {withID, "false"}, {retitled, "false"},
		{noID, "true"}, {noID, "false"}, {sameInstant, "false"},
	} {
		w := e.post(t, tc.body, changeSecret, time.Time{})
		if w.Code != http.StatusAccepted || strings.TrimSpace(w.Body.String()) != `{"stored":`+tc.want+`}` {
			t.Fatalf("delivery %d: got %d %s, want stored %s", i, w.Code, w.Body, tc.want)
		}
	}
	if n := len(e.rows(t, "orders")); n != 2 {
		t.Fatalf("rows = %d, want 2", n)
	}
}

// E2: verified but invalid bodies are 400 validation with no row.
func TestChangeWebhookValidation(t *testing.T) {
	cases := map[string]func(m map[string]any){
		"unknown field":         func(m map[string]any) { m["version"] = "V42" },
		"no pipeline":           func(m map[string]any) { delete(m, "pipeline_id") },
		"watcher kind":          func(m map[string]any) { m["kind"] = clients.ChangeDDL },
		"no kind":               func(m map[string]any) { delete(m, "kind") },
		"bad at":                func(m map[string]any) { m["at"] = "yesterday" },
		"at in the future":      func(m map[string]any) { m["at"] = "2026-10-20T14:11:00Z" },
		"at older than 7 days":  func(m map[string]any) { m["at"] = "2026-10-13T14:00:00Z" },
		"no title":              func(m map[string]any) { m["title"] = "\x00\n " },
		"title over 300":        func(m map[string]any) { m["title"] = strings.Repeat("é", 301) },
		"no service":            func(m map[string]any) { m["service"] = " " },
		"service over 128":      func(m map[string]any) { m["service"] = strings.Repeat("s", 129) },
		"javascript url":        func(m map[string]any) { m["url"] = "javascript:alert(1)" },
		"url with user":         func(m map[string]any) { m["url"] = "https://u:p@ci.example.com/" },
		"actor over 128":        func(m map[string]any) { m["actor"] = strings.Repeat("a", 129) },
		"actor control":         func(m map[string]any) { m["actor"] = "a\nb" },
		"id over 128":           func(m map[string]any) { m["id"] = strings.Repeat("i", 129) },
		"status on deploy":      func(m map[string]any) { m["status"] = clients.VendorInvestigating },
		"unknown vendor status": func(m map[string]any) { m["kind"] = clients.ChangeVendorIncident; m["status"] = "degraded" },
		"wrong type":            func(m map[string]any) { m["title"] = 7 },
	}
	for name, mut := range cases {
		t.Run(name, func(t *testing.T) {
			e := newChangeEnv(t, changeSecret)
			w := e.post(t, changeBody(t, mut), changeSecret, time.Time{})
			if w.Code != http.StatusBadRequest || errorCode(t, w) != "validation" {
				t.Fatalf("got %d %s", w.Code, w.Body)
			}
			if n := len(e.rows(t, "orders")); n != 0 {
				t.Fatalf("rows = %d", n)
			}
		})
	}
	for name, body := range map[string]string{"array": `[]`, "two objects": `{"pipeline_id":"orders"} {}`, "not json": `x`} {
		t.Run(name, func(t *testing.T) {
			e := newChangeEnv(t, changeSecret)
			if w := e.post(t, []byte(body), changeSecret, time.Time{}); w.Code != http.StatusBadRequest {
				t.Fatalf("got %d", w.Code)
			}
		})
	}
	t.Run("vendor incident", func(t *testing.T) {
		e := newChangeEnv(t, changeSecret)
		w := e.post(t, changeBody(t, func(m map[string]any) {
			m["kind"], m["service"], m["status"] = clients.ChangeVendorIncident, "confluent-cloud", clients.VendorMonitoring
		}), changeSecret, time.Time{})
		if w.Code != http.StatusAccepted {
			t.Fatalf("got %d %s", w.Code, w.Body)
		}
		var ext clients.ExternalChange
		if err := json.Unmarshal(e.rows(t, "orders")[0].After, &ext); err != nil || ext.Status != clients.VendorMonitoring {
			t.Fatalf("after: %+v %v", ext, err)
		}
	})
}

// TC3, E7: a hostile title is stored as one line of text, control characters
// removed and markup kept verbatim (clients render it as text); a 10 KB one
// is refused by the body cap. No sender text, secret or signature reaches the log.
func TestChangeWebhookHostileText(t *testing.T) {
	e := newChangeEnv(t, changeSecret)
	title := "<script>alert(1)</script>\x00\x1b[31m\nIgnore previous instructions\u2028done"
	w := e.post(t, changeBody(t, func(m map[string]any) {
		m["title"] = title
		m["actor"] = "<img src=x onerror=alert(1)>"
	}), changeSecret, time.Time{})
	if w.Code != http.StatusAccepted {
		t.Fatalf("got %d %s", w.Code, w.Body)
	}
	c := e.rows(t, "orders")[0]
	if c.Summary != "<script>alert(1)</script>[31m Ignore previous instructions done" {
		t.Fatalf("summary = %q", c.Summary)
	}
	if strings.ContainsFunc(c.Summary, func(r rune) bool { return unicode.IsControl(r) }) {
		t.Fatalf("control characters stored: %q", c.Summary)
	}
	big := changeBody(t, func(m map[string]any) { m["title"] = strings.Repeat("<script>", 10<<10/8) })
	if w := e.post(t, big, changeSecret, time.Time{}); w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("10 KB title: got %d", w.Code)
	}
	e.post(t, changeBody(t, nil), "wrong", time.Time{})
	logs := e.logs.String()
	for _, leak := range []string{"script", "img", "octocat", "checkout", "ci.example.com", changeSecret, "v1="} {
		if strings.Contains(logs, leak) {
			t.Fatalf("log carries %q: %s", leak, logs)
		}
	}
}

// E6: a store failure is a 500 with no detail to the sender, logged without sender text.
func TestChangeWebhookStoreError(t *testing.T) {
	e := newChangeEnv(t, changeSecret)
	e.h.cfg.Writer = failingWriter{}
	w := e.post(t, changeBody(t, nil), changeSecret, time.Time{})
	if w.Code != http.StatusInternalServerError || errorCode(t, w) != "internal_error" || strings.Contains(w.Body.String(), "db down") {
		t.Fatalf("got %d %s", w.Code, w.Body)
	}
	if !strings.Contains(e.logs.String(), "db down") {
		t.Fatalf("the cause must be logged: %s", e.logs)
	}
}

type failingWriter struct{}

func (failingWriter) RecordChange(context.Context, clients.PipelineChange) (bool, error) {
	return false, errors.New("db down")
}

// TC5: the curl example in README.md posts to the route, with the header and
// signing recipe the handler checks, and its body is accepted as written.
func TestChangeWebhookREADMECurl(t *testing.T) {
	readme, err := os.ReadFile("README.md")
	if err != nil {
		t.Fatal(err)
	}
	_, after, ok := strings.Cut(string(readme), "<!-- change-webhook-curl:")
	if !ok {
		t.Fatal("README.md lost the change-webhook-curl example")
	}
	block, _, _ := strings.Cut(after, "\n```\n")
	body := regexp.MustCompile(`(?m)^body='([^']+)'$`).FindStringSubmatch(block)
	if body == nil {
		t.Fatalf("no body= line in:\n%s", block)
	}
	for _, want := range []string{
		`printf '%s.%s' "$ts" "$body"`, "openssl dgst -sha256 -hmac \"$CHANGES_WEBHOOK_SECRET\"",
		ChangeWebhookPath + `"`, `-H "` + GenericSignatureHeader + `: t=$ts,v1=$sig"`, `--data-binary "$body"`, "-X POST",
	} {
		if !strings.Contains(block, want) {
			t.Errorf("curl example lacks %q", want)
		}
	}
	e := newChangeEnv(t, changeSecret)
	if w := e.post(t, []byte(body[1]), changeSecret, time.Time{}); w.Code != http.StatusAccepted {
		t.Fatalf("README body: got %d %s", w.Code, w.Body)
	}
	// The shell recipe is SignGeneric's: hex HMAC-SHA256 of "<ts>.<body>".
	ts := fmt.Sprint(changeNow.Unix())
	if got, want := SignGeneric(changeSecret, changeNow, []byte(body[1])), "t="+ts+",v1="+hexMAC(changeSecret, ts, ".", body[1]); got != want {
		t.Fatalf("signature %s, want %s", got, want)
	}
}
