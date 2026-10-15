package notify_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"piperoom/internal/notify"
)

// receiver is an httptest webhook target that records every request.
type receiver struct {
	srv   *httptest.Server
	calls atomic.Int32
	mu    sync.Mutex
	reqs  []recorded
}

type recorded struct {
	header http.Header
	body   []byte
}

// newReceiver answers request n (from 1) with statuses[n-1], or 200 past the
// end; a status of 0 sleeps until the request ends (a hung receiver).
func newReceiver(t *testing.T, statuses ...int) *receiver {
	t.Helper()
	r := &receiver{}
	r.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		n := int(r.calls.Add(1))
		b, _ := io.ReadAll(req.Body)
		r.mu.Lock()
		r.reqs = append(r.reqs, recorded{header: req.Header.Clone(), body: b})
		r.mu.Unlock()
		status := http.StatusOK
		if n <= len(statuses) {
			status = statuses[n-1]
		}
		if status == 0 {
			<-req.Context().Done()
			return
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte("receiver body that must never be logged"))
	}))
	t.Cleanup(r.srv.Close)
	return r
}

func (r *receiver) requests() []recorded {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recorded(nil), r.reqs...)
}

func sample() notify.OwnerPaged {
	return notify.OwnerPaged{
		RoomID: "r_01", RoomTitle: "orders: connector down", PipelineID: "orders", Layer: "consumer",
		Owners: []notify.Owner{{ID: "kabir", Name: "Kabir", Email: "kabir@example.com"}, {ID: "u_x", Name: "u_x"}},
		Reason: "lag keeps growing", At: time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC), Seq: 42,
	}
}

func newHook(t *testing.T, url, secret string, log *slog.Logger) *notify.Webhook {
	t.Helper()
	w, err := notify.NewWebhook(notify.WebhookConfig{
		URL: url, Secret: secret, PublicURL: "https://piperoom.example.com/", Timeout: 200 * time.Millisecond,
		RetryWait: time.Millisecond, Logger: log,
	})
	if err != nil {
		t.Fatalf("new webhook: %v", err)
	}
	return w
}

func TestDeliverBodyHeadersAndSignature(t *testing.T) {
	r := newReceiver(t)
	secret := "s3cret-value"
	if err := newHook(t, r.srv.URL+"/hook", secret, nil).Deliver(context.Background(), sample()); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	reqs := r.requests()
	if len(reqs) != 1 {
		t.Fatalf("requests = %d, want 1", len(reqs))
	}
	got := reqs[0]
	if ct := got.header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q", ct)
	}
	if ev := got.header.Get(notify.HeaderEvent); ev != "owner.paged" {
		t.Errorf("%s = %q", notify.HeaderEvent, ev)
	}
	if sig := got.header.Get(notify.HeaderSignature); sig != notify.Signature([]byte(secret), got.body) || !strings.HasPrefix(sig, "sha256=") {
		t.Errorf("signature %q does not match the body", sig)
	}
	var body map[string]any
	if err := json.Unmarshal(got.body, &body); err != nil {
		t.Fatalf("body: %v", err)
	}
	want := map[string]any{
		"type": "owner.paged", "room_id": "r_01", "room_title": "orders: connector down",
		"room_url": "https://piperoom.example.com/rooms/r_01", "pipeline_id": "orders", "layer": "consumer",
		"reason": "lag keeps growing", "proposal_id": "", "auto": false, "at": "2026-10-15T09:00:00Z", "seq": float64(42),
	}
	for k, v := range want {
		if body[k] != v {
			t.Errorf("body[%q] = %v, want %v", k, body[k], v)
		}
	}
	owners, _ := body["owners"].([]any)
	if len(owners) != 2 {
		t.Fatalf("owners = %v", body["owners"])
	}
	first := owners[0].(map[string]any)
	if first["id"] != "kabir" || first["name"] != "Kabir" || first["email"] != "kabir@example.com" {
		t.Errorf("owner 0 = %v", first)
	}
	if _, has := owners[1].(map[string]any)["email"]; has {
		t.Errorf("owner without an email carries the key: %v", owners[1])
	}
}

func TestDeliverUnsignedNoPublicURLEmptyOwners(t *testing.T) {
	r := newReceiver(t)
	w, err := notify.NewWebhook(notify.WebhookConfig{URL: r.srv.URL, RetryWait: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	p := sample()
	p.Owners = nil
	if err := w.Deliver(context.Background(), p); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	got := r.requests()[0]
	if sig := got.header.Get(notify.HeaderSignature); sig != "" {
		t.Errorf("unsigned delivery has a signature %q", sig)
	}
	var body map[string]json.RawMessage
	if err := json.Unmarshal(got.body, &body); err != nil {
		t.Fatal(err)
	}
	if _, has := body["room_url"]; has {
		t.Errorf("room_url present without PIPEROOM_PUBLIC_URL: %s", body["room_url"])
	}
	if string(body["owners"]) != "[]" {
		t.Errorf("owners = %s, want []", body["owners"])
	}
}

func TestDeliverRetries(t *testing.T) {
	cases := []struct {
		name      string
		statuses  []int
		wantCalls int32
		wantErr   bool
	}{
		{"ok first time", []int{200}, 1, false},
		{"one retry on 500", []int{500, 204}, 2, false},
		{"gives up after the retry", []int{503, 502}, 2, true},
		{"no retry on 400", []int{400}, 1, true},
		{"no retry on 404", []int{404}, 1, true},
		{"redirect is a failure, no retry", []int{302}, 1, true},
		{"timeout is retried once", []int{0, 0}, 2, true},
		{"timeout then ok", []int{0, 200}, 2, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newReceiver(t, tc.statuses...)
			err := newHook(t, r.srv.URL, "k", nil).Deliver(context.Background(), sample())
			if (err != nil) != tc.wantErr {
				t.Fatalf("err = %v, want error %v", err, tc.wantErr)
			}
			if err != nil && !errors.Is(err, notify.ErrDelivery) {
				t.Errorf("err %v does not wrap ErrDelivery", err)
			}
			if n := r.calls.Load(); n != tc.wantCalls {
				t.Errorf("calls = %d, want %d", n, tc.wantCalls)
			}
		})
	}
}

func TestDeliverNetworkErrorRetriedOnce(t *testing.T) {
	r := newReceiver(t)
	url := r.srv.URL
	r.srv.Close() // nothing listens any more
	err := newHook(t, url, "", nil).Deliver(context.Background(), sample())
	if !errors.Is(err, notify.ErrDelivery) {
		t.Fatalf("err = %v, want ErrDelivery", err)
	}
	if strings.Contains(err.Error(), "127.0.0.1") || strings.Contains(err.Error(), url) {
		t.Errorf("error quotes the URL: %v", err)
	}
}

func TestDeliverCancelledContextStops(t *testing.T) {
	r := newReceiver(t, 0, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	w, err := notify.NewWebhook(notify.WebhookConfig{URL: r.srv.URL, Timeout: 5 * time.Second, RetryWait: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	if err := w.Deliver(ctx, sample()); !errors.Is(err, notify.ErrDelivery) {
		t.Fatalf("err = %v", err)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("Deliver did not stop when its context ended")
	}
	if n := r.calls.Load(); n != 1 {
		t.Errorf("calls = %d, want 1: no retry after the caller gave up", n)
	}
}

func TestDeliverLogsNoSecretBodyOrURL(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	r := newReceiver(t, 500, 500)
	secret := "do-not-log-this-secret"
	url := r.srv.URL + "/hooks/T0K3N-in-path"
	if err := newHook(t, url, secret, log).Deliver(context.Background(), sample()); err == nil {
		t.Fatal("want an error after two 500s")
	}
	out := buf.String()
	if !strings.Contains(out, "status=500") {
		t.Errorf("log lacks the status code: %s", out)
	}
	for _, leak := range []string{secret, "T0K3N-in-path", "lag keeps growing", "kabir@example.com", "receiver body", "sha256="} {
		if strings.Contains(out, leak) {
			t.Errorf("log contains %q: %s", leak, out)
		}
	}
}

func TestNewWebhookRejectsBadURL(t *testing.T) {
	for _, u := range []string{"", "  ", "ftp://x/y", "/relative", "http://"} {
		if _, err := notify.NewWebhook(notify.WebhookConfig{URL: u}); err == nil {
			t.Errorf("NewWebhook(%q): want error", u)
		}
	}
	_, err := notify.NewWebhook(notify.WebhookConfig{URL: "ftp://user:pa55@x/y"})
	if err == nil || strings.Contains(err.Error(), "pa55") {
		t.Errorf("error = %v; must refuse and not quote the URL", err)
	}
}
