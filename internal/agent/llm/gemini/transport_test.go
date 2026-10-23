package gemini

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"golang.org/x/oauth2"

	"piperoom/internal/agent/llm"
	"piperoom/internal/agent/llm/retry"
	"piperoom/internal/config"
)

func mustBase(t *testing.T, raw string, privateHTTP bool) config.BaseURL {
	t.Helper()
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatal(err)
	}
	return config.BaseURL{URL: u, Host: u.Hostname(), PrivateHTTP: privateHTTP}
}

func TestDialDenyList(t *testing.T) {
	https := mustBase(t, "https://generativelanguage.googleapis.com/v1beta", false)
	httpLocal := mustBase(t, "http://localhost:8000", false)
	httpPrivate := mustBase(t, "http://gemini-proxy.internal:8000", true)
	for _, tc := range []struct {
		base  config.BaseURL
		addr  string
		allow bool
	}{
		{https, "142.250.1.1:443", true},
		{https, "169.254.169.254:80", false},
		{https, "[fd00:ec2::254]:80", false},
		{https, "100.100.100.200:80", false},
		{https, "[::ffff:169.254.169.254]:80", false},
		{https, "169.254.170.2:80", false},
		{https, "[fe80::1]:443", false},
		{https, "0.0.0.0:443", false},
		{httpLocal, "127.0.0.1:8000", true},
		{httpLocal, "10.0.0.5:8000", false},
		{httpPrivate, "10.0.0.5:8000", true},
		{httpPrivate, "8.8.8.8:8000", false},
		{httpPrivate, "169.254.169.254:80", false},
	} {
		err := dialAllowed(tc.base, tc.addr)
		if (err == nil) != tc.allow {
			t.Errorf("%s via %s: err = %v, want allowed=%v", tc.addr, tc.base.URL, err, tc.allow)
		}
		if err != nil && !errors.Is(err, errDenied) {
			t.Errorf("%s: err = %v, want errDenied", tc.addr, err)
		}
	}
}

// TestModelTransportRefusesMetadata dials for real: the hook refuses the
// metadata address before any packet leaves.
func TestModelTransportRefusesMetadata(t *testing.T) {
	base := mustBase(t, "http://169.254.169.254", false)
	c := &http.Client{Transport: modelTransport(base, false, nil)}
	_, err := c.Get("http://169.254.169.254/computeMetadata/v1/")
	if !errors.Is(err, errDenied) {
		t.Fatalf("err = %v, want errDenied", err)
	}
}

func TestRedirectNotFollowed(t *testing.T) {
	var hit atomic.Bool
	elsewhere := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { hit.Store(true) }))
	defer elsewhere.Close()
	s := newRecorder(t, reply{status: http.StatusTemporaryRedirect, header: map[string]string{"Location": elsewhere.URL + "/steal"}})
	m := newTestModel(t, s.URL, nil)
	_, err := m.Turn(context.Background(), baseReq("gemini-2.5-pro"))
	var ae *llm.APIError
	if !errors.As(err, &ae) || !errors.Is(err, errRedirect) {
		t.Fatalf("err = %v, want a refused redirect", err)
	}
	if hit.Load() {
		t.Fatal("the redirect was followed")
	}
}

func TestHostGuard(t *testing.T) {
	g := &hostGuard{host: "api.example:443", base: http.DefaultTransport}
	req, _ := http.NewRequest(http.MethodGet, "https://elsewhere.example/", nil)
	if _, err := g.RoundTrip(req); !errors.Is(err, errDenied) {
		t.Fatalf("err = %v", err)
	}
}

type fakeResolver map[string][]netip.Addr

func (f fakeResolver) LookupNetIP(_ context.Context, _, host string) ([]netip.Addr, error) {
	if a, ok := f[host]; ok {
		return a, nil
	}
	return nil, errors.New("no such host")
}

// TestProxy: off, the environment's proxy is never consulted; on, a target
// whose name resolves to a metadata address is refused before the proxy is
// asked (so no CONNECT is sent), and a good target goes to the proxy.
func TestProxy(t *testing.T) {
	proxyURL, _ := url.Parse("http://proxy.example:3128")
	asked := 0
	fromEnv := func(*http.Request) (*url.URL, error) { asked++; return proxyURL, nil }

	base := mustBase(t, "https://gemini.example", false)
	if tr := modelTransport(base, false, fromEnv); tr.Proxy != nil {
		t.Fatal("PIPEROOM_MODEL_PROXY=off must leave Proxy nil")
	}

	res := fakeResolver{
		"evil.example":   {netip.MustParseAddr("169.254.169.254")},
		"evil6.example":  {netip.MustParseAddr("::ffff:169.254.169.254")},
		"gemini.example": {netip.MustParseAddr("142.250.1.1")},
	}
	pf := checkedProxy(base, fromEnv, res)
	for _, target := range []string{"https://evil.example/v1", "https://evil6.example/v1", "http://metadata.google.internal/", "http://100.100.100.200/"} {
		req, _ := http.NewRequest(http.MethodPost, target, nil)
		if _, err := pf(req); !errors.Is(err, errDenied) {
			t.Fatalf("%s: err = %v, want errDenied", target, err)
		}
	}
	if asked != 0 {
		t.Fatalf("the proxy was asked %d times for a denied target", asked)
	}
	req, _ := http.NewRequest(http.MethodPost, "https://gemini.example/v1", nil)
	got, err := pf(req)
	if err != nil || got.String() != proxyURL.String() || asked != 1 {
		t.Fatalf("good target: %v %v (asked %d)", got, err, asked)
	}

	// End to end through a fake proxy: a CONNECT to a metadata target never
	// reaches it.
	var connects int
	fake := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connects++
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer fake.Close()
	fu, _ := url.Parse(fake.URL)
	tr := modelTransport(mustBase(t, "https://evil.example", false), true, func(*http.Request) (*url.URL, error) { return fu, nil })
	tr.Proxy = checkedProxy(mustBase(t, "https://evil.example", false), func(*http.Request) (*url.URL, error) { return fu, nil }, res)
	_, err = (&http.Client{Transport: tr}).Get("https://evil.example/v1")
	if err == nil || connects != 0 {
		t.Fatalf("err = %v, CONNECTs = %d; want refused before the proxy", err, connects)
	}
}

// TestKeyNeverLeaks: the key goes only in its header; a failing request's
// error, APIError.Body and the debug log carry no trace of it.
func TestKeyNeverLeaks(t *testing.T) {
	echo := `{"error":{"code":401,"message":"bad key ` + testKey + ` for request","status":"UNAUTHENTICATED"}}`
	s := newRecorder(t, reply{status: 401, body: echo})
	var logs bytes.Buffer
	log := slog.New(slog.NewJSONHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	m := newTestModel(t, s.URL, func(o *Options) { o.Log = log })
	_, err := m.Turn(context.Background(), baseReq("gemini-2.5-pro"))
	var ae *llm.APIError
	if !errors.As(err, &ae) || ae.Kind != llm.ErrAuth {
		t.Fatalf("err = %v", err)
	}
	r := s.requests()[0]
	if r.header.Get("x-goog-api-key") != testKey {
		t.Fatal("the key header was not sent")
	}
	if strings.Contains(r.path, testKey) || r.header.Get("Authorization") != "" {
		t.Fatal("the key went somewhere other than its header")
	}
	for what, text := range map[string]string{"error": err.Error(), "body": ae.Body, "log": logs.String(), "options": slogText(m)} {
		if strings.Contains(text, testKey) {
			t.Fatalf("the key is in the %s: %s", what, text)
		}
	}
	if !strings.Contains(logs.String(), "bad key") {
		t.Fatalf("the redacted body should reach the debug log: %s", logs.String())
	}
}

func slogText(v any) string {
	var b bytes.Buffer
	slog.New(slog.NewTextHandler(&b, nil)).Info("x", "m", v)
	return b.String()
}

func TestNewValidates(t *testing.T) {
	for _, o := range []Options{
		{},
		{Backend: config.GeminiBackendVertex, Region: "europe-west4"},
		{Backend: config.GeminiBackendVertex, Project: "acme", Region: "../x"},
		{Backend: "other", APIKey: "k"},
	} {
		if _, err := New(context.Background(), o); err == nil {
			t.Errorf("New(%+v) succeeded", o)
		}
	}
	m, err := New(context.Background(), Options{APIKey: "k"})
	if err != nil {
		t.Fatal(err)
	}
	if got := m.url("gemini-2.5-pro"); got != "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-pro:generateContent" {
		t.Fatalf("url = %s", got)
	}
	ts := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "t"})
	for region, want := range map[string]string{
		"europe-west4": "https://europe-west4-aiplatform.googleapis.com/v1/projects/acme/locations/europe-west4/publishers/google/models/gemini-2.5-pro:generateContent",
		"global":       "https://aiplatform.googleapis.com/v1/projects/acme/locations/global/publishers/google/models/gemini-2.5-pro:generateContent",
	} {
		m, err := New(context.Background(), Options{Backend: config.GeminiBackendVertex, Project: "acme", Region: region, TokenSource: ts})
		if err != nil {
			t.Fatal(err)
		}
		if got := m.url("gemini-2.5-pro"); got != want {
			t.Fatalf("url = %s, want %s", got, want)
		}
	}
	_, err = m.Turn(context.Background(), llm.Request{Model: "../../v1/x", Messages: []llm.Message{userText("hi")}})
	if !errors.Is(err, llm.ErrNotSent) {
		t.Fatalf("a model id with a path must not be sent: %v", err)
	}
}

// TestVertexTwoTransports: on Vertex the token comes from application
// default credentials through the credential client, which here reaches a
// fake metadata/token server; the model request carries only the token, goes
// to the model host on the deny-listed transport, and no token request goes
// to the model host. The model transport itself refuses the metadata address.
func TestVertexTwoTransports(t *testing.T) {
	var tokenReqs []string
	var mu sync.Mutex
	meta := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		tokenReqs = append(tokenReqs, r.Host+r.URL.Path)
		mu.Unlock()
		_, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"access_token":"ya29.fake-vertex-token","token_type":"Bearer","expires_in":3600}`)
	}))
	defer meta.Close()
	metaAddr := meta.Listener.Addr().String()

	// The credential client may reach the metadata address: its dialer sends
	// every dial to the fake server, standing in for 169.254.169.254.
	cred := &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, network, metaAddr)
		},
	}}
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", serviceAccountFile(t, "http://169.254.169.254/token"))

	s := newRecorder(t, ok(`{"candidates":[{"content":{"parts":[{"text":"hi"}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":100,"candidatesTokenCount":1}}`))
	b, err := config.ParseBaseURL("PIPEROOM_MODEL_BASE_URL", s.URL+"/v1", false)
	if err != nil {
		t.Fatal(err)
	}
	m, err := New(context.Background(), Options{
		Backend: config.GeminiBackendVertex, Project: "acme", Region: "europe-west4",
		BaseURL: &b, CredentialClient: cred,
	})
	if err != nil {
		t.Fatal(err)
	}
	m.policy = retry.Policy{}
	if _, err := m.Turn(context.Background(), baseReq("gemini-2.5-pro")); err != nil {
		t.Fatal(err)
	}
	reqs := s.requests()
	if len(reqs) != 1 {
		t.Fatalf("model requests = %d", len(reqs))
	}
	r := reqs[0]
	if r.path != "/v1/projects/acme/locations/europe-west4/publishers/google/models/gemini-2.5-pro:generateContent" {
		t.Fatalf("path = %s", r.path)
	}
	if r.header.Get("Authorization") != "Bearer ya29.fake-vertex-token" || r.header.Get("x-goog-api-key") != "" {
		t.Fatalf("auth headers = %v", r.header)
	}
	if strings.Contains(r.path, "token") || bytes.Contains(r.body, []byte("assertion")) {
		t.Fatal("a token request went to the model host")
	}
	mu.Lock()
	got := append([]string(nil), tokenReqs...)
	mu.Unlock()
	if len(got) != 1 || got[0] != "169.254.169.254/token" {
		t.Fatalf("token requests = %v, want one to the metadata address", got)
	}

	// The same adapter's model transport cannot reach the metadata address.
	mt := modelTransport(mustBase(t, "http://169.254.169.254", false), false, nil)
	req, _ := http.NewRequest(http.MethodPost, "http://169.254.169.254/token", nil)
	if _, err := mt.RoundTrip(req); !errors.Is(err, errDenied) {
		t.Fatalf("model transport to metadata: %v", err)
	}
}

// serviceAccountFile writes a throwaway service account key whose token URI
// is tokenURI.
func serviceAccountFile(t *testing.T, tokenURI string) string {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	pemKey := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})
	b, _ := json.Marshal(map[string]string{
		"type": "service_account", "project_id": "acme", "private_key_id": "k1",
		"private_key": string(pemKey), "client_email": "piperoom@acme.iam.gserviceaccount.com",
		"client_id": "1", "token_uri": tokenURI,
	})
	p := filepath.Join(t.TempDir(), "sa.json")
	if err := os.WriteFile(p, b, 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}
