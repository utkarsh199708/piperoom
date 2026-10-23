package openai

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"piperoom/internal/agent/llm"
	"piperoom/internal/agent/llm/retry"
	"piperoom/internal/config"
)

// fixedLookup answers every name with addrs.
func fixedLookup(addrs ...string) func(context.Context, string) ([]netip.Addr, error) {
	return func(context.Context, string) ([]netip.Addr, error) {
		var out []netip.Addr
		for _, a := range addrs {
			out = append(out, netip.MustParseAddr(a))
		}
		return out, nil
	}
}

func TestNamesResolvingToMetadataAreRefusedAtDial(t *testing.T) {
	s := newStub(t, okText("x"))
	_, port, _ := net.SplitHostPort(strings.TrimPrefix(s.URL, "http://"))
	for _, addr := range []string{"169.254.169.254", "fd00:ec2::254", "100.100.100.200", "::ffff:169.254.169.254", "169.254.170.2", "fe80::1"} {
		t.Run(addr, func(t *testing.T) {
			b, err := config.ParseBaseURL("PIPEROOM_MODEL_BASE_URL", "https://model.example.test:"+port+"/v1", false)
			if err != nil {
				t.Fatal(err)
			}
			m, err := newModel(Options{Flavor: FlavorOpenAI, BaseURL: &b, Key: testKey, Retry: &retry.Policy{}},
				hooks{lookup: fixedLookup(addr)})
			if err != nil {
				t.Fatal(err)
			}
			_, err = m.Turn(context.Background(), simpleRequest("gpt-4.1"))
			if !errors.Is(err, errDenied) {
				t.Fatalf("err = %v, want the dial refused", err)
			}
			if ae := apiErr(t, err); ae.Kind != llm.ErrNetwork || ae.Status != 0 {
				t.Fatalf("APIError = %+v", ae)
			}
			if strings.Contains(err.Error(), testKey.Reveal()) {
				t.Fatal("the error names the key")
			}
		})
	}
	if n := len(s.requests()); n != 0 {
		t.Fatalf("%d requests reached a server", n)
	}
}

func TestOneDeniedAddressRefusesTheName(t *testing.T) {
	g := guard{lookup: fixedLookup("93.184.215.14", "169.254.169.254")}
	if _, err := g.resolve(context.Background(), "model.example.test"); !errors.Is(err, errDenied) {
		t.Fatalf("err = %v; a name with any denied address is refused", err)
	}
	if _, err := g.resolve(context.Background(), "metadata.google.internal"); !errors.Is(err, errDenied) {
		t.Fatalf("err = %v; the metadata name is refused before any lookup", err)
	}
}

func TestControlHookChecksTheDialedAddress(t *testing.T) {
	g := guard{}
	for _, a := range []string{"169.254.169.254:80", "[fd00:ec2::254]:80", "100.100.100.200:443", "[::ffff:169.254.169.254]:443", "0.0.0.0:80"} {
		if err := g.control(context.Background(), "tcp", a, nil); !errors.Is(err, errDenied) {
			t.Errorf("control(%s) = %v, want refused", a, err)
		}
	}
	if err := g.control(context.Background(), "tcp", "93.184.215.14:443", nil); err != nil {
		t.Errorf("a public address over https: %v", err)
	}
}

func TestPlainHTTPAddressRules(t *testing.T) {
	loopbackOnly := guard{plainHTTP: true}
	private := guard{plainHTTP: true, privateHTTP: true}
	https := guard{}
	for _, tc := range []struct {
		addr                         string
		loopback, privateOK, httpsOK bool
	}{
		{"127.0.0.1", true, true, true},
		{"::1", true, true, true},
		{"10.0.0.5", false, true, true},
		{"172.20.1.1", false, true, true},
		{"192.168.1.10", false, true, true},
		{"fd12:3456::1", false, true, true},
		{"8.8.8.8", false, false, true},
		{"2001:4860::8888", false, false, true},
		{"169.254.169.254", false, false, false},
		{"fd00:ec2::254", false, false, false},
		{"100.100.100.200", false, false, false},
		{"::ffff:169.254.169.254", false, false, false},
		{"::ffff:10.0.0.5", false, true, true},
	} {
		a := netip.MustParseAddr(tc.addr)
		if got := loopbackOnly.check(a) == nil; got != tc.loopback {
			t.Errorf("http loopback-only %s: allowed=%v, want %v", tc.addr, got, tc.loopback)
		}
		if got := private.check(a) == nil; got != tc.privateOK {
			t.Errorf("http private %s: allowed=%v, want %v", tc.addr, got, tc.privateOK)
		}
		if got := https.check(a) == nil; got != tc.httpsOK {
			t.Errorf("https %s: allowed=%v, want %v", tc.addr, got, tc.httpsOK)
		}
	}
}

func TestPlainHTTPNameResolvingOutsideTheRuleIsRefused(t *testing.T) {
	s := newStub(t, okText("x"))
	_, port, _ := net.SplitHostPort(strings.TrimPrefix(s.URL, "http://"))
	for _, tc := range []struct {
		base, resolves string
		allowPrivate   bool
	}{
		{"http://localhost:" + port, "10.0.0.5", false},   // localhost that is not loopback
		{"http://vllm.internal:" + port, "8.8.8.8", true}, // a private-http name resolving to a public address
		{"http://vllm.internal:" + port, "169.254.169.254", true},
	} {
		b, err := config.ParseBaseURL("PIPEROOM_MODEL_BASE_URL", tc.base+"/v1", tc.allowPrivate)
		if err != nil {
			t.Fatalf("%s: %v", tc.base, err)
		}
		m, err := newModel(Options{Flavor: FlavorVLLM, BaseURL: &b, AllowPrivateHTTP: tc.allowPrivate, Retry: &retry.Policy{}},
			hooks{lookup: fixedLookup(tc.resolves)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := m.Turn(context.Background(), simpleRequest("m")); !errors.Is(err, errDenied) {
			t.Errorf("%s -> %s: err = %v, want refused", tc.base, tc.resolves, err)
		}
	}
	// The same name resolving to loopback is served.
	b, _ := config.ParseBaseURL("PIPEROOM_MODEL_BASE_URL", "http://vllm.internal:"+port+"/v1", true)
	m, err := newModel(Options{Flavor: FlavorVLLM, BaseURL: &b, AllowPrivateHTTP: true, Retry: &retry.Policy{}}, hooks{lookup: fixedLookup("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.Turn(context.Background(), simpleRequest("m")); err != nil {
		t.Fatalf("private http to loopback: %v", err)
	}
}

// fakeProxy counts CONNECT requests and refuses them.
type fakeProxy struct {
	*httptest.Server
	connects atomic.Int64
	targets  sync.Map
}

func newFakeProxy(t *testing.T) *fakeProxy {
	p := &fakeProxy{}
	p.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodConnect {
			p.connects.Add(1)
			p.targets.Store(r.Host, true)
		}
		http.Error(w, "fake proxy", http.StatusBadGateway)
	}))
	t.Cleanup(p.Close)
	return p
}

// tlsTarget is an https model host on 127.0.0.1 whose certificate names
// example.com, and the TLS config that trusts it.
func tlsTarget(t *testing.T) (*httptest.Server, string, *tls.Config) {
	t.Helper()
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		okText("direct")(w)
	}))
	t.Cleanup(srv.Close)
	_, port, _ := net.SplitHostPort(strings.TrimPrefix(srv.URL, "https://"))
	cfg := srv.Client().Transport.(*http.Transport).TLSClientConfig.Clone()
	return srv, port, cfg
}

func TestProxyOffIgnoresTheProxy(t *testing.T) {
	proxy := newFakeProxy(t)
	t.Setenv("HTTPS_PROXY", proxy.URL)
	t.Setenv("https_proxy", proxy.URL)
	_, port, cfg := tlsTarget(t)
	b, err := config.ParseBaseURL("PIPEROOM_MODEL_BASE_URL", "https://example.com:"+port+"/v1", false)
	if err != nil {
		t.Fatal(err)
	}
	proxyURL, _ := url.Parse(proxy.URL)
	m, err := newModel(Options{Flavor: FlavorOpenAI, BaseURL: &b, Key: testKey, Retry: &retry.Policy{}}, hooks{
		lookup:    fixedLookup("127.0.0.1"),
		proxy:     func(*http.Request) (*url.URL, error) { return proxyURL, nil },
		tlsConfig: cfg,
	})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := m.Turn(context.Background(), simpleRequest("gpt-4.1"))
	if err != nil || resp.Message.Parts[0].Text != "direct" {
		t.Fatalf("Turn: %v %+v", err, resp)
	}
	if n := proxy.connects.Load(); n != 0 {
		t.Fatalf("PIPEROOM_MODEL_PROXY off, but the proxy got %d CONNECTs", n)
	}
	if m.client.Transport.(*keyTransport).base.(*http.Transport).Proxy != nil {
		t.Fatal("the transport has a proxy function with the proxy off")
	}
}

func TestProxyOnRefusesMetadataBeforeConnect(t *testing.T) {
	proxy := newFakeProxy(t)
	proxyURL, _ := url.Parse(proxy.URL)
	build := func(resolves string) *Model {
		b, err := config.ParseBaseURL("PIPEROOM_MODEL_BASE_URL", "https://model.example.test/v1", false)
		if err != nil {
			t.Fatal(err)
		}
		m, err := newModel(Options{Flavor: FlavorOpenAI, BaseURL: &b, Key: testKey, Proxy: true, Retry: &retry.Policy{}}, hooks{
			lookup: func(_ context.Context, host string) ([]netip.Addr, error) {
				if host == "model.example.test" {
					return []netip.Addr{netip.MustParseAddr(resolves)}, nil
				}
				return nil, errors.New("unexpected lookup " + host)
			},
			proxy: func(*http.Request) (*url.URL, error) { return proxyURL, nil },
		})
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	for _, addr := range []string{"169.254.169.254", "fd00:ec2::254", "100.100.100.200", "::ffff:169.254.169.254"} {
		_, err := build(addr).Turn(context.Background(), simpleRequest("gpt-4.1"))
		if !errors.Is(err, errDenied) {
			t.Fatalf("%s: err = %v, want refused", addr, err)
		}
	}
	if n := proxy.connects.Load(); n != 0 {
		t.Fatalf("a denied target reached the proxy: %d CONNECTs", n)
	}
	// An allowed target goes through the proxy (which refuses it here).
	_, err := build("93.184.215.14").Turn(context.Background(), simpleRequest("gpt-4.1"))
	if err == nil {
		t.Fatal("the fake proxy answers 502; Turn must fail")
	}
	if n := proxy.connects.Load(); n != 1 {
		t.Fatalf("CONNECTs = %d, want 1 for an allowed target", n)
	}
	if _, ok := proxy.targets.Load("model.example.test:443"); !ok {
		t.Fatal("the CONNECT did not name the configured host")
	}
}

// capture is a slog handler that keeps every record's text.
type capture struct {
	mu  *sync.Mutex
	buf *bytes.Buffer
	lvl slog.Level
}

func newCapture(lvl slog.Level) (*slog.Logger, *capture) {
	c := &capture{mu: &sync.Mutex{}, buf: &bytes.Buffer{}, lvl: lvl}
	return slog.New(c), c
}

func (c *capture) Enabled(_ context.Context, l slog.Level) bool { return l >= c.lvl }
func (c *capture) Handle(_ context.Context, r slog.Record) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.buf.WriteString(r.Level.String() + " " + r.Message)
	r.Attrs(func(a slog.Attr) bool {
		c.buf.WriteString(" " + a.Key + "=" + a.Value.Resolve().String())
		return true
	})
	c.buf.WriteString("\n")
	return nil
}
func (c *capture) WithAttrs([]slog.Attr) slog.Handler { return c }
func (c *capture) WithGroup(string) slog.Handler      { return c }
func (c *capture) String() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.buf.String()
}

const promptEcho = "prompt-echo-58d1"

func TestBodyOnlyAtDebugAndKeyNowhere(t *testing.T) {
	// The error body echoes the prompt and the key (a proxy that quotes the
	// Authorization header).
	echo := "Invalid request near '" + promptEcho + "'; header was Bearer " + testKey.Reveal()
	for _, lvl := range []slog.Level{slog.LevelInfo, slog.LevelDebug} {
		log, c := newCapture(lvl)
		s := newStub(t, errReply(400, "", echo), errReply(401, "invalid_api_key", echo))
		m := build(t, FlavorOpenAI, s.URL, Options{Log: log, Retry: &retry.Policy{}}, hooks{})
		for range 2 {
			resp, err := m.Turn(context.Background(), simpleRequest("gpt-4.1"))
			ae := apiErr(t, err)
			for name, text := range map[string]string{"error": err.Error(), "APIError": ae.Error(), "logs": c.String()} {
				if strings.Contains(text, testKey.Reveal()) {
					t.Fatalf("level %s: the key is in the %s: %s", lvl, name, text)
				}
			}
			if strings.Contains(err.Error(), promptEcho) {
				t.Fatalf("the error text carries the body: %s", err.Error())
			}
			if strings.Contains(ae.Body, testKey.Reveal()) || !strings.Contains(ae.Body, promptEcho) {
				t.Fatalf("APIError.Body = %q; want the redacted body without the key", ae.Body)
			}
			ev, _ := json.Marshal(resp)
			if strings.Contains(string(ev), testKey.Reveal()) {
				t.Fatal("the key is in the response")
			}
		}
		got := c.String()
		if lvl == slog.LevelInfo && strings.Contains(got, promptEcho) {
			t.Fatalf("the body reached a log above debug: %s", got)
		}
		if lvl == slog.LevelDebug && !strings.Contains(got, promptEcho) {
			t.Fatalf("the debug log has no body: %s", got)
		}
	}
}

func TestKeyNotInNetworkErrorsOrLogs(t *testing.T) {
	log, c := newCapture(slog.LevelDebug)
	s := newStub(t)
	url := s.URL
	s.Close()
	m := build(t, FlavorAzure, url, Options{Log: log}, hooks{})
	_, err := m.Turn(context.Background(), simpleRequest("prod"))
	if apiErr(t, err).Kind != llm.ErrNetwork {
		t.Fatalf("err = %v", err)
	}
	for _, text := range []string{err.Error(), c.String()} {
		if strings.Contains(text, testKey.Reveal()) {
			t.Fatalf("the key leaked: %s", text)
		}
		if strings.Contains(text, "/v1/chat/completions") {
			t.Fatalf("the request URL (its path can hold a token) is in: %s", text)
		}
	}
	// The key never prints through the Model either.
	for _, v := range []string{m.key.String(), stringOf(m), fmt.Sprintf("%v %+v %#v %s", m, m, m, m), fmt.Sprintf("%+v %#v", m.client.Transport, m.client.Transport)} {
		if strings.Contains(v, testKey.Reveal()) {
			t.Fatalf("the key prints: %s", v)
		}
	}
}

func stringOf(v any) string {
	var b bytes.Buffer
	slog.New(slog.NewTextHandler(&b, nil)).Info("m", "model", v)
	return b.String()
}
