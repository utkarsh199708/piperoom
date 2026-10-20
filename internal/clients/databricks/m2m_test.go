package databricks

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// m2mAuth builds an authenticator whose token endpoint is the given handler,
// with a fresh bounded client. The service principal credentials are fixed.
func m2mAuth(t *testing.T, h http.Handler) *oauthM2M {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	return newOAuthM2M("app", testSecret, srv.URL, newM2MClient(http.DefaultTransport))
}

func TestOAuthM2MAuthenticateAndCache(t *testing.T) {
	var calls atomic.Int32
	var gotMethod, gotUser, gotPass string
	var gotAuth bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		gotMethod = r.Method
		gotUser, gotPass, gotAuth = r.BasicAuth()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"tok-1","token_type":"Bearer","expires_in":3600}`))
	}))
	t.Cleanup(srv.Close)
	a := newOAuthM2M("app", testSecret, srv.URL, newM2MClient(http.DefaultTransport))

	req := httptest.NewRequest(http.MethodPost, "https://example.test/sql", nil)
	if err := a.Authenticate(req); err != nil {
		t.Fatal(err)
	}
	if got := req.Header.Get("Authorization"); got != "Bearer tok-1" {
		t.Fatalf("auth header = %q", got)
	}
	if gotMethod != http.MethodPost || !gotAuth || gotUser != "app" || gotPass != testSecret {
		t.Fatalf("token request = method %s, basic auth %q/%q (ok %v)", gotMethod, gotUser, gotPass, gotAuth)
	}

	// The token is cached: a second call reuses it, the endpoint is not hit
	// again, and the client secret never reaches a request body or header we
	// did not set ourselves.
	req2 := httptest.NewRequest(http.MethodPost, "https://example.test/sql", nil)
	if err := a.Authenticate(req2); err != nil {
		t.Fatal(err)
	}
	if req2.Header.Get("Authorization") != "Bearer tok-1" {
		t.Fatalf("second auth header = %q", req2.Header.Get("Authorization"))
	}
	if n := calls.Load(); n != 1 {
		t.Fatalf("token endpoint called %d times", n)
	}
}

func TestOAuthM2MHungTokenEndpointFailsWithinCap(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	a := m2mAuth(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))

	// A request context far longer than the client's 10 s timeout, so the
	// fetch is bounded by the client's timeout, not by the query deadline.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	req := httptest.NewRequest(http.MethodPost, "https://example.test/sql", nil).WithContext(ctx)
	start := time.Now()
	err := a.Authenticate(req)
	took := time.Since(start)
	if err == nil {
		t.Fatal("want error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err = %v, want context.DeadlineExceeded", err)
	}
	if took < 8*time.Second || took > 15*time.Second {
		t.Fatalf("took %s, want within the 10 s client timeout", took)
	}
}

func TestOAuthM2MBadStatus(t *testing.T) {
	a := m2mAuth(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		// A token endpoint that echoes the secret it was given, as a broken
		// server might: the error must not carry it.
		_, _ = w.Write([]byte(`{"error":"invalid_client","error_description":"bad secret ` + testSecret + `"}`))
	}))
	req := httptest.NewRequest(http.MethodPost, "https://example.test/sql", nil)
	err := a.Authenticate(req)
	if err == nil {
		t.Fatal("want error")
	}
	if !strings.Contains(err.Error(), "HTTP 400") {
		t.Fatalf("err = %v, want the status code", err)
	}
	if strings.Contains(err.Error(), testSecret) {
		t.Fatalf("secret in error: %v", err)
	}
}

func TestOAuthM2MOversizedBody(t *testing.T) {
	big := strings.Repeat("x", maxResponseBytes+1024)
	a := m2mAuth(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(big))
	}))
	req := httptest.NewRequest(http.MethodPost, "https://example.test/sql", nil)
	err := a.Authenticate(req)
	if !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("err = %v, want ErrResponseTooLarge", err)
	}
}

// TestOpenM2MHungTokenEndpointQueryNeverStarts runs the real driver with an
// M2M authenticator whose token endpoint hangs: the first query fails within
// the cap and the warehouse never sees the request, because the driver
// authenticates before it sends anything.
func TestOpenM2MHungTokenEndpointQueryNeverStarts(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(tokenSrv.Close)

	var warehouseRequests atomic.Int32
	warehouseSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		warehouseRequests.Add(1)
	}))
	t.Cleanup(warehouseSrv.Close)

	host, port, err := net.SplitHostPort(strings.TrimPrefix(warehouseSrv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	p, _ := strconv.Atoi(port)
	cfg := Config{Host: host, Port: p, HTTPPath: "/sql/1.0/warehouses/abc123", Catalog: "main", ClientID: "app", ClientSecret: testSecret}

	start := time.Now()
	d, err := openWith(context.Background(), cfg, "http://"+host, tokenSrv.URL, http.DefaultTransport)
	if err == nil {
		_ = d.Close()
		t.Fatal("open succeeded against a hung token endpoint")
	}
	if took := time.Since(start); took > 15*time.Second {
		t.Fatalf("took %s, want within the cap", took)
	}
	if n := warehouseRequests.Load(); n != 0 {
		t.Fatalf("warehouse saw %d requests; the query must never start", n)
	}
	if strings.Contains(err.Error(), testSecret) {
		t.Fatalf("secret in error: %v", err)
	}
}
