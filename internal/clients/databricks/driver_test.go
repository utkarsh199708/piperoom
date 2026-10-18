package databricks

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

// These tests run the real databricks-sql-go driver against an httptest
// server that never speaks Thrift: the session open fails at the HTTP layer,
// which is where a bad token, a wrong http path, a hung workspace and an
// oversized response show up.

// testToken is an obviously fake credential: the client does not need the
// real PAT prefix, and a PAT-shaped literal would trip secret scanners.
const testToken = "fake-test-pat-not-a-real-token"

func driverCfg(t *testing.T, srv *httptest.Server) (Config, string) {
	t.Helper()
	host, port, err := net.SplitHostPort(strings.TrimPrefix(srv.URL, "http://"))
	if err != nil {
		t.Fatal(err)
	}
	p, _ := strconv.Atoi(port)
	return Config{Host: host, Port: p, HTTPPath: "/sql/1.0/warehouses/abc123", Catalog: "main", Token: testToken}, "http://" + host
}

func openAgainst(ctx context.Context, t *testing.T, h http.Handler) error {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	cfg, host := driverCfg(t, srv)
	d, err := open(ctx, cfg, host, http.DefaultTransport)
	if err == nil {
		_ = d.Close()
		t.Fatal("open succeeded against a fake server")
	}
	if strings.Contains(err.Error(), testToken) {
		t.Fatalf("token in error: %v", err)
	}
	t.Logf("error: %v", err)
	return err
}

func status(code int, body string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(code)
		_, _ = w.Write([]byte(body))
	})
}

func TestDriverForbiddenIsPermissionDenied(t *testing.T) {
	for _, code := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		// The body echoes the token, as a careless proxy might.
		err := openAgainst(context.Background(), t, status(code, "denied for Bearer "+testToken))
		if !errors.Is(err, ErrPermissionDenied) {
			t.Errorf("%d: want ErrPermissionDenied, got %v", code, err)
		}
	}
}

func TestDriverNotFoundIsEndpointNotFound(t *testing.T) {
	err := openAgainst(context.Background(), t, status(http.StatusNotFound, "no such warehouse"))
	if !errors.Is(err, ErrEndpointNotFound) {
		t.Fatalf("want ErrEndpointNotFound, got %v", err)
	}
}

func hang(release chan struct{}) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	})
}

func TestDriverTimeout(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := openAgainst(ctx, t, hang(release))
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("want DeadlineExceeded, got %v", err)
	}
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("took %s", took)
	}
}

func TestDriverCancel(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(200*time.Millisecond, cancel)
	err := openAgainst(ctx, t, hang(release))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want Canceled, got %v", err)
	}
}

func TestDriverOversizeResponse(t *testing.T) {
	big := strings.Repeat("x", maxResponseBytes+1024)
	err := openAgainst(context.Background(), t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/x-thrift")
		_, _ = w.Write([]byte(big))
	}))
	if !errors.Is(err, ErrResponseTooLarge) {
		t.Fatalf("want ErrResponseTooLarge, got %v", err)
	}
}
