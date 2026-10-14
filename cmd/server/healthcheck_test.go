package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestHealthURL(t *testing.T) {
	for _, tc := range []struct{ in, want string }{
		{"0.0.0.0:8090", "http://127.0.0.1:8090/healthz"},
		{":8090", "http://127.0.0.1:8090/healthz"},
		{"[::]:8090", "http://127.0.0.1:8090/healthz"},
		{"127.0.0.1:9000", "http://127.0.0.1:9000/healthz"},
		{"10.1.2.3:8090", "http://10.1.2.3:8090/healthz"},
	} {
		got, err := healthURL(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("healthURL(%q) = %q, %v; want %q", tc.in, got, err, tc.want)
		}
	}
	if _, err := healthURL("no-port"); err == nil {
		t.Error("healthURL(no-port): want an error")
	}
}

func TestHealthcheck(t *testing.T) {
	status := http.StatusOK
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			t.Errorf("probed %s, want /healthz", r.URL.Path)
		}
		w.WriteHeader(status)
	}))
	defer srv.Close()
	addr := strings.TrimPrefix(srv.URL, "http://")

	if err := healthcheck(context.Background(), addr); err != nil {
		t.Fatalf("healthy server: %v", err)
	}
	status = http.StatusServiceUnavailable
	if err := healthcheck(context.Background(), addr); err == nil {
		t.Fatal("503: want an error")
	}
	srv.Close()
	if err := healthcheck(context.Background(), addr); err == nil {
		t.Fatal("server down: want an error")
	}
}
