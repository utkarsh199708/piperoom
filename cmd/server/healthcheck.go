package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"
)

// healthURL turns the listen address into the URL a probe running inside the
// same container can call. A wildcard or empty host (0.0.0.0, ::, "") listens
// on every interface, so the probe uses loopback.
func healthURL(listenAddr string) (string, error) {
	host, port, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return "", fmt.Errorf("server: parse listen address %q: %w", listenAddr, err)
	}
	if ip := net.ParseIP(host); host == "" || (ip != nil && ip.IsUnspecified()) {
		host = "127.0.0.1"
	}
	return "http://" + net.JoinHostPort(host, port) + "/healthz", nil
}

// healthcheck calls GET /healthz on the local server and returns nil only for
// a 200. It exists because the container image is distroless: it has no shell,
// curl or wget for a Docker HEALTHCHECK, so the server binary is its own probe.
func healthcheck(ctx context.Context, listenAddr string) error {
	url, err := healthURL(listenAddr)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("server: healthcheck request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("server: healthcheck: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("server: healthcheck: %s answered %d", url, resp.StatusCode)
	}
	return nil
}
