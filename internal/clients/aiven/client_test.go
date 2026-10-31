package aiven

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/setup/targets/managed/replay"
)

func TestCassettes(t *testing.T) {
	for _, tc := range avCases {
		t.Run(tc.name, func(t *testing.T) {
			r := replay.Replay(t, filepath.Join("testdata", "aiven", tc.name+".json"), Scrub)
			tc.run(t, context.Background(), r)
			if n := r.Remaining(); n != 0 {
				t.Errorf("%d interactions unused", n)
			}
		})
	}
}

func TestCassettesCoverTheErrorStatusesAndHoldNoSecret(t *testing.T) {
	seen := map[int]bool{}
	for _, tc := range avCases {
		raw, err := os.ReadFile(filepath.Join("testdata", "aiven", tc.name+".json"))
		if err != nil {
			t.Fatal(err)
		}
		for _, code := range []int{400, 403, 404, 409, 429} {
			if strings.Contains(string(raw), fmt.Sprintf(`"status": %d`, code)) {
				seen[code] = true
			}
		}
		for _, s := range []string{"pw-live-1", "AVNS_pw-live-2", tToken, "aivenv1"} {
			if strings.Contains(string(raw), s) {
				t.Errorf("%s holds %q", tc.name, s)
			}
		}
	}
	for _, code := range []int{400, 403, 404, 409, 429} {
		if !seen[code] {
			t.Errorf("no cassette answers %d", code)
		}
	}
}

// The token is in the Authorization header, never in a URL; it goes to the
// API host over https only.
func TestTokenInAHeaderNeverAURL(t *testing.T) {
	var seen []string
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		seen = append(seen, r.URL.String()+" | "+r.Header.Get("Authorization"))
		return &http.Response{StatusCode: 200, Body: http.NoBody, Header: http.Header{}}, nil
	})
	c := testClient(t, rt)
	ctx := context.Background()
	_, _ = c.ListConnectors(ctx)
	_, _ = c.Status(ctx, "it_orders")
	_, _ = c.CreateConnector(ctx, pgConfig())
	_, _ = c.CreateIntegration(ctx, tKafka)
	if len(seen) != 4 {
		t.Fatalf("requests = %v", seen)
	}
	for _, s := range seen {
		u, h, _ := strings.Cut(s, " | ")
		if strings.Contains(u, tToken) || !strings.HasPrefix(u, "https://api.aiven.io/v1/project/project-0/") {
			t.Errorf("URL %s", u)
		}
		if h != "aivenv1 "+tToken {
			t.Errorf("header %q", h)
		}
	}
	req, _ := http.NewRequest(http.MethodGet, "https://evil.example.com/v1/x", nil)
	if _, err := c.http.Do(req); err == nil {
		t.Error("the token would go to another host")
	}
	req, _ = http.NewRequest(http.MethodGet, "http://api.aiven.io/v1/x", nil)
	if _, err := c.http.Do(req); err == nil {
		t.Error("the token would go over http")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestNewRefuses(t *testing.T) {
	for name, cfg := range map[string]Config{
		"short token":     {Token: "short", Project: tProject, Service: tService},
		"token with a nl": {Token: "0123456789abcdefghij\nX", Project: tProject, Service: tService},
		"http base":       {BaseURL: "http://api.aiven.io", Token: tToken, Project: tProject, Service: tService},
		"userinfo base":   {BaseURL: "https://u:p@api.aiven.io", Token: tToken, Project: tProject, Service: tService},
		"path base":       {BaseURL: "https://api.aiven.io/v1", Token: tToken, Project: tProject, Service: tService},
		"project":         {Token: tToken, Project: "Acme Prod", Service: tService},
		"service":         {Token: tToken, Project: tProject, Service: "../x"},
	} {
		if _, err := New(cfg); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestValidationBeforeRequests(t *testing.T) {
	c := testClient(t, replay.NewCanned())
	ctx := context.Background()
	for name, call := range map[string]func() error{
		"status name":     func() error { _, err := c.Status(ctx, "a/b"); return err },
		"create no class": func() error { _, err := c.CreateConnector(ctx, map[string]string{"name": "x"}); return err },
		"create bad name": func() error {
			_, err := c.CreateConnector(ctx, map[string]string{"name": "a b", "connector.class": "c"})
			return err
		},
		"integration":       func() error { _, err := c.CreateIntegration(ctx, "Kafka_0"); return err },
		"service named bad": func() error { _, err := c.ServiceNamed(ctx, ""); return err },
	} {
		if err := call(); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
	if err := c.RestartTask(ctx, "x", 0); !errors.Is(err, clients.ErrNotSupported) {
		t.Errorf("RestartTask = %v", err)
	}
}
