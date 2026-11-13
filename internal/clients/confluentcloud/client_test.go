package confluentcloud

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
	for _, tc := range ccCases {
		t.Run(tc.name, func(t *testing.T) {
			r := replay.Replay(t, filepath.Join("testdata", "confluent", tc.name+".json"), Scrub)
			tc.run(t, context.Background(), r)
			if n := r.Remaining(); n != 0 {
				t.Errorf("%d interactions unused", n)
			}
		})
	}
}

func TestCassettesCoverTheErrorStatuses(t *testing.T) {
	seen := map[int]bool{}
	for _, tc := range ccCases {
		raw, err := os.ReadFile(filepath.Join("testdata", "confluent", tc.name+".json"))
		if err != nil {
			t.Fatal(err)
		}
		for _, code := range []int{400, 401, 403, 404, 409, 429} {
			if strings.Contains(string(raw), fmt.Sprintf(`"status": %d`, code)) {
				seen[code] = true
			}
		}
		if strings.Contains(string(raw), "pw-live-1") {
			t.Errorf("%s holds the password", tc.name)
		}
	}
	for _, code := range []int{400, 401, 403, 404, 409, 429} {
		if !seen[code] {
			t.Errorf("no cassette answers %d", code)
		}
	}
}

// Basic auth over https only (netsec's rule), and never to another host.
func TestBasicAuthOverHTTPSOnly(t *testing.T) {
	if _, err := New(Config{BaseURL: "http://api.confluent.cloud", APIKey: tKey, APISecret: tSecret, Environment: tEnv, Cluster: tCluster}); err == nil {
		t.Error("an http base URL with a login was accepted")
	}
	if _, err := New(Config{BaseURL: "https://u:p@api.confluent.cloud", APIKey: tKey, APISecret: tSecret, Environment: tEnv, Cluster: tCluster}); err == nil {
		t.Error("a base URL with userinfo was accepted")
	}
	if _, err := New(Config{Environment: tEnv, Cluster: tCluster}); !errors.Is(err, ErrInvalid) {
		t.Errorf("no key: %v", err)
	}
	for _, ep := range []string{"http://pkc-1.confluent.cloud", "https://evil.example.com", "https://pkc-1.confluent.cloud/kafka", "https://u:p@pkc-1.confluent.cloud"} {
		if _, err := NewACLClient(ACLConfig{Endpoint: ep, Cluster: tCluster, Key: tKey, Secret: tSecret}); err == nil {
			t.Errorf("ACL endpoint %s accepted", ep)
		}
	}
	var sent []string
	rt := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		u, p, ok := r.BasicAuth()
		sent = append(sent, fmt.Sprintf("%s %v %s %s", r.URL.Host, ok, u, p))
		return &http.Response{StatusCode: 200, Body: http.NoBody, Header: http.Header{}}, nil
	})
	c := testClient(t, rt)
	if _, err := c.ListConnectors(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(sent) != 1 || sent[0] != "api.confluent.cloud true "+tKey+" "+tSecret {
		t.Errorf("sent = %v", sent)
	}
	// A request to another host through the same transport is refused.
	req, _ := http.NewRequest(http.MethodGet, "https://evil.example.com/x", nil)
	if _, err := c.http.Do(req); err == nil {
		t.Error("credentials would go to another host")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestValidationBeforeRequests(t *testing.T) {
	c := testClient(t, replay.NewCanned())
	a := testACL(t, replay.NewCanned())
	ctx := context.Background()
	for name, call := range map[string]func() error{
		"name":         func() error { _, err := c.Status(ctx, "a/b"); return err },
		"plugin":       func() error { _, err := c.Validate(ctx, "../x", map[string]string{"a": "b"}); return err },
		"empty config": func() error { _, err := c.CreateConnector(ctx, "x", nil); return err },
		"acl op": func() error {
			return a.CreateACL(ctx, ACL{ResourceType: "TOPIC", ResourceName: "t", PatternType: "LITERAL", Principal: tPrincipal, Host: "*", Operation: "ALL", Permission: "ALLOW"})
		},
		"acl deny": func() error {
			return a.CreateACL(ctx, ACL{ResourceType: "TOPIC", ResourceName: "t", PatternType: "LITERAL", Principal: tPrincipal, Host: "*", Operation: "READ", Permission: "DENY"})
		},
		"acl principal": func() error {
			return a.CreateACL(ctx, ACL{ResourceType: "TOPIC", ResourceName: "t", PatternType: "LITERAL", Principal: "User:*", Host: "*", Operation: "READ", Permission: "ALLOW"})
		},
		"acl host": func() error {
			return a.CreateACL(ctx, ACL{ResourceType: "TOPIC", ResourceName: "t", PatternType: "LITERAL", Principal: tPrincipal, Host: "10.0.0.1", Operation: "READ", Permission: "ALLOW"})
		},
		"acl wildcard": func() error {
			return a.CreateACL(ctx, ACL{ResourceType: "TOPIC", ResourceName: "*", PatternType: "LITERAL", Principal: tPrincipal, Host: "*", Operation: "READ", Permission: "ALLOW"})
		},
		"bad environment": func() error {
			_, err := New(Config{APIKey: tKey, APISecret: tSecret, Environment: "prod", Cluster: tCluster})
			return err
		},
	} {
		if err := call(); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func TestRestartIsNotSupported(t *testing.T) {
	c := testClient(t, replay.NewCanned())
	if err := c.RestartConnector(context.Background(), "x"); !errors.Is(err, clients.ErrNotSupported) {
		t.Errorf("RestartConnector = %v", err)
	}
}
