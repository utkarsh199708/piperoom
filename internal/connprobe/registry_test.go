package connprobe

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"piperoom/internal/config"
	"piperoom/internal/owners"
)

type routes = map[string]func(http.ResponseWriter, *http.Request)

func writeBody(s string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(s)) }
}

func writeStatus(code int) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(code)
		_, _ = w.Write([]byte(`{"error_code":40401,"message":"internal ` + secretValue + `"}`))
	}
}

// fakeRegistry answers like Schema Registry with the subjects of
// pg.public.orders; the subject has no level of its own, the global is BACKWARD.
func fakeRegistry(t *testing.T, over routes) http.Handler {
	t.Helper()
	base := routes{
		"/subjects":                        writeBody(`["pg.public.orders-value","pg.public.orders-key","other-value"]`),
		"/config":                          writeBody(`{"compatibilityLevel":"BACKWARD"}`),
		"/config/pg.public.orders-value":   writeStatus(http.StatusNotFound),
		"/config/pg.public.orders-key":     writeStatus(http.StatusNotFound),
		"/subjects/pg.public.orders-value": writeBody(`[1]`),
	}
	for k, v := range over {
		base[k] = v
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			t.Errorf("probe sent %s %s; probes only read", r.Method, r.URL.Path)
		}
		if h, ok := base[r.URL.EscapedPath()]; ok {
			h(w, r)
			return
		}
		http.NotFound(w, r)
	})
}

func registryTarget(url string) Target {
	return Target{
		Kind:      KindSchemaRegistry,
		Config:    config.Config{SchemaRegistryURL: url},
		Pipelines: []owners.Pipeline{{ID: "orders", TopicPrefix: "pg", SourceTables: []string{"public.orders"}}},
	}
}

func TestRegistryOK(t *testing.T) {
	srv := httptest.NewServer(fakeRegistry(t, nil))
	defer srv.Close()
	r := Run(context.Background(), registryTarget(srv.URL), loopGuard())
	wantChecks(t, r, "config", StatusOK, "reach", StatusOK, "login", StatusOK, "subjects", StatusOK, "compatibility", StatusOK)
	if d := check(t, r, "compatibility").Detail; !strings.Contains(d, "pg.public.orders-value BACKWARD (global default)") {
		t.Errorf("compatibility detail %q", d)
	}
	if d := check(t, r, "subjects").Detail; d != "1 of 1 value subjects, 1 key subjects" {
		t.Errorf("subjects detail %q", d)
	}
	assertNoSecret(t, r, secretValue, "other-value")
}

func TestRegistrySubjectLevelAndNone(t *testing.T) {
	srv := httptest.NewServer(fakeRegistry(t, routes{
		"/config/pg.public.orders-value": writeBody(`{"compatibilityLevel":"NONE"}`),
		"/config/pg.public.orders-key":   writeBody(`{"compatibilityLevel":"FULL_TRANSITIVE"}`),
	}))
	defer srv.Close()
	r := Run(context.Background(), registryTarget(srv.URL), loopGuard())
	c := check(t, r, "compatibility")
	if c.Status != StatusWarn || !strings.Contains(c.Problem, "pg.public.orders-value") || !strings.Contains(c.Problem, "incompatible") {
		t.Errorf("compatibility: %+v", c)
	}
	if !strings.Contains(c.Detail, "pg.public.orders-key FULL_TRANSITIVE") {
		t.Errorf("detail %q", c.Detail)
	}
	if !r.OK {
		t.Error("a warning must not fail the probe")
	}
}

func TestRegistryUnknownLevelHidden(t *testing.T) {
	srv := httptest.NewServer(fakeRegistry(t, routes{"/config": writeBody(`{"compatibilityLevel":"` + secretValue + `"}`)}))
	defer srv.Close()
	r := Run(context.Background(), registryTarget(srv.URL), loopGuard())
	if d := check(t, r, "compatibility").Detail; !strings.Contains(d, "unknown") {
		t.Errorf("detail %q", d)
	}
	assertNoSecret(t, r, secretValue)
}

func TestRegistryMissingSubject(t *testing.T) {
	srv := httptest.NewServer(fakeRegistry(t, routes{"/subjects": writeBody(`["other-value"]`)}))
	defer srv.Close()
	r := Run(context.Background(), registryTarget(srv.URL), loopGuard())
	wantChecks(t, r, "config", StatusOK, "reach", StatusOK, "login", StatusOK, "subjects", StatusWarn, "compatibility", StatusOK)
	if p := check(t, r, "subjects").Problem; !strings.Contains(p, "pg.public.orders-value") {
		t.Errorf("problem %q", p)
	}
	// With no pipeline subject, the global level is reported.
	if d := check(t, r, "compatibility").Detail; d != "global BACKWARD" {
		t.Errorf("detail %q", d)
	}
}

func TestRegistryStatuses(t *testing.T) {
	big := strings.Repeat("y", maxHTTPBody+1)
	tests := []struct {
		name string
		over routes
		want []string
		in   string
	}{
		{"401", routes{"/subjects": writeStatus(http.StatusUnauthorized)},
			[]string{"config", StatusOK, "reach", StatusOK, "login", StatusFail, "subjects", StatusSkip, "compatibility", StatusSkip}, "needs a login"},
		{"500", routes{"/subjects": writeStatus(http.StatusInternalServerError)},
			[]string{"config", StatusOK, "reach", StatusFail, "login", StatusSkip, "subjects", StatusSkip, "compatibility", StatusSkip}, "HTTP 500"},
		{"404", routes{"/subjects": writeStatus(http.StatusNotFound)},
			[]string{"config", StatusOK, "reach", StatusFail, "login", StatusSkip, "subjects", StatusSkip, "compatibility", StatusSkip}, "404"},
		{"malformed", routes{"/subjects": writeBody(`{"subjects":`)},
			[]string{"config", StatusOK, "reach", StatusFail, "login", StatusSkip, "subjects", StatusSkip, "compatibility", StatusSkip}, "not what"},
		{"oversized", routes{"/subjects": writeBody(big)},
			[]string{"config", StatusOK, "reach", StatusFail, "login", StatusSkip, "subjects", StatusSkip, "compatibility", StatusSkip}, "4 MB"},
		{"config forbidden", routes{"/config": writeStatus(http.StatusForbidden)},
			[]string{"config", StatusOK, "reach", StatusOK, "login", StatusOK, "subjects", StatusOK, "compatibility", StatusFail}, "global compatibility"},
		{"config malformed", routes{"/config": writeBody(`nope`)},
			[]string{"config", StatusOK, "reach", StatusOK, "login", StatusOK, "subjects", StatusOK, "compatibility", StatusFail}, "not Schema Registry JSON"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(fakeRegistry(t, tc.over))
			defer srv.Close()
			r := Run(context.Background(), registryTarget(srv.URL), loopGuard())
			wantChecks(t, r, tc.want...)
			if r.OK {
				t.Error("want not ok")
			}
			for _, c := range r.Checks {
				if c.Status == StatusFail && !strings.Contains(c.Problem, tc.in) {
					t.Errorf("problem %q does not contain %q", c.Problem, tc.in)
				}
			}
			assertNoSecret(t, r, secretValue)
		})
	}
}

func TestRegistryBasicAuthNoLeak(t *testing.T) {
	srv, ca := tlsServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, _ := r.BasicAuth()
		if u != testUser || p != testPassword {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		fakeRegistry(t, nil).ServeHTTP(w, r)
	}))
	tg := registryTarget(srv.URL)
	tg.Config.SchemaRegistryUsername, tg.Config.SchemaRegistryPassword, tg.Config.SchemaRegistryTLSCAFile = testUser, testPassword, ca
	r := Run(context.Background(), tg, loopGuard())
	wantChecks(t, r, "config", StatusOK, "reach", StatusOK, "login", StatusOK, "subjects", StatusOK, "compatibility", StatusOK)
	assertNoSecret(t, r, testUser, testPassword)

	tg.Config.SchemaRegistryPassword = "nope-" + testPassword
	r = Run(context.Background(), tg, loopGuard())
	c := check(t, r, "login")
	if c.Status != StatusFail || !strings.Contains(c.Fix, "SCHEMA_REGISTRY_PASSWORD") {
		t.Errorf("login: %+v", c)
	}
	assertNoSecret(t, r, testUser, testPassword)
}

func TestRegistryTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	r := Run(ctx, registryTarget(srv.URL), loopGuard())
	if c := check(t, r, "reach"); c.Status != StatusFail || !strings.Contains(c.Problem, "No answer") {
		t.Errorf("reach: %+v", c)
	}
}

func TestRegistryGuardAndConfig(t *testing.T) {
	srv := httptest.NewServer(fakeRegistry(t, nil))
	defer srv.Close()
	r := Run(context.Background(), registryTarget(srv.URL), &Guard{})
	if c := check(t, r, "reach"); c.Status != StatusFail || !strings.Contains(c.Problem, "not allowed") {
		t.Errorf("reach: %+v", c)
	}
	r = Run(context.Background(), registryTarget(""), loopGuard())
	wantChecks(t, r, "config", StatusFail, "reach", StatusSkip, "login", StatusSkip, "subjects", StatusSkip, "compatibility", StatusSkip)
	if p := check(t, r, "config").Problem; !strings.Contains(p, "SCHEMA_REGISTRY_URL") {
		t.Errorf("problem %q", p)
	}
}
