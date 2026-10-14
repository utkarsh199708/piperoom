package integrations

import (
	"context"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/integrations/spec"
)

func connectHandler(withDebezium bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/connectors" && r.URL.Query().Get("expand") == "info":
			class := "io.confluent.connect.jdbc.JdbcSinkConnector"
			if withDebezium {
				class = debeziumPGClass
			}
			_, _ = w.Write([]byte(`{"orders-source":{"info":{"config":{"connector.class":"` + class + `"}}}}`))
		case r.URL.Path == "/connectors":
			_, _ = w.Write([]byte(`["orders-source"]`))
		default:
			http.NotFound(w, r)
		}
	}
}

func TestConnectChecks(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		want    map[string]spec.CheckStatus
	}{
		{"pass", connectHandler(true), map[string]spec.CheckStatus{"reach": spec.CheckPass, "api": spec.CheckPass, "debezium": spec.CheckPass}},
		{"no debezium", connectHandler(false), map[string]spec.CheckStatus{"api": spec.CheckPass, "debezium": spec.CheckWarn}},
		{"401", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnauthorized) }, map[string]spec.CheckStatus{"api": spec.CheckFail}},
		{"404", http.NotFound, map[string]spec.CheckStatus{"api": spec.CheckFail}},
		{"500", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusInternalServerError) }, map[string]spec.CheckStatus{"api": spec.CheckFail}},
		{"redirect not followed", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "http://169.254.169.254/latest/meta-data", http.StatusFound)
		}, map[string]spec.CheckStatus{"api": spec.CheckFail}},
		{"not connect", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`<html>` + canary + `</html>`)) }, map[string]spec.CheckStatus{"api_shape": spec.CheckFail}},
		{"timeout", func(w http.ResponseWriter, r *http.Request) {
			select {
			case <-r.Context().Done():
			case <-time.After(2 * time.Second):
			}
		}, map[string]spec.CheckStatus{"api": spec.CheckFail}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(tc.handler)
			defer srv.Close()
			checks := fastDeps().withDefaults().connectKind().Test(context.Background(), vals(map[string]string{"url": srv.URL}, nil))
			for id, st := range tc.want {
				wantStatus(t, checks, id, st)
			}
			for _, c := range checks {
				if strings.Contains(c.Detail, "<html>") {
					t.Fatal("response body echoed into a check")
				}
			}
			noCanary(t, checks)
		})
	}
}

func TestConnectTimeoutDetail(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer srv.Close()
	checks := fastDeps().withDefaults().connectKind().Test(context.Background(), vals(map[string]string{"url": srv.URL}, nil))
	if c := wantStatus(t, checks, "api", spec.CheckFail); !strings.Contains(c.Detail, "timed out") {
		t.Fatalf("detail = %q", c.Detail)
	}
}

func TestConnectDown(t *testing.T) {
	checks := fastDeps().withDefaults().connectKind().Test(context.Background(), vals(map[string]string{"url": "http://127.0.0.1:" + closedPort(t)}, nil))
	wantStatus(t, checks, "reach", spec.CheckFail)
	wantStatus(t, checks, "api", spec.CheckSkipped)
}

func TestConnectRefusesLoginOverHTTP(t *testing.T) {
	checks := fastDeps().withDefaults().connectKind().Test(context.Background(),
		vals(map[string]string{"url": "http://connect:8083", "username": "u"}, map[string]string{"password": canary}))
	wantStatus(t, checks, "input", spec.CheckFail)
	noCanary(t, checks)
}

func TestConnectHTTPSWithLoginAndCA(t *testing.T) {
	var gotUser, gotPass string
	srv := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser, gotPass, _ = r.BasicAuth()
		connectHandler(true)(w, r)
	}))
	defer srv.Close()
	caPEM := string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}))
	v := vals(map[string]string{"url": srv.URL, "username": "piperoom"}, map[string]string{"password": canary, "ca_cert": caPEM})
	d := fastDeps().withDefaults()
	checks := d.connectKind().Test(context.Background(), v)
	wantStatus(t, checks, "api", spec.CheckPass)
	noCanary(t, checks)
	if gotUser != "piperoom" || gotPass != canary {
		t.Fatalf("basic auth = %q/%q", gotUser, "<hidden>")
	}

	// Build returns a working Connect client over the same settings.
	client, closeFn, err := d.connectKind().Build(context.Background(), v)
	if err != nil {
		t.Fatal(err)
	}
	defer closeFn()
	names, err := client.(clients.ConnectClient).ListConnectors(context.Background())
	if err != nil || len(names) != 1 {
		t.Fatalf("ListConnectors = %v, %v", names, err)
	}

	// Without the CA, verification fails with a fix.
	delete(v.Secrets, "ca_cert")
	checks = d.connectKind().Test(context.Background(), v)
	if c := wantStatus(t, checks, "api", spec.CheckFail); !strings.Contains(c.Detail, "TLS") || c.Fix == "" {
		t.Fatalf("check = %+v", c)
	}
}

func TestRegistryChecks(t *testing.T) {
	var accept string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		accept = r.Header.Get("Accept")
		if r.URL.Path != "/subjects" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`["orders-value","orders-key"]`))
	}))
	defer srv.Close()
	d := fastDeps().withDefaults()
	checks := d.registryKind().Test(context.Background(), vals(map[string]string{"url": srv.URL}, nil))
	wantStatus(t, checks, "api", spec.CheckPass)
	if c := wantStatus(t, checks, "subjects", spec.CheckPass); c.Detail != "2 subject(s)" {
		t.Fatalf("detail = %q", c.Detail)
	}
	if accept != schemaRegistryAcc {
		t.Fatalf("accept = %q", accept)
	}
	client, closeFn, err := d.registryKind().Build(context.Background(), vals(map[string]string{"url": srv.URL}, nil))
	if err != nil {
		t.Fatal(err)
	}
	defer closeFn()
	subs, err := client.(clients.SchemaRegistryClient).Subjects(context.Background())
	if err != nil || len(subs) != 2 {
		t.Fatalf("Subjects = %v, %v", subs, err)
	}

	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte(`{}`)) }))
	defer bad.Close()
	checks = d.registryKind().Test(context.Background(), vals(map[string]string{"url": bad.URL}, nil))
	wantStatus(t, checks, "api_shape", spec.CheckFail)
}

func TestHTTPKindsRefuseMetadata(t *testing.T) {
	d := fastDeps().withDefaults()
	checks := d.registryKind().Test(context.Background(), vals(map[string]string{"url": "http://169.254.169.254"}, nil))
	if c := wantStatus(t, checks, "reach", spec.CheckFail); !strings.Contains(c.Detail, "metadata") {
		t.Fatalf("detail = %q", c.Detail)
	}
	// Build too: the client's dialer goes through the guard.
	client, closeFn, err := d.registryKind().Build(context.Background(), vals(map[string]string{"url": "http://169.254.169.254"}, nil))
	if err != nil {
		t.Fatal(err)
	}
	defer closeFn()
	if _, err := client.(clients.SchemaRegistryClient).Subjects(context.Background()); err == nil || !strings.Contains(err.Error(), "metadata") {
		t.Fatalf("Subjects err = %v", err)
	}
}
