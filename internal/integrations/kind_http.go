package integrations

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"

	"piperoom/internal/clients/connect"
	"piperoom/internal/clients/schemareg"
	"piperoom/internal/integrations/spec"
)

// HTTP kind ids.
const (
	KindConnectREST    = "connect-rest"
	KindSchemaRegistry = "schema-registry"
)

const (
	urlPattern        = `^https?://[^\s/?#]+(/[^\s?#]*)?$`
	debeziumPGClass   = "io.debezium.connector.postgresql.PostgresConnector"
	schemaRegistryAcc = "application/vnd.schemaregistry.v1+json"
)

func httpFields(urlHelp, userLabel, passLabel string) []spec.Field {
	return []spec.Field{
		{Name: "url", Label: "URL", Help: urlHelp, Type: spec.FieldText, Required: true, Pattern: urlPattern},
		{Name: "username", Label: userLabel, Help: "Empty = no login (only over a private network)", Type: spec.FieldText},
		{Name: "password", Label: passLabel, Type: spec.FieldSecret},
		{Name: "ca_cert", Label: "CA certificate", Help: "PEM; empty = the system's trusted CAs", Type: spec.FieldPEM},
		{Name: "client_cert", Label: "Client certificate", Help: "PEM, for mutual TLS", Type: spec.FieldPEM},
		{Name: "client_key", Label: "Client key", Help: "PEM, for mutual TLS", Type: spec.FieldPEM},
	}
}

func httpOptsFrom(v spec.Values, dial dialFunc) httpOpts {
	return httpOpts{
		BaseURL: strings.TrimSpace(v.Get("url")), Username: strings.TrimSpace(v.Get("username")), Password: v.Secret("password"),
		TLS:  tlsMaterial{CA: v.Secret("ca_cert"), Cert: v.Secret("client_cert"), Key: v.Secret("client_key")},
		Dial: dial,
	}
}

// hostPort returns the host and port of u (default port by scheme).
func hostPort(u *url.URL) (string, string) {
	port := u.Port()
	if port == "" {
		port = "80"
		if u.Scheme == "https" {
			port = "443"
		}
	}
	return u.Hostname(), port
}

// restTest is the shared shape of the Connect and Registry tests: reach,
// then one GET that must answer 200, then extra checks on its body.
func (d Deps) restTest(what, path, accept, okTitle string, extra func(cl *checklist, body []byte, c *http.Client, base *url.URL)) func(ctx context.Context, v spec.Values) []spec.Check {
	return func(ctx context.Context, v spec.Values) []spec.Check {
		ctx, cancel := testCtx(ctx, d.TestTimeout)
		defer cancel()
		cl := newChecklist(ctx, d.CheckTimeout, v)
		c, base, err := newHTTPClient(httpOptsFrom(v, d.dial()))
		if err != nil {
			cl.add("input", "Form values are valid", fail(strings.TrimPrefix(err.Error(), "integrations: bad input: "), ""))
			return cl.result()
		}
		c.Timeout = 0 // the per-check context bounds each call
		defer c.CloseIdleConnections()
		host, port := hostPort(base)
		if cl.run("reach", "Can reach "+net.JoinHostPort(host, port), reachCheck(d.dial(), host, port)) != spec.CheckPass {
			cl.skip("api", okTitle, "skipped: the host is not reachable")
			return cl.result()
		}
		var body []byte
		st := cl.run("api", okTitle, func(ctx context.Context) outcome {
			status, b, err := getBody(ctx, c, base, path, accept)
			switch {
			case err != nil:
				return httpFailure(err, what)
			case status == http.StatusUnauthorized || status == http.StatusForbidden:
				return fail(fmt.Sprintf("%s rejected the login (HTTP %d)", what, status), "Check the username and password (for Confluent Cloud: an API key for this resource).")
			case status == http.StatusNotFound:
				return fail(fmt.Sprintf("GET %s answered 404", path), "Check the URL: it must be the base URL of the "+what+" REST API.")
			case status >= 300 && status < 400:
				return fail(fmt.Sprintf("GET %s answered a redirect (HTTP %d); redirects are not followed", path, status), "Use the final URL, usually https:// instead of http://.")
			case status != http.StatusOK:
				return fail(fmt.Sprintf("GET %s answered HTTP %d", path, status), "")
			}
			body = b
			return pass("")
		})
		if st == spec.CheckPass && extra != nil {
			extra(cl, body, c, base)
		}
		return cl.result()
	}
}

func (d Deps) connectKind() spec.Kind {
	test := d.restTest("Kafka Connect", "/connectors", "application/json", "GET /connectors works", func(cl *checklist, body []byte, c *http.Client, base *url.URL) {
		var names []string
		if err := json.Unmarshal(body, &names); err != nil {
			cl.add("api_shape", "Answer is a Kafka Connect API", fail("GET /connectors did not return a JSON list of names", "Check the URL: it must be the Kafka Connect REST API."))
			return
		}
		cl.run("debezium", "A Debezium Postgres connector exists", func(ctx context.Context) outcome {
			status, b, err := getBody(ctx, c, base, "/connectors?expand=info", "application/json")
			if err != nil || status != http.StatusOK {
				return warn("could not read connector classes", "")
			}
			var info map[string]struct {
				Info struct {
					Config map[string]string `json:"config"`
				} `json:"info"`
			}
			if err := json.Unmarshal(b, &info); err != nil {
				return warn("this Connect version does not support ?expand=info; connector classes not checked", "")
			}
			for _, ci := range info {
				if ci.Info.Config["connector.class"] == debeziumPGClass {
					return pass(fmt.Sprintf("%d connector(s)", len(names)))
				}
			}
			return warn(fmt.Sprintf("no %s among %d connector(s)", debeziumPGClass, len(names)),
				"Check that this is the Connect cluster that runs the pipeline's source connector.")
		})
	})
	return spec.Kind{
		ID: KindConnectREST, Category: spec.CategoryConnect, Title: "Kafka Connect REST",
		Summary: "The Connect cluster that runs Debezium", Logo: "kafka-connect",
		Reads:    []string{"connector and task status, with error traces", "connector configs (secret-looking values are masked)"},
		Never:    []string{"changes a connector unless every required layer owner approves a fix"},
		Fields:   httpFields("e.g. https://connect.corp:8083", "Username", "Password"),
		Required: true, Test: test,
		Build: func(_ context.Context, v spec.Values) (any, func(), error) {
			c, base, err := newHTTPClient(httpOptsFrom(v, d.dial()))
			if err != nil {
				return nil, nil, err
			}
			return connect.New(base.String(), c), c.CloseIdleConnections, nil
		},
	}
}

func (d Deps) registryKind() spec.Kind {
	test := d.restTest("Schema Registry", "/subjects", schemaRegistryAcc, "GET /subjects works", func(cl *checklist, body []byte, _ *http.Client, _ *url.URL) {
		var subjects []string
		if err := json.Unmarshal(body, &subjects); err != nil {
			cl.add("api_shape", "Answer is a Schema Registry API", fail("GET /subjects did not return a JSON list", "Check the URL: it must be the Schema Registry base URL."))
			return
		}
		cl.add("subjects", "Subjects are visible", pass(fmt.Sprintf("%d subject(s)", len(subjects))))
	})
	return spec.Kind{
		ID: KindSchemaRegistry, Category: spec.CategoryRegistry, Title: "Schema Registry",
		Summary: "Confluent, Apicurio (ccompat API) or Karapace", Logo: "schema-registry",
		Reads:  []string{"subjects, versions and schemas", "compatibility levels"},
		Never:  []string{"registers a schema or changes compatibility unless every required layer owner approves a fix"},
		Fields: httpFields("e.g. https://psrc-xxxxx.us-east-1.aws.confluent.cloud", "Username / API key", "Password / API secret"),
		Test:   test,
		Build: func(_ context.Context, v spec.Values) (any, func(), error) {
			c, base, err := newHTTPClient(httpOptsFrom(v, d.dial()))
			if err != nil {
				return nil, nil, err
			}
			return schemareg.New(base.String(), c), c.CloseIdleConnections, nil
		},
	}
}
