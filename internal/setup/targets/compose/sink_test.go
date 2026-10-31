package compose

import (
	"errors"
	"strings"
	"testing"

	"piperoom/internal/setup/knowledge"
	"piperoom/internal/setup/protocol"
)

// The sink-postgres template (row U3c) renders the dev warehouse beside the
// source, the validator accepts it, and its port and .env keys are its own.
func TestRenderSinkPostgres(t *testing.T) {
	d := &Driver{class: protocol.ClassLocal}
	got, err := d.Render(WriteParams{Templates: []string{TemplateKafka, TemplatePostgres, TemplateConnect, TemplateSinkPostgres}})
	if err != nil {
		t.Fatalf("Render: %v", err)
	}
	if err := ValidateYAML(got.Doc, protocol.ClassLocal); err != nil {
		t.Fatalf("the sink render fails the validator: %v", err)
	}
	if strings.Join(got.Services, ",") != "connect,kafka,postgres,warehouse" {
		t.Fatalf("services = %v", got.Services)
	}
	if strings.Join(got.EnvKeys, ",") != "POSTGRES_DB,POSTGRES_PASSWORD,POSTGRES_USER,WAREHOUSE_DB,WAREHOUSE_PASSWORD,WAREHOUSE_USER" {
		t.Fatalf("env keys = %v", got.EnvKeys)
	}
	if !strings.Contains(string(got.Doc), "127.0.0.1:5433:5432") {
		t.Fatal("the warehouse is not published on loopback 5433")
	}

	// The warehouse alone renders (a sink for a source outside the stack).
	alone, err := d.Render(WriteParams{Templates: []string{TemplateSinkPostgres}, Ports: map[string]int{"warehouse": 15433}})
	if err != nil {
		t.Fatalf("Render(sink alone): %v", err)
	}
	if !strings.Contains(string(alone.Doc), "127.0.0.1:15433:5432") {
		t.Fatalf("the warehouse port param was not applied:\n%s", alone.Doc)
	}
	if _, err := d.Render(WriteParams{Templates: []string{TemplateKafka}, Ports: map[string]int{"warehouse": 15433}}); !errors.Is(err, ErrParams) {
		t.Fatalf("a warehouse port without the template: %v", err)
	}
}

// The validator accepts the new template and nothing else: a warehouse that
// interpolates a key outside the .env allow-list, or asks for a forbidden
// field, is refused like any other service.
func TestValidatorRefusesTamperedSink(t *testing.T) {
	d := &Driver{class: protocol.ClassLocal}
	got, err := d.Render(WriteParams{Templates: []string{TemplateSinkPostgres}})
	if err != nil {
		t.Fatal(err)
	}
	doc := string(got.Doc)
	for name, tampered := range map[string]string{
		"unknown env key":     strings.Replace(doc, "${WAREHOUSE_DB}", "${WAREHOUSE_TOKEN}", 1),
		"runner env":          strings.Replace(doc, "${WAREHOUSE_USER}", "${HOME}", 1),
		"privileged":          strings.Replace(doc, "  warehouse:\n", "  warehouse:\n    privileged: true\n", 1),
		"public port, local":  strings.Replace(doc, "127.0.0.1:5433:5432", "0.0.0.0:5433:5432", 1),
		"unpinned image":      strings.Replace(doc, "docker.io/postgres:17@sha256:", "docker.io/postgres:latest@x", 1),
		"host bind of /":      strings.Replace(doc, "- /var/lib/postgresql/data", "- /:/host", 1),
		"docker socket mount": strings.Replace(doc, "- /var/lib/postgresql/data", "- /var/run/docker.sock:/var/run/docker.sock", 1),
	} {
		if tampered == doc {
			t.Fatalf("%s: the tamper did not apply", name)
		}
		if err := ValidateYAML([]byte(tampered), protocol.ClassLocal); err == nil {
			t.Errorf("%s: the validator accepted a tampered warehouse", name)
		}
	}
}

// plugin_install resolves the JDBC sink from the knowledge index's pinned
// sink archives, never from the host that serves it.
func TestJDBCSinkPluginResolves(t *testing.T) {
	art, err := knowledgePlugin("3.2.0.Final", SinkConnectorJDBC)
	if err != nil {
		t.Fatalf("knowledgePlugin(jdbc): %v", err)
	}
	if !strings.HasSuffix(art.URL, "/debezium-connector-jdbc-3.2.0.Final-plugin.tar.gz") || !strings.HasPrefix(art.URL, "https://repo1.maven.org/") {
		t.Fatalf("URL = %q", art.URL)
	}
	if !sha512RE.MatchString(art.Digest) || art.Dir != "debezium-connector-jdbc" {
		t.Fatalf("artifact = %+v", art)
	}
	if _, err := knowledgePlugin("1.0.0.Final", SinkConnectorJDBC); !errors.Is(err, knowledge.ErrUnknownRelease) {
		t.Fatalf("an unpinned release: %v", err)
	}
	// The source connectors still resolve through the plugin list.
	if _, err := knowledgePlugin("3.2.0.Final", "postgres"); err != nil {
		t.Fatalf("postgres: %v", err)
	}
}

func TestPropertiesWriteWarehouseEnv(t *testing.T) {
	h := newHarness(t, protocol.ClassLocal)
	if _, err := h.run(t, "properties_write", PropertiesParams{File: ".env", Entries: map[string]string{
		"WAREHOUSE_USER": "warehouse", "WAREHOUSE_DB": "analytics", "WAREHOUSE_PASSWORD": "${secret:warehouse_password}",
	}}); err != nil {
		t.Fatalf("properties_write: %v", err)
	}
	if _, err := h.run(t, "properties_write", PropertiesParams{File: ".env", Entries: map[string]string{
		"WAREHOUSE_PASSWORD": "hunter2",
	}}); err == nil {
		t.Fatal("a plain-text warehouse password was written")
	}
}
