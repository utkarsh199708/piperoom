package ops

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"piperoom/internal/setup/knowledge"
	"piperoom/internal/setup/protocol"
)

// embeddedRegistry is a registry in embedded mode over a temp project dir. An
// Exec spy fails the test if any op tries to run a program.
func embeddedRegistry(t *testing.T) (*Registry, string) {
	t.Helper()
	dir := t.TempDir()
	r, err := New(Config{ProjectDir: dir, Exec: func(_ context.Context, name string, args ...string) ([]byte, int, error) {
		t.Errorf("an embedded op ran a program: %s %v", name, args)
		return nil, 1, errors.New("no exec in this test")
	}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	if err := RegisterEmbedded(r); err != nil {
		t.Fatalf("RegisterEmbedded: %v", err)
	}
	return r, dir
}

func run(t *testing.T, r *Registry, op string, params any) (protocol.OpResult, error) {
	t.Helper()
	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	return r.Execute(context.Background(), protocol.OpRequest{ID: "op_1", Op: op, Params: raw})
}

func writeProjectFile(t *testing.T, dir, name, content string) {
	t.Helper()
	p := filepath.Join(dir, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

// TestEmbeddedCatalogueRunsNothing (U6b "Done when"): the embedded catalogue
// has no op that builds or runs the reference project, and the embedded ops
// themselves never run a program.
func TestEmbeddedCatalogueRunsNothing(t *testing.T) {
	r, _ := embeddedRegistry(t)
	for _, name := range r.Names() {
		for _, bad := range []string{"reference_run", "build", "_run", "mvn", "gradle", "jar_run", "exec", "shell"} {
			if strings.Contains(name, bad) {
				t.Errorf("the embedded catalogue has %q, which looks like it builds or runs something", name)
			}
		}
	}
	for op, tier := range map[string]string{OpProjectInspect: protocol.TierT0, OpReferenceWrite: protocol.TierT1, OpEmbeddedLogRead: protocol.TierT0} {
		if got, ok := r.Floor(op); !ok || got != tier {
			t.Errorf("%s floor = %q (registered %v), want %q", op, got, ok, tier)
		}
	}
	plain, err := New(Config{ProjectDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = plain.Close() })
	if _, ok := plain.Floor(OpReferenceWrite); ok {
		t.Error("New alone registers reference_write")
	}
}

// In embedded mode no write op but reference_write runs, whatever the
// params; reads still run; and file_write cannot reach piperoom-reference/
// in any mode.
func TestEmbeddedModeWritesOnlyTheReferenceProject(t *testing.T) {
	r, dir := embeddedRegistry(t)
	for _, op := range []string{"file_write", "file_delete"} {
		params := json.RawMessage(`{"path":"notes.txt","content":"x"}`)
		if op == "file_delete" {
			params = json.RawMessage(`{"path":"notes.txt"}`)
		}
		if got := r.CheckTarget(protocol.OpRequest{Op: op, Params: params}); got != protocol.RefuseTarget {
			t.Errorf("CheckTarget(%s) = %q, want %q", op, got, protocol.RefuseTarget)
		}
		if _, err := r.Execute(context.Background(), protocol.OpRequest{ID: "x", Op: op, Params: params}); !errors.Is(err, ErrEmbeddedMode) {
			t.Errorf("%s in embedded mode: err = %v, want ErrEmbeddedMode", op, err)
		}
	}
	if _, err := os.Stat(filepath.Join(dir, "notes.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a refused file_write left a file: %v", err)
	}
	if got := r.CheckTarget(protocol.OpRequest{Op: OpProjectInspect, Params: json.RawMessage(`{}`)}); got != "" {
		t.Errorf("CheckTarget(project_inspect) = %q, want none", got)
	}

	// Outside embedded mode, file_write still never reaches the reference
	// project: it is a mount directory.
	plain, err := New(Config{ProjectDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = plain.Close() })
	if err := plain.RegisterMountDir(knowledge.EmbeddedRootDir); err != nil {
		t.Fatal(err)
	}
	op, _ := plain.Get("file_write")
	if err := op.Validate(json.RawMessage(`{"path":"piperoom-reference/pom.xml","content":"<project/>"}`), plain.snapshotEnv()); err == nil {
		t.Error("file_write accepted a path under piperoom-reference/")
	}
}

func referenceParams() ReferenceParams {
	return ReferenceParams{
		Build: "maven", Source: "postgres", GroupID: "com.example.cdc", ArtifactID: "orders-cdc",
		Release: "3.2.0.Final", Host: "db.internal", Port: 5432, Database: "orders", User: "debezium",
		PasswordSecret: "orders_db_password", TopicPrefix: "orders", Tables: []string{"public.orders"},
		SlotName: "orders_cdc",
	}
}

func TestReferenceWrite(t *testing.T) {
	r, dir := embeddedRegistry(t)
	res, err := run(t, r, OpReferenceWrite, referenceParams())
	if err != nil {
		t.Fatalf("reference_write: %v (%s)", err, res.Summary)
	}
	var data struct {
		Dir   string   `json:"dir"`
		Files []string `json:"files"`
	}
	if err := json.Unmarshal(res.Data, &data); err != nil {
		t.Fatal(err)
	}
	if data.Dir != knowledge.EmbeddedRootDir || len(data.Files) == 0 {
		t.Fatalf("data = %+v", data)
	}
	for _, f := range data.Files {
		if !strings.HasPrefix(f, knowledge.EmbeddedRootDir+"/") {
			t.Errorf("reference_write wrote %q, outside %s/", f, knowledge.EmbeddedRootDir)
		}
		if _, err := os.Stat(filepath.Join(dir, filepath.FromSlash(f))); err != nil {
			t.Errorf("%s was not written: %v", f, err)
		}
	}
	pom, err := os.ReadFile(filepath.Join(dir, knowledge.EmbeddedRootDir, "pom.xml"))
	if err != nil {
		t.Fatalf("no pom.xml: %v", err)
	}
	if !strings.Contains(string(pom), "debezium") {
		t.Fatal("the rendered pom names no Debezium artifact")
	}
	for _, f := range data.Files {
		b, _ := os.ReadFile(filepath.Join(dir, filepath.FromSlash(f)))
		if strings.Contains(string(b), "password=") && !strings.Contains(string(b), "${secret:") {
			t.Errorf("%s carries a password value", f)
		}
	}

	// Bad params never render.
	bad := referenceParams()
	bad.GroupID = "com.example;rm -rf /"
	if _, err := run(t, r, OpReferenceWrite, bad); !errors.Is(err, ErrParams) {
		t.Fatalf("bad group id: err = %v, want ErrParams", err)
	}
}

func TestProjectInspectMaven(t *testing.T) {
	r, dir := embeddedRegistry(t)
	writeProjectFile(t, dir, "pom.xml", `<?xml version="1.0"?>
<project>
  <properties>
    <maven.compiler.release>11</maven.compiler.release>
    <debezium.version>3.2.0.Final</debezium.version>
    <kafka.version>3.7.1</kafka.version>
  </properties>
  <dependencies>
    <dependency><groupId>io.debezium</groupId><artifactId>debezium-embedded</artifactId><version>${debezium.version}</version></dependency>
    <dependency><groupId>org.apache.kafka</groupId><artifactId>connect-runtime</artifactId><version>${kafka.version}</version></dependency>
  </dependencies>
</project>`)
	// Source files are never read: a coordinate in Java code must not count.
	writeProjectFile(t, dir, "src/main/java/App.java", `// "io.debezium:debezium-embedded:9.9.9.Final"`)
	// The reference project's own versions are not the person's.
	writeProjectFile(t, dir, "piperoom-reference/pom.xml", `<project><dependencies><dependency><groupId>io.debezium</groupId><artifactId>debezium-embedded</artifactId><version>1.0.0.Final</version></dependency></dependencies></project>`)

	res, err := run(t, r, OpProjectInspect, map[string]any{})
	if err != nil {
		t.Fatalf("project_inspect: %v", err)
	}
	var data struct {
		BuildFiles []string            `json:"build_files"`
		BuildTool  string              `json:"build_tool"`
		Versions   inspectedVersions   `json:"versions"`
		Findings   []knowledge.Finding `json:"findings"`
	}
	if err := json.Unmarshal(res.Data, &data); err != nil {
		t.Fatal(err)
	}
	if len(data.BuildFiles) != 1 || data.BuildFiles[0] != "pom.xml" || data.BuildTool != "maven" {
		t.Fatalf("build files = %v (%s), want only pom.xml", data.BuildFiles, data.BuildTool)
	}
	want := inspectedVersions{Engine: "3.2.0.Final", Kafka: "3.7.1", Java: 11}
	if data.Versions != want {
		t.Fatalf("versions = %+v, want %+v", data.Versions, want)
	}
	rules := map[string]string{}
	for _, f := range data.Findings {
		rules[f.Rule] = f.Severity
	}
	if rules["dbz3-java17"] != knowledge.SeverityError {
		t.Fatalf("findings = %+v, want dbz3-java17 as an error (Debezium 3.2 on Java 11)", data.Findings)
	}
	if !strings.Contains(res.Summary, "debezium-embedded 3.2.0.Final") {
		t.Fatalf("summary = %q", res.Summary)
	}
}

func TestProjectInspectGradle(t *testing.T) {
	r, dir := embeddedRegistry(t)
	writeProjectFile(t, dir, "gradle.properties", "debeziumVersion=2.7.3.Final\n")
	writeProjectFile(t, dir, "build.gradle", `
plugins { id 'java' }
java { toolchain { languageVersion = JavaLanguageVersion.of(17) } }
dependencies {
  implementation "io.debezium:debezium-embedded:${debeziumVersion}"
  implementation 'org.apache.kafka:kafka-clients:3.7.0'
}`)
	writeProjectFile(t, dir, "orders/gradle/libs.versions.toml", `
[versions]
quarkus = "3.15.1"
[libraries]
quarkus-bom = { module = "io.quarkus.platform:quarkus-bom", version.ref = "quarkus" }
`)
	res, err := run(t, r, OpProjectInspect, map[string]any{})
	if err != nil {
		t.Fatalf("project_inspect: %v", err)
	}
	var data struct {
		BuildFiles []string          `json:"build_files"`
		Versions   inspectedVersions `json:"versions"`
	}
	if err := json.Unmarshal(res.Data, &data); err != nil {
		t.Fatal(err)
	}
	want := inspectedVersions{Engine: "2.7.3.Final", Kafka: "3.7.0", Quarkus: "3.15.1", Java: 17}
	if data.Versions != want {
		t.Fatalf("versions = %+v, want %+v (files %v)", data.Versions, want, data.BuildFiles)
	}
}

func TestProjectInspectRefusesEscapes(t *testing.T) {
	r, _ := embeddedRegistry(t)
	for _, dir := range []string{"../", "/etc", ".piperoom/../.."} {
		if _, err := run(t, r, OpProjectInspect, map[string]any{"dir": dir}); err == nil {
			t.Errorf("project_inspect accepted dir %q", dir)
		}
	}
}

const sampleLog = `2026-10-30 10:00:00 INFO  app starting, row {"id":1,"email":"ada@example.com"}
piperoom-reference started: io.debezium.connector.postgresql.PostgresConnector
piperoom-reference event destination=orders.public.orders
14:00:01 [main] piperoom-reference event destination=orders.public.orders
piperoom-reference canary 4f2a9c71e0b85d36 seen destination=orders.public.piperoom_canary
piperoom-reference event destination=orders.public.piperoom_canary
piperoom-reference event destination=orders.public.orders
`

func TestEmbeddedLogRead(t *testing.T) {
	r, dir := embeddedRegistry(t)
	writeProjectFile(t, dir, "piperoom-reference/run.log", sampleLog)
	res, err := run(t, r, OpEmbeddedLogRead, map[string]any{"path": "piperoom-reference/run.log", "nonce": "4f2a9c71e0b85d36"})
	if err != nil {
		t.Fatalf("embedded_log_read: %v", err)
	}
	var log EmbeddedLog
	if err := json.Unmarshal(res.Data, &log); err != nil {
		t.Fatal(err)
	}
	if !log.Started || log.Stopped || log.Events != 4 || !log.NonceSeen || log.EventsAfterNonce != 2 {
		t.Fatalf("log = %+v", log)
	}
	if log.Connector != "io.debezium.connector.postgresql.PostgresConnector" {
		t.Fatalf("connector = %q", log.Connector)
	}
	if len(log.Destinations) != 2 {
		t.Fatalf("destinations = %v", log.Destinations)
	}
	for _, l := range log.Lines {
		if !strings.HasPrefix(l, "piperoom-reference ") {
			t.Errorf("a non-reference line was returned: %q", l)
		}
	}
	if strings.Contains(string(res.Data), "ada@example.com") {
		t.Fatal("row data from the person's own log reached the result")
	}

	// Another nonce was not seen; a stop after the start is reported.
	stopped := ParseEmbeddedLog(sampleLog+"piperoom-reference engine stopped with an error: Connection refused\n", "aaaaaaaa")
	if stopped.NonceSeen || !stopped.Stopped || !strings.Contains(stopped.Error, "Connection refused") {
		t.Fatalf("stopped = %+v", stopped)
	}
	// A restart begins a new run.
	restarted := ParseEmbeddedLog(sampleLog+"piperoom-reference started: x\n", "4f2a9c71e0b85d36")
	if restarted.Events != 0 || restarted.NonceSeen {
		t.Fatalf("restarted = %+v", restarted)
	}
}

func TestEmbeddedLogReadRefusals(t *testing.T) {
	r, dir := embeddedRegistry(t)
	writeProjectFile(t, dir, "pom.xml", "<project/>")
	for name, params := range map[string]map[string]any{
		"outside the project": {"path": "../outside.log"},
		"absolute":            {"path": "/var/log/syslog"},
		"a build file":        {"path": "pom.xml"},
		"the secret store":    {"path": ".piperoom/secrets.env"},
		"bad nonce":           {"path": "run.log", "nonce": "x; rm"},
		"no path":             {},
	} {
		if _, err := run(t, r, OpEmbeddedLogRead, params); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
}
