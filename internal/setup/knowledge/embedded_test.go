package knowledge

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

func pgParams(build BuildTool) EmbeddedParams {
	return EmbeddedParams{
		Build: build, Source: SourcePostgres,
		GroupID: "com.example.cdc", ArtifactID: "orders-cdc", Release: "3.2.0.Final",
		Host: "db.internal", Port: 5432, Database: "orders", User: "debezium",
		PasswordSecret: "orders_db_password", TopicPrefix: "orders",
		Tables:   []string{"public.orders", "public.customers"},
		SlotName: "orders_cdc",
	}
}

func mysqlParams(build BuildTool) EmbeddedParams {
	return EmbeddedParams{
		Build: build, Source: SourceMySQL,
		GroupID: "com.example.cdc", ArtifactID: "inventory-cdc", Release: "3.2.0.Final",
		Host: "10.0.0.7", Port: 3306, Database: "inventory", User: "debezium",
		PasswordSecret: "inventory_db_password", TopicPrefix: "inventory",
		Tables:   []string{"inventory.products"},
		ServerID: 184054,
	}
}

func fileMap(t *testing.T, files []EmbeddedFile) map[string]string {
	t.Helper()
	m := make(map[string]string, len(files))
	for _, f := range files {
		if _, dup := m[f.Path]; dup {
			t.Fatalf("file %s rendered twice", f.Path)
		}
		if strings.Contains(f.Path, "..") || strings.HasPrefix(f.Path, "/") || strings.Contains(f.Path, `\`) {
			t.Fatalf("rendered path %q is not a clean relative path", f.Path)
		}
		m[f.Path] = string(f.Content)
	}
	return m
}

// All four combinations render the expected files, with the Debezium release
// from the index, the connector of the source, the secret as a reference and
// nothing left of the template syntax.
func TestRenderEmbeddedAllCombos(t *testing.T) {
	cases := []struct {
		name      string
		params    EmbeddedParams
		wantFiles []string
		wantIn    map[string][]string
		notIn     map[string][]string
	}{
		{
			name:      "maven postgres",
			params:    pgParams(BuildMaven),
			wantFiles: []string{"pom.xml", "piperoom-reference.properties", "README.md", "src/main/java/com/example/cdc/ReferenceApp.java"},
			wantIn: map[string][]string{
				"pom.xml": {
					"<groupId>com.example.cdc</groupId>", "<artifactId>orders-cdc</artifactId>",
					"<version.debezium>3.2.0.Final</version.debezium>", "<artifactId>debezium-bom</artifactId>",
					"<artifactId>debezium-connector-postgres</artifactId>", "<maven.compiler.release>17</maven.compiler.release>",
					"<mainClass>com.example.cdc.ReferenceApp</mainClass>",
				},
				"piperoom-reference.properties": {
					"connector.class=io.debezium.connector.postgresql.PostgresConnector\n",
					"database.password=${secret:orders_db_password}\n", "database.dbname=orders\n",
					"slot.name=orders_cdc\n", "publication.name=orders_cdc\n", "publication.autocreate.mode=filtered\n",
					`table.include.list=public\\.orders,public\\.customers` + "\n", "topic.prefix=orders\n",
					"database.hostname=db.internal\n", "database.port=5432\n",
				},
				"src/main/java/com/example/cdc/ReferenceApp.java": {"package com.example.cdc;", "ConvertingAsyncEngineBuilderFactory", "piperoom-reference event"},
				"README.md": {"PIPEROOM_SECRET_orders_db_password", "mvn -B -q package -DskipTests", "java -jar target/orders-cdc-1.0.0.jar", "pg_drop_replication_slot('orders_cdc')"},
			},
			notIn: map[string][]string{
				"pom.xml":                       {"debezium-connector-mysql", "debezium-storage-file"},
				"piperoom-reference.properties": {"schema.history", "server.id"},
			},
		},
		{
			name:      "gradle postgres",
			params:    pgParams(BuildGradle),
			wantFiles: []string{"build.gradle", "settings.gradle", "piperoom-reference.properties", "README.md", "src/main/java/com/example/cdc/ReferenceApp.java"},
			wantIn: map[string][]string{
				"build.gradle": {
					`platform("io.debezium:debezium-bom:${debeziumVersion}")`, "def debeziumVersion = '3.2.0.Final'",
					"'io.debezium:debezium-connector-postgres'", "options.release = 17", "mainClass = 'com.example.cdc.ReferenceApp'",
				},
				"settings.gradle": {"rootProject.name = 'orders-cdc'"},
				"README.md":       {"gradle --no-daemon -q installDist", "build/install/orders-cdc/bin/orders-cdc", `build\install\orders-cdc\bin\orders-cdc.bat`},
			},
			notIn: map[string][]string{"build.gradle": {"debezium-connector-mysql", "debezium-storage-file"}},
		},
		{
			name:      "maven mysql",
			params:    mysqlParams(BuildMaven),
			wantFiles: []string{"pom.xml", "piperoom-reference.properties", "README.md", "src/main/java/com/example/cdc/ReferenceApp.java"},
			wantIn: map[string][]string{
				"pom.xml": {"<artifactId>debezium-connector-mysql</artifactId>", "<artifactId>debezium-storage-file</artifactId>"},
				"piperoom-reference.properties": {
					"connector.class=io.debezium.connector.mysql.MySqlConnector\n", "database.server.id=184054\n",
					"database.include.list=inventory\n", `table.include.list=inventory\\.products` + "\n",
					"schema.history.internal=io.debezium.storage.file.history.FileSchemaHistory\n",
					"database.password=${secret:inventory_db_password}\n",
				},
			},
			notIn: map[string][]string{
				"pom.xml":                       {"debezium-connector-postgres"},
				"piperoom-reference.properties": {"slot.name", "publication", "database.dbname"},
				"README.md":                     {"pg_drop_replication_slot"},
			},
		},
		{
			name:      "gradle mysql",
			params:    mysqlParams(BuildGradle),
			wantFiles: []string{"build.gradle", "settings.gradle", "piperoom-reference.properties", "README.md", "src/main/java/com/example/cdc/ReferenceApp.java"},
			wantIn: map[string][]string{
				"build.gradle":    {"'io.debezium:debezium-connector-mysql'", "'io.debezium:debezium-storage-file'"},
				"settings.gradle": {"rootProject.name = 'inventory-cdc'"},
			},
			notIn: map[string][]string{"build.gradle": {"debezium-connector-postgres"}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			files, err := RenderEmbedded(tc.params)
			if err != nil {
				t.Fatalf("RenderEmbedded: %v", err)
			}
			m := fileMap(t, files)
			if len(m) != len(tc.wantFiles) {
				t.Errorf("rendered %d files, want %d: %v", len(m), len(tc.wantFiles), keys(m))
			}
			for _, f := range tc.wantFiles {
				if _, ok := m[f]; !ok {
					t.Errorf("missing %s (have %v)", f, keys(m))
				}
			}
			for f, subs := range tc.wantIn {
				for _, s := range subs {
					if !strings.Contains(m[f], s) {
						t.Errorf("%s does not contain %q:\n%s", f, s, m[f])
					}
				}
			}
			for f, subs := range tc.notIn {
				for _, s := range subs {
					if strings.Contains(m[f], s) {
						t.Errorf("%s contains %q", f, s)
					}
				}
			}
			for path, body := range m {
				if strings.Contains(body, "{{") || strings.Contains(body, "<no value>") {
					t.Errorf("%s has template residue", path)
				}
			}
			if pom, ok := m["pom.xml"]; ok {
				assertWellFormedXML(t, pom)
			}
		})
	}
}

func keys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func assertWellFormedXML(t *testing.T, doc string) {
	t.Helper()
	dec := xml.NewDecoder(strings.NewReader(doc))
	for {
		_, err := dec.Token()
		if errors.Is(err, io.EOF) {
			return
		}
		if err != nil {
			t.Fatalf("pom.xml is not well-formed XML: %v", err)
		}
	}
}

// The password is never rendered: only the ${secret:NAME} reference, on the
// one line that carries it.
func TestRenderEmbeddedNeverRendersAPassword(t *testing.T) {
	for _, p := range []EmbeddedParams{pgParams(BuildMaven), mysqlParams(BuildGradle)} {
		files, err := RenderEmbedded(p)
		if err != nil {
			t.Fatalf("RenderEmbedded: %v", err)
		}
		props := fileMap(t, files)["piperoom-reference.properties"]
		var pwLines []string
		for _, line := range strings.Split(props, "\n") {
			if strings.Contains(line, "password") && !strings.HasPrefix(line, "#") {
				pwLines = append(pwLines, line)
			}
		}
		want := "database.password=${secret:" + p.PasswordSecret + "}"
		if len(pwLines) != 1 || pwLines[0] != want {
			t.Errorf("password lines = %q, want exactly [%q]", pwLines, want)
		}
	}
}

// Every Debezium release in the plugin index renders all four combinations,
// with the Java release the rules require.
func TestRenderEmbeddedEveryIndexedRelease(t *testing.T) {
	if err := ValidateEmbedded(); err != nil {
		t.Fatalf("ValidateEmbedded: %v", err)
	}
	wantJava := map[string]string{"2.7.2.Final": "11", "3.0.7.Final": "17", "3.1.3.Final": "17", "3.2.0.Final": "17"}
	for _, release := range Releases() {
		want, ok := wantJava[release]
		if !ok {
			t.Fatalf("release %s has no expected Java release in this test; add it", release)
		}
		p := pgParams(BuildMaven)
		p.Release = release
		files, err := RenderEmbedded(p)
		if err != nil {
			t.Fatalf("%s: %v", release, err)
		}
		pom := fileMap(t, files)["pom.xml"]
		if !strings.Contains(pom, "<maven.compiler.release>"+want+"</maven.compiler.release>") {
			t.Errorf("%s: pom does not compile for Java %s", release, want)
		}
		if !strings.Contains(pom, "<version.debezium>"+release+"</version.debezium>") {
			t.Errorf("%s: pom does not pin the release", release)
		}
	}
}

// Bad params are refused before anything renders, with ErrEmbeddedParams and
// an error that does not echo the bad value.
func TestRenderEmbeddedRefusesBadParams(t *testing.T) {
	many := make([]string, maxTables+1)
	for i := range many {
		many[i] = fmt.Sprintf("public.t%d", i)
	}
	cases := []struct {
		name   string
		base   func(BuildTool) EmbeddedParams
		mutate func(*EmbeddedParams)
		echo   string // a fragment of the bad value that must not appear in the error
	}{
		{"unknown build tool", pgParams, func(p *EmbeddedParams) { p.Build = "ant" }, ""},
		{"unknown source", pgParams, func(p *EmbeddedParams) { p.Source = "oracle" }, ""},
		{"group id uppercase", pgParams, func(p *EmbeddedParams) { p.GroupID = "Com.Example" }, ""},
		{"group id with a Java keyword", pgParams, func(p *EmbeddedParams) { p.GroupID = "com.class.cdc" }, ""},
		{"group id XML injection", pgParams, func(p *EmbeddedParams) { p.GroupID = "com.example</groupId><x>" }, "</groupId>"},
		{"group id empty", pgParams, func(p *EmbeddedParams) { p.GroupID = "" }, ""},
		{"artifact id path traversal", pgParams, func(p *EmbeddedParams) { p.ArtifactID = "../evil" }, "../evil"},
		{"artifact id Gradle injection", pgParams, func(p *EmbeddedParams) { p.ArtifactID = "x'\nexec('rm')" }, "exec"},
		{"release not in the index", pgParams, func(p *EmbeddedParams) { p.Release = "3.2.1.Final" }, ""},
		{"release free text", pgParams, func(p *EmbeddedParams) { p.Release = "latest" }, ""},
		{"host with a shell char", pgParams, func(p *EmbeddedParams) { p.Host = "db;rm -rf /" }, "rm -rf"},
		{"host with user info", pgParams, func(p *EmbeddedParams) { p.Host = "admin@db" }, ""},
		{"host with a newline (properties injection)", pgParams, func(p *EmbeddedParams) { p.Host = "db\ndatabase.password=hunter2" }, "hunter2"},
		{"port zero", pgParams, func(p *EmbeddedParams) { p.Port = 0 }, ""},
		{"port too high", pgParams, func(p *EmbeddedParams) { p.Port = 70000 }, ""},
		{"database with a space", pgParams, func(p *EmbeddedParams) { p.Database = "or ders" }, ""},
		{"user with a quote", pgParams, func(p *EmbeddedParams) { p.User = "x'y" }, ""},
		{"password secret is a literal password", pgParams, func(p *EmbeddedParams) { p.PasswordSecret = "hunter2!" }, "hunter2"},
		{"password secret is a whole reference", pgParams, func(p *EmbeddedParams) { p.PasswordSecret = "${secret:pw}" }, ""},
		{"password secret empty", pgParams, func(p *EmbeddedParams) { p.PasswordSecret = "" }, ""},
		{"topic prefix with a space", pgParams, func(p *EmbeddedParams) { p.TopicPrefix = "a b" }, ""},
		{"table without a schema", pgParams, func(p *EmbeddedParams) { p.Tables = []string{"orders"} }, ""},
		{"table injection", pgParams, func(p *EmbeddedParams) { p.Tables = []string{"public.orders,.*"} }, ""},
		{"table listed twice", pgParams, func(p *EmbeddedParams) { p.Tables = []string{"public.a", "public.a"} }, ""},
		{"too many tables", pgParams, func(p *EmbeddedParams) { p.Tables = many }, ""},
		{"postgres without a slot", pgParams, func(p *EmbeddedParams) { p.SlotName = "" }, ""},
		{"postgres slot uppercase", pgParams, func(p *EmbeddedParams) { p.SlotName = "Orders" }, ""},
		{"postgres publication bad", pgParams, func(p *EmbeddedParams) { p.PublicationName = "pub-1" }, ""},
		{"postgres with a server id", pgParams, func(p *EmbeddedParams) { p.ServerID = 5 }, ""},
		{"mysql server id zero", mysqlParams, func(p *EmbeddedParams) { p.ServerID = 0 }, ""},
		{"mysql with a slot", mysqlParams, func(p *EmbeddedParams) { p.SlotName = "s" }, ""},
		{"mysql table in another database", mysqlParams, func(p *EmbeddedParams) { p.Tables = []string{"other.products"} }, ""},
	}
	for _, tc := range cases {
		for _, build := range []BuildTool{BuildMaven, BuildGradle} {
			t.Run(tc.name+"/"+string(build), func(t *testing.T) {
				p := tc.base(build)
				tc.mutate(&p)
				files, err := RenderEmbedded(p)
				if !errors.Is(err, ErrEmbeddedParams) {
					t.Fatalf("RenderEmbedded err = %v, want ErrEmbeddedParams", err)
				}
				if files != nil {
					t.Fatalf("rendered %d files for bad params", len(files))
				}
				if tc.echo != "" && strings.Contains(err.Error(), tc.echo) {
					t.Errorf("error %q echoes the bad value", err)
				}
			})
		}
	}
}

// The commands are argv slices, never a shell line, and name only the
// project's own files.
func TestEmbeddedCommands(t *testing.T) {
	mvn := pgParams(BuildMaven).Commands()
	if got := strings.Join(mvn.Build, " "); got != "mvn -B -q package -DskipTests" {
		t.Errorf("maven build = %q", got)
	}
	if got := strings.Join(mvn.Run, " "); got != "java -jar target/orders-cdc-1.0.0.jar" {
		t.Errorf("maven run = %q", got)
	}
	if mvn.RunWindows != nil {
		t.Errorf("maven run on Windows = %q, want the same argv", mvn.RunWindows)
	}
	gr := mysqlParams(BuildGradle).Commands()
	if got := strings.Join(gr.Build, " "); got != "gradle --no-daemon -q installDist" {
		t.Errorf("gradle build = %q", got)
	}
	if got := strings.Join(gr.Run, " "); got != "build/install/inventory-cdc/bin/inventory-cdc" {
		t.Errorf("gradle run = %q", got)
	}
	if got := strings.Join(gr.RunWindows, " "); got != `build\install\inventory-cdc\bin\inventory-cdc.bat` {
		t.Errorf("gradle run on Windows = %q", got)
	}
}

// Rendering is deterministic: the same params give the same bytes, so a
// re-run of the write step is a no-op.
func TestRenderEmbeddedDeterministic(t *testing.T) {
	a, err := RenderEmbedded(mysqlParams(BuildMaven))
	if err != nil {
		t.Fatal(err)
	}
	b, err := RenderEmbedded(mysqlParams(BuildMaven))
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != len(b) {
		t.Fatalf("len %d != %d", len(a), len(b))
	}
	for i := range a {
		if a[i].Path != b[i].Path || !bytes.Equal(a[i].Content, b[i].Content) {
			t.Errorf("file %d differs between renders", i)
		}
	}
}

func TestJavaRelease(t *testing.T) {
	cases := map[string]int{
		"1.9.7.Final": 8, "2.0.0.Alpha1": 11, "2.7.2.Final": 11, "3.0.0.Alpha1": 17,
		"2.7.4.Final": 11, "3.0.0.CR1": 17, "3.2.0.Final": 17,
	}
	for in, want := range cases {
		got, err := JavaRelease(in)
		if err != nil || got != want {
			t.Errorf("JavaRelease(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	if _, err := JavaRelease("next"); err == nil {
		t.Error("JavaRelease(next) succeeded")
	}
}

func TestParseDbzVersionOrder(t *testing.T) {
	ordered := []string{"2.7.0.Alpha1", "2.7.0.Alpha2", "2.7.0.Beta1", "2.7.0.CR1", "2.7.0.Final", "2.7.2.Final", "3.0.0.Alpha1", "3.0.0", "3.0.2.Final", "3.10.0"}
	for i := 1; i < len(ordered); i++ {
		a, errA := parseDbzVersion(ordered[i-1])
		b, errB := parseDbzVersion(ordered[i])
		if errA != nil || errB != nil {
			t.Fatalf("parse %q, %q: %v %v", ordered[i-1], ordered[i], errA, errB)
		}
		if a.compare(b) >= 0 {
			t.Errorf("%s should sort before %s", ordered[i-1], ordered[i])
		}
	}
	if v, _ := parseDbzVersion("3.0.0.Final"); v.compare(mustDbzVersion("3.0.0")) != 0 {
		t.Error("3.0.0.Final and 3.0.0 should be equal")
	}
	for _, bad := range []string{"", "3", "x.y.z", "3.2.0.Snapshot", "3.2.0.Final; rm", "1.2.3.4.5"} {
		if _, err := parseDbzVersion(bad); err == nil {
			t.Errorf("parseDbzVersion(%q) succeeded", bad)
		}
	}
}
