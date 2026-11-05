package ops

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"testing"
)

func TestEnvDetect(t *testing.T) {
	lookups := map[string]string{"docker": "/usr/bin/docker", "java": "/usr/bin/java"}
	r, err := New(Config{
		ProjectDir: t.TempDir(),
		LookPath: func(name string) (string, error) {
			if p, ok := lookups[name]; ok {
				return p, nil
			}
			return "", errors.New("not found")
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	writeFile(t, r.env.ProjectDir, "compose.yaml", "services: {}\n")
	writeFile(t, r.env.ProjectDir, "Dockerfile", "FROM scratch\n")

	res, err := runOpErr(t, r, "env_detect", map[string]string{})
	if err != nil || !res.OK {
		t.Fatalf("env_detect = %+v, %v", res, err)
	}
	var info envInfo
	if err := json.Unmarshal(res.Data, &info); err != nil {
		t.Fatal(err)
	}
	if info.OS == "" || info.Arch == "" {
		t.Fatalf("os/arch empty: %+v", info)
	}
	if strings.Join(info.Tools, ",") != "docker,java" {
		t.Fatalf("tools = %v", info.Tools)
	}
	files := map[string]bool{}
	for _, f := range info.ProjectFiles {
		files[f.Name] = true
	}
	if !files["compose.yaml"] || !files["Dockerfile"] {
		t.Fatalf("project files = %+v", info.ProjectFiles)
	}
}

func TestJavaVersion(t *testing.T) {
	cases := []struct {
		name    string
		out     string
		code    int
		runErr  error
		want    string
		wantErr error
	}{
		{name: "openjdk", out: "openjdk version \"21.0.2\" 2024-01-16\nOpenJDK Runtime Environment\n", want: "21.0.2"},
		{name: "oracle", out: "java version \"17.0.9\" 2023-10-17 LTS\n", want: "17.0.9"},
		{name: "exit non-zero", out: "error", code: 1, wantErr: ErrExec},
		{name: "missing binary", runErr: errors.New("executable file not found"), wantErr: ErrExec},
		{name: "garbage", out: "hello", wantErr: ErrExec},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r, err := New(Config{
				ProjectDir: t.TempDir(),
				Exec: func(context.Context, string, ...string) ([]byte, int, error) {
					return []byte(c.out), c.code, c.runErr
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			res, err := runOpErr(t, r, "java_version", map[string]string{})
			if c.wantErr != nil {
				if !errors.Is(err, c.wantErr) {
					t.Fatalf("err = %v, want %v", err, c.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("java_version: %v", err)
			}
			var info javaVersionInfo
			if err := json.Unmarshal(res.Data, &info); err != nil {
				t.Fatal(err)
			}
			if info.Version != c.want {
				t.Fatalf("version = %q, want %q", info.Version, c.want)
			}
		})
	}
}

func TestJarListFlagsLayoutProblems(t *testing.T) {
	r := testRegistry(t)
	writeFile(t, r.env.ProjectDir, "plugins/flat.jar", "x")
	writeFile(t, r.env.ProjectDir, "plugins/postgres/debezium-connector-postgres-2.7.3.Final.jar", "x")
	writeFile(t, r.env.ProjectDir, "plugins/postgres/debezium-core-2.7.3.Final.jar", "x")
	writeFile(t, r.env.ProjectDir, "plugins/kafka/debezium-connector-kafka-3.0.0.Final.jar", "x")
	writeFile(t, r.env.ProjectDir, "plugins/kafka/debezium-core-3.0.0.Final.jar", "x")
	writeFile(t, r.env.ProjectDir, "plugins/kafka/debezium-api-3.0.0.Final.jar", "x")
	writeFile(t, r.env.ProjectDir, "plugins/kafka/kafka-clients-3.6.0.jar", "x")

	res, err := runOpErr(t, r, "jar_list", map[string]string{"path": "plugins"})
	if err != nil {
		t.Fatalf("jar_list: %v", err)
	}
	var listing jarListing
	if err := json.Unmarshal(res.Data, &listing); err != nil {
		t.Fatal(err)
	}
	if len(listing.Flat) != 1 || listing.Flat[0] != "flat.jar" {
		t.Fatalf("flat = %v, want [flat.jar]", listing.Flat)
	}
	if strings.Join(listing.Versions, ",") != "2.7.3.Final,3.0.0.Final" {
		t.Fatalf("versions = %v", listing.Versions)
	}
	if len(listing.Missing) != 1 || listing.Missing[0].Dir != "postgres" ||
		strings.Join(listing.Missing[0].Missing, ",") != "debezium-api-2.7.3.Final.jar" {
		t.Fatalf("missing = %+v", listing.Missing)
	}
	if len(listing.Issues) < 3 {
		t.Fatalf("issues = %v, want flat, two versions and missing jars", listing.Issues)
	}
	if !strings.Contains(res.Summary, "two Debezium versions") {
		t.Fatalf("summary = %q", res.Summary)
	}
}

func TestJarListSingleConnectorDir(t *testing.T) {
	r := testRegistry(t)
	dir := "plugins/debezium-connector-postgres"
	writeFile(t, r.env.ProjectDir, dir+"/debezium-connector-postgres-3.0.0.Final.jar", "x")
	writeFile(t, r.env.ProjectDir, dir+"/debezium-core-3.0.0.Final.jar", "x")
	writeFile(t, r.env.ProjectDir, dir+"/debezium-api-3.0.0.Final.jar", "x")
	res, err := runOpErr(t, r, "jar_list", map[string]string{"path": dir})
	if err != nil {
		t.Fatal(err)
	}
	var listing jarListing
	if err := json.Unmarshal(res.Data, &listing); err != nil {
		t.Fatal(err)
	}
	if len(listing.Flat) != 0 {
		t.Fatalf("flat = %v in a connector dir", listing.Flat)
	}
	if len(listing.Missing) != 0 {
		t.Fatalf("missing = %+v", listing.Missing)
	}
	if len(listing.Versions) != 1 || listing.Versions[0] != "3.0.0.Final" {
		t.Fatalf("versions = %v", listing.Versions)
	}
}

func TestJarListMissingDir(t *testing.T) {
	r := testRegistry(t)
	if _, err := runOpErr(t, r, "jar_list", map[string]string{"path": "nope"}); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("err = %v, want os.ErrNotExist", err)
	}
}

// A hostile name on disk is skipped, never opened through the root.
func TestJarListSkipsDeniedNames(t *testing.T) {
	r := testRegistry(t)
	writeFile(t, r.env.ProjectDir, ".git/x.jar", "x")
	res, err := runOpErr(t, r, "jar_list", map[string]string{"path": "."})
	if err != nil {
		t.Fatal(err)
	}
	var listing jarListing
	if err := json.Unmarshal(res.Data, &listing); err != nil {
		t.Fatal(err)
	}
	if len(listing.Jars) != 0 {
		t.Fatalf("jars = %v, want none", listing.Jars)
	}
}
