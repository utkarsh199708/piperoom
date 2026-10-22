package ops

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// finish redacts Data, not only Summary: protocol.go promises "MaxDataBytes
// caps OpResult.Data after redaction", and an op that forgets to redact at the
// source must not ship a secret.
func TestFinishRedactsData(t *testing.T) {
	r := testRegistry(t)
	res := r.finish("op_1", Result{
		Summary: "ok",
		Data: map[string]any{
			"output": "Picked up JAVA_TOOL_OPTIONS: -Djavax.net.ssl.keyStorePassword=hunter2secret99",
			"url":    "postgres://app:sup3rs3cretpw@db:5432/orders",
		},
	}, nil)
	raw := string(res.Data)
	if strings.Contains(raw, "hunter2secret99") || strings.Contains(raw, "sup3rs3cretpw") {
		t.Fatalf("finish shipped a secret in Data: %s", raw)
	}
	if !strings.Contains(raw, "redacted") {
		t.Fatalf("Data shows no mask: %s", raw)
	}
}

// java -version's stderr echoes JAVA_TOOL_OPTIONS, which can carry keystore
// passwords; the op redacts before the output reaches Data.
func TestJavaVersionRedactsToolOptions(t *testing.T) {
	out := "Picked up JAVA_TOOL_OPTIONS: -Djavax.net.ssl.keyStorePassword=hunter2secret99\nsomething broke\n"
	r, err := New(Config{
		ProjectDir: t.TempDir(),
		Exec: func(context.Context, string, ...string) ([]byte, int, error) {
			return []byte(out), 1, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	res, err := runOpErr(t, r, "java_version", map[string]string{})
	if !errors.Is(err, ErrExec) {
		t.Fatalf("err = %v, want ErrExec", err)
	}
	if s := string(res.Data) + res.Summary; strings.Contains(s, "hunter2secret99") {
		t.Fatalf("java_version leaked the keystore password: %s", s)
	}
}

// file_read refuses the well-known credential files whose formats the redact
// pass does not parse, and still reads ordinary configs.
func TestFileReadRefusesCredentialFiles(t *testing.T) {
	r := testRegistry(t)
	denied := []string{
		".netrc", "_netrc", ".pgpass", "pgpass.conf", ".my.cnf", ".htpasswd",
		"credentials", "id_rsa", "id_rsa.pub", "id_ed25519", "id_ecdsa", "id_dsa",
		"server.ppk", "tls.pem", "server.key", "store.p12", "cert.pfx",
		"kafka.jks", "client.keystore", "client.truststore", "vault.kdbx",
		"conf/.netrc", "deep/dir/id_rsa",
	}
	for _, p := range denied {
		writeFile(t, r.env.ProjectDir, p, "machine db login app password hunter2secret99\n")
		if _, err := runOpErr(t, r, "file_read", map[string]string{"path": p}); !errors.Is(err, ErrSecretPath) {
			t.Fatalf("file_read %s: err = %v, want ErrSecretPath", p, err)
		}
	}
	writeFile(t, r.env.ProjectDir, "application.properties", "retries=3\n")
	if _, err := runOpErr(t, r, "file_read", map[string]string{"path": "application.properties"}); err != nil {
		t.Fatalf("file_read application.properties: %v", err)
	}
}

// file_delete refuses what file_write refuses (compose files, executed files,
// mounted directories) plus the secret files, so a delete cannot break the
// stack or destroy the secret store.
func TestFileDeleteDenyList(t *testing.T) {
	r := testRegistry(t)
	if err := r.RegisterMountDir("connect/plugins"); err != nil {
		t.Fatal(err)
	}
	denied := []string{
		"compose.yaml", "docker-compose.yml", ".env", "run.sh", "Makefile",
		"secrets.json", ".netrc", "connect/plugins/debezium-core-2.7.3.Final.jar",
	}
	for _, p := range denied {
		writeFile(t, r.env.ProjectDir, p, "x")
		if _, err := runOpErr(t, r, "file_delete", map[string]string{"path": p}); err == nil {
			t.Fatalf("file_delete %s succeeded, want a refusal", p)
		}
		if _, err := os.Stat(filepath.Join(r.env.ProjectDir, filepath.FromSlash(p))); err != nil {
			t.Fatalf("file_delete %s removed the file despite the refusal: %v", p, err)
		}
	}
	writeFile(t, r.env.ProjectDir, "notes.txt", "x")
	if _, err := runOpErr(t, r, "file_delete", map[string]string{"path": "notes.txt"}); err != nil {
		t.Fatalf("file_delete notes.txt: %v", err)
	}
}

// file_write refuses the build, hook, script and unit files added for the
// enterprise pass: anything a person or a tool later executes.
func TestFileWriteDenyListAdditions(t *testing.T) {
	r := testRegistry(t)
	denied := []string{
		"Makefile", "GNUmakefile", "justfile", "build.mk", "Containerfile",
		".pre-commit-config.yaml", "lefthook.yml", ".husky/pre-commit",
		".githooks/post-checkout", "setup.py", "deploy.rb", "run.pl",
		"index.js", "mod.mjs", "tool.cjs", "env.zsh", "mod.psm1",
		"backup.timer", "api.socket", "watch.path", "data.mount",
	}
	for _, p := range denied {
		if _, err := runOpErr(t, r, "file_write", map[string]string{"path": p, "content": "x"}); !errors.Is(err, ErrExecutedName) {
			t.Fatalf("file_write %s: err = %v, want ErrExecutedName", p, err)
		}
	}
	if _, err := runOpErr(t, r, "file_write", map[string]string{"path": "notes.md", "content": "x"}); err != nil {
		t.Fatalf("file_write notes.md: %v", err)
	}
}

// jar_list names a subdirectory's jars by their relative path, so two
// same-named jars in different connector directories stay apart.
func TestJarListNamesSubdirJars(t *testing.T) {
	r := testRegistry(t)
	writeFile(t, r.env.ProjectDir, "plugins/postgres/debezium-core-2.7.3.Final.jar", "x")
	writeFile(t, r.env.ProjectDir, "plugins/mysql/debezium-core-2.7.3.Final.jar", "x")
	writeFile(t, r.env.ProjectDir, "plugins/root.jar", "x")

	res, err := runOpErr(t, r, "jar_list", map[string]string{"path": "plugins"})
	if err != nil {
		t.Fatalf("jar_list: %v", err)
	}
	var listing jarListing
	if err := json.Unmarshal(res.Data, &listing); err != nil {
		t.Fatal(err)
	}
	got := strings.Join(listing.Jars, ",")
	want := "root.jar,mysql/debezium-core-2.7.3.Final.jar,postgres/debezium-core-2.7.3.Final.jar"
	if got != want {
		t.Fatalf("jars = %q, want %q", got, want)
	}
}
