package ops

import (
	"context"
	"encoding/json"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
)

func testRegistry(t *testing.T, targets ...events.SetupTarget) *Registry {
	t.Helper()
	r, err := New(Config{ProjectDir: t.TempDir(), Targets: targets})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func runOp(t *testing.T, r *Registry, op string, params any) protocol.OpResult {
	t.Helper()
	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	res, _ := r.Execute(context.Background(), protocol.OpRequest{ID: "op_1", Op: op, Params: raw, Tier: protocol.TierT0})
	return res
}

func runOpErr(t *testing.T, r *Registry, op string, params any) (protocol.OpResult, error) {
	t.Helper()
	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatalf("marshal params: %v", err)
	}
	return r.Execute(context.Background(), protocol.OpRequest{ID: "op_1", Op: op, Params: raw, Tier: protocol.TierT0})
}

func writeFile(t *testing.T, dir, name, content string) {
	t.Helper()
	full := filepath.Join(dir, filepath.FromSlash(name))
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestFileRead(t *testing.T) {
	r := testRegistry(t)
	writeFile(t, r.env.ProjectDir, "connect/worker.properties", "bootstrap.servers=localhost:29092\n")
	res := runOp(t, r, "file_read", map[string]string{"path": "connect/worker.properties"})
	if !res.OK {
		t.Fatalf("file_read failed: %s", res.Summary)
	}
	var data struct {
		Content string `json:"content"`
	}
	if err := json.Unmarshal(res.Data, &data); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(data.Content, "bootstrap.servers") {
		t.Fatalf("content = %q", data.Content)
	}
	if res.Receipt.Kind != "file" || res.Receipt.Target != "connect/worker.properties" {
		t.Fatalf("receipt = %+v", res.Receipt)
	}
}

func TestFileReadErrorPaths(t *testing.T) {
	r := testRegistry(t)
	writeFile(t, r.env.ProjectDir, "sub/a.txt", "x")
	cases := []struct {
		name   string
		params map[string]string
		want   error
	}{
		{"missing file", map[string]string{"path": "nope.txt"}, fs.ErrNotExist},
		{"directory", map[string]string{"path": "sub"}, ErrNotRegular},
		{"dot-dot", map[string]string{"path": "../outside.txt"}, ErrPath},
		{"absolute", map[string]string{"path": "/etc/passwd"}, ErrPath},
		{"git", map[string]string{"path": ".git/config"}, ErrPath},
		{"env file", map[string]string{"path": ".env"}, ErrSecretPath},
		{"env.local", map[string]string{"path": "conf/.env.local"}, ErrSecretPath},
		{"secret store file", map[string]string{"path": "secrets.env"}, ErrSecretPath},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := runOpErr(t, r, "file_read", c.params)
			if !errors.Is(err, c.want) {
				t.Fatalf("err = %v, want %v", err, c.want)
			}
		})
	}
}

func TestFileReadTruncated(t *testing.T) {
	r := testRegistry(t)
	writeFile(t, r.env.ProjectDir, "big.log", strings.Repeat("a", maxReadBytes+100))
	res, err := runOpErr(t, r, "file_read", map[string]string{"path": "big.log"})
	if err != nil {
		t.Fatalf("file_read: %v", err)
	}
	if !res.Truncated {
		t.Fatal("Truncated = false, want true")
	}
	var data struct {
		Content string `json:"content"`
	}
	if err := json.Unmarshal(res.Data, &data); err != nil {
		t.Fatal(err)
	}
	if len(data.Content) != maxReadContent {
		t.Fatalf("content = %d bytes, want %d", len(data.Content), maxReadContent)
	}
}

func TestFileReadRejectsBinary(t *testing.T) {
	r := testRegistry(t)
	if err := os.WriteFile(filepath.Join(r.env.ProjectDir, "a.jar"), []byte{0xff, 0xfe, 0x00}, 0o600); err != nil {
		t.Fatal(err)
	}
	// A .jar name is a lexical non-issue for reads; the encoding is what
	// stops it.
	if _, err := runOpErr(t, r, "file_read", map[string]string{"path": "a.jar"}); !errors.Is(err, ErrNotText) {
		t.Fatalf("err = %v, want ErrNotText", err)
	}
}

func TestFileReadCapsData(t *testing.T) {
	r := testRegistry(t)
	writeFile(t, r.env.ProjectDir, "big.txt", strings.Repeat("a", maxReadBytes))
	res, err := runOpErr(t, r, "file_read", map[string]string{"path": "big.txt"})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Data) > protocol.MaxDataBytes {
		t.Fatalf("Data = %d bytes, cap %d", len(res.Data), protocol.MaxDataBytes)
	}
}

func TestDirList(t *testing.T) {
	r := testRegistry(t)
	writeFile(t, r.env.ProjectDir, "a.yaml", "x")
	writeFile(t, r.env.ProjectDir, "sub/b.txt", "x")
	writeFile(t, r.env.ProjectDir, ".git/config", "x")
	writeFile(t, r.env.ProjectDir, ".piperoom/secrets.env", "x")
	if err := os.Symlink("a.yaml", filepath.Join(r.env.ProjectDir, "ln")); err == nil {
		defer os.Remove(filepath.Join(r.env.ProjectDir, "ln"))
	}
	res := runOp(t, r, "dir_list", map[string]string{})
	if !res.OK {
		t.Fatalf("dir_list failed: %s", res.Summary)
	}
	var data struct {
		Entries []dirEntry `json:"entries"`
	}
	if err := json.Unmarshal(res.Data, &data); err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, e := range data.Entries {
		names[e.Name] = true
	}
	if !names["a.yaml"] || !names["sub"] {
		t.Fatalf("entries = %+v", data.Entries)
	}
	if names[".git"] || names[".piperoom"] || names["ln"] || names["secrets.env"] {
		t.Fatalf("denied or non-regular entry listed: %+v", data.Entries)
	}
}

func TestFileWrite(t *testing.T) {
	r := testRegistry(t)
	res, err := runOpErr(t, r, "file_write", map[string]string{
		"path": "reference/build.gradle", "content": "plugins { id 'java' }\n",
	})
	if err != nil {
		t.Fatalf("file_write: %v", err)
	}
	if !res.OK {
		t.Fatalf("file_write failed: %s", res.Summary)
	}
	got, err := os.ReadFile(filepath.Join(r.env.ProjectDir, "reference", "build.gradle"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "plugins { id 'java' }\n" {
		t.Fatalf("content = %q", got)
	}
}

func TestFileWriteRefusals(t *testing.T) {
	mount := "connect/plugins"
	r, err := New(Config{ProjectDir: t.TempDir(), MountDirs: []string{mount}})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	big := strings.Repeat("a", maxWriteBytes+1)
	cases := []struct {
		name string
		path string
		body string
		want error
	}{
		{".env", ".env", "A=1", ErrExecutedName},
		{".env.local", "conf/.env.local", "A=1", ErrExecutedName},
		{"Dockerfile", "Dockerfile", "FROM scratch", ErrExecutedName},
		{"jar", "lib/x.jar", "x", ErrExecutedName},
		{"shell", "run.sh", "#!/bin/sh", ErrExecutedName},
		{"powershell", "run.ps1", "x", ErrExecutedName},
		{"batch", "run.bat", "x", ErrExecutedName},
		{"service", "connect.service", "x", ErrExecutedName},
		{"compose", "compose.yaml", "services: {}", ErrExecutedName},
		{"compose override", "compose.override.yml", "services: {}", ErrExecutedName},
		{"docker-compose", "docker-compose.yml", "services: {}", ErrExecutedName},
		{"k8s manifest", "k8s/connect.yaml", "a: b", ErrExecutedName},
		{"manifests dir", "manifests/connect.yml", "a: b", ErrExecutedName},
		{"mounted dir", mount + "/postgres/x.txt", "x", ErrExecutedName},
		{"secret key", "notes.txt", "db.password = hunter2", ErrExecutedName},
		{"secret json", "notes.json", `{"api_key": "hunter2"}`, ErrExecutedName},
		{"too big", "big.txt", big, ErrTooLarge},
		{"dot-dot", "../escape.txt", "x", ErrPath},
		{"absolute", "/tmp/escape.txt", "x", ErrPath},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := runOpErr(t, r, "file_write", map[string]string{"path": c.path, "content": c.body})
			if !errors.Is(err, c.want) {
				t.Fatalf("err = %v, want %v", err, c.want)
			}
		})
	}
}

func TestFileWriteRefusesBinaryContent(t *testing.T) {
	// A JSON request cannot carry invalid UTF-8 (encoding/json replaces it),
	// so the check is exercised directly: it is the last line for a caller
	// that builds params in Go.
	if err := checkWriteContent("ok\xff\xfe"); !errors.Is(err, ErrNotText) {
		t.Fatalf("err = %v, want ErrNotText", err)
	}
}

func TestFileWriteRefusesNonRegularDestination(t *testing.T) {
	r := testRegistry(t)
	if err := os.Mkdir(filepath.Join(r.env.ProjectDir, "adir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := runOpErr(t, r, "file_write", map[string]string{"path": "adir", "content": "x"}); !errors.Is(err, ErrNotRegular) {
		t.Fatalf("err = %v, want ErrNotRegular", err)
	}
}

// A hard link at the destination is replaced by the rename, never written
// through: the other name keeps its content.
func TestFileWriteReplacesHardLink(t *testing.T) {
	r := testRegistry(t)
	writeFile(t, r.env.ProjectDir, "a.txt", "original")
	if err := os.Link(filepath.Join(r.env.ProjectDir, "a.txt"), filepath.Join(r.env.ProjectDir, "b.txt")); err != nil {
		t.Skipf("hard links are not available: %v", err)
	}
	if _, err := runOpErr(t, r, "file_write", map[string]string{"path": "b.txt", "content": "new"}); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(filepath.Join(r.env.ProjectDir, "a.txt"))
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "original" {
		t.Fatalf("a.txt = %q; file_write wrote through the hard link", got)
	}
}

func TestFileDelete(t *testing.T) {
	r := testRegistry(t)
	writeFile(t, r.env.ProjectDir, "gone.txt", "x")
	if _, err := runOpErr(t, r, "file_delete", map[string]string{"path": "gone.txt"}); err != nil {
		t.Fatalf("file_delete: %v", err)
	}
	if _, err := os.Stat(filepath.Join(r.env.ProjectDir, "gone.txt")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("file still exists: %v", err)
	}
	if _, err := runOpErr(t, r, "file_delete", map[string]string{"path": "gone.txt"}); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("missing file err = %v, want fs.ErrNotExist", err)
	}
	if err := os.Mkdir(filepath.Join(r.env.ProjectDir, "dir"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := runOpErr(t, r, "file_delete", map[string]string{"path": "dir"}); !errors.Is(err, ErrNotRegular) {
		t.Fatalf("directory err = %v, want ErrNotRegular", err)
	}
}

func TestFileWriteSymlinkDestinationRefused(t *testing.T) {
	r := testRegistry(t)
	outside := t.TempDir()
	target := filepath.Join(outside, "outside.txt")
	if err := os.WriteFile(target, []byte("outside"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(r.env.ProjectDir, "link.txt")); err != nil {
		t.Skipf("symlinks are not available: %v", err)
	}
	if _, err := runOpErr(t, r, "file_write", map[string]string{"path": "link.txt", "content": "x"}); !errors.Is(err, ErrPath) {
		t.Fatalf("err = %v, want ErrPath", err)
	}
	got, _ := os.ReadFile(target)
	if string(got) != "outside" {
		t.Fatalf("outside file changed: %q", got)
	}
}

func TestFileWriteCapsContent(t *testing.T) {
	r := testRegistry(t)
	// Exactly at the cap is accepted; one byte over is refused above.
	body := strings.Repeat("a", maxWriteBytes)
	if _, err := runOpErr(t, r, "file_write", map[string]string{"path": "at-cap.txt", "content": body}); err != nil {
		t.Fatalf("at cap: %v", err)
	}
}

func TestFileReadSymlinkEscapeNoLeak(t *testing.T) {
	r := testRegistry(t)
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("outside-secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(outside, "secret.txt"), filepath.Join(r.env.ProjectDir, "leak.txt")); err != nil {
		t.Skipf("symlinks are not available: %v", err)
	}
	res, err := runOpErr(t, r, "file_read", map[string]string{"path": "leak.txt"})
	if !errors.Is(err, ErrPath) {
		t.Fatalf("err = %v, want ErrPath", err)
	}
	if strings.Contains(res.Summary, "outside-secret") || len(res.Data) > 0 {
		t.Fatalf("result leaked the target: %+v", res)
	}
}

// A request with an unknown op or an expired deadline is handled by the
// registry, not by a panic.
func TestExecuteDeadlineAndUnknown(t *testing.T) {
	r := testRegistry(t)
	if _, err := runOpErr(t, r, "no_such_op", map[string]string{}); !errors.Is(err, ErrUnknownOp) {
		t.Fatalf("unknown op err = %v, want ErrUnknownOp", err)
	}
	raw, _ := json.Marshal(map[string]string{})
	res, _ := r.Execute(context.Background(), protocol.OpRequest{
		ID: "op_2", Op: "env_detect", Params: raw, Tier: protocol.TierT0,
	})
	if !res.OK {
		t.Fatalf("env_detect = %+v", res)
	}
	res, err := r.Execute(context.Background(), protocol.OpRequest{
		ID: "op_3", Op: "env_detect", Params: raw, Tier: protocol.TierT0,
		Deadline: time.Now().Add(-time.Second),
	})
	if err == nil || res.OK {
		t.Fatalf("expired deadline = %+v, %v; want a cut op", res, err)
	}
}

// TestFileReadRedactsSecrets: the content of a read file goes through
// redact.Text, like http_get's body, so a credential in a plain config file
// (not caught by the secret-path deny-list) never reaches the server verbatim.
func TestFileReadRedactsSecrets(t *testing.T) {
	r := testRegistry(t)
	writeFile(t, r.env.ProjectDir, "connect/worker.properties",
		"bootstrap.servers=localhost:29092\ndatabase.password=hunter2secret\n")
	res := runOp(t, r, "file_read", map[string]string{"path": "connect/worker.properties"})
	if !res.OK {
		t.Fatalf("file_read failed: %s", res.Summary)
	}
	var data struct {
		Content string `json:"content"`
	}
	if err := json.Unmarshal(res.Data, &data); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(data.Content, "hunter2secret") {
		t.Fatalf("secret survived redaction: %q", data.Content)
	}
	if !strings.Contains(data.Content, "bootstrap.servers") {
		t.Fatalf("non-secret content lost: %q", data.Content)
	}
}
