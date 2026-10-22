package ops

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// TestFileOpsRefuseMaterialisedPaths: every file op refuses a path the runner
// materialised from its secret store, whether the list came from the registry
// configuration or from the store's .piperoom/materialised file (design §2.4).
func TestFileOpsRefuseMaterialisedPaths(t *testing.T) {
	r := testRegistry(t)
	writeFile(t, r.env.ProjectDir, "connect/secrets.properties", "password=s3cret-value\n")
	writeFile(t, r.env.ProjectDir, ".piperoom/materialised", "connect/secrets.properties\n")

	cases := []struct {
		op     string
		params map[string]string
	}{
		{"file_read", map[string]string{"path": "connect/secrets.properties"}},
		{"dir_list", map[string]string{"path": "connect/secrets.properties"}},
		{"file_write", map[string]string{"path": "connect/secrets.properties", "content": "x"}},
		{"file_delete", map[string]string{"path": "connect/secrets.properties"}},
	}
	for _, c := range cases {
		t.Run(c.op, func(t *testing.T) {
			if _, err := runOpErr(t, r, c.op, c.params); !errors.Is(err, ErrSecretPath) {
				t.Fatalf("%s = %v, want ErrSecretPath", c.op, err)
			}
		})
	}

	// A case variant is the same file on Windows and a likely alias elsewhere.
	if _, err := runOpErr(t, r, "file_read", map[string]string{"path": "CONNECT/Secrets.Properties"}); !errors.Is(err, ErrSecretPath) {
		t.Fatalf("case variant = %v, want ErrSecretPath", err)
	}

	// The registry can be handed the list directly (the wiring path before a
	// template driver registers a rendered file).
	r2, err := New(Config{ProjectDir: r.env.ProjectDir, Materialised: []string{"connect/secrets.properties"}})
	if err != nil {
		t.Fatal(err)
	}
	defer r2.Close()
	if _, err := runOpErr(t, r2, "file_read", map[string]string{"path": "connect/secrets.properties"}); !errors.Is(err, ErrSecretPath) {
		t.Fatalf("configured materialised path = %v, want ErrSecretPath", err)
	}
}

// TestFileOpsRefuseSSHDir: .ssh is a denied directory on every OS, with the
// same case and trailing-dot variants as .git.
func TestFileOpsRefuseSSHDir(t *testing.T) {
	r := testRegistry(t)
	writeFile(t, r.env.ProjectDir, ".ssh/config", "Host db\n")
	writeFile(t, r.env.ProjectDir, ".ssh/id_rsa", "x\n")
	for _, p := range []string{".ssh/config", ".SSH/config", ".ssh./config", ".ssh /config"} {
		if _, err := runOpErr(t, r, "file_read", map[string]string{"path": p}); !errors.Is(err, ErrPath) {
			t.Fatalf("file_read %q = %v, want ErrPath", p, err)
		}
	}
	for _, p := range []string{".ssh/authorized_keys", ".SSH/authorized_keys"} {
		if _, err := runOpErr(t, r, "file_write", map[string]string{"path": p, "content": "x"}); !errors.Is(err, ErrPath) {
			t.Fatalf("file_write %q = %v, want ErrPath", p, err)
		}
	}
	if _, err := runOpErr(t, r, "dir_list", map[string]string{"path": ".ssh"}); !errors.Is(err, ErrPath) {
		t.Fatalf("dir_list .ssh = %v, want ErrPath", err)
	}
	if _, err := runOpErr(t, r, "file_delete", map[string]string{"path": ".ssh/config"}); !errors.Is(err, ErrPath) {
		t.Fatalf("file_delete .ssh/config = %v, want ErrPath", err)
	}
}

// TestFileWriteRefusesCredentialNames: a file whose name is a credential file
// is refused for writing too, not only for reading and deleting.
func TestFileWriteRefusesCredentialNames(t *testing.T) {
	r := testRegistry(t)
	for _, p := range []string{"credentials", "id_rsa", "id_ed25519", "tls/server.pem", "tls/server.key", "conf/.netrc"} {
		if _, err := runOpErr(t, r, "file_write", map[string]string{"path": p, "content": "note"}); !errors.Is(err, ErrSecretPath) {
			t.Fatalf("file_write %q = %v, want ErrSecretPath", p, err)
		}
	}
	if _, err := runOpErr(t, r, "file_write", map[string]string{"path": "notes.md", "content": "x"}); err != nil {
		t.Fatalf("file_write notes.md = %v", err)
	}
}

// TestFileReadRefusesSymlinkedSecretStore: a symlink to .piperoom is refused
// at the link component, never followed.
func TestFileReadRefusesSymlinkedSecretStore(t *testing.T) {
	r := testRegistry(t)
	writeFile(t, r.env.ProjectDir, ".piperoom/ops.jsonl", "{\"id\":\"op1\"}\n")
	if err := os.Symlink(".piperoom", filepath.Join(r.env.ProjectDir, "alias")); err != nil {
		t.Skipf("the OS refused to create a symlink: %v", err)
	}
	defer os.Remove(filepath.Join(r.env.ProjectDir, "alias"))
	if _, err := runOpErr(t, r, "file_read", map[string]string{"path": "alias/ops.jsonl"}); !errors.Is(err, ErrPath) {
		t.Fatalf("file_read through a symlink = %v, want ErrPath", err)
	}
	if _, err := runOpErr(t, r, "dir_list", map[string]string{"path": "alias"}); !errors.Is(err, ErrPath) {
		t.Fatalf("dir_list of a symlink = %v, want ErrPath", err)
	}
}
