package runner_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/runner"
)

// mustStore opens a store in dir and fails the test on error.
func mustStore(t *testing.T, dir string) *runner.Store {
	t.Helper()
	s, err := runner.OpenStore(dir, "")
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	return s
}

// readStoreFile returns the raw store file for a project directory.
func readStoreFile(t *testing.T, dir string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, ".piperoom", "secrets.env"))
	if err != nil {
		t.Fatalf("read store: %v", err)
	}
	return string(b)
}

// receiptKeyFromFile extracts the receipt key hex from the raw store file.
func receiptKeyFromFile(t *testing.T, dir string) string {
	t.Helper()
	for _, line := range strings.Split(readStoreFile(t, dir), "\n") {
		name, value, ok := strings.Cut(strings.TrimSpace(line), "=")
		if ok && name == runner.ReceiptKeyName {
			return strings.Trim(value, `"`)
		}
	}
	t.Fatalf("no %s line in the store", runner.ReceiptKeyName)
	return ""
}

// TestStorePermissionsAndGitignore: the store is created 0600 in a 0700
// directory and the generated .gitignore names it (design §2.4).
func TestStorePermissionsAndGitignore(t *testing.T) {
	dir := t.TempDir()
	_ = mustStore(t, dir)

	storeDir := filepath.Join(dir, ".piperoom")
	fi, err := os.Stat(storeDir)
	if err != nil {
		t.Fatal(err)
	}
	if !fi.IsDir() {
		t.Fatalf("store dir is not a directory")
	}
	fi, err = os.Stat(filepath.Join(storeDir, "secrets.env"))
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" {
		if got := fi.Mode().Perm(); got != 0o600 {
			t.Fatalf("store mode = %o, want 600", got)
		}
		if d, err := os.Stat(storeDir); err == nil && d.Mode().Perm() != 0o700 {
			t.Fatalf("store dir mode = %o, want 700", d.Mode().Perm())
		}
	}
	ignore, err := os.ReadFile(filepath.Join(dir, ".gitignore"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(ignore), ".piperoom/") {
		t.Fatalf(".gitignore does not list .piperoom/: %q", ignore)
	}

	// A second open must not append the entry again.
	_ = mustStore(t, dir)
	ignore, _ = os.ReadFile(filepath.Join(dir, ".gitignore"))
	if strings.Count(string(ignore), ".piperoom/") != 1 {
		t.Fatalf(".gitignore has the entry twice: %q", ignore)
	}
}

// TestStoreSetGetAndRestart: a value survives a restart and the key is stable
// (so an OutMAC minted before the restart still verifies).
func TestStoreSetGetAndRestart(t *testing.T) {
	dir := t.TempDir()
	st := mustStore(t, dir)
	if err := st.Set("DB_PASSWORD", "s3cret-value"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	if v, ok := st.Get("DB_PASSWORD"); !ok || v != "s3cret-value" {
		t.Fatalf("Get = %q, %v", v, ok)
	}
	if names := st.Names(); len(names) != 1 || names[0] != "DB_PASSWORD" {
		t.Fatalf("Names = %v", names)
	}
	if got := readStoreFile(t, dir); strings.Contains(got, runner.ReceiptKeyName) == false {
		t.Fatalf("store has no receipt key line")
	}

	res := protocol.OpResult{OK: true, Summary: "ok", Data: json.RawMessage(`{"x":1}`)}
	res.Receipt.OutMAC = st.ResultMAC(true, res.Summary, res.Data)

	// Restart: a new Store over the same file.
	st2 := mustStore(t, dir)
	if v, ok := st2.Get("DB_PASSWORD"); !ok || v != "s3cret-value" {
		t.Fatalf("after restart Get = %q, %v", v, ok)
	}
	if !st2.VerifyResult(res) {
		t.Fatalf("a MAC minted before the restart does not verify")
	}
	// The receipt key is never a listed secret.
	for _, name := range st2.Names() {
		if name == runner.ReceiptKeyName {
			t.Fatalf("Names lists the receipt key")
		}
	}
}

// TestStoreUpsertNotDuplicate: Set replaces a value (password rotation) and
// leaves one line for the name.
func TestStoreUpsertNotDuplicate(t *testing.T) {
	dir := t.TempDir()
	st := mustStore(t, dir)
	if err := st.Set("READ_PASSWORD", "first-value"); err != nil {
		t.Fatal(err)
	}
	if err := st.Set("READ_PASSWORD", "second-value"); err != nil {
		t.Fatal(err)
	}
	if v, _ := st.Get("READ_PASSWORD"); v != "second-value" {
		t.Fatalf("Get = %q, want the replaced value", v)
	}
	if n := strings.Count(readStoreFile(t, dir), "READ_PASSWORD="); n != 1 {
		t.Fatalf("store has %d READ_PASSWORD lines, want 1", n)
	}
}

// TestStoreRefusesDuplicateLines: a hand-edited store with two lines for one
// name is ambiguous, so OpenStore refuses it rather than pick one.
func TestStoreRefusesDuplicateLines(t *testing.T) {
	dir := t.TempDir()
	st := mustStore(t, dir)
	if err := st.Set("DB_PASSWORD", "s3cret-value"); err != nil {
		t.Fatal(err)
	}
	raw := readStoreFile(t, dir) + "DB_PASSWORD=\"other-value\"\n"
	if err := os.WriteFile(filepath.Join(dir, ".piperoom", "secrets.env"), []byte(raw), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := runner.OpenStore(dir, ""); !errors.Is(err, runner.ErrStoreCorrupt) {
		t.Fatalf("OpenStore = %v, want ErrStoreCorrupt", err)
	}
}

// TestStoreValueBounds: a value under six bytes is refused at secret_set so
// literal redaction cannot mangle ordinary text.
func TestStoreValueBounds(t *testing.T) {
	st := mustStore(t, t.TempDir())
	for _, v := range []string{"", "a", "abcde"} {
		if err := st.Set("TOO_SHORT", v); !errors.Is(err, runner.ErrSecretShort) {
			t.Fatalf("Set(%q) = %v, want ErrSecretShort", v, err)
		}
		if st.Has("TOO_SHORT") {
			t.Fatalf("a refused value was stored")
		}
	}
	if err := st.Set("BIG", strings.Repeat("x", runner.MaxSecretValueBytes+1)); !errors.Is(err, runner.ErrSecretLong) {
		t.Fatalf("Set(too long) = %v, want ErrSecretLong", err)
	}
	if err := st.Set("OK", "abcdef"); err != nil {
		t.Fatalf("Set(6 bytes) = %v", err)
	}
}

// TestStoreNameValidation covers the protocol's name rule and the reserved
// receipt key.
func TestStoreNameValidation(t *testing.T) {
	st := mustStore(t, t.TempDir())
	bad := []string{"", "1abc", "a-b", "a.b", "a/b", "a b", "a\tb", "é", "a:b", strings.Repeat("A", runner.MaxSecretNameBytes+1), runner.ReceiptKeyName}
	for _, name := range bad {
		if err := st.Set(name, "abcdef"); !errors.Is(err, runner.ErrSecretName) {
			t.Fatalf("Set(%q) = %v, want ErrSecretName", name, err)
		}
	}
	good := []string{"DB_PASSWORD", "_private", "a1", "A_B_C9"}
	for _, name := range good {
		if err := runner.ValidateSecretName(name); err != nil {
			t.Fatalf("ValidateSecretName(%q) = %v", name, err)
		}
	}
}

// TestStoreMissingSecret: a missing name never materialises.
func TestStoreMissingSecret(t *testing.T) {
	st := mustStore(t, t.TempDir())
	if _, ok := st.Get("NOPE"); ok {
		t.Fatalf("Get(NOPE) found a value")
	}
	if _, err := st.Materialise("${secret:NOPE}"); !errors.Is(err, runner.ErrSecretMissing) {
		t.Fatalf("Materialise = %v, want ErrSecretMissing", err)
	}
	if _, err := st.MaterialiseEnv([]string{"X=${secret:NOPE}"}); !errors.Is(err, runner.ErrSecretMissing) {
		t.Fatalf("MaterialiseEnv = %v, want ErrSecretMissing", err)
	}
}

// TestStoreMaterialise: ${secret:NAME} resolves in memory, once, and the
// reserved receipt key never resolves.
func TestStoreMaterialise(t *testing.T) {
	st := mustStore(t, t.TempDir())
	if err := st.Set("DB_PASSWORD", "s3cret-value"); err != nil {
		t.Fatal(err)
	}
	got, err := st.Materialise("password=${secret:DB_PASSWORD}")
	if err != nil {
		t.Fatalf("Materialise: %v", err)
	}
	if got != "password=s3cret-value" {
		t.Fatalf("Materialise = %q", got)
	}
	// A stored value that carries a reference is not expanded again.
	if err := st.Set("REF", "${secret:DB_PASSWORD}"); err != nil {
		t.Fatal(err)
	}
	got, err = st.Materialise("${secret:REF}")
	if err != nil || got != "${secret:DB_PASSWORD}" {
		t.Fatalf("Materialise(REF) = %q, %v; want one pass only", got, err)
	}
	if _, err := st.Materialise("${secret:" + runner.ReceiptKeyName + "}"); !errors.Is(err, runner.ErrSecretMissing) {
		t.Fatalf("Materialise(receipt key) = %v, want ErrSecretMissing", err)
	}

	env, err := st.MaterialiseEnv([]string{"PGPASSWORD=${secret:DB_PASSWORD}", "PATH=/bin"})
	if err != nil {
		t.Fatalf("MaterialiseEnv: %v", err)
	}
	if env[0] != "PGPASSWORD=s3cret-value" || env[1] != "PATH=/bin" {
		t.Fatalf("MaterialiseEnv = %v", env)
	}
}

// TestStoreRedact: every stored value is replaced literally by its
// ${secret:NAME}, longest value first, and redaction is idempotent.
func TestStoreRedact(t *testing.T) {
	st := mustStore(t, t.TempDir())
	if err := st.Set("SHORT", "abcdef"); err != nil {
		t.Fatal(err)
	}
	if err := st.Set("LONG", "abcdef123"); err != nil {
		t.Fatal(err)
	}
	in := "a abcdef123 b abcdef c"
	got := st.Redact(in)
	want := "a ${secret:LONG} b ${secret:SHORT} c"
	if got != want {
		t.Fatalf("Redact = %q, want %q", got, want)
	}
	if again := st.Redact(got); again != got {
		t.Fatalf("Redact is not idempotent: %q", again)
	}
	if strings.Contains(got, "abcdef") {
		t.Fatalf("Redact left a value: %q", got)
	}
}

// TestStoreRedactWrappedValue: wrapping a raw stored value in reference syntax
// must not smuggle it past the idempotence skip. Only a well-formed
// ${secret:NAME} span that carries no stored value is copied untouched;
// anything else is matched byte by byte and the value inside is redacted.
func TestStoreRedactWrappedValue(t *testing.T) {
	st := mustStore(t, t.TempDir())
	if err := st.Set("DB_PASS", "hunter2secret"); err != nil {
		t.Fatal(err)
	}
	if err := st.Set("OTHER", "zz9plural"); err != nil {
		t.Fatal(err)
	}
	cases := []string{
		// The value itself posing as the reference's name.
		"prefix ${secret:hunter2secret} suffix",
		// A malformed reference with the value before the first "}".
		"${secret: oops hunter2secret and more }",
		// A well-formed name that carries another stored value inside it.
		"${secret:xxzz9pluralyy}",
		// An unterminated reference followed by the value.
		"${secret:hunter2secret with no close",
	}
	for _, in := range cases {
		got := st.Redact(in)
		if strings.Contains(got, "hunter2secret") || strings.Contains(got, "zz9plural") {
			t.Fatalf("Redact(%q) leaked a value: %q", in, got)
		}
	}
	// A genuine reference is still copied untouched, so redaction stays
	// idempotent, the receipt key's placeholder included.
	for _, ref := range []string{"${secret:DB_PASS}", "${secret:" + runner.ReceiptKeyName + "}"} {
		if got := st.Redact(ref); got != ref {
			t.Fatalf("Redact(%q) = %q, want it untouched", ref, got)
		}
	}
}

// TestStoreSanitizeResult: a stored value is replaced in Summary, Data
// (strings and keys) and the receipt, and the size caps still hold.
func TestStoreSanitizeResult(t *testing.T) {
	st := mustStore(t, t.TempDir())
	if err := st.Set("DB_PASSWORD", "s3cret-value"); err != nil {
		t.Fatal(err)
	}
	res := protocol.OpResult{
		OK:      true,
		Summary: "connected with s3cret-value",
		Data:    json.RawMessage(`{"password":"s3cret-value","nested":[{"x":"pre-s3cret-value-post"}]}`),
		Receipt: events.SetupReceipt{
			Kind:   "exec",
			Argv:   []string{"psql", "--password=s3cret-value"},
			Target: "s3cret-value",
			OutMAC: "stale",
		},
	}
	got := st.SanitizeResult(res)
	blob, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(blob), "s3cret-value") {
		t.Fatalf("SanitizeResult left the value: %s", blob)
	}
	if !strings.Contains(string(blob), "${secret:DB_PASSWORD}") {
		t.Fatalf("SanitizeResult left no placeholder: %s", blob)
	}
	if got.Receipt.OutMAC != "" {
		t.Fatalf("SanitizeResult kept a stale OutMAC")
	}
	if !got.Redacted {
		t.Fatal("SanitizeResult replaced a value in Data but did not mark the result Redacted")
	}
	// Data with no stored value (a genuine reference included) is not marked.
	clean := st.SanitizeResult(protocol.OpResult{OK: true, Data: json.RawMessage(`{"password":"${secret:DB_PASSWORD}"}`)})
	if clean.Redacted {
		t.Fatalf("SanitizeResult marked untouched Data Redacted: %s", clean.Data)
	}
	// A value only in Summary does not mark Data.
	summaryOnly := st.SanitizeResult(protocol.OpResult{OK: true, Summary: "s3cret-value", Data: json.RawMessage(`{"x":1}`)})
	if summaryOnly.Redacted {
		t.Fatal("a Summary-only redaction marked Data Redacted")
	}

	// Data that cannot be parsed is withheld, never passed through.
	bad := st.SanitizeResult(protocol.OpResult{Data: json.RawMessage("s3cret-value not json")})
	if strings.Contains(string(bad.Data), "s3cret-value") || !bad.Truncated {
		t.Fatalf("unparseable Data was not withheld: %s", bad.Data)
	}
}

// TestStoreHMAC: OutMAC verifies, and a tampered Summary, Data or MAC fails.
func TestStoreHMAC(t *testing.T) {
	dir := t.TempDir()
	st := mustStore(t, dir)
	res := protocol.OpResult{OK: true, Summary: "up", Data: json.RawMessage(`{"services":["connect"]}`)}
	res.Receipt.OutMAC = st.ResultMAC(res.OK, res.Summary, res.Data)
	if len(res.Receipt.OutMAC) != 64 {
		t.Fatalf("OutMAC = %q, want 64 hex characters", res.Receipt.OutMAC)
	}
	if !st.VerifyResult(res) {
		t.Fatalf("VerifyResult = false, want true")
	}

	tampered := res
	tampered.Summary = "down"
	if st.VerifyResult(tampered) {
		t.Fatalf("VerifyResult accepted a tampered Summary")
	}
	tampered = res
	tampered.Data = json.RawMessage(`{"services":[]}`)
	if st.VerifyResult(tampered) {
		t.Fatalf("VerifyResult accepted tampered Data")
	}
	tampered = res
	tampered.Receipt.OutMAC = strings.Repeat("0", 64)
	if st.VerifyResult(tampered) {
		t.Fatalf("VerifyResult accepted a tampered MAC")
	}
	tampered = res
	tampered.Receipt.OutMAC = ""
	if st.VerifyResult(tampered) {
		t.Fatalf("VerifyResult accepted an empty MAC")
	}

	other := mustStore(t, t.TempDir())
	if other.VerifyResult(res) {
		t.Fatalf("a different runner's key verified the MAC")
	}
}

// TestStoreMaterialisedPaths: the list persists, matches a path and everything
// under it, and refuses anything that is not project-relative.
func TestStoreMaterialisedPaths(t *testing.T) {
	dir := t.TempDir()
	st := mustStore(t, dir)
	if err := st.RegisterMaterialised("connect/secrets.properties"); err != nil {
		t.Fatalf("RegisterMaterialised: %v", err)
	}
	if err := st.RegisterMaterialised(`connect\secrets.properties`); err != nil {
		t.Fatalf("RegisterMaterialised (backslashes): %v", err)
	}
	if got := st.MaterialisedPaths(); len(got) != 1 || got[0] != "connect/secrets.properties" {
		t.Fatalf("MaterialisedPaths = %v", got)
	}
	for _, p := range []string{"connect/secrets.properties", "CONNECT/Secrets.Properties", "connect/secrets.properties/x"} {
		if !st.IsMaterialised(p) {
			t.Fatalf("IsMaterialised(%q) = false", p)
		}
	}
	if st.IsMaterialised("connect/other.properties") {
		t.Fatalf("IsMaterialised(connect/other.properties) = true")
	}
	// The dotless ı (U+0131) upper-cases to "I", so "confıg" opens "config" on
	// a case-folding file system; strings.EqualFold alone misses it.
	if err := st.RegisterMaterialised("config/app.properties"); err != nil {
		t.Fatalf("RegisterMaterialised: %v", err)
	}
	if !st.IsMaterialised("confıg/app.properties") {
		t.Fatalf("IsMaterialised(confıg, dotless ı) = false")
	}
	for _, p := range []string{"", ".", "/abs/x", "../escape", ".piperoom/x", ".git/x"} {
		if err := st.RegisterMaterialised(p); err == nil {
			t.Fatalf("RegisterMaterialised(%q) succeeded", p)
		}
	}

	// A restart keeps the denial.
	st2 := mustStore(t, dir)
	if !st2.IsMaterialised("connect/secrets.properties") {
		t.Fatalf("the materialised path did not survive a restart")
	}
	if runtime.GOOS != "windows" {
		fi, err := os.Stat(filepath.Join(dir, ".piperoom", "materialised"))
		if err != nil {
			t.Fatal(err)
		}
		if fi.Mode().Perm() != 0o600 {
			t.Fatalf("materialised list mode = %o, want 600", fi.Mode().Perm())
		}
	}
}

// TestStoreRefusesSymlinks: the store file and its directory are refused when
// they are symlinks, so the runner never follows one.
func TestStoreRefusesSymlinks(t *testing.T) {
	outside := t.TempDir()

	t.Run("store file", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.MkdirAll(filepath.Join(dir, ".piperoom"), 0o700); err != nil {
			t.Fatal(err)
		}
		target := filepath.Join(outside, "secrets.env")
		if err := os.WriteFile(target, []byte(runner.ReceiptKeyName+"=\""+strings.Repeat("ab", 32)+"\"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(target, filepath.Join(dir, ".piperoom", "secrets.env")); err != nil {
			t.Skipf("the OS refused to create a symlink: %v", err)
		}
		if _, err := runner.OpenStore(dir, ""); !errors.Is(err, runner.ErrStoreUnsafe) {
			t.Fatalf("OpenStore = %v, want ErrStoreUnsafe", err)
		}
	})

	t.Run("store directory", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.Symlink(outside, filepath.Join(dir, ".piperoom")); err != nil {
			t.Skipf("the OS refused to create a symlink: %v", err)
		}
		if _, err := runner.OpenStore(dir, ""); !errors.Is(err, runner.ErrStoreUnsafe) {
			t.Fatalf("OpenStore = %v, want ErrStoreUnsafe", err)
		}
	})
}

// TestStoreSatisfiesClientSecretStore: CreateReadRole's writers store a
// generated password through ctx-aware Store and get only the name back.
func TestStoreSatisfiesClientSecretStore(t *testing.T) {
	var _ interface {
		Store(ctx context.Context, name, value string) error
	} = (*runner.Store)(nil)

	st := mustStore(t, t.TempDir())
	if err := st.Store(context.Background(), "READ_ROLE_PASSWORD", "generated-pw"); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if v, _ := st.Get("READ_ROLE_PASSWORD"); v != "generated-pw" {
		t.Fatalf("Get = %q", v)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := st.Store(ctx, "READ_ROLE_PASSWORD", "other-value"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Store(cancelled) = %v, want context.Canceled", err)
	}
}

// TestStoreCustomFile: PIPEROOM_SETUP_SECRETS_FILE is honoured only where the
// store stays protected. A path inside the project but outside .piperoom/
// would escape both the generated .gitignore entry and the ops deny-list, so
// it is refused fail-closed; a path under .piperoom/ and a path outside the
// project (the operator's choice) stay allowed.
func TestStoreCustomFile(t *testing.T) {
	dir := t.TempDir()

	refused := []string{
		"conf/runner.secrets",                        // relative, inside the project, outside .piperoom
		"runner.secrets",                             // relative, at the project root
		"../escape.secrets",                          // relative, climbing out of the project
		filepath.Join(dir, "conf", "runner.secrets"), // absolute, inside the project, outside .piperoom
	}
	for _, p := range refused {
		if _, err := runner.OpenStore(dir, p); !errors.Is(err, runner.ErrStorePath) {
			t.Fatalf("OpenStore(%q) = %v, want ErrStorePath", p, err)
		}
	}
	// A refused path must leave nothing behind: no store file and no directory.
	if _, err := os.Stat(filepath.Join(dir, "conf")); !os.IsNotExist(err) {
		t.Fatalf("a refused path created its directory")
	}

	// A custom name under .piperoom/ keeps both protections and is allowed.
	st, err := runner.OpenStore(dir, filepath.Join(".piperoom", "alt.env"))
	if err != nil {
		t.Fatalf("OpenStore(.piperoom/alt.env): %v", err)
	}
	if err := st.Set("DB_PASSWORD", "s3cret-value"); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, ".piperoom", "alt.env")); err != nil {
		t.Fatalf("custom store file: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, ".piperoom", "secrets.env")); !os.IsNotExist(err) {
		t.Fatalf("the default store was created as well")
	}
	ignore, _ := os.ReadFile(filepath.Join(dir, ".gitignore"))
	if !strings.Contains(string(ignore), ".piperoom/") {
		t.Fatalf(".gitignore does not list .piperoom/: %q", ignore)
	}

	// An absolute path outside the project is the operator's choice.
	outside := filepath.Join(t.TempDir(), "runner.secrets")
	if _, err := runner.OpenStore(dir, outside); err != nil {
		t.Fatalf("OpenStore(outside the project) = %v", err)
	}
	if _, err := os.Stat(outside); err != nil {
		t.Fatalf("outside store file: %v", err)
	}
}
