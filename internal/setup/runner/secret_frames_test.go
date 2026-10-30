package runner_test

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"piperoom/internal/events"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/runner"
)

// frameRecorder records every frame the runner sends, as raw JSON, so a test
// can grep the whole transport for a value (design §2.4: "a test greps every
// frame").
type frameRecorder struct {
	mu  sync.Mutex
	raw []string
}

func (fr *frameRecorder) read(t *testing.T, conn *websocket.Conn, ctx context.Context) protocol.Frame {
	t.Helper()
	_, data, err := conn.Read(ctx)
	if err != nil {
		t.Fatalf("read frame: %v", err)
	}
	fr.mu.Lock()
	fr.raw = append(fr.raw, string(data))
	fr.mu.Unlock()
	var f protocol.Frame
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatalf("decode frame: %v", err)
	}
	return f
}

func (fr *frameRecorder) frames() []string {
	fr.mu.Lock()
	defer fr.mu.Unlock()
	return append([]string(nil), fr.raw...)
}

// syncBuffer is a bytes.Buffer safe for a log handler written from the runner's
// goroutines while the test reads it afterwards.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// assertNoValue greps every runner→server frame, the log and stdout for a
// value; a hit is a leak.
func assertNoValue(t *testing.T, what, value string, rec *frameRecorder, logs, stdout *syncBuffer) {
	t.Helper()
	for i, raw := range rec.frames() {
		if strings.Contains(raw, value) {
			t.Fatalf("frame %d carries %s: %s", i, what, raw)
		}
	}
	if strings.Contains(logs.String(), value) {
		t.Fatalf("the log carries %s", what)
	}
	if strings.Contains(stdout.String(), value) {
		t.Fatalf("stdout carries %s", what)
	}
}

// TestOpResultLiterallyRedactedAndMACed: a value stored through FrameSecret is
// replaced literally in Summary, Data and the receipt, the receipt MAC
// verifies, and no frame, log line or terminal line carries the value or the
// receipt key.
func TestOpResultLiterallyRedactedAndMACed(t *testing.T) {
	const value = "correct-horse-9"
	dir := t.TempDir()

	cat := &fakeCatalogue{
		floor: map[string]string{"env_detect": protocol.TierT0},
		onExecute: func(context.Context, protocol.OpRequest) (protocol.OpResult, error) {
			return protocol.OpResult{
				OK:      true,
				Summary: "connected with " + value,
				Data:    json.RawMessage(`{"password":"` + value + `","nested":[{"x":"pre-` + value + `-post"}]}`),
				Receipt: events.SetupReceipt{
					Kind:   "exec",
					Argv:   []string{"psql", "--password=" + value},
					Target: value,
				},
			}, nil
		},
	}

	rec := &frameRecorder{}
	got := make(chan protocol.OpResult, 1)
	// A T1 op so the local op log records the redacted result and its MAC.
	p := testProposal("p1")
	p.Steps[0] = events.ProposalStep{Action: "env_detect", Layer: "environment", Params: json.RawMessage(`{}`)}
	hash, raw := proposalHash(t, p), proposalRaw(t, p)
	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok-secret-value-123", time.Now, nil)
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameSecret,
			Secret: &protocol.SecretValue{Secret: protocol.Secret{Name: "DB_PASSWORD", Value: value}}})
		si := 0
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: &protocol.OpRequest{
			ID: "op1", Op: "env_detect", Params: json.RawMessage(`{}`), Tier: protocol.TierT1,
			Class: protocol.ClassLocal, ProposalID: "p1", StepIndex: &si, ParamsHash: hash, Proposal: raw,
		}})
		f := rec.read(t, conn, ctx)
		if f.Type != protocol.FrameResult || f.Result == nil {
			t.Fatalf("got %+v, want a result", f)
		}
		got <- *f.Result
	})
	defer srv.Close()

	var logs, stdout syncBuffer
	r, err := runner.New(runner.Options{
		ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: dir,
		Stdin: strings.NewReader(""), Stdout: &stdout,
		Log:       slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Catalogue: cat,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done

	res := <-got
	if !res.OK {
		t.Fatalf("result not OK: %+v", res)
	}
	blob, _ := json.Marshal(res)
	if strings.Contains(string(blob), value) {
		t.Fatalf("the result carries the value: %s", blob)
	}
	if !strings.Contains(res.Summary, "${secret:DB_PASSWORD}") ||
		!strings.Contains(string(res.Data), "${secret:DB_PASSWORD}") ||
		res.Receipt.Argv[1] != "--password=${secret:DB_PASSWORD}" ||
		res.Receipt.Target != "${secret:DB_PASSWORD}" {
		t.Fatalf("the result has no placeholder: %s", blob)
	}
	if res.Receipt.OutMAC == "" {
		t.Fatalf("the result has no OutMAC")
	}

	// The value was stored, and the MAC verifies against the same key after a
	// reopen (a restarted runner).
	st, err := runner.OpenStore(dir, "")
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := st.Get("DB_PASSWORD"); !ok || v != value {
		t.Fatalf("store Get = %q, %v", v, ok)
	}
	if !st.VerifyResult(res) {
		t.Fatalf("OutMAC does not verify")
	}

	assertNoValue(t, "the secret value", value, rec, &logs, &stdout)
	assertNoValue(t, "the session token", "tok-secret-value-123", rec, &logs, &stdout)
	key := receiptKeyFromFile(t, dir)
	if key == "" {
		t.Fatalf("the store has no receipt key")
	}
	assertNoValue(t, "the receipt key", key, rec, &logs, &stdout)

	// The local op log is a witness too: it carries the redacted result and
	// the MAC, never the value.
	opLog, err := os.ReadFile(filepath.Join(dir, ".piperoom", "ops.jsonl"))
	if err != nil {
		t.Fatalf("read op log: %v", err)
	}
	if strings.Contains(string(opLog), value) {
		t.Fatalf("the op log carries the value: %s", opLog)
	}
	if !strings.Contains(string(opLog), res.Receipt.OutMAC) {
		t.Fatalf("the op log does not carry the receipt MAC: %s", opLog)
	}
}

// TestSecretFrameRefusesCloudSetupNames: a managed-cloud setup credential is
// typed on the runner only (docs/design/setup-managed.md §2), so the
// server's secret frame cannot set one, whatever its case; another name in
// the same session is stored.
func TestSecretFrameRefusesCloudSetupNames(t *testing.T) {
	const value = "cloud-key-value-7"
	dir := t.TempDir()
	refused := []string{"ccloud_api_key", "CCLOUD_CLUSTER_SECRET", "aiven_token", "aws_secret_access_key", "AWS_SESSION_TOKEN"}
	cat := &fakeCatalogue{
		floor: map[string]string{"env_detect": protocol.TierT0},
		onExecute: func(context.Context, protocol.OpRequest) (protocol.OpResult, error) {
			return protocol.OpResult{OK: true, Summary: "ok"}, nil
		},
	}
	rec := &frameRecorder{}
	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok-secret-value-123", time.Now, nil)
		for _, name := range append(refused, "db_password") {
			writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameSecret,
				Secret: &protocol.SecretValue{Secret: protocol.Secret{Name: name, Value: value}}})
		}
		// One op round trip: the secret frames before it were handled.
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: &protocol.OpRequest{
			ID: "op1", Op: "env_detect", Params: json.RawMessage(`{}`), Tier: protocol.TierT0, Class: protocol.ClassLocal,
		}})
		if f := rec.read(t, conn, ctx); f.Type != protocol.FrameResult {
			t.Errorf("got %+v, want a result", f)
		}
	})
	defer srv.Close()

	var logs, stdout syncBuffer
	r, err := runner.New(runner.Options{
		ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: dir,
		Stdin: strings.NewReader(""), Stdout: &stdout,
		Log:       slog.New(slog.NewTextHandler(&logs, nil)),
		Catalogue: cat,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done

	st, err := r.SecretStore()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range refused {
		if !runner.IsCloudSetupSecret(name) {
			t.Errorf("IsCloudSetupSecret(%q) = false", name)
		}
		if st.Has(name) {
			t.Errorf("the secret frame stored %s", name)
		}
		if !strings.Contains(stdout.String(), "secret "+name+" refused") {
			t.Errorf("stdout does not name the refused %s: %q", name, stdout.String())
		}
	}
	if !st.Has("db_password") {
		t.Error("an ordinary name was not stored")
	}
	if runner.IsCloudSetupSecret("db_password") || runner.IsCloudSetupSecret("aiven_tokens_db") {
		t.Error("an ordinary name is classed as a cloud credential")
	}
	assertNoValue(t, "the secret value", value, rec, &logs, &stdout)
}

// TestSecretSetOpPrompts: secret_set is a T1 op the runner executes itself. It
// prompts on the terminal, stores the value, and the value is in no result,
// log or terminal line.
func TestSecretSetOpPrompts(t *testing.T) {
	const value = "hunter2-value-xyz"
	dir := t.TempDir()
	p := testProposal("p1")
	p.Steps[0] = events.ProposalStep{
		Action: "secret_set", Layer: "environment",
		Params: json.RawMessage(`{"name":"NEW_SECRET"}`), Description: "store the secret",
	}
	hash, raw := proposalHash(t, p), proposalRaw(t, p)
	rec := &frameRecorder{}
	got := make(chan protocol.OpResult, 1)
	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok", time.Now, nil)
		si := 0
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: &protocol.OpRequest{
			ID: "op1", Op: "secret_set", Params: json.RawMessage(`{"name":"NEW_SECRET"}`),
			Tier: protocol.TierT1, Class: protocol.ClassLocal,
			ProposalID: "p1", StepIndex: &si, ParamsHash: hash, Proposal: raw,
		}})
		f := rec.read(t, conn, ctx)
		if f.Type != protocol.FrameResult || f.Result == nil {
			t.Fatalf("got %+v, want a result", f)
		}
		got <- *f.Result
	})
	defer srv.Close()

	var logs, stdout syncBuffer
	cat := &fakeCatalogue{floor: map[string]string{"secret_set": protocol.TierT1}}
	r, err := runner.New(runner.Options{
		ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: dir,
		Stdin: strings.NewReader(value + "\n"), Stdout: &stdout,
		Log:       slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Catalogue: cat,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done

	res := <-got
	if !res.OK {
		t.Fatalf("secret_set failed: %+v", res)
	}
	if res.Summary != "stored secret NEW_SECRET" {
		t.Fatalf("Summary = %q, want the name only", res.Summary)
	}
	if len(res.Data) != 0 {
		t.Fatalf("secret_set shipped Data: %s", res.Data)
	}
	if res.Receipt.Target != "secret:NEW_SECRET" {
		t.Fatalf("Receipt.Target = %q", res.Receipt.Target)
	}
	if cat.callCount() != 0 {
		t.Fatalf("the catalogue ran secret_set; the runner must execute it itself")
	}

	st, err := runner.OpenStore(dir, "")
	if err != nil {
		t.Fatal(err)
	}
	if v, ok := st.Get("NEW_SECRET"); !ok || v != value {
		t.Fatalf("store Get = %q, %v", v, ok)
	}
	if !st.VerifyResult(res) {
		t.Fatalf("OutMAC does not verify")
	}
	assertNoValue(t, "the secret value", value, rec, &logs, &stdout)
	assertNoValue(t, "the receipt key", receiptKeyFromFile(t, dir), rec, &logs, &stdout)
}

// TestSecretSetOpRefusesShortValue: a value under six bytes is refused at
// secret_set, and the refusal quotes the name only.
func TestSecretSetOpRefusesShortValue(t *testing.T) {
	const value = "abcde" // five bytes
	dir := t.TempDir()
	p := testProposal("p1")
	p.Steps[0] = events.ProposalStep{
		Action: "secret_set", Layer: "environment",
		Params: json.RawMessage(`{"name":"NEW_SECRET"}`), Description: "store the secret",
	}
	hash, raw := proposalHash(t, p), proposalRaw(t, p)
	rec := &frameRecorder{}
	got := make(chan protocol.OpResult, 1)
	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok", time.Now, nil)
		si := 0
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameOp, Op: &protocol.OpRequest{
			ID: "op1", Op: "secret_set", Params: json.RawMessage(`{"name":"NEW_SECRET"}`),
			Tier: protocol.TierT1, Class: protocol.ClassLocal,
			ProposalID: "p1", StepIndex: &si, ParamsHash: hash, Proposal: raw,
		}})
		f := rec.read(t, conn, ctx)
		if f.Type != protocol.FrameResult || f.Result == nil {
			t.Fatalf("got %+v, want a result", f)
		}
		got <- *f.Result
	})
	defer srv.Close()

	var logs, stdout syncBuffer
	r, err := runner.New(runner.Options{
		ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: dir,
		Stdin: strings.NewReader(value + "\n"), Stdout: &stdout,
		Log:       slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
		Catalogue: &fakeCatalogue{floor: map[string]string{"secret_set": protocol.TierT1}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done

	res := <-got
	if res.OK {
		t.Fatalf("a five-byte value was accepted: %+v", res)
	}
	if !strings.Contains(res.Summary, "too short") || !strings.Contains(res.Summary, "NEW_SECRET") {
		t.Fatalf("summary = %q, want the bound and the name", res.Summary)
	}
	st := mustStore(t, dir)
	if st.Has("NEW_SECRET") {
		t.Fatalf("the refused value was stored")
	}
	assertNoValue(t, "the short value", value, rec, &logs, &stdout)
}

// TestRegisterCredentialsConfirms: the runner prints the origin, the pipeline
// and the names, waits for "y", and sends exactly one Credentials frame with
// exactly the requested names. No other frame, log line or terminal line
// carries a value.
func TestRegisterCredentialsConfirms(t *testing.T) {
	dir := t.TempDir()
	st := mustStore(t, dir)
	if err := st.Set("READ_ROLE_PASSWORD", "generated-pw-1"); err != nil {
		t.Fatal(err)
	}
	if err := st.Set("READ_API_KEY", "generated-key-2"); err != nil {
		t.Fatal(err)
	}

	rec := &frameRecorder{}
	got := make(chan protocol.Frame, 1)
	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok", time.Now, nil)
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameRegisterCredentials,
			RegisterCredentials: &protocol.RegisterCredentials{
				RequestID: "rq1", PipelineID: "orders",
				Names: []string{"READ_ROLE_PASSWORD", "READ_API_KEY"},
			}})
		got <- rec.read(t, conn, ctx)
	})
	defer srv.Close()

	var logs, stdout syncBuffer
	r, err := runner.New(runner.Options{
		ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: dir,
		Stdin: strings.NewReader("y\n"), Stdout: &stdout,
		Log: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done

	f := <-got
	if f.Type != protocol.FrameCredentials || f.Credentials == nil {
		t.Fatalf("got %+v, want one credentials frame", f)
	}
	if f.Credentials.RequestID != "rq1" || len(f.Credentials.Secrets) != 2 {
		t.Fatalf("credentials = %+v", f.Credentials)
	}
	if f.Credentials.Secrets[0] != (protocol.Secret{Name: "READ_ROLE_PASSWORD", Value: "generated-pw-1"}) ||
		f.Credentials.Secrets[1] != (protocol.Secret{Name: "READ_API_KEY", Value: "generated-key-2"}) {
		t.Fatalf("credentials = %+v", f.Credentials.Secrets)
	}
	for _, want := range []string{srv.URL, "orders", "READ_ROLE_PASSWORD", "READ_API_KEY"} {
		if !strings.Contains(stdout.String(), want) {
			t.Fatalf("stdout does not name %q: %s", want, stdout.String())
		}
	}
	// Exactly one frame carries each value: the Credentials frame.
	for _, value := range []string{"generated-pw-1", "generated-key-2"} {
		n := 0
		for _, raw := range rec.frames() {
			if strings.Contains(raw, value) {
				n++
			}
		}
		if n != 1 {
			t.Fatalf("%q appears in %d frames, want 1", value, n)
		}
		if strings.Contains(logs.String(), value) || strings.Contains(stdout.String(), value) {
			t.Fatalf("%q leaked to the log or stdout", value)
		}
	}
}

// TestRegisterCredentialsDeclined: "N" sends no credentials and answers the
// request with RefuseCredentials, RequestID in Refusal.ID.
func TestRegisterCredentialsDeclined(t *testing.T) {
	dir := t.TempDir()
	st := mustStore(t, dir)
	if err := st.Set("READ_ROLE_PASSWORD", "generated-pw-1"); err != nil {
		t.Fatal(err)
	}
	rec := &frameRecorder{}
	got := make(chan protocol.Frame, 1)
	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok", time.Now, nil)
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameRegisterCredentials,
			RegisterCredentials: &protocol.RegisterCredentials{
				RequestID: "rq1", PipelineID: "orders", Names: []string{"READ_ROLE_PASSWORD"},
			}})
		got <- rec.read(t, conn, ctx)
	})
	defer srv.Close()

	var logs, stdout syncBuffer
	r, err := runner.New(runner.Options{
		ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: dir,
		Stdin: strings.NewReader("n\n"), Stdout: &stdout,
		Log: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done

	f := <-got
	if f.Type != protocol.FrameRefused || f.Refused == nil {
		t.Fatalf("got %+v, want a refusal", f)
	}
	if f.Refused.ID != "rq1" || f.Refused.Reason != protocol.RefuseCredentials {
		t.Fatalf("refusal = %+v", f.Refused)
	}
	assertNoValue(t, "the secret value", "generated-pw-1", rec, &logs, &stdout)
}

// TestRegisterCredentialsUnknownName: a name the store does not have refuses
// the whole request; a partial set is never sent.
func TestRegisterCredentialsUnknownName(t *testing.T) {
	dir := t.TempDir()
	st := mustStore(t, dir)
	if err := st.Set("READ_ROLE_PASSWORD", "generated-pw-1"); err != nil {
		t.Fatal(err)
	}
	rec := &frameRecorder{}
	got := make(chan protocol.Frame, 1)
	srv, done := runServer(t, func(conn *websocket.Conn, ctx context.Context) {
		doHandshake(t, conn, ctx, "tok", time.Now, nil)
		writeFrameJSON(t, conn, ctx, protocol.Frame{Type: protocol.FrameRegisterCredentials,
			RegisterCredentials: &protocol.RegisterCredentials{
				RequestID: "rq1", PipelineID: "orders",
				Names: []string{"READ_ROLE_PASSWORD", "NOT_IN_THE_STORE"},
			}})
		got <- rec.read(t, conn, ctx)
	})
	defer srv.Close()

	var logs, stdout syncBuffer
	r, err := runner.New(runner.Options{
		ServerURL: srv.URL, Code: "ABCDEFGH", Yes: true, ProjectDir: dir,
		Stdin: strings.NewReader("y\n"), Stdout: &stdout,
		Log: slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug})),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); err != nil {
		t.Fatalf("Run = %v", err)
	}
	<-done

	f := <-got
	if f.Type != protocol.FrameRefused || f.Refused == nil || f.Refused.ID != "rq1" {
		t.Fatalf("got %+v, want a credentials refusal", f)
	}
	assertNoValue(t, "the secret value", "generated-pw-1", rec, &logs, &stdout)
}
