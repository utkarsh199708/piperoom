//go:build integration

package testenv

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/owners"
)

// QuickstartServer is a running `server --quickstart` against the dev stack,
// set up the way docs and `dev/README.md` tell a new user to: `discover
// -write-owners` first, then the server on that owners file.
type QuickstartServer struct {
	// URL is the base URL the server printed, without the trailing slash
	// (http://127.0.0.1:<port>).
	URL string
	// User is the one user of the run: the -owner given to discover, and so
	// the user --quickstart writes its users file for. Requests send it as
	// X-User (dev login).
	User string
	// PipelineID is the discovered pipeline of the dev connector.
	PipelineID string
	// Owners is the owners file discover wrote, as owners.Load reads it.
	Owners *owners.Owners
	// OwnersShape says how that file assigns layers (top-level layers:
	// block or per-pipeline owners: blocks), for the test log.
	OwnersShape string
	// WithKey is true when ANTHROPIC_API_KEY was set for the run; without
	// it the model endpoint is a closed loopback port (quickstartEnv).
	WithKey bool
	// BinDir holds the binaries built for the run (server, discover).
	BinDir string

	log    *tailBuffer
	client *http.Client
}

// quickstartStartTimeout bounds the build, discover and the server start.
const quickstartStartTimeout = 8 * time.Minute

// Quickstart runs the quickstart path against the dev stack and returns the
// server once /healthz answers: it builds cmd/discover and cmd/server with
// `go build -o` into a temp dir, runs `discover -write-owners` into another
// temp dir for a unique it_ owner, starts `server --quickstart --owners` on
// a free loopback port (PIPEROOM_HTTP_ADDR=127.0.0.1:0, URL read from the
// one line it prints), and waits for /healthz. The server is stopped when
// the test ends. It skips like Require when the stack is not reachable.
func Quickstart(t *testing.T) *QuickstartServer {
	t.Helper()
	Require(t)
	withKey := os.Getenv("ANTHROPIC_API_KEY") != ""

	ctx, cancel := context.WithTimeout(context.Background(), quickstartStartTimeout)
	defer cancel()

	bin := t.TempDir()
	BuildBinary(ctx, t, bin, "server")
	BuildBinary(ctx, t, bin, "discover")

	// discover -write-owners, into its own directory (the server's working
	// directory too, so nothing lands in the checkout).
	work := t.TempDir()
	user := Name("qs")
	ownersPath := filepath.Join(work, "owners.yaml")
	var dout, derr bytes.Buffer
	dcmd := exec.CommandContext(ctx, filepath.Join(bin, exeName("discover")), "-write-owners", ownersPath, "-owner", user)
	dcmd.Dir = work
	dcmd.Stdout, dcmd.Stderr = &dout, &derr
	if err := dcmd.Run(); err != nil {
		t.Fatalf("testenv: discover -write-owners: %v\nstdout:\n%s\nstderr:\n%s", err, dout.String(), derr.String())
	}
	own, err := owners.Load(ownersPath)
	if err != nil {
		t.Fatalf("testenv: the file discover -write-owners wrote does not load: %v", err)
	}
	shape, err := checkOwnersShape(own, user)
	if err != nil {
		t.Fatalf("%v", err)
	}
	t.Logf("testenv: discover -write-owners wrote %s", shape)
	pid, err := pipelineFor(own, DevConnector)
	if err != nil {
		t.Fatalf("%v", err)
	}

	qs := &QuickstartServer{
		User: user, PipelineID: pid, Owners: own, OwnersShape: shape, WithKey: withKey, BinDir: bin,
		log:    newTailBuffer(256 << 10),
		client: &http.Client{Timeout: 30 * time.Second},
	}

	// Not CommandContext: the server must outlive this start-up deadline and
	// stop in Cleanup, gracefully first.
	scmd := exec.Command(filepath.Join(bin, exeName("server")), "--quickstart", "--owners", ownersPath)
	scmd.Dir = work
	scmd.Env = quickstartEnv(os.Environ(), withKey)
	scmd.Stderr = qs.log
	// An io.Pipe rather than StdoutPipe: Wait may run while the reader below
	// is still reading, and closing the writer after Wait ends the reader.
	stdout, stdoutW := io.Pipe()
	scmd.Stdout = stdoutW
	if err := scmd.Start(); err != nil {
		t.Fatalf("testenv: start server --quickstart: %v", err)
	}
	exited := make(chan error, 1)
	go func() {
		err := scmd.Wait()
		_ = stdoutW.Close()
		exited <- err
	}()
	t.Cleanup(func() { stopServer(t, scmd, exited, qs.log) })

	// The URL line, then drain stdout so the server never blocks on it. The
	// reader goroutine ends when the process closes stdout (it exits).
	lines := make(chan string, 1)
	go func() {
		sc := bufio.NewScanner(stdout)
		first := true
		for sc.Scan() {
			if first {
				lines <- sc.Text()
				first = false
			}
		}
		if first {
			close(lines)
		}
	}()
	select {
	case line, ok := <-lines:
		if !ok {
			t.Fatalf("testenv: server --quickstart closed stdout without printing a URL\nserver log:\n%s", qs.log.String())
		}
		if qs.URL, err = parseQuickstartURL(line); err != nil {
			t.Fatalf("%v\nserver log:\n%s", err, qs.log.String())
		}
	case err := <-exited:
		exited <- err // for Cleanup
		t.Fatalf("testenv: server --quickstart exited before printing a URL: %v\nserver log:\n%s", err, qs.log.String())
	case <-ctx.Done():
		t.Fatalf("testenv: server --quickstart printed no URL within %s\nserver log:\n%s", quickstartStartTimeout, qs.log.String())
	}

	WaitFor(t, time.Minute, func() (bool, error) {
		resp, err := qs.client.Get(qs.URL + "/healthz")
		if err != nil {
			return false, nil
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		return resp.StatusCode == http.StatusOK, nil
	})
	t.Logf("testenv: server --quickstart at %s (user %s, pipeline %s, ANTHROPIC_API_KEY set: %v)", qs.URL, user, pid, withKey)
	return qs
}

// stopServer interrupts the server and waits for it; after 15 s, or where an
// interrupt cannot be sent (Windows), it kills it. On a failed test it logs
// the tail of the server log.
func stopServer(t *testing.T, cmd *exec.Cmd, exited chan error, log *tailBuffer) {
	t.Helper()
	if runtime.GOOS == "windows" || cmd.Process.Signal(os.Interrupt) != nil {
		_ = cmd.Process.Kill()
	}
	select {
	case <-exited:
	case <-time.After(15 * time.Second):
		_ = cmd.Process.Kill()
		<-exited
	}
	if t.Failed() {
		t.Logf("testenv: server --quickstart log (tail):\n%s", log.String())
	}
}

// BuildBinary builds ./cmd/<name> of this module into dir with `go build -o`
// and returns its path. Tests run the binary instead of `go run`, so a
// signal reaches the program itself, not the go tool.
func BuildBinary(ctx context.Context, t *testing.T, dir, name string) string {
	t.Helper()
	root, err := moduleRoot(ctx)
	if err != nil {
		t.Fatalf("%v", err)
	}
	out := filepath.Join(dir, exeName(name))
	cmd := exec.CommandContext(ctx, "go", "build", "-o", out, "./cmd/"+name)
	cmd.Dir = root
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("testenv: go build ./cmd/%s: %v\n%s", name, err, b)
	}
	return out
}

// moduleRoot is the directory of this module's go.mod.
func moduleRoot(ctx context.Context) (string, error) {
	b, err := exec.CommandContext(ctx, "go", "env", "GOMOD").Output()
	if err != nil {
		return "", fmt.Errorf("testenv: go env GOMOD: %w", err)
	}
	gomod := strings.TrimSpace(string(b))
	if gomod == "" || gomod == os.DevNull {
		return "", errors.New("testenv: not inside the piperoom module")
	}
	return filepath.Dir(gomod), nil
}

// Log returns the tail of the server's log (stderr).
func (q *QuickstartServer) Log() string { return q.log.String() }

// Do sends a request as the run's user (dev login, X-User) and decodes a
// 2xx JSON answer into out (nil skips decoding). It returns the status.
func (q *QuickstartServer) Do(t *testing.T, method, path string, body, out any) int {
	t.Helper()
	var rdr io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("testenv: encode %s %s: %v", method, path, err)
		}
		rdr = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, q.URL+path, rdr)
	if err != nil {
		t.Fatalf("testenv: %s %s: %v", method, path, err)
	}
	req.Header.Set("X-User", q.User)
	req.Header.Set("Content-Type", "application/json")
	resp, err := q.client.Do(req)
	if err != nil {
		t.Fatalf("testenv: %s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		t.Fatalf("testenv: %s %s: read body: %v", method, path, err)
	}
	if resp.StatusCode/100 != 2 {
		t.Logf("testenv: %s %s: %d %s", method, path, resp.StatusCode, bytes.TrimSpace(data))
		return resp.StatusCode
	}
	if out != nil {
		if err := json.Unmarshal(data, out); err != nil {
			t.Fatalf("testenv: %s %s: decode: %v\n%s", method, path, err, data)
		}
	}
	return resp.StatusCode
}

// Events returns the room's whole log (GET /api/rooms/{id}/events, paged).
func (q *QuickstartServer) Events(t *testing.T, roomID string) []events.Event {
	t.Helper()
	var all []events.Event
	var after int64
	for {
		var page struct {
			Events  []events.Event `json:"events"`
			LastSeq int64          `json:"last_seq"`
		}
		path := fmt.Sprintf("/api/rooms/%s/events?after=%d&limit=1000", roomID, after)
		if code := q.Do(t, http.MethodGet, path, nil, &page); code != http.StatusOK {
			t.Fatalf("testenv: GET %s: status %d", path, code)
		}
		all = append(all, page.Events...)
		if len(page.Events) == 0 {
			return all
		}
		after = page.Events[len(page.Events)-1].Seq
		if after >= page.LastSeq {
			return all
		}
	}
}
