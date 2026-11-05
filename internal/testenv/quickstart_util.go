package testenv

// Pure helpers of Quickstart (quickstart.go, build tag integration). They
// live in an untagged file so plain `go test ./internal/testenv/...` covers
// them without the dev stack.

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"runtime"
	"slices"
	"sort"
	"strings"
	"sync"

	"piperoom/internal/events"
	"piperoom/internal/owners"
)

// DevConnector is the Debezium source connector the dev stack registers
// (dev/connectors/pg-orders-connector.json); discover names its pipeline
// after it.
const DevConnector = "pg-orders-connector"

// deadModelURL is ANTHROPIC_BASE_URL for a run without ANTHROPIC_API_KEY:
// port 1 on loopback refuses at once, so the model call fails the same way
// on every machine (no network, no stray credentials profile, no 401 that
// depends on the API being reachable).
const deadModelURL = "http://127.0.0.1:1"

// quickstartDropEnv are the variables a developer's shell may carry that
// would change what --quickstart does or refuse its start: the test sets the
// ones it needs itself. USERS_FILE must be unset or the server uses that file
// instead of writing the one-user file this test checks.
var quickstartDropEnv = []string{
	"USERS_FILE", "OWNERS_FILE", "PIPEROOM_HTTP_ADDR", "PIPEROOM_WRITE_MODE", "PIPEROOM_AUTH_MODE",
	"PIPEROOM_DEV_ALLOW_NONLOOPBACK", "PIPEROOM_SOLO_USER", "PIPEROOM_TENANCY_MODE", "PIPEROOM_WEB_DIR",
	"PIPEROOM_PUBLIC_URL", "PIPEROOM_WATCH_MODE",
}

// quickstartEnv builds the server's environment from environ: the variables
// above are dropped, the listen address is a free loopback port, and the
// watcher is off so only the test's alert opens a room. Without a key every
// ANTHROPIC_* variable and PIPEROOM_MODEL_PROVIDER are dropped too and the
// model endpoint points at a closed port, so the agent fails fast and the
// same way everywhere.
func quickstartEnv(environ []string, withKey bool) []string {
	out := make([]string, 0, len(environ)+4)
	for _, kv := range environ {
		k, _, _ := strings.Cut(kv, "=")
		if slices.Contains(quickstartDropEnv, k) {
			continue
		}
		if !withKey && (strings.HasPrefix(k, "ANTHROPIC_") || k == "PIPEROOM_MODEL_PROVIDER") {
			continue
		}
		out = append(out, kv)
	}
	out = append(out, "PIPEROOM_HTTP_ADDR=127.0.0.1:0", "PIPEROOM_WATCH_MODE=off")
	if !withKey {
		out = append(out, "ANTHROPIC_BASE_URL="+deadModelURL)
	}
	return out
}

// parseQuickstartURL checks the one line --quickstart prints on stdout: an
// http URL on a loopback address. It returns the URL without the trailing
// slash, ready to append "/api/...".
func parseQuickstartURL(line string) (string, error) {
	line = strings.TrimSpace(line)
	u, err := url.Parse(line)
	if err != nil {
		return "", fmt.Errorf("testenv: quickstart printed %q, not a URL: %w", line, err)
	}
	if u.Scheme != "http" || u.Host == "" {
		return "", fmt.Errorf("testenv: quickstart printed %q, want http://<loopback>:<port>/", line)
	}
	if h := u.Hostname(); h != "127.0.0.1" && h != "::1" && h != "localhost" {
		return "", fmt.Errorf("testenv: quickstart printed %q, which is not on loopback", line)
	}
	if u.Port() == "" || u.Port() == "0" {
		return "", fmt.Errorf("testenv: quickstart printed %q without the bound port", line)
	}
	return strings.TrimSuffix(u.Scheme+"://"+u.Host+u.Path, "/"), nil
}

// pipelineFor returns the id of the pipeline whose connectors list
// connector.
func pipelineFor(own *owners.Owners, connector string) (string, error) {
	var ids []string
	for _, p := range own.Pipelines {
		ids = append(ids, p.ID)
		if slices.Contains(p.Connectors, connector) {
			return p.ID, nil
		}
	}
	return "", fmt.Errorf("testenv: no pipeline lists connector %q (pipelines: %v)", connector, ids)
}

// errOwnersShape marks an owners file the quickstart user cannot approve
// anything with.
var errOwnersShape = errors.New("testenv: quickstart user owns no layer")

// checkOwnersShape says how the owners file assigns layers and fails when
// the quickstart's one-user users file would give that user nothing.
// --quickstart lists only the top-level layers: block in the users file it
// writes (cmd/server/quickstart.go, ownedLayers); a file that assigns layers
// only through per-pipeline owners: blocks starts a server whose one user
// owns nothing and can approve nothing.
func checkOwnersShape(own *owners.Owners, user string) (string, error) {
	var perPipeline []string
	for _, p := range own.Pipelines {
		if len(p.Owners) > 0 {
			perPipeline = append(perPipeline, p.ID)
		}
	}
	top := make([]string, 0, len(own.Layers))
	for l := range own.Layers {
		top = append(top, l)
	}
	sort.Strings(top)
	shape := fmt.Sprintf("top-level layers: %v; pipelines with their own owners: block: %v", top, perPipeline)
	if len(top) == 0 {
		return shape, fmt.Errorf("%w: the owners file has no top-level layers: block (%s), so --quickstart's users file gives %q no layers",
			errOwnersShape, shape, user)
	}
	if got := own.LayersFor(user, nil); len(got) == 0 {
		return shape, fmt.Errorf("%w: %q is not named in the owners file (%s)", errOwnersShape, user, shape)
	}
	return shape, nil
}

// sameLayers reports whether got and want hold the same layer names, in any
// order.
func sameLayers(got, want []string) bool {
	a, b := slices.Clone(got), slices.Clone(want)
	sort.Strings(a)
	sort.Strings(b)
	return slices.Equal(a, b)
}

// topLayers returns the layer names of the owners file's layers: block,
// sorted.
func topLayers(own *owners.Owners) []string {
	out := make([]string, 0, len(own.Layers))
	for l := range own.Layers {
		out = append(out, l)
	}
	sort.Strings(out)
	return out
}

// firstOfType returns the first event of type typ, or false.
func firstOfType(evs []events.Event, typ events.Type) (events.Event, bool) {
	for _, e := range evs {
		if e.Type == typ {
			return e, true
		}
	}
	return events.Event{}, false
}

// agentErrored returns the detail of the first agent.status with state
// "error", or false.
func agentErrored(evs []events.Event) (string, bool) {
	for _, e := range evs {
		if e.Type != events.AgentStatus {
			continue
		}
		var p events.AgentStatusPayload
		if json.Unmarshal(e.Payload, &p) == nil && p.State == "error" {
			return p.Detail, true
		}
	}
	return "", false
}

// agentRan reports whether the agent made at least one tool call: the
// investigation reached the real read-only tools, not just a status line.
func agentRan(evs []events.Event) bool {
	_, ok := firstOfType(evs, events.AgentToolCall)
	return ok
}

// typeCounts summarises a room log for a failure message.
func typeCounts(evs []events.Event) string {
	n := map[events.Type]int{}
	var order []events.Type
	for _, e := range evs {
		if n[e.Type] == 0 {
			order = append(order, e.Type)
		}
		n[e.Type]++
	}
	parts := make([]string, len(order))
	for i, t := range order {
		parts[i] = fmt.Sprintf("%s=%d", t, n[t])
	}
	return strings.Join(parts, " ")
}

// exeName adds ".exe" on Windows, where `go build -o` output must carry it to
// run.
func exeName(name string) string {
	if runtime.GOOS == "windows" {
		return name + ".exe"
	}
	return name
}

// tailBuffer is an io.Writer that keeps the last max bytes written, for
// a subprocess's log on failure. Safe for concurrent use (os/exec copies
// stderr from its own goroutine).
type tailBuffer struct {
	mu  sync.Mutex
	max int
	buf []byte
}

func newTailBuffer(max int) *tailBuffer { return &tailBuffer{max: max} }

func (b *tailBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf = append(b.buf, p...)
	if over := len(b.buf) - b.max; over > 0 {
		b.buf = append(b.buf[:0], b.buf[over:]...)
	}
	return len(p), nil
}

func (b *tailBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return string(b.buf)
}
