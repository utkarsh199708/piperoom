package dbtools

import (
	"context"
	"encoding/json"
	"errors"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"piperoom/internal/actions/sourceactions"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// KAN-502: every value source_end_session tells the model to copy from
// source_blocking_sessions must be in that tool's Summary, because the model
// never sees Data. These tests tie the Summary text to the action's input.

// backendStart has microseconds, like Postgres' backend_start: the action
// matches pid and start time exactly.
var backendStart = time.Date(2026, 10, 20, 9, 15, 30, 123456000, time.UTC)

var startedAtRe = regexp.MustCompile(`(?m)^pid=(\d+) .*? started_at=(\S+)`)

// summaryStartedAt returns the started_at the Summary prints for pid, as the
// model would copy it; ok is false when the Summary has none.
func summaryStartedAt(summary string, pid int64) (string, bool) {
	for _, m := range startedAtRe.FindAllStringSubmatch(summary, -1) {
		if m[1] == strconv.FormatInt(pid, 10) {
			return m[2], true
		}
	}
	return "", false
}

func blockingPipeline() owners.Pipeline {
	return owners.Pipeline{ID: "orders", Connectors: []string{"pg-orders-connector"}, SourceTables: []string{"public.orders"}}
}

func endSessionInput(startedAt string) json.RawMessage {
	b, _ := json.Marshal(map[string]any{"connector": "pg-orders-connector", "pid": 41, "started_at": startedAt})
	return b
}

func TestSourceBlockingSessionsStartedAtFeedsEndSession(t *testing.T) {
	inScope := postgres.DBSession{
		PID: 41, StartedAt: backendStart, XactStart: backendStart.Add(time.Minute),
		User: "app", App: "orders-api", State: "idle in transaction", Query: "update orders set x=1",
		BlocksSlot: true, InScope: true, OldXact: true, SameDB: true, ClientBackend: true,
	}
	hidden := inScope
	hidden.InScope = false // the "identity hidden" branch of the Summary

	for _, tc := range []struct {
		name     string
		session  postgres.DBSession
		scoped   bool
		wantText string
	}{
		{"in scope", inScope, true, "user=app"},
		{"identity hidden", hidden, true, "identity hidden"},
		{"unscoped", inScope, false, "user=app"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			src := &fakeSource{blockers: []postgres.DBSession{tc.session}}
			d := settingsDeps(src)
			ctx := roomCtx()
			actCtx := tools.WithPipeline(context.Background(), blockingPipeline())
			if !tc.scoped {
				ctx = tools.WithUnscoped(context.Background())
				actCtx = context.Background()
			}
			r, err := scopedTool(t, d, "source_blocking_sessions").Run(ctx, json.RawMessage(`{}`))
			if err != nil {
				t.Fatalf("source_blocking_sessions: %v", err)
			}
			if !strings.Contains(r.Summary, tc.wantText) {
				t.Fatalf("summary is not the %s branch: %q", tc.name, r.Summary)
			}
			got, ok := summaryStartedAt(r.Summary, 41)
			if !ok {
				t.Fatalf("summary has no started_at for pid 41: %q", r.Summary)
			}
			parsed, err := time.Parse(time.RFC3339, got)
			if err != nil || !parsed.Equal(backendStart) {
				t.Fatalf("started_at %q parses to %v, %v; want %v exactly", got, parsed, err, backendStart)
			}

			act := sourceactions.NewEndSession(sourceactions.EndSessionDeps{Connect: d.Connect, Source: src})
			if err := act.Validate(actCtx, endSessionInput(got)); err != nil {
				t.Fatalf("source_end_session.Validate refuses the started_at copied from the summary (%q): %v", got, err)
			}

			// Negative: a start cut to whole seconds is another session to the
			// action, which is why the summary keeps the fraction.
			if err := act.Validate(actCtx, endSessionInput(backendStart.Truncate(time.Second).Format(time.RFC3339))); !errors.Is(err, postgres.ErrSessionChanged) {
				t.Fatalf("whole-second start = %v, want ErrSessionChanged", err)
			}
			// Negative: a value that is not RFC3339 is refused as bad params.
			for _, bad := range []string{"2026-10-20 09:15:30", "yesterday", "09:15:30", ""} {
				if err := act.Validate(actCtx, endSessionInput(bad)); err == nil {
					t.Errorf("Validate accepted started_at %q", bad)
				}
			}
		})
	}
}

// A Summary without started_at must fail the extraction, so the round trip
// above cannot pass on a summary that dropped the field.
func TestSummaryStartedAtNegative(t *testing.T) {
	old := "pid=41 user=app app=orders-api state=idle in transaction age=20m blocks_slot=yes query=\"select 1\"\n"
	if _, ok := summaryStartedAt(old, 41); ok {
		t.Fatal("a summary without started_at must not yield one")
	}
	if _, ok := summaryStartedAt("pid=41 state=active age=1m blocks_slot=no started_at=2026-10-20T09:15:30.123456Z (outside the pipeline's tables; identity hidden)\n", 42); ok {
		t.Fatal("started_at of another pid must not be returned")
	}
}

func TestSourceBlockingSessionsStartedAtUnknown(t *testing.T) {
	src := &fakeSource{blockers: []postgres.DBSession{{PID: 5, State: "active", InScope: true, User: "u", App: "a", XactStart: time.Now().Add(-time.Hour)}}}
	r, err := runScoped(t, settingsDeps(src), "source_blocking_sessions", `{}`)
	if err != nil {
		t.Fatalf("source_blocking_sessions: %v", err)
	}
	if got, _ := summaryStartedAt(r.Summary, 5); got != "-" {
		t.Fatalf("a session with no start shows started_at=%q, want -: %q", got, r.Summary)
	}
}

// The Summary stays within tools.MaxSummaryBytes with started_at on every
// row it shows, on both branches and with queries that %q expands.
func TestSourceBlockingSessionsSummaryLength(t *testing.T) {
	// 200 control bytes: %q turns each into a 4-byte \x escape.
	nasty := strings.Repeat("\x01", 400)
	mk := func(n int, inScope bool, query string) []postgres.DBSession {
		out := make([]postgres.DBSession, n)
		for i := range out {
			out[i] = postgres.DBSession{
				PID: int64(1000 + i), StartedAt: backendStart.Add(time.Duration(i) * time.Second), XactStart: backendStart,
				User: strings.Repeat("u", 64), App: strings.Repeat("a", 64), State: "idle in transaction",
				Query: query, BlocksSlot: true, InScope: inScope,
			}
		}
		return out
	}
	for _, tc := range []struct {
		name     string
		sessions []postgres.DBSession
	}{
		{"one in scope", mk(1, true, "select 1")},
		{"one hidden", mk(1, false, "")},
		{"20 in scope", mk(maxBlockerLines, true, strings.Repeat("x", 400))},
		{"20 hidden", mk(maxBlockerLines, false, "")},
		{"130 in scope", mk(130, true, "select 1")},
		{"130 hidden", mk(130, false, "")},
		{"20 in scope with expanding queries", mk(maxBlockerLines, true, nasty)},
		{"mixed with expanding queries", append(mk(15, true, nasty), mk(15, false, "")...)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, err := runScoped(t, settingsDeps(&fakeSource{blockers: tc.sessions}), "source_blocking_sessions", `{}`)
			if err != nil {
				t.Fatalf("source_blocking_sessions: %v", err)
			}
			if len(r.Summary) > tools.MaxSummaryBytes {
				t.Fatalf("summary is %d bytes, max %d", len(r.Summary), tools.MaxSummaryBytes)
			}
			var rows int
			for _, line := range strings.Split(r.Summary, "\n") {
				if !strings.HasPrefix(line, "pid=") {
					continue
				}
				rows++
				if !strings.Contains(line, " started_at=") {
					t.Fatalf("row without started_at (cut line?): %q", line)
				}
			}
			if rows == 0 {
				t.Fatalf("no rows in the summary: %q", r.Summary)
			}
			if !strings.HasSuffix(r.Summary, "\n") {
				t.Fatalf("summary ends mid-line: ...%q", r.Summary[max(0, len(r.Summary)-80):])
			}
			// Every session the Summary does not list is accounted for.
			data, _ := r.Data.([]postgres.DBSession)
			if more := len(data) - rows; more > 0 && !strings.Contains(r.Summary, "more session") {
				t.Fatalf("%d sessions unlisted without a count line: %q", more, r.Summary[max(0, len(r.Summary)-200):])
			}
		})
	}
}

// The tool description tells the model where to copy started_at to.
func TestSourceBlockingSessionsDescribesStartedAt(t *testing.T) {
	tl := scopedTool(t, settingsDeps(&fakeSource{}), "source_blocking_sessions")
	if !strings.Contains(tl.Description(), "started_at") {
		t.Fatalf("description does not name started_at: %q", tl.Description())
	}
}
