package kafkatools

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
)

// hangingConnect is a Connect whose REST API accepts but never answers.
func hangingConnect() *fakeConnect {
	return &fakeConnect{
		listFn: func(context.Context) ([]string, error) { return []string{"pg-orders-connector"}, nil },
		configFn: func(ctx context.Context, _ string) (map[string]string, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
}

// ctxCheckingReader fails first on a dead context, like connect.LogReader.
func ctxCheckingReader(seen *[]string) *fakeLogReader {
	at := time.Date(2026, 10, 15, 4, 10, 9, 0, time.UTC)
	return &fakeLogReader{fn: func(ctx context.Context, q clients.WorkerLogQuery) ([]clients.LogLine, error) {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		*seen = append(*seen, q.Connector)
		return []clients.LogLine{{At: at, Level: "ERROR", Message: "must be owner of table orders"}}, nil
	}}
}

// Q4 R11: a hung Connect REST API must not cost the worker log.
func TestConnectWorkerLogSurvivesHungConnect(t *testing.T) {
	for _, tc := range []struct{ name, input string }{
		{"explicit connector", `{"connector":"pg-orders-connector"}`},
		{"default connector", `{}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var seen []string
			d, _ := scopeFakes(t)
			d.Connect = hangingConnect()
			d.ConnectLog = ctxCheckingReader(&seen)
			d.Now = func() time.Time { return logNow }
			start := time.Now()
			r, err := scopeTool(t, d, "connect_worker_log").Run(roomCtx(), json.RawMessage(tc.input))
			if err != nil {
				t.Fatalf("log evidence lost because Config hung (took %v): %v", time.Since(start), err)
			}
			if time.Since(start) > 6*time.Second {
				t.Errorf("took %v, the config lookup must be short", time.Since(start))
			}
			if !strings.Contains(r.Summary, "must be owner of table") {
				t.Errorf("summary lacks the log line: %s", r.Summary)
			}
			if !strings.Contains(r.Summary, "Connect did not answer") {
				t.Errorf("summary does not say Connect did not answer: %s", r.Summary)
			}
			found := false
			for _, n := range seen {
				if n == "pg" {
					found = true
				}
			}
			if !found {
				t.Errorf("pipeline topic.prefix %q not searched, names: %v", "pg", seen)
			}
		})
	}
}

// Q4 R11: an erroring Config on an explicit connector also gets the fallback.
func TestConnectWorkerLogConfigErrorFallsBackToTopicPrefix(t *testing.T) {
	var seen []string
	d, _ := scopeFakes(t)
	d.Connect = &fakeConnect{
		listFn:   func(context.Context) ([]string, error) { return []string{"pg-orders-connector"}, nil },
		configFn: func(context.Context, string) (map[string]string, error) { return nil, errors.New("boom") },
	}
	d.ConnectLog = ctxCheckingReader(&seen)
	d.Now = func() time.Time { return logNow }
	r, err := scopeTool(t, d, "connect_worker_log").Run(roomCtx(), json.RawMessage(`{"connector":"pg-orders-connector"}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(seen) != 2 || seen[1] != "pg" {
		t.Errorf("names searched = %v, want connector then pg", seen)
	}
	if !strings.Contains(r.Summary, "Connect did not answer") {
		t.Errorf("summary: %s", r.Summary)
	}
}
