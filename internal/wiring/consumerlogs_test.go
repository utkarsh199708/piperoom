package wiring

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"piperoom/internal/clients/workerlogs"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/tools/logtools"
)

func consumerLogOwners(selector string) *owners.Owners {
	p := owners.Pipeline{ID: "orders", Connectors: []string{"orders-cdc"}, ConsumerGroups: []string{"order-sync"}}
	if selector != "" {
		p.ConsumerLogs = &owners.ConsumerLogsSpec{Selector: selector}
	}
	return &owners.Owners{Pipelines: []owners.Pipeline{p}}
}

func runConsumerLogs(t *testing.T, w *Wired) tools.Result {
	t.Helper()
	reg := registerQ3(t, w)
	tl, ok := reg.Get(logtools.ConsumerName)
	if !ok {
		t.Fatal("consumer_logs is not registered")
	}
	res, err := tl.Run(tools.WithPipelineID(context.Background(), "orders"), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("consumer_logs: %v", err)
	}
	return res
}

// TestConsumerLogsNotConfigured (H2): a CDC set registers consumer_logs
// whatever is configured; without a Loki, or with a typed nil client, it
// answers "not configured" instead of failing or going missing.
func TestConsumerLogsNotConfigured(t *testing.T) {
	if res := runConsumerLogs(t, &Wired{Owners: consumerLogOwners(`{service="devconsumer"}`)}); !strings.Contains(res.Summary, "not configured on this server") {
		t.Errorf("no Loki: %q", res.Summary)
	}
	var typedNil *workerlogs.Client
	if res := runConsumerLogs(t, &Wired{Owners: consumerLogOwners(`{service="devconsumer"}`), WorkerLogs: typedNil}); !strings.Contains(res.Summary, "not configured on this server") {
		t.Errorf("typed nil client: %q", res.Summary)
	}
}

// TestConsumerLogsShareWorkerLoki: with the worker log client set, consumer
// logs read the same Loki with the same token, over the pipeline's own
// selector; a pipeline without one is not configured.
func TestConsumerLogsShareWorkerLoki(t *testing.T) {
	var queries, auths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		queries = append(queries, r.URL.Query().Get("query"))
		auths = append(auths, r.Header.Get("Authorization"))
		fmt.Fprint(w, `{"status":"success","data":{"resultType":"streams","result":[]}}`)
	}))
	defer srv.Close()
	c, err := workerlogs.New(srv.URL, `{service="connect"}`, "tok")
	if err != nil {
		t.Fatal(err)
	}
	res := runConsumerLogs(t, &Wired{Owners: consumerLogOwners(`{service="devconsumer"}`), WorkerLogs: c})
	if len(queries) != 1 || queries[0] != `{service="devconsumer"}` || auths[0] != "Bearer tok" || !strings.Contains(res.Summary, "No matching lines") {
		t.Fatalf("queries %q auth %q summary %q", queries, auths, res.Summary)
	}
	if res := runConsumerLogs(t, &Wired{Owners: consumerLogOwners(""), WorkerLogs: c}); !strings.Contains(res.Summary, "not configured for pipeline orders") || len(queries) != 1 {
		t.Errorf("no selector: %q, %d reads", res.Summary, len(queries))
	}
}

type consumerSpy struct{ selectors []string }

func (s *consumerSpy) StreamLogs(context.Context, workerlogs.StreamQuery) ([]workerlogs.LogLine, error) {
	return nil, nil
}

// TestConsumerLogsReadersOverride: Wired.ConsumerLogs, when set, is the
// source consumer_logs opens, given the pipeline's selector.
func TestConsumerLogsReadersOverride(t *testing.T) {
	spy := &consumerSpy{}
	readers := func(sel string) (logtools.StreamReader, error) {
		spy.selectors = append(spy.selectors, sel)
		return spy, nil
	}
	runConsumerLogs(t, &Wired{Owners: consumerLogOwners(`{app="order-sync"}`), WorkerLogs: &workerLogsSpy{}, ConsumerLogs: readers})
	if len(spy.selectors) != 1 || spy.selectors[0] != `{app="order-sync"}` {
		t.Fatalf("selectors = %q", spy.selectors)
	}
}
