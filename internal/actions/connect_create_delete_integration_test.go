//go:build integration

package actions

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients/connect"
	"piperoom/internal/owners"
	"piperoom/internal/testenv"
	"piperoom/internal/tools"
)

// TestConnectCreateDeleteActionsIntegration registers and deletes a unique
// it_ MirrorHeartbeatConnector through the actions, then re-registers it as a
// delete-then-create proposal (replace: true).
func TestConnectCreateDeleteActionsIntegration(t *testing.T) {
	cfgCfg := testenv.Require(t)
	c := connect.New(cfgCfg.ConnectURL, &http.Client{Timeout: 10 * time.Second})
	bg := tools.WithUnscoped(context.Background())
	name := testenv.Name("cc")
	connCfg := map[string]string{
		"connector.class":                     "org.apache.kafka.connect.mirror.MirrorHeartbeatConnector",
		"tasks.max":                           "1",
		"source.cluster.alias":                name + "_src",
		"target.cluster.alias":                name + "_dst",
		"target.cluster.bootstrap.servers":    "kafka:9092",
		"heartbeats.topic.replication.factor": "1",
		"emit.heartbeats.interval.seconds":    "3600",
		"key.converter":                       "org.apache.kafka.connect.converters.ByteArrayConverter",
		"value.converter":                     "org.apache.kafka.connect.converters.ByteArrayConverter",
	}
	t.Cleanup(func() { deleteConnectorHTTP(t, cfgCfg.ConnectURL, name) })

	pl := owners.Pipeline{ID: name, TopicPrefix: name, Connectors: []string{name}}
	ctx := tools.WithPipeline(context.Background(), pl)
	reg := NewRegistry()
	if err := RegisterLifecycle(reg, LifecycleDeps{Connect: c}); err != nil {
		t.Fatal(err)
	}
	create := mustGet(t, reg, CreateConnectorName)
	del := mustGet(t, reg, DeleteConnectorName)

	params, _ := json.Marshal(map[string]any{"connector": name, "config": connCfg})
	if err := create.Validate(ctx, params); err != nil {
		t.Fatalf("create Validate: %v", err)
	}
	res, err := create.(Preview).Preview(ctx, params)
	if err != nil {
		t.Fatalf("create Preview (Connect's validate refuses errors): %v", err)
	}
	t.Logf("create preview: dry run %+v", res.DryRun)
	if res.DryRun == nil || !res.DryRun.OK {
		t.Fatalf("create Preview dry run = %+v", res.DryRun)
	}
	out, err := create.Execute(ctx, params)
	if err != nil {
		t.Fatalf("create Execute: %v", err)
	}
	t.Logf("create: %s", out)
	testenv.WaitFor(t, 60*time.Second, func() (bool, error) {
		st, err := c.Status(bg, name)
		return err == nil && st.State == "RUNNING", nil
	})

	// The name is registered now: a plain create is refused.
	if err := create.Validate(ctx, params); !errors.Is(err, connect.ErrConnectorExists) {
		t.Fatalf("create of a registered name: %v", err)
	}

	if err := del.Validate(ctx, json.RawMessage(`{"connector":"`+name+`"}`)); err != nil {
		t.Fatalf("delete Validate: %v", err)
	}
	dres, err := del.(Preview).Preview(ctx, json.RawMessage(`{"connector":"`+name+`"}`))
	if err != nil {
		t.Fatalf("delete Preview: %v", err)
	}
	if !strings.Contains(dres.Expected, "stored offsets under the name") {
		t.Fatalf("delete preview = %+v", dres)
	}
	out, err = del.Execute(ctx, json.RawMessage(`{"connector":"`+name+`"}`))
	if err != nil {
		t.Fatalf("delete Execute: %v", err)
	}
	t.Logf("delete: %s", out)

	// Re-register is delete then create in one proposal: the create step carries
	// replace: true and runs after the delete.
	replace, _ := json.Marshal(map[string]any{"connector": name, "config": connCfg, "replace": true})
	if err := create.Validate(ctx, replace); err != nil {
		t.Fatalf("re-register Validate: %v", err)
	}
	out, err = create.Execute(ctx, replace)
	if err != nil {
		t.Fatalf("re-register Execute: %v", err)
	}
	t.Logf("re-register: %s", out)
	if _, err := del.Execute(ctx, json.RawMessage(`{"connector":"`+name+`"}`)); err != nil {
		t.Fatalf("delete after re-register: %v", err)
	}
}

// deleteConnectorHTTP deletes a leftover it_ connector without failing the
// test. Connect can hold a delete through a rebalance longer than the HTTP
// client's 10 s timeout, so it checks the list and retries.
func deleteConnectorHTTP(t *testing.T, connectURL, name string) {
	t.Helper()
	hc := &http.Client{Timeout: 15 * time.Second}
	for attempt := 0; attempt < 3; attempt++ {
		req, err := http.NewRequest(http.MethodDelete, connectURL+"/connectors/"+name, nil)
		if err != nil {
			return
		}
		resp, err := hc.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			resp.Body.Close()
			if resp.StatusCode < 300 || resp.StatusCode == http.StatusNotFound {
				return
			}
			t.Logf("cleanup delete %s: status %d", name, resp.StatusCode)
		} else {
			t.Logf("cleanup delete %s: %v", name, err)
		}
		// A timed-out request may still have deleted it: stop when it is gone.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		req, _ = http.NewRequestWithContext(ctx, http.MethodGet, connectURL+"/connectors", nil)
		resp, err = hc.Do(req)
		if err == nil {
			var names []string
			_ = json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&names)
			resp.Body.Close()
			if !slices.Contains(names, name) {
				cancel()
				return
			}
		}
		cancel()
		time.Sleep(time.Second)
	}
}
