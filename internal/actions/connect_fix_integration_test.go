//go:build integration

package actions

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"piperoom/internal/clients/connect"
	"piperoom/internal/owners"
	"piperoom/internal/testenv"
	"piperoom/internal/tools"
)

// itHeartbeatConnector creates an it_ MirrorHeartbeatConnector (it ships with
// Kafka Connect and needs no source database, unlike a Debezium connector with
// its own replication slot) and deletes it at cleanup. It returns its name.
func itHeartbeatConnector(t *testing.T, connectURL string) string {
	t.Helper()
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	name := "it_" + hex.EncodeToString(b)
	cfg := map[string]string{
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
	body, _ := json.Marshal(cfg)
	connectHTTP(t, http.MethodPut, connectURL+"/connectors/"+name+"/config", body)
	t.Cleanup(func() { connectHTTP(t, http.MethodDelete, connectURL+"/connectors/"+name, nil) })
	return name
}

// connectHTTP sends one request to Connect and fails the test on a non-2xx answer.
func connectHTTP(t *testing.T, method, url string, body []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, url, err)
	}
	defer resp.Body.Close()
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		t.Fatalf("%s %s: status %d: %s", method, url, resp.StatusCode, msg)
	}
}

// TestPauseAndUpdateConfigIntegration pauses an it_ connector and changes two
// of its config keys in one step, through the actions and the real client,
// scoped to a pipeline that owns only that connector.
func TestPauseAndUpdateConfigIntegration(t *testing.T) {
	cfg := testenv.Require(t)
	c := connect.New(cfg.ConnectURL, &http.Client{Timeout: 10 * time.Second})
	name := itHeartbeatConnector(t, cfg.ConnectURL)
	ctx := tools.WithPipeline(context.Background(), owners.Pipeline{ID: name, TopicPrefix: name, Connectors: []string{name}})

	testenv.WaitFor(t, 60*time.Second, func() (bool, error) {
		st, err := c.Status(ctx, name)
		if err != nil {
			return false, nil // not registered yet
		}
		return st.State == "RUNNING", nil
	})

	reg := NewRegistry()
	if err := Register(reg, Deps{Connect: c}); err != nil {
		t.Fatal(err)
	}
	pause := mustGet(t, reg, "connect_pause_connector")
	params := json.RawMessage(`{"connector":"` + name + `"}`)
	if err := pause.Validate(ctx, params); err != nil {
		t.Fatalf("pause Validate: %v", err)
	}
	out, err := pause.Execute(ctx, params)
	if err != nil {
		t.Fatalf("pause Execute: %v", err)
	}
	t.Logf("pause: %s", out)
	testenv.WaitFor(t, 30*time.Second, func() (bool, error) {
		st, err := c.Status(ctx, name)
		return err == nil && st.State == "PAUSED", err
	})

	update := mustGet(t, reg, "connect_update_connector_config")
	two := json.RawMessage(`{"connector":"` + name + `","changes":{"tombstones.on.delete":"false","decimal.handling.mode":"string"}}`)
	if err := update.Validate(ctx, two); err != nil {
		t.Fatalf("update Validate: %v", err)
	}
	out, err = update.Execute(ctx, two)
	if err != nil {
		t.Fatalf("update Execute: %v", err)
	}
	t.Logf("update: %s", out)
	got, err := c.Config(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	if got["tombstones.on.delete"] != "false" || got["decimal.handling.mode"] != "string" || got["emit.heartbeats.interval.seconds"] != "3600" {
		t.Fatalf("config after update: %v", got)
	}
}
