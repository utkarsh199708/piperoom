//go:build integration

package testenv_test

// The setup stack Testenv.Setup brings up (U1-15): a rendered Compose project
// with Kafka, Connect and the source Postgres, the Debezium Postgres plugin
// installed from its pinned archive, and Connect listing the connector class.
// It then proves the S1 guard on the real project: a compose file with a
// denied field is refused before compose_up runs docker.

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"piperoom/internal/setup/targets/compose"
	"piperoom/internal/testenv"
)

func TestSetupStackComesUp(t *testing.T) {
	st := testenv.Setup(t)

	// The plugin the setup run installed is the one the worker lists: the
	// stack is the environment a ConnectorConfig is written for.
	classes, err := connectorPlugins(st.ConnectURL)
	if err != nil {
		t.Fatalf("read /connector-plugins: %v", err)
	}
	if !contains(classes, "io.debezium.connector.postgresql.PostgresConnector") {
		t.Fatalf("Connect lists %v, want the Debezium Postgres connector", classes)
	}

	// compose_ps answers for the three services.
	ps := testenv.Data(t, st.Exec(t, time.Minute, "compose_ps", compose.StackParams{}))
	if count, _ := ps["count"].(float64); count < 3 {
		t.Errorf("compose_ps count = %v, want kafka, connect and postgres", ps["count"])
	}

	// TC2 on the real project: one denied field on disk, then compose_up is
	// refused before any container is touched. The good file is put back so
	// the cleanup's compose_down reads a valid stack.
	good, err := os.ReadFile(filepath.Join(st.Dir, compose.DefaultFile))
	if err != nil {
		t.Fatalf("read %s: %v", compose.DefaultFile, err)
	}
	bad := withPrivileged(t, good)
	if err := os.WriteFile(filepath.Join(st.Dir, compose.DefaultFile), bad, 0o644); err != nil {
		t.Fatalf("write the edited compose file: %v", err)
	}
	t.Cleanup(func() {
		if err := os.WriteFile(filepath.Join(st.Dir, compose.DefaultFile), good, 0o644); err != nil {
			t.Errorf("restore %s: %v", compose.DefaultFile, err)
		}
	})
	op, ok := st.Registry.Get("compose_up")
	if !ok {
		t.Fatal("compose_up is not registered")
	}
	raw := []byte(`{}`)
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if _, err := op.Execute(ctx, raw, st.Env); err == nil {
		t.Fatal("compose_up accepted a compose file with privileged: true")
	} else if rule := compose.RuleOf(err); rule != compose.RulePrivilege {
		t.Fatalf("refusal rule = %q, want %q (%v)", rule, compose.RulePrivilege, err)
	}
}

// withPrivileged adds privileged: true to the connect service of a rendered
// document.
func withPrivileged(t *testing.T, doc []byte) []byte {
	t.Helper()
	var root map[string]any
	if err := yaml.Unmarshal(doc, &root); err != nil {
		t.Fatalf("parse the render: %v", err)
	}
	services, ok := root["services"].(map[string]any)
	if !ok {
		t.Fatalf("the render has no services: %v", root)
	}
	connect, ok := services["connect"].(map[string]any)
	if !ok {
		t.Fatalf("the render has no connect service: %v", services)
	}
	connect["privileged"] = true
	bad, err := yaml.Marshal(root)
	if err != nil {
		t.Fatalf("marshal the edited render: %v", err)
	}
	return bad
}

// connectorPlugins lists Connect's plugin classes.
func connectorPlugins(base string) ([]string, error) {
	// testenv.connectorPluginClasses is unexported; this test reads the same
	// endpoint through its own small client.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/connector-plugins", nil)
	if err != nil {
		return nil, err
	}
	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var plugins []struct {
		Class string `json:"class"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&plugins); err != nil {
		return nil, err
	}
	out := make([]string, 0, len(plugins))
	for _, p := range plugins {
		out = append(out, p.Class)
	}
	return out, nil
}

func contains(list []string, want string) bool {
	for _, v := range list {
		if strings.EqualFold(v, want) {
			return true
		}
	}
	return false
}
