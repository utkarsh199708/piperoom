//go:build integration

package kafka

import (
	"context"
	"testing"
	"time"
)

// TestConnectClusterIntegration reads the dev stack's Connect worker group
// (GROUP_ID piperoom-connect) and its _connect_* storage topics.
func TestConnectClusterIntegration(t *testing.T) {
	c := newClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	groups, err := c.ConnectWorkerGroups(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, g := range groups {
		if g.GroupID != "piperoom-connect" {
			continue
		}
		found = true
		if len(g.Workers) == 0 || g.Workers[0].URL == "" {
			t.Errorf("dev Connect group without a worker URL: %+v", g)
		}
	}
	if !found {
		t.Fatalf("piperoom-connect not among %+v", groups)
	}
	topics, err := c.InternalTopics(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	roles := map[string]string{}
	for _, it := range topics {
		roles[it.Name] = it.Role
		if it.Name == "_connect_configs" && (it.Partitions != 1 || it.CleanupPolicy != "compact") {
			t.Errorf("dev config topic: %+v", it)
		}
	}
	if roles["_connect_configs"] != InternalRoleConfig || roles["_connect_offsets"] != InternalRoleOffset || roles["_connect_status"] != InternalRoleStatus {
		t.Errorf("roles = %v", roles)
	}
}
