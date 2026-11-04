package actions

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// The Kubernetes API URL of a pipeline's connections: (the Strimzi read
// tools' endpoint) is not a host a created connector may name, although it
// is a literal value like the pipeline's other host settings.
func TestCreateConnectorHostGuardSkipsKubernetesAPI(t *testing.T) {
	f := newLifecycleFake()
	f.names = nil
	a := createAction(t, f)
	p := testPipeline()
	p.Connections = owners.Connections{"kubernetes_api_url": {Value: "https://kube-api.example.com:6443"}}
	ctx := tools.WithPipeline(context.Background(), p)
	in, _ := json.Marshal(map[string]any{"connector": "pg-orders-connector", "config": map[string]string{
		"connector.class":   "io.debezium.connector.postgresql.PostgresConnector",
		"database.hostname": "kube-api.example.com",
	}})
	if err := a.Validate(ctx, in); !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("the Kubernetes API host was allowed: %v", err)
	}
}
