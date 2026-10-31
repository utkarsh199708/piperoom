//go:build manual

package mskconnect

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	awsconfig "github.com/aws/aws-sdk-go-v2/config"

	"piperoom/internal/setup/targets/managed/replay"
)

// TestManualSandbox re-records the MSK Connect cassettes on the sandbox
// account (docs/design/setup-managed.md §6) with the SDK's default
// credential chain (an SSO profile after `aws sso login`). It writes scrubbed
// cassettes to PIPEROOM_MSK_SANDBOX_OUT, never to testdata/ directly: a
// lead compares them with the hand-written ones and replaces those in a
// dated PR.
//
//	AWS_PROFILE=piperoom-sandbox PIPEROOM_MSK_SANDBOX_REGION=eu-west-1 \
//	PIPEROOM_MSK_SANDBOX_CLUSTER_ARN=arn:aws:kafka:... \
//	PIPEROOM_MSK_SANDBOX_OUT=/tmp/msk-cassettes \
//	  go test -tags manual -run TestManualSandbox ./internal/clients/mskconnect/
//
// With PIPEROOM_MSK_SANDBOX_PLUGIN_ARN, _ROLE_ARN, _SUBNETS (comma list),
// _GROUP and _LOG_GROUP also set, it creates a connector it_<random> with a
// bare config and deletes it again (the sandbox teardown script removes it
// if the test stops between the two).
func TestManualSandbox(t *testing.T) {
	region := os.Getenv("PIPEROOM_MSK_SANDBOX_REGION")
	cluster := os.Getenv("PIPEROOM_MSK_SANDBOX_CLUSTER_ARN")
	out := os.Getenv("PIPEROOM_MSK_SANDBOX_OUT")
	if region == "" || cluster == "" || out == "" {
		t.Skip("set PIPEROOM_MSK_SANDBOX_REGION, _CLUSTER_ARN and _OUT")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	record := func(name string, fn func(c *Client)) {
		rec := replay.NewRecorder(http.DefaultTransport, Scrub, replay.ProviderAWS, name, "sandbox run "+time.Now().UTC().Format("2006-01-02"), time.Now())
		cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region), awsconfig.WithHTTPClient(&http.Client{Transport: rec, Timeout: CallTimeout}))
		if err != nil {
			t.Fatalf("loading the AWS config: %v", err)
		}
		c, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		fn(c)
		if err := replay.Save(rec.Cassette(), filepath.Join(out, name+".json")); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
	}
	record("live_identity", func(c *Client) {
		if _, err := c.Identity(ctx); err != nil {
			t.Errorf("Identity: %v", err)
		}
	})
	record("live_describe_cluster", func(c *Client) {
		cl, err := c.DescribeCluster(ctx, cluster)
		if err != nil {
			t.Errorf("DescribeCluster: %v", err)
			return
		}
		if _, err := c.DescribeNetwork(ctx, cl.Subnets, cl.SecurityGroups); err != nil {
			t.Errorf("DescribeNetwork: %v", err)
		}
	})
	record("live_lists", func(c *Client) {
		if _, err := c.ListPlugins(ctx, ""); err != nil {
			t.Errorf("ListPlugins: %v", err)
		}
		if _, err := c.ListConnectors(ctx); err != nil {
			t.Errorf("ListConnectors: %v", err)
		}
	})
	plugin, role := os.Getenv("PIPEROOM_MSK_SANDBOX_PLUGIN_ARN"), os.Getenv("PIPEROOM_MSK_SANDBOX_ROLE_ARN")
	subnets, group := os.Getenv("PIPEROOM_MSK_SANDBOX_SUBNETS"), os.Getenv("PIPEROOM_MSK_SANDBOX_GROUP")
	if plugin == "" || role == "" || subnets == "" || group == "" {
		t.Log("connector create/delete skipped: plugin, role, subnets or group not set")
		return
	}
	var b [4]byte
	_, _ = rand.Read(b[:])
	name := "it_" + hex.EncodeToString(b[:])
	record("live_create_delete_connector", func(c *Client) {
		cl, err := c.DescribeCluster(ctx, cluster)
		if err != nil {
			t.Fatalf("DescribeCluster: %v", err)
		}
		created, err := c.CreateConnector(ctx, ConnectorSpec{
			Name: name, KafkaConnectVersion: "2.7.1", BootstrapServers: cl.BootstrapIAM,
			Subnets: splitComma(subnets), SecurityGroups: []string{group}, PluginArn: plugin, PluginRevision: 1,
			ServiceRoleArn: role, LogGroup: os.Getenv("PIPEROOM_MSK_SANDBOX_LOG_GROUP"),
			Config: map[string]string{"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "tasks.max": "1"},
			Tags:   map[string]string{"piperoom-sandbox": "true"},
		})
		if err != nil {
			t.Fatalf("CreateConnector: %v", err)
		}
		if _, err := c.DescribeConnector(ctx, created.Arn); err != nil {
			t.Errorf("DescribeConnector: %v", err)
		}
		if _, err := c.DeleteConnector(ctx, created.Arn, ""); err != nil {
			t.Errorf("DeleteConnector: %v (the teardown script removes %s)", err, name)
		}
	})
}

func splitComma(s string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(s); i++ {
		if i == len(s) || s[i] == ',' {
			if i > start {
				out = append(out, s[start:i])
			}
			start = i + 1
		}
	}
	return out
}
