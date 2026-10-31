package mskconnect

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"

	"piperoom/internal/actions"
	"piperoom/internal/setup/targets/managed/replay"
)

// The fixed identifiers every MSK cassette uses: the scrubber's own
// placeholders, so a recording and a replay name the same resources.
const (
	tAccount   = "000000000000"
	tRegion    = "us-east-1"
	tCluster   = "arn:aws:kafka:us-east-1:000000000000:cluster/demo/0a1b2c3d-4e5f-6789-abcd-ef0123456789-1"
	tConnector = "arn:aws:kafkaconnect:us-east-1:000000000000:connector/it_orders/0a1b2c3d-4e5f-6789-abcd-ef0123456789-2"
	tPlugin    = "arn:aws:kafkaconnect:us-east-1:000000000000:custom-plugin/debezium-pg/9f8e7d6c-5b4a-3210-fedc-ba9876543210-3"
	tWorker    = "arn:aws:kafkaconnect:us-east-1:000000000000:worker-configuration/piperoom-orders/11112222-3333-4444-5555-66667777aaaa-4"
	tRole      = "arn:aws:iam::000000000000:role/PiperoomMSKConnect"
	tBucketARN = "arn:aws:s3:::piperoom-bucket"
	tBucket    = "piperoom-bucket"
	tKey       = "plugins/debezium-connector-postgres-2.7.3.zip"
	tSubnetA   = "subnet-0a1b2c3d4e5f60001"
	tSubnetB   = "subnet-0a1b2c3d4e5f60002"
	tGroup     = "sg-0a1b2c3d4e5f60001"
	tVPC       = "vpc-0a1b2c3d4e5f60001"
	tBootstrap = "scrubbed.amazonaws.com:9098,scrubbed.amazonaws.com:9098"
	tLogGroup  = "/piperoom/it_orders"
)

// Scrub is the scrubber every MSK cassette is recorded and replayed with.
var Scrub = replay.Scrubber{}

// noBackoff makes the SDK's retries immediate in tests.
type noBackoff struct{}

func (noBackoff) BackoffDelay(int, error) (time.Duration, error) { return 0, nil }

// testConfig is an aws.Config over rt: no credentials (nothing is signed),
// immediate retries, no retry quota.
func testConfig(rt http.RoundTripper) aws.Config {
	return aws.Config{
		Region:      tRegion,
		Credentials: aws.AnonymousCredentials{},
		HTTPClient:  &http.Client{Transport: rt, Timeout: 5 * time.Second},
		Retryer: func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) {
				o.Backoff = noBackoff{}
				o.RateLimiter = ratelimit.None
			})
		},
	}
}

// Response helpers for the hand-written shapes.
func kcJSON(status int, body string) replay.Response {
	return replay.Response{Status: status, Headers: map[string]string{"Content-Type": "application/json"}, Body: body}
}

func kcErr(status int, code, msg string) replay.Response {
	return replay.Response{Status: status, Headers: map[string]string{"Content-Type": "application/json", "X-Amzn-Errortype": code}, Body: `{"message":"` + msg + `"}`}
}

func xmlOK(body string) replay.Response {
	return replay.Response{Status: 200, Headers: map[string]string{"Content-Type": "text/xml"}, Body: body}
}

func iamErr(status int, code, msg string) replay.Response {
	return replay.Response{Status: status, Headers: map[string]string{"Content-Type": "text/xml"},
		Body: `<ErrorResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/"><Error><Type>Sender</Type><Code>` + code + `</Code><Message>` + msg + `</Message></Error><RequestId>00000000-0000-0000-0000-000000000000</RequestId></ErrorResponse>`}
}

func ec2Err(status int, code, msg string) replay.Response {
	return replay.Response{Status: status, Headers: map[string]string{"Content-Type": "text/xml"},
		Body: `<Response><Errors><Error><Code>` + code + `</Code><Message>` + msg + `</Message></Error></Errors><RequestID>00000000-0000-0000-0000-000000000000</RequestID></Response>`}
}

const describeConnectorBody = `{"connectorArn":"` + tConnector + `","connectorName":"it_orders","connectorState":"%STATE%","currentVersion":"K3AEGXETSR30VB","kafkaConnectVersion":"2.7.1",` +
	`"connectorConfiguration":{"connector.class":"io.debezium.connector.postgresql.PostgresConnector","database.hostname":"orders.example.internal","database.user":"debezium","database.password":"pw-live-1","topic.prefix":"orders","tasks.max":"1"},` +
	`"serviceExecutionRoleArn":"` + tRole + `","plugins":[{"customPlugin":{"customPluginArn":"` + tPlugin + `","revision":1}}],` +
	`"workerConfiguration":{"workerConfigurationArn":"` + tWorker + `","revision":1},` +
	`"logDelivery":{"workerLogDelivery":{"cloudWatchLogs":{"enabled":true,"logGroup":"` + tLogGroup + `"}}},` +
	`"kafkaCluster":{"apacheKafkaCluster":{"bootstrapServers":"` + tBootstrap + `","vpc":{"subnets":["` + tSubnetA + `","` + tSubnetB + `"],"securityGroups":["` + tGroup + `"]}}}%DESC%}`

func connectorBody(state, code, msg string) string {
	b := strings.Replace(describeConnectorBody, "%STATE%", state, 1)
	desc := ""
	if code != "" {
		desc = `,"stateDescription":{"code":"` + code + `","message":"` + msg + `"}`
	}
	return strings.Replace(b, "%DESC%", desc, 1)
}

const listConnectorsBody = `{"connectors":[{"connectorArn":"` + tConnector + `","connectorName":"it_orders","connectorState":"RUNNING"},` +
	`{"connectorArn":"arn:aws:kafkaconnect:us-east-1:000000000000:connector/it_orders_old/0a1b2c3d-4e5f-6789-abcd-ef0123456789-9","connectorName":"it_orders_old","connectorState":"RUNNING"}]}`

func plugin(state, desc string) string {
	return `{"customPluginArn":"` + tPlugin + `","name":"debezium-pg","customPluginState":"` + state + `","latestRevision":{"revision":1,"contentType":"ZIP"}` + desc + `}`
}

// mskCase is one cassette: the hand-written responses (used only by the
// generator) and the calls with their assertions (run by both).
type mskCase struct {
	name      string
	responses []replay.Response
	run       func(t *testing.T, ctx context.Context, c *Client)
}

func wantErr(t *testing.T, err, class error) {
	t.Helper()
	if !errors.Is(err, class) {
		t.Fatalf("err = %v, want %v", err, class)
	}
}

func connectorSpec() ConnectorSpec {
	return ConnectorSpec{
		Name: "it_orders", KafkaConnectVersion: "2.7.1", BootstrapServers: tBootstrap,
		Subnets: []string{tSubnetA, tSubnetB}, SecurityGroups: []string{tGroup},
		PluginArn: tPlugin, PluginRevision: 1, WorkerConfigArn: tWorker, WorkerConfigRevision: 1,
		ServiceRoleArn: tRole, LogGroup: tLogGroup,
		Config: map[string]string{
			"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "database.hostname": "orders.example.internal",
			"database.user": "debezium", "database.password": "pw-live-1", "topic.prefix": "orders", "tasks.max": "1",
		},
		Tags: map[string]string{"piperoom-room": "room-1", "piperoom-pipeline": "orders"},
	}
}

var mskCases = []mskCase{
	{
		name:      "identity_ok",
		responses: []replay.Response{xmlOK(`<GetCallerIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><GetCallerIdentityResult><Arn>arn:aws:sts::000000000000:assumed-role/PiperoomSetup/piperoom-runner</Arn><UserId>AROAEXAMPLEROLE:piperoom-runner</UserId><Account>000000000000</Account></GetCallerIdentityResult><ResponseMetadata><RequestId>00000000-0000-0000-0000-000000000000</RequestId></ResponseMetadata></GetCallerIdentityResponse>`)},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			id, err := c.Identity(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if id.Account != tAccount || id.Principal != "PiperoomSetup" {
				t.Errorf("Identity = %+v", id)
			}
		},
	},
	{
		name: "describe_cluster_ok",
		responses: []replay.Response{
			kcJSON(200, `{"clusterInfo":{"clusterArn":"`+tCluster+`","clusterName":"demo","clusterType":"PROVISIONED","state":"ACTIVE","provisioned":{"brokerNodeGroupInfo":{"clientSubnets":["`+tSubnetA+`","`+tSubnetB+`"],"securityGroups":["`+tGroup+`"],"instanceType":"kafka.m5.large"},"currentBrokerSoftwareInfo":{"kafkaVersion":"3.6.0"},"numberOfBrokerNodes":2}}}`),
			kcJSON(200, `{"bootstrapBrokerStringSaslIam":"b-1.demo.abc123.c2.kafka.us-east-1.amazonaws.com:9098,b-2.demo.abc123.c2.kafka.us-east-1.amazonaws.com:9098","bootstrapBrokerStringTls":"b-1.demo.abc123.c2.kafka.us-east-1.amazonaws.com:9094"}`),
		},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			cl, err := c.DescribeCluster(ctx, tCluster)
			if err != nil {
				t.Fatal(err)
			}
			if cl.State != "ACTIVE" || cl.KafkaVersion != "3.6.0" || len(cl.Subnets) != 2 || cl.SecurityGroups[0] != tGroup {
				t.Errorf("Cluster = %+v", cl)
			}
			if !strings.Contains(cl.BootstrapIAM, ":9098") || cl.BootstrapSCRAM != "" {
				t.Errorf("bootstrap = %q / %q", cl.BootstrapIAM, cl.BootstrapSCRAM)
			}
		},
	},
	{
		name:      "describe_cluster_forbidden",
		responses: []replay.Response{kcErr(403, "ForbiddenException", "User is not authorized to perform kafka:DescribeClusterV2")},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			_, err := c.DescribeCluster(ctx, tCluster)
			wantErr(t, err, ErrForbidden)
			if !strings.Contains(err.Error(), "kafka:DescribeClusterV2") {
				t.Errorf("the API message is lost: %v", err)
			}
		},
	},
	{
		name:      "list_plugins_ok",
		responses: []replay.Response{kcJSON(200, `{"customPlugins":[`+plugin("ACTIVE", "")+`]}`)},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			ps, err := c.ListPlugins(ctx, "debezium")
			if err != nil {
				t.Fatal(err)
			}
			if len(ps) != 1 || ps[0].State != "ACTIVE" || ps[0].Revision != 1 || ps[0].ContentType != "ZIP" {
				t.Errorf("plugins = %+v", ps)
			}
		},
	},
	{
		name: "describe_plugin_creating_then_active",
		responses: []replay.Response{
			kcJSON(200, plugin("CREATING", "")), kcJSON(200, plugin("CREATING", "")), kcJSON(200, plugin("ACTIVE", "")),
		},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			for i, want := range []string{"CREATING", "CREATING", "ACTIVE"} {
				p, err := c.DescribePlugin(ctx, tPlugin)
				if err != nil {
					t.Fatal(err)
				}
				if p.State != want {
					t.Errorf("poll %d: state %s, want %s", i, p.State, want)
				}
			}
		},
	},
	{
		name:      "describe_plugin_create_failed",
		responses: []replay.Response{kcJSON(200, plugin("CREATE_FAILED", `,"stateDescription":{"code":"ACCESS_DENIED","message":"Access denied to S3 object plugins/debezium-connector-postgres-2.7.3.zip"}`))},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			p, err := c.DescribePlugin(ctx, tPlugin)
			if err != nil {
				t.Fatal(err)
			}
			if p.State != "CREATE_FAILED" || p.StateCode != "ACCESS_DENIED" || !strings.Contains(p.StateMessage, "Access denied") {
				t.Errorf("plugin = %+v", p)
			}
		},
	},
	{
		name:      "describe_plugin_not_found",
		responses: []replay.Response{kcErr(404, "NotFoundException", "Custom plugin not found")},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			_, err := c.DescribePlugin(ctx, tPlugin)
			wantErr(t, err, ErrNotFound)
		},
	},
	{
		name:      "status_running_redacted",
		responses: []replay.Response{kcJSON(200, listConnectorsBody), kcJSON(200, connectorBody("RUNNING", "", ""))},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			st, err := c.Status(ctx, "it_orders")
			if err != nil {
				t.Fatal(err)
			}
			if st.Name != "it_orders" || st.State != "RUNNING" || len(st.Tasks) != 0 {
				t.Errorf("Status = %+v", st)
			}
		},
	},
	{
		name:      "config_redacted",
		responses: []replay.Response{kcJSON(200, listConnectorsBody), kcJSON(200, connectorBody("RUNNING", "", ""))},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			cfg, err := c.Config(ctx, "it_orders")
			if err != nil {
				t.Fatal(err)
			}
			if cfg["database.password"] != actions.RedactedValue || cfg["database.user"] != "debezium" {
				t.Errorf("Config = %v", cfg)
			}
		},
	},
	{
		name:      "list_connectors_ok",
		responses: []replay.Response{kcJSON(200, listConnectorsBody)},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			names, err := c.ListConnectors(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Join(names, ",") != "it_orders,it_orders_old" {
				t.Errorf("ListConnectors = %v", names)
			}
		},
	},
	{
		name:      "find_connector_missing",
		responses: []replay.Response{kcJSON(200, `{"connectors":[]}`)},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			_, err := c.FindConnector(ctx, "it_none")
			wantErr(t, err, ErrNotFound)
		},
	},
	{
		name: "describe_connector_creating_then_running",
		responses: []replay.Response{
			kcJSON(200, connectorBody("CREATING", "", "")), kcJSON(200, connectorBody("CREATING", "", "")), kcJSON(200, connectorBody("RUNNING", "", "")),
		},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			for i, want := range []string{"CREATING", "CREATING", "RUNNING"} {
				cn, err := c.DescribeConnector(ctx, tConnector)
				if err != nil {
					t.Fatal(err)
				}
				if cn.State != want {
					t.Errorf("poll %d: %s, want %s", i, cn.State, want)
				}
				if cn.Config["database.password"] != actions.RedactedValue {
					t.Errorf("the password left the client: %q", cn.Config["database.password"])
				}
			}
		},
	},
	{
		name: "describe_connector_failed",
		responses: []replay.Response{kcJSON(200, connectorBody("FAILED", "UNKNOWN",
			"There is an issue with the connector. Code: UnknownError.Unknown. Message: The last operation failed. Retry the operation. org.apache.kafka.common.errors.TimeoutException: Timed out waiting for a node assignment."))},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			cn, err := c.DescribeConnector(ctx, tConnector)
			if err != nil {
				t.Fatal(err)
			}
			if cn.State != "FAILED" || !strings.Contains(cn.StateMessage, "TimeoutException") || cn.LogGroup != tLogGroup || len(cn.Subnets) != 2 {
				t.Errorf("connector = %+v", cn)
			}
		},
	},
	{
		name:      "describe_connector_not_found",
		responses: []replay.Response{kcErr(404, "NotFoundException", "Connector not found")},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			_, err := c.DescribeConnector(ctx, tConnector)
			wantErr(t, err, ErrNotFound)
		},
	},
	{
		name:      "describe_connector_cut_body",
		responses: []replay.Response{{Status: 200, Headers: map[string]string{"Content-Type": "application/json"}, Body: `{"connectorArn":"` + tConnector + `","connec`, Cut: true}},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			_, err := c.DescribeConnector(ctx, tConnector)
			wantErr(t, err, ErrAWS)
		},
	},
	{
		name:      "connector_tags",
		responses: []replay.Response{kcJSON(200, `{"tags":{"piperoom-room":"room-1","piperoom-pipeline":"orders"}}`)},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			tags, err := c.ConnectorTags(ctx, tConnector)
			if err != nil {
				t.Fatal(err)
			}
			if tags["piperoom-room"] != "room-1" {
				t.Errorf("tags = %v", tags)
			}
		},
	},
	{
		name:      "create_plugin_ok",
		responses: []replay.Response{kcJSON(200, `{"customPluginArn":"`+tPlugin+`","customPluginState":"CREATING","name":"debezium-pg","revision":1}`)},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			got, err := c.CreatePlugin(ctx, PluginSpec{Name: "debezium-pg", ContentType: ContentZIP, BucketArn: tBucketARN, FileKey: tKey, Tags: map[string]string{"piperoom-room": "room-1"}})
			if err != nil {
				t.Fatal(err)
			}
			if got.Arn != tPlugin || got.State != "CREATING" || got.Revision != 1 {
				t.Errorf("Created = %+v", got)
			}
		},
	},
	{
		name:      "create_plugin_bad_request",
		responses: []replay.Response{kcErr(400, "BadRequestException", "The S3 object could not be found")},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			_, err := c.CreatePlugin(ctx, PluginSpec{Name: "debezium-pg", ContentType: ContentZIP, BucketArn: tBucketARN, FileKey: tKey})
			wantErr(t, err, ErrBadRequest)
		},
	},
	{
		name:      "create_worker_config_ok",
		responses: []replay.Response{kcJSON(200, `{"workerConfigurationArn":"`+tWorker+`","name":"piperoom-orders","workerConfigurationState":"ACTIVE","latestRevision":{"revision":1}}`)},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			wc, err := c.CreateWorkerConfig(ctx, WorkerConfigSpec{Name: "piperoom-orders", Properties: "key.converter=org.apache.kafka.connect.json.JsonConverter\nvalue.converter=org.apache.kafka.connect.json.JsonConverter\n"})
			if err != nil {
				t.Fatal(err)
			}
			if wc.Arn != tWorker || wc.Revision != 1 || wc.State != "ACTIVE" {
				t.Errorf("WorkerConfig = %+v", wc)
			}
		},
	},
	{
		name:      "create_connector_ok",
		responses: []replay.Response{kcJSON(200, `{"connectorArn":"`+tConnector+`","connectorName":"it_orders","connectorState":"CREATING"}`)},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			got, err := c.CreateConnector(ctx, connectorSpec())
			if err != nil {
				t.Fatal(err)
			}
			if got.Arn != tConnector || got.State != "CREATING" {
				t.Errorf("Created = %+v", got)
			}
		},
	},
	{
		name:      "create_connector_conflict",
		responses: []replay.Response{kcErr(409, "ConflictException", "A connector with the name it_orders already exists")},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			_, err := c.CreateConnector(ctx, connectorSpec())
			wantErr(t, err, ErrConflict)
			if strings.Contains(err.Error(), "pw-live-1") {
				t.Errorf("the error carries the password: %v", err)
			}
		},
	},
	{
		name: "create_connector_throttled",
		responses: []replay.Response{
			kcErr(429, "TooManyRequestsException", "Rate exceeded"), kcErr(429, "TooManyRequestsException", "Rate exceeded"),
			kcErr(429, "TooManyRequestsException", "Rate exceeded"), kcErr(429, "TooManyRequestsException", "Rate exceeded"),
		},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			_, err := c.CreateConnector(ctx, connectorSpec())
			wantErr(t, err, ErrThrottled)
		},
	},
	{
		name: "create_connector_throttled_then_ok",
		responses: []replay.Response{
			kcErr(429, "TooManyRequestsException", "Rate exceeded"),
			kcJSON(200, `{"connectorArn":"`+tConnector+`","connectorName":"it_orders","connectorState":"CREATING"}`),
		},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			if _, err := c.CreateConnector(ctx, connectorSpec()); err != nil {
				t.Fatalf("a throttle followed by a success: %v", err)
			}
		},
	},
	{
		name:      "delete_connector_ok",
		responses: []replay.Response{kcJSON(200, `{"connectorArn":"`+tConnector+`","connectorState":"DELETING"}`)},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			got, err := c.DeleteConnector(ctx, tConnector, "K3AEGXETSR30VB")
			if err != nil {
				t.Fatal(err)
			}
			if got.State != "DELETING" {
				t.Errorf("Deleted = %+v", got)
			}
		},
	},
	{
		name:      "delete_connector_not_found",
		responses: []replay.Response{kcErr(404, "NotFoundException", "Connector not found")},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			_, err := c.DeleteConnector(ctx, tConnector, "")
			wantErr(t, err, ErrNotFound)
		},
	},
	{
		name: "network_ok",
		responses: []replay.Response{
			xmlOK(`<DescribeSubnetsResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/"><requestId>00000000-0000-0000-0000-000000000000</requestId><subnetSet>` +
				`<item><subnetId>` + tSubnetA + `</subnetId><vpcId>` + tVPC + `</vpcId><cidrBlock>10.0.1.0/24</cidrBlock><availabilityZone>us-east-1a</availabilityZone><state>available</state></item>` +
				`<item><subnetId>` + tSubnetB + `</subnetId><vpcId>` + tVPC + `</vpcId><cidrBlock>10.0.2.0/24</cidrBlock><availabilityZone>us-east-1b</availabilityZone><state>available</state></item>` +
				`</subnetSet></DescribeSubnetsResponse>`),
			xmlOK(`<DescribeSecurityGroupsResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/"><requestId>00000000-0000-0000-0000-000000000000</requestId><securityGroupInfo>` +
				`<item><groupId>` + tGroup + `</groupId><groupName>msk-connect</groupName><vpcId>` + tVPC + `</vpcId>` +
				`<ipPermissions><item><ipProtocol>tcp</ipProtocol><fromPort>9098</fromPort><toPort>9098</toPort><groups><item><groupId>` + tGroup + `</groupId></item></groups></item></ipPermissions>` +
				`<ipPermissionsEgress><item><ipProtocol>-1</ipProtocol><ipRanges><item><cidrIp>0.0.0.0/0</cidrIp></item></ipRanges></item></ipPermissionsEgress></item>` +
				`</securityGroupInfo></DescribeSecurityGroupsResponse>`),
			xmlOK(`<DescribeRouteTablesResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/"><requestId>00000000-0000-0000-0000-000000000000</requestId><routeTableSet>` +
				`<item><routeTableId>rtb-0a1b2c3d4e5f60001</routeTableId><vpcId>` + tVPC + `</vpcId><routeSet><item><destinationCidrBlock>10.0.0.0/16</destinationCidrBlock><gatewayId>local</gatewayId><state>active</state></item></routeSet>` +
				`<associationSet><item><routeTableAssociationId>rtbassoc-0a1b2c3d4e5f60001</routeTableAssociationId><routeTableId>rtb-0a1b2c3d4e5f60001</routeTableId><main>true</main></item></associationSet></item>` +
				`</routeTableSet></DescribeRouteTablesResponse>`),
		},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			n, err := c.DescribeNetwork(ctx, []string{tSubnetA, tSubnetB}, []string{tGroup})
			if err != nil {
				t.Fatal(err)
			}
			if len(n.Subnets) != 2 || n.Subnets[0].CIDR != "10.0.1.0/24" {
				t.Errorf("subnets = %+v", n.Subnets)
			}
			if len(n.SecurityGroups) != 1 || n.SecurityGroups[0].Egress[0].Protocol != "-1" || n.SecurityGroups[0].Ingress[0].Groups[0] != tGroup {
				t.Errorf("groups = %+v", n.SecurityGroups)
			}
			if len(n.RouteTables) != 1 || !n.RouteTables[0].Main || n.RouteTables[0].Routes[0].Target != "local" {
				t.Errorf("route tables = %+v", n.RouteTables)
			}
		},
	},
	{
		name:      "network_forbidden",
		responses: []replay.Response{ec2Err(403, "UnauthorizedOperation", "You are not authorized to perform this operation.")},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			_, err := c.DescribeNetwork(ctx, []string{tSubnetA}, []string{tGroup})
			wantErr(t, err, ErrForbidden)
		},
	},
	{
		name:      "network_subnet_not_found",
		responses: []replay.Response{ec2Err(400, "InvalidSubnetID.NotFound", "The subnet ID 'subnet-0a1b2c3d4e5f60001' does not exist")},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			_, err := c.DescribeNetwork(ctx, []string{tSubnetA}, nil)
			wantErr(t, err, ErrNotFound)
		},
	},
	{
		name: "role_ok",
		responses: []replay.Response{
			xmlOK(`<GetRoleResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/"><GetRoleResult><Role><Path>/</Path><RoleName>PiperoomMSKConnect</RoleName><RoleId>AROAEXAMPLEROLE</RoleId><Arn>` + tRole + `</Arn><CreateDate>2026-10-01T00:00:00Z</CreateDate>` +
				`<AssumeRolePolicyDocument>%7B%22Version%22%3A%222012-10-17%22%2C%22Statement%22%3A%5B%7B%22Effect%22%3A%22Allow%22%2C%22Principal%22%3A%7B%22Service%22%3A%22kafkaconnect.amazonaws.com%22%7D%2C%22Action%22%3A%22sts%3AAssumeRole%22%7D%5D%7D</AssumeRolePolicyDocument>` +
				`</Role></GetRoleResult><ResponseMetadata><RequestId>00000000-0000-0000-0000-000000000000</RequestId></ResponseMetadata></GetRoleResponse>`),
			xmlOK(`<SimulatePrincipalPolicyResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/"><SimulatePrincipalPolicyResult><IsTruncated>false</IsTruncated><EvaluationResults>` +
				`<member><EvalActionName>kafka-cluster:Connect</EvalActionName><EvalResourceName>*</EvalResourceName><EvalDecision>allowed</EvalDecision></member>` +
				`<member><EvalActionName>logs:PutLogEvents</EvalActionName><EvalResourceName>*</EvalResourceName><EvalDecision>implicitDeny</EvalDecision></member>` +
				`</EvaluationResults></SimulatePrincipalPolicyResult><ResponseMetadata><RequestId>00000000-0000-0000-0000-000000000000</RequestId></ResponseMetadata></SimulatePrincipalPolicyResponse>`),
		},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			r, err := c.GetRole(ctx, tRole)
			if err != nil {
				t.Fatal(err)
			}
			if !r.TrustsKafkaConnect || r.TrustProblem != "" || r.Name != "PiperoomMSKConnect" {
				t.Errorf("Role = %+v", r)
			}
			ds, err := c.SimulateRole(ctx, tRole, []string{"kafka-cluster:Connect", "logs:PutLogEvents"}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if len(ds) != 2 || ds[0].Decision != "allowed" || ds[1].Decision != "implicitDeny" {
				t.Errorf("decisions = %+v", ds)
			}
		},
	},
	{
		// GetRole decodes IAM's URL-encoded document and reports what
		// TrustProblem finds: here a second, AWS principal (review L10).
		name: "role_trusted_by_another_account",
		responses: []replay.Response{
			xmlOK(`<GetRoleResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/"><GetRoleResult><Role><Path>/</Path><RoleName>PiperoomMSKConnect</RoleName><RoleId>AROAEXAMPLEROLE</RoleId><Arn>` + tRole + `</Arn><CreateDate>2026-10-01T00:00:00Z</CreateDate>` +
				`<AssumeRolePolicyDocument>` + url.QueryEscape(`{"Statement":[{"Effect":"Allow","Principal":{"Service":"kafkaconnect.amazonaws.com","AWS":"arn:aws:iam::111111111111:root"},"Action":"sts:AssumeRole"}]}`) + `</AssumeRolePolicyDocument>` +
				`</Role></GetRoleResult></GetRoleResponse>`),
		},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			r, err := c.GetRole(ctx, tRole)
			if err != nil {
				t.Fatal(err)
			}
			if !r.TrustsKafkaConnect || r.TrustProblem == "" {
				t.Errorf("Role = %+v, want a trust problem", r)
			}
		},
	},
	{
		// SimulateRole names the resources and reports every
		// resource-specific decision: a grant on the role's own ARN is
		// implicitDeny against "*" and allowed on the ARN (review round 3).
		name: "simulate_on_resources",
		responses: []replay.Response{
			xmlOK(`<SimulatePrincipalPolicyResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/"><SimulatePrincipalPolicyResult><IsTruncated>false</IsTruncated><EvaluationResults>` +
				`<member><EvalActionName>iam:PutRolePolicy</EvalActionName><EvalResourceName>*</EvalResourceName><EvalDecision>implicitDeny</EvalDecision>` +
				`<ResourceSpecificResults><member><EvalResourceName>` + tRole + `</EvalResourceName><EvalResourceDecision>allowed</EvalResourceDecision></member></ResourceSpecificResults></member>` +
				`</EvaluationResults></SimulatePrincipalPolicyResult></SimulatePrincipalPolicyResponse>`),
		},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			ds, err := c.SimulateRole(ctx, tRole, []string{"iam:PutRolePolicy"}, []string{"*", tRole})
			if err != nil {
				t.Fatal(err)
			}
			if len(ds) != 2 || ds[1].Resource != tRole || ds[1].Decision != "allowed" {
				t.Errorf("decisions = %+v", ds)
			}
		},
	},
	{
		// RolePolicies reads the inline and the attached policies.
		name: "role_policies_ok",
		responses: []replay.Response{
			xmlOK(`<ListRolePoliciesResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/"><ListRolePoliciesResult><IsTruncated>false</IsTruncated><PolicyNames><member>connect</member></PolicyNames></ListRolePoliciesResult></ListRolePoliciesResponse>`),
			xmlOK(`<GetRolePolicyResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/"><GetRolePolicyResult><RoleName>PiperoomMSKConnect</RoleName><PolicyName>connect</PolicyName><PolicyDocument>` +
				url.QueryEscape(`{"Statement":[{"Effect":"Allow","Action":"kafka-cluster:Connect","Resource":"*"}]}`) + `</PolicyDocument></GetRolePolicyResult></GetRolePolicyResponse>`),
			xmlOK(`<ListAttachedRolePoliciesResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/"><ListAttachedRolePoliciesResult><IsTruncated>false</IsTruncated><AttachedPolicies><member><PolicyName>hop</PolicyName><PolicyArn>arn:aws:iam::000000000000:policy/hop</PolicyArn></member></AttachedPolicies></ListAttachedRolePoliciesResult></ListAttachedRolePoliciesResponse>`),
			xmlOK(`<GetPolicyResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/"><GetPolicyResult><Policy><PolicyName>hop</PolicyName><Arn>arn:aws:iam::000000000000:policy/hop</Arn><DefaultVersionId>v2</DefaultVersionId></Policy></GetPolicyResult></GetPolicyResponse>`),
			xmlOK(`<GetPolicyVersionResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/"><GetPolicyVersionResult><PolicyVersion><VersionId>v2</VersionId><IsDefaultVersion>true</IsDefaultVersion><Document>` +
				url.QueryEscape(`{"Statement":[{"Effect":"Allow","Action":"sts:AssumeRole","Resource":"arn:aws:iam::000000000000:role/Admin"}]}`) + `</Document></PolicyVersion></GetPolicyVersionResult></GetPolicyVersionResponse>`),
		},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			ps, err := c.RolePolicies(ctx, tRole)
			if err != nil {
				t.Fatal(err)
			}
			if len(ps) != 2 || ps[0].Kind != "inline" || ps[1].Kind != "attached" || ps[1].Arn != "arn:aws:iam::000000000000:policy/hop" {
				t.Fatalf("policies = %+v", ps)
			}
			if len(EscalationGrants(ps[0].Document)) != 0 || len(EscalationGrants(ps[1].Document)) == 0 {
				t.Errorf("grants: inline %v, attached %v", EscalationGrants(ps[0].Document), EscalationGrants(ps[1].Document))
			}
		},
	},
	{
		// A policy that cannot be read fails the whole read (fail closed).
		name: "role_policies_denied",
		responses: []replay.Response{
			xmlOK(`<ListRolePoliciesResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/"><ListRolePoliciesResult><IsTruncated>false</IsTruncated><PolicyNames></PolicyNames></ListRolePoliciesResult></ListRolePoliciesResponse>`),
			iamErr(403, "AccessDenied", "User is not authorized to perform: iam:ListAttachedRolePolicies"),
		},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			ps, err := c.RolePolicies(ctx, tRole)
			wantErr(t, err, ErrForbidden)
			var perm *PermissionError
			if !errors.As(err, &perm) || perm.Action != "iam:ListAttachedRolePolicies" {
				t.Errorf("err = %v, want a PermissionError naming iam:ListAttachedRolePolicies", err)
			}
			if ps != nil {
				t.Errorf("a failed read returned %+v", ps)
			}
		},
	},
	{
		name:      "role_not_found",
		responses: []replay.Response{iamErr(404, "NoSuchEntity", "The role with name PiperoomMSKConnect cannot be found.")},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			_, err := c.GetRole(ctx, tRole)
			wantErr(t, err, ErrNotFound)
		},
	},
	{
		name:      "head_object_ok",
		responses: []replay.Response{{Status: 200, Headers: map[string]string{"Content-Length": "48213977", "Content-Type": "application/zip", "X-Amz-Version-Id": "v-0001", "Etag": `"0123456789abcdef0123456789abcdef"`}}},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			o, err := c.HeadObject(ctx, tBucket, tKey, "", tAccount)
			if err != nil {
				t.Fatal(err)
			}
			if o.Size != 48213977 || o.VersionID != "v-0001" {
				t.Errorf("Object = %+v", o)
			}
		},
	},
	{
		name:      "head_object_forbidden",
		responses: []replay.Response{{Status: 403}},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			_, err := c.HeadObject(ctx, tBucket, tKey, "", tAccount)
			wantErr(t, err, ErrForbidden)
		},
	},
	{
		name:      "head_object_not_found",
		responses: []replay.Response{{Status: 404}},
		run: func(t *testing.T, ctx context.Context, c *Client) {
			_, err := c.HeadObject(ctx, tBucket, tKey, "", tAccount)
			wantErr(t, err, ErrNotFound)
		},
	},
}
