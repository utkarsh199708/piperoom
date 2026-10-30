package managed

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"

	"piperoom/internal/clients/mskconnect"
	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/targets/managed/replay"
)

// The identifiers every MSK flow cassette uses: the scrubber's placeholders.
const (
	mAccount   = "000000000000"
	mCluster   = "arn:aws:kafka:us-east-1:000000000000:cluster/demo/0a1b2c3d-4e5f-6789-abcd-ef0123456789-1"
	mConnector = "arn:aws:kafkaconnect:us-east-1:000000000000:connector/it_orders/0a1b2c3d-4e5f-6789-abcd-ef0123456789-2"
	mPlugin    = "arn:aws:kafkaconnect:us-east-1:000000000000:custom-plugin/debezium-pg/9f8e7d6c-5b4a-3210-fedc-ba9876543210-3"
	mWorker    = "arn:aws:kafkaconnect:us-east-1:000000000000:worker-configuration/piperoom-orders/11112222-3333-4444-5555-66667777aaaa-4"
	mRole      = "arn:aws:iam::000000000000:role/PiperoomMSKConnect"
	mBucket    = "piperoom-bucket"
	mKey       = "plugins/debezium-connector-postgres-2.7.3.zip"
	mSubnetA   = "subnet-0a1b2c3d4e5f60001"
	mSubnetB   = "subnet-0a1b2c3d4e5f60002"
	mGroup     = "sg-0a1b2c3d4e5f60001"
	mVPC       = "vpc-0a1b2c3d4e5f60001"
	mLogGroup  = "/piperoom/it_orders"
	mRoom      = "room-1"
)

var mskTarget = events.SetupTarget{Kind: KindAWS, Address: mAccount + "/us-east-1/" + mCluster}

// mskSource is the Source line of the hand-written MSK flow cassettes.
const mskSource = "hand-written from the AWS SDK for Go v2 shapes (kafkaconnect v1.33.0, kafka v1.56.0, ec2 v1.317.0, iam v1.56.0, s3 v1.106.0, sts v1.45.0); requests as the managed ops send them through mskconnect; not from a live account"

type noBackoff struct{}

func (noBackoff) BackoffDelay(int, error) (time.Duration, error) { return 0, nil }

func awsConfig(rt http.RoundTripper) aws.Config {
	return aws.Config{
		Region: "us-east-1", Credentials: aws.AnonymousCredentials{},
		HTTPClient: &http.Client{Transport: rt, Timeout: 5 * time.Second},
		Retryer: func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) { o.Backoff = noBackoff{}; o.RateLimiter = ratelimit.None })
		},
	}
}

// fakeStore materialises ${secret:db_password} only.
type fakeStore struct{}

func (fakeStore) Materialise(text string) (string, error) {
	if text == "${secret:db_password}" {
		return "pw-live-1", nil
	}
	return "", errors.New("unknown secret reference")
}

func kcJSON(body string) replay.Response {
	return replay.Response{Status: 200, Headers: map[string]string{"Content-Type": "application/json"}, Body: body}
}

func kcErr(status int, code, msg string) replay.Response {
	return replay.Response{Status: status, Headers: map[string]string{"Content-Type": "application/json", "X-Amzn-Errortype": code}, Body: `{"message":"` + msg + `"}`}
}

func xmlOK(body string) replay.Response {
	return replay.Response{Status: 200, Headers: map[string]string{"Content-Type": "text/xml"}, Body: body}
}

var stsOK = xmlOK(`<GetCallerIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/"><GetCallerIdentityResult><Arn>arn:aws:sts::000000000000:assumed-role/PiperoomSetup/piperoom-runner</Arn><UserId>AROAEXAMPLEROLE:piperoom-runner</UserId><Account>000000000000</Account></GetCallerIdentityResult><ResponseMetadata><RequestId>00000000-0000-0000-0000-000000000000</RequestId></ResponseMetadata></GetCallerIdentityResponse>`)

var clusterOK = kcJSON(`{"clusterInfo":{"clusterArn":"` + mCluster + `","clusterName":"demo","clusterType":"PROVISIONED","state":"ACTIVE","provisioned":{"brokerNodeGroupInfo":{"clientSubnets":["` + mSubnetA + `","` + mSubnetB + `"],"securityGroups":["` + mGroup + `"]},"currentBrokerSoftwareInfo":{"kafkaVersion":"3.6.0"},"numberOfBrokerNodes":2}}}`)

var bootstrapOK = kcJSON(`{"bootstrapBrokerStringSaslIam":"b-1.demo.abc123.c2.kafka.us-east-1.amazonaws.com:9098,b-2.demo.abc123.c2.kafka.us-east-1.amazonaws.com:9098"}`)

var listOK = kcJSON(`{"connectors":[{"connectorArn":"` + mConnector + `","connectorName":"it_orders","connectorState":"RUNNING"}]}`)

func connector(state, code, msg string) replay.Response {
	desc := ""
	if code != "" {
		desc = `,"stateDescription":{"code":"` + code + `","message":"` + msg + `"}`
	}
	return kcJSON(`{"connectorArn":"` + mConnector + `","connectorName":"it_orders","connectorState":"` + state + `","currentVersion":"K3AEGXETSR30VB","kafkaConnectVersion":"2.7.1",` +
		`"connectorConfiguration":{"connector.class":"io.debezium.connector.postgresql.PostgresConnector","database.hostname":"10.0.3.15","database.password":"pw-live-1","topic.prefix":"orders"},` +
		`"logDelivery":{"workerLogDelivery":{"cloudWatchLogs":{"enabled":true,"logGroup":"` + mLogGroup + `"}}}` + desc + `}`)
}

func pluginState(state, desc string) replay.Response {
	return kcJSON(`{"customPluginArn":"` + mPlugin + `","name":"debezium-pg","customPluginState":"` + state + `","latestRevision":{"revision":1,"contentType":"ZIP"}` + desc + `}`)
}

func subnets() replay.Response {
	return xmlOK(`<DescribeSubnetsResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/"><requestId>00000000-0000-0000-0000-000000000000</requestId><subnetSet>` +
		`<item><subnetId>` + mSubnetA + `</subnetId><vpcId>` + mVPC + `</vpcId><cidrBlock>10.0.1.0/24</cidrBlock><availabilityZone>us-east-1a</availabilityZone></item>` +
		`<item><subnetId>` + mSubnetB + `</subnetId><vpcId>` + mVPC + `</vpcId><cidrBlock>10.0.2.0/24</cidrBlock><availabilityZone>us-east-1b</availabilityZone></item>` +
		`</subnetSet></DescribeSubnetsResponse>`)
}

func groups() replay.Response {
	return xmlOK(`<DescribeSecurityGroupsResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/"><requestId>00000000-0000-0000-0000-000000000000</requestId><securityGroupInfo>` +
		`<item><groupId>` + mGroup + `</groupId><groupName>msk-connect</groupName><vpcId>` + mVPC + `</vpcId><ipPermissions></ipPermissions>` +
		`<ipPermissionsEgress><item><ipProtocol>-1</ipProtocol><ipRanges><item><cidrIp>0.0.0.0/0</cidrIp></item></ipRanges></item></ipPermissionsEgress></item>` +
		`</securityGroupInfo></DescribeSecurityGroupsResponse>`)
}

// routes is the VPC's main table: the VPC's own range only, no peering or
// transit route (the msk-vpc-no-route scenario's shape).
func routes() replay.Response {
	return xmlOK(`<DescribeRouteTablesResponse xmlns="http://ec2.amazonaws.com/doc/2016-11-15/"><requestId>00000000-0000-0000-0000-000000000000</requestId><routeTableSet>` +
		`<item><routeTableId>rtb-0a1b2c3d4e5f60001</routeTableId><vpcId>` + mVPC + `</vpcId><routeSet><item><destinationCidrBlock>10.0.0.0/16</destinationCidrBlock><gatewayId>local</gatewayId><state>active</state></item></routeSet>` +
		`<associationSet><item><routeTableAssociationId>rtbassoc-0a1b2c3d4e5f60001</routeTableAssociationId><routeTableId>rtb-0a1b2c3d4e5f60001</routeTableId><main>true</main></item></associationSet></item>` +
		`</routeTableSet></DescribeRouteTablesResponse>`)
}

func connectorParams() map[string]any {
	return map[string]any{
		"name": "it_orders", "kafka_connect_version": "2.7.1", "subnets": []string{mSubnetA, mSubnetB}, "security_groups": []string{mGroup},
		"plugin_arn": mPlugin, "plugin_revision": 1, "worker_config_arn": mWorker, "worker_config_revision": 1,
		"service_role_arn": mRole, "log_group": mLogGroup,
		"config": map[string]string{"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "database.hostname": "10.0.3.15",
			"database.user": "debezium", "database.password": "${secret:db_password}", "topic.prefix": "orders", "plugin.name": "pgoutput"},
	}
}

// flow is one op run through the catalogue over a cassette.
type flow struct {
	name      string
	op        string
	params    any
	responses []replay.Response
	check     func(t *testing.T, res protocol.OpResult, err error)
}

func okFlow(want ...string) func(t *testing.T, res protocol.OpResult, err error) {
	return func(t *testing.T, res protocol.OpResult, err error) {
		t.Helper()
		if err != nil || !res.OK {
			t.Fatalf("err = %v, OK %v, summary %q", err, res.OK, res.Summary)
		}
		for _, w := range want {
			if !strings.Contains(res.Summary+string(res.Data), w) {
				t.Errorf("result lacks %q: %s | %s", w, res.Summary, res.Data)
			}
		}
	}
}

func failFlow(class error, want ...string) func(t *testing.T, res protocol.OpResult, err error) {
	return func(t *testing.T, res protocol.OpResult, err error) {
		t.Helper()
		if !errors.Is(err, class) || res.OK {
			t.Fatalf("err = %v (OK %v), want %v", err, res.OK, class)
		}
		for _, w := range want {
			if !strings.Contains(res.Summary+string(res.Data), w) {
				t.Errorf("result lacks %q: %s | %s", w, res.Summary, res.Data)
			}
		}
	}
}

var mskFlows = []flow{
	{name: "describe_cluster", op: "msk_describe_cluster", params: map[string]any{},
		responses: []replay.Response{stsOK, clusterOK, bootstrapOK}, check: okFlow("ACTIVE", "IAM listener on", `"kafka_version":"3.6.0"`)},
	{name: "describe_connector_running", op: ReadMSKConnector, params: map[string]any{"name": "it_orders"},
		responses: []replay.Response{stsOK, listOK, connector("RUNNING", "", "")}, check: okFlow(`"state":"RUNNING"`, `"database.password":"\u003credacted\u003e"`)},
	{name: "describe_connector_creating", op: ReadMSKConnector, params: map[string]any{"name": "it_orders"},
		responses: []replay.Response{stsOK, listOK, connector("CREATING", "", "")}, check: okFlow(`"state":"CREATING"`, `"connect_state":"UNASSIGNED"`)},
	{name: "describe_connector_failed_timeout", op: ReadMSKConnector, params: map[string]any{"name": "it_orders"},
		responses: []replay.Response{stsOK, listOK, connector("FAILED", "UNKNOWN",
			"There is an issue with the connector. Code: UnknownError.Unknown. Message: org.apache.kafka.connect.errors.ConnectException: Could not connect to 172.31.5.10:5432: connect timed out")},
		check: okFlow(`"state":"FAILED"`, "connect timed out")},
	{name: "vpc_check_ok", op: "msk_vpc_check", params: map[string]any{"subnets": []string{mSubnetA, mSubnetB}, "security_groups": []string{mGroup}, "database_ip": "10.0.3.15", "database_port": 5432},
		responses: []replay.Response{stsOK, subnets(), groups(), routes()}, check: okFlow(`"verdict":"ok"`)},
	{name: "vpc_check_no_route", op: "msk_vpc_check", params: map[string]any{"subnets": []string{mSubnetA, mSubnetB}, "security_groups": []string{mGroup}, "database_ip": "172.31.5.10", "database_port": 5432},
		responses: []replay.Response{stsOK, subnets(), groups(), routes()}, check: failFlow(ErrCheckFailed, `"verdict":"no_route"`, "no route from subnet-0a1b2c3d4e5f60001 to 172.31.5.10")},
	{name: "role_check_ok", op: "msk_role_check", params: map[string]any{"role_arn": mRole},
		responses: []replay.Response{stsOK,
			xmlOK(`<GetRoleResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/"><GetRoleResult><Role><Path>/</Path><RoleName>PiperoomMSKConnect</RoleName><RoleId>AROAEXAMPLEROLE</RoleId><Arn>` + mRole + `</Arn><CreateDate>2026-10-01T00:00:00Z</CreateDate>` +
				`<AssumeRolePolicyDocument>%7B%22Version%22%3A%222012-10-17%22%2C%22Statement%22%3A%5B%7B%22Effect%22%3A%22Allow%22%2C%22Principal%22%3A%7B%22Service%22%3A%22kafkaconnect.amazonaws.com%22%7D%2C%22Action%22%3A%22sts%3AAssumeRole%22%7D%5D%7D</AssumeRolePolicyDocument></Role></GetRoleResult></GetRoleResponse>`),
			xmlOK(`<SimulatePrincipalPolicyResponse xmlns="https://iam.amazonaws.com/doc/2010-05-08/"><SimulatePrincipalPolicyResult><IsTruncated>false</IsTruncated><EvaluationResults>` +
				`<member><EvalActionName>kafka-cluster:Connect</EvalActionName><EvalDecision>allowed</EvalDecision></member><member><EvalActionName>kafka-cluster:DescribeCluster</EvalActionName><EvalDecision>allowed</EvalDecision></member>` +
				`<member><EvalActionName>logs:CreateLogStream</EvalActionName><EvalDecision>allowed</EvalDecision></member><member><EvalActionName>logs:PutLogEvents</EvalActionName><EvalDecision>allowed</EvalDecision></member>` +
				`</EvaluationResults></SimulatePrincipalPolicyResult></SimulatePrincipalPolicyResponse>`)},
		check: okFlow(`"ok":true`)},
	{name: "plugin_object_denied", op: "msk_plugin_object_check", params: map[string]any{"bucket": mBucket, "key": mKey},
		responses: []replay.Response{stsOK, {Status: 403}}, check: failFlow(mskconnect.ErrForbidden, `"error_class":"forbidden"`, "s3:GetObject")},
	{name: "create_plugin_ok", op: "msk_create_plugin", params: map[string]any{"name": "debezium-pg", "bucket": mBucket, "key": mKey, "content_type": "ZIP", "expected_size": 48213977},
		responses: []replay.Response{stsOK, {Status: 200, Headers: map[string]string{"Content-Length": "48213977", "Content-Type": "application/zip"}},
			kcJSON(`{"customPluginArn":"` + mPlugin + `","customPluginState":"CREATING","name":"debezium-pg","revision":1}`)},
		check: okFlow("CREATING", "msk_wait")},
	{name: "wait_plugin_s3_denied", op: "msk_wait", params: map[string]any{"kind": "plugin", "arn": mPlugin},
		responses: []replay.Response{stsOK, pluginState("CREATING", ""), pluginState("CREATE_FAILED",
			`,"stateDescription":{"code":"ACCESS_DENIED","message":"The service execution role cannot access s3://piperoom-bucket/plugins/debezium-connector-postgres-2.7.3.zip: Access Denied"}`)},
		check: failFlow(ErrCheckFailed, "CREATE_FAILED", "ACCESS_DENIED")},
	{name: "create_worker_config_ok", op: "msk_create_worker_config", params: map[string]any{"name": "piperoom-orders", "entries": map[string]string{
		"key.converter": "org.apache.kafka.connect.json.JsonConverter", "value.converter": "org.apache.kafka.connect.json.JsonConverter", "value.converter.schemas.enable": "false"}},
		responses: []replay.Response{stsOK, kcJSON(`{"workerConfigurationArn":"` + mWorker + `","name":"piperoom-orders","workerConfigurationState":"ACTIVE","latestRevision":{"revision":1}}`)},
		check:     okFlow("ACTIVE", "value.converter.schemas.enable=false")},
	{name: "create_connector_ok", op: "msk_create_connector", params: connectorParams(),
		responses: []replay.Response{stsOK, clusterOK, bootstrapOK, kcJSON(`{"connectorArn":"` + mConnector + `","connectorName":"it_orders","connectorState":"CREATING"}`)},
		check:     okFlow("CREATING", "${secret:db_password}")},
	{name: "wait_connector_running", op: "msk_wait", params: map[string]any{"kind": "connector", "arn": mConnector},
		responses: []replay.Response{stsOK, connector("CREATING", "", ""), connector("RUNNING", "", "")}, check: okFlow(`"done":true`, `"polls":2`)},
	{name: "delete_connector_ok", op: "msk_delete_connector", params: map[string]any{"name": "it_orders"},
		responses: []replay.Response{stsOK, listOK, kcJSON(`{"tags":{"piperoom-room":"room-1","piperoom-created-by":"piperoom-setup"}}`), connector("RUNNING", "", ""),
			kcJSON(`{"connectorArn":"` + mConnector + `","connectorState":"DELETING"}`)},
		check: okFlow("DELETING")},
	{name: "delete_connector_not_ours", op: "msk_delete_connector", params: map[string]any{"name": "it_orders"},
		responses: []replay.Response{stsOK, listOK, kcJSON(`{"tags":{"piperoom-room":"room-other"}}`)},
		check:     failFlow(ErrNotCreatedByRoom, "not created by this room")},
	{name: "create_connector_conflict", op: "msk_create_connector", params: connectorParams(),
		responses: []replay.Response{stsOK, clusterOK, bootstrapOK, kcErr(409, "ConflictException", "A connector with the name it_orders already exists")},
		check:     failFlow(mskconnect.ErrConflict, `"error_class":"conflict"`)},
}

// newMSK registers the MSK ops over api in a registry with the aws target.
func newMSK(t *testing.T, api MSKAPI, class string, patterns []string) (*ops.Registry, *MSKDriver) {
	t.Helper()
	reg, err := ops.New(ops.Config{ProjectDir: t.TempDir(), Targets: []events.SetupTarget{mskTarget}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reg.Close() })
	d, err := RegisterMSK(reg, MSKOptions{Client: api, Target: mskTarget, Class: class, ProdPatterns: patterns, RoomID: mRoom, Secrets: fakeStore{}, PollEvery: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	return reg, d
}

func exec(t *testing.T, reg *ops.Registry, op string, params any) (protocol.OpResult, error) {
	t.Helper()
	raw, err := json.Marshal(params)
	if err != nil {
		t.Fatal(err)
	}
	return reg.Execute(context.Background(), protocol.OpRequest{ID: "req-1", Op: op, Params: raw})
}

// TestMSKFlows runs every MSK op over its cassette through the catalogue,
// with the real mskconnect client and SDK: the board's "unit tests over the
// cassettes".
func TestMSKFlows(t *testing.T) {
	for _, f := range mskFlows {
		t.Run(f.name, func(t *testing.T) {
			r := replay.Replay(t, "testdata/msk/"+f.name+".json", replay.Scrubber{})
			c, err := mskconnect.New(awsConfig(r))
			if err != nil {
				t.Fatal(err)
			}
			reg, _ := newMSK(t, c, "", nil)
			res, err := exec(t, reg, f.op, f.params)
			f.check(t, res, err)
			if strings.Contains(res.Summary+string(res.Data), "pw-live-1") {
				t.Errorf("the materialised password reached the result")
			}
			if n := r.Remaining(); n != 0 {
				t.Errorf("%d interactions unused", n)
			}
		})
	}
}
