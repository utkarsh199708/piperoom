package managed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"piperoom/internal/clients/mskconnect"
	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

// MSKAPI is what the MSK ops need from internal/clients/mskconnect; the
// *mskconnect.Client implements it.
type MSKAPI interface {
	Identity(ctx context.Context) (mskconnect.Identity, error)
	DescribeCluster(ctx context.Context, arn string) (mskconnect.Cluster, error)
	ListPlugins(ctx context.Context, prefix string) ([]mskconnect.Plugin, error)
	DescribePlugin(ctx context.Context, arn string) (mskconnect.Plugin, error)
	FindConnector(ctx context.Context, name string) (string, error)
	DescribeConnector(ctx context.Context, arn string) (mskconnect.Connector, error)
	ConnectorTags(ctx context.Context, arn string) (map[string]string, error)
	DescribeNetwork(ctx context.Context, subnets, groups []string) (mskconnect.Network, error)
	GetRole(ctx context.Context, arn string) (mskconnect.Role, error)
	SimulateRole(ctx context.Context, arn string, actions []string) ([]mskconnect.Decision, error)
	HeadObject(ctx context.Context, bucket, key, version string) (mskconnect.Object, error)
	CreatePlugin(ctx context.Context, s mskconnect.PluginSpec) (mskconnect.Created, error)
	CreateWorkerConfig(ctx context.Context, s mskconnect.WorkerConfigSpec) (mskconnect.WorkerConfig, error)
	CreateConnector(ctx context.Context, s mskconnect.ConnectorSpec) (mskconnect.Created, error)
	DeleteConnector(ctx context.Context, arn, version string) (mskconnect.Created, error)
}

var _ MSKAPI = (*mskconnect.Client)(nil)

// MSKOptions configures the MSK driver.
type MSKOptions struct {
	// Client is the runner's MSK Connect client (its own AWS credentials).
	Client MSKAPI
	// Target is the room's SetupTarget of kind "aws":
	// <account id>/<region>[/<cluster arn>].
	Target events.SetupTarget
	// Class is the person's class floor for the target ("" for none).
	Class string
	// ProdPatterns are PIPEROOM_SETUP_PROD_PATTERNS; the account id is
	// matched against them.
	ProdPatterns []string
	// RoomID tags what the create ops create (TagRoom); the delete op
	// refuses a connector without this tag. Required.
	RoomID string
	// Secrets materialises ${secret:} references in a connector config.
	Secrets Materialiser
	// PollEvery is msk_wait's interval; zero means 5 s.
	PollEvery time.Duration
}

// MSKDriver holds the MSK target's resolved account, region and class.
type MSKDriver struct {
	api       MSKAPI
	account   string
	region    string
	cluster   string
	address   string
	class     string
	room      string
	secrets   Materialiser
	pollEvery time.Duration
}

// Class is the target's derived class.
func (d *MSKDriver) Class() string { return d.class }

// RoleActions are the service execution role's actions msk_role_check
// simulates (design §3.1).
var RoleActions = []string{"kafka-cluster:Connect", "kafka-cluster:DescribeCluster", "logs:CreateLogStream", "logs:PutLogEvents"}

// RegisterMSK adds the MSK ops to r.
func RegisterMSK(r *ops.Registry, o MSKOptions) (*MSKDriver, error) {
	if r == nil || o.Client == nil {
		return nil, errors.New("managed: a registry and an MSK client are required")
	}
	if o.Target.Kind != KindAWS {
		return nil, fmt.Errorf("managed: the MSK target has kind %q, want %q", o.Target.Kind, KindAWS)
	}
	parts := strings.SplitN(o.Target.Address, "/", 3)
	if len(parts) < 2 || !accountRE.MatchString(parts[0]) || !regionRE.MatchString(parts[1]) {
		return nil, fmt.Errorf("managed: the aws target address %q is not <account id>/<region>[/<cluster arn>]", clipText(o.Target.Address, 120))
	}
	d := &MSKDriver{api: o.Client, account: parts[0], region: parts[1], address: parts[0] + "/" + parts[1], room: o.RoomID, secrets: o.Secrets, pollEvery: o.PollEvery}
	if len(parts) == 3 {
		if err := d.checkARN(parts[2], mskconnect.CheckClusterARN); err != nil {
			return nil, fmt.Errorf("managed: the aws target's cluster: %w", err)
		}
		d.cluster = parts[2]
	}
	if !roomRE.MatchString(o.RoomID) {
		return nil, fmt.Errorf("managed: a room id of %s is required to tag what the room creates", roomRE.String())
	}
	if d.pollEvery == 0 {
		d.pollEvery = 5 * time.Second
	}
	person := o.Class
	if o.Target.Class != "" && person == "" {
		person = o.Target.Class
	}
	class, err := deriveClass(person, o.ProdPatterns, d.account)
	if err != nil {
		return nil, err
	}
	d.class = class
	for _, op := range d.ops() {
		if err := r.Register(op); err != nil {
			return nil, fmt.Errorf("managed: registering %s: %w", op.Name(), err)
		}
	}
	return d, nil
}

func (d *MSKDriver) ops() []ops.Op {
	return []ops.Op{
		mskOp{d: d, name: "msk_describe_cluster", describe: "describe the MSK cluster: state, Kafka version, subnets, security groups, bootstrap brokers (kafka:DescribeClusterV2, GetBootstrapBrokers)",
			schema: objectSchema(nil, map[string]any{"cluster_arn": stringSchema("")}), run: d.describeCluster, check: d.checkClusterParams},
		mskOp{d: d, name: "msk_list_plugins", describe: "list MSK Connect custom plugins by name prefix (kafkaconnect:ListCustomPlugins)",
			schema: objectSchema(nil, map[string]any{"name_prefix": stringSchema("")}), run: d.listPlugins, check: d.checkPluginsParams},
		mskOp{d: d, name: ReadMSKConnector, describe: "read an MSK Connect connector's state and redacted configuration by name (ListConnectors, DescribeConnector); the ladder's rung 1",
			schema: objectSchema([]string{"name"}, map[string]any{"name": stringSchema("")}), run: d.describeConnector, check: d.checkNameParams},
		mskOp{d: d, name: "msk_vpc_check", describe: "check that the connector's subnets route to the database address and its security groups allow egress to it (ec2 DescribeSubnets, DescribeSecurityGroups, DescribeRouteTables)",
			schema: objectSchema([]string{"subnets", "security_groups", "database_ip", "database_port"}, map[string]any{
				"subnets": stringListSchema(""), "security_groups": stringListSchema(""), "database_ip": stringSchema(""),
				"database_port": map[string]any{"type": "integer", "minimum": 1, "maximum": 65535}}),
			run: d.vpcCheck, check: d.checkVPCParams},
		mskOp{d: d, name: "msk_role_check", describe: "check the service execution role: trust in kafkaconnect.amazonaws.com and the simulated MSK IAM and log actions (iam GetRole, SimulatePrincipalPolicy)",
			schema: objectSchema([]string{"role_arn"}, map[string]any{"role_arn": stringSchema("")}), run: d.roleCheck, check: d.checkRoleParams},
		mskOp{d: d, name: "msk_plugin_object_check", describe: "check the plugin archive the person uploaded to S3: it exists and its size matches (s3 HeadObject; the digest is not verified)",
			schema: objectSchema([]string{"bucket", "key"}, map[string]any{"bucket": stringSchema(""), "key": stringSchema(""), "version": stringSchema(""),
				"expected_size": map[string]any{"type": "integer", "minimum": 1}}),
			run: d.pluginObjectCheck, check: d.checkObjectParams},
		mskOp{d: d, name: "msk_wait", describe: "poll a custom plugin to ACTIVE or a connector to RUNNING, or report the failure with MSK's state description",
			schema: objectSchema([]string{"kind", "arn"}, map[string]any{"kind": map[string]any{"type": "string", "enum": []string{"plugin", "connector"}}, "arn": stringSchema("")}),
			run:    d.wait, check: d.checkWaitParams},
		mskOp{d: d, name: "msk_create_plugin", write: true, describe: "create an MSK Connect custom plugin from the archive the person uploaded to S3 (kafkaconnect:CreateCustomPlugin); asynchronous, follow with msk_wait",
			schema: objectSchema([]string{"name", "bucket", "key", "content_type"}, map[string]any{"name": stringSchema(""), "bucket": stringSchema(""), "key": stringSchema(""),
				"version": stringSchema(""), "content_type": map[string]any{"type": "string", "enum": []string{mskconnect.ContentZIP, mskconnect.ContentJAR}},
				"expected_size": map[string]any{"type": "integer", "minimum": 1}}),
			run: d.createPlugin, check: d.checkCreatePlugin, preview: d.previewPlugin},
		mskOp{d: d, name: "msk_create_worker_config", write: true, describe: "create an MSK Connect worker configuration rendered from the template under the key allow-list (kafkaconnect:CreateWorkerConfiguration)",
			schema: objectSchema([]string{"name", "entries"}, map[string]any{"name": stringSchema(""), "entries": stringMapSchema()}),
			run:    d.createWorkerConfig, check: d.checkWorkerParams, preview: d.previewWorker},
		mskOp{d: d, name: "msk_create_connector", write: true, describe: "create the MSK Connect connector (1 MCU, 1 worker, IAM auth, TLS) with its config's ${secret:} references materialised on the runner (kafkaconnect:CreateConnector); asynchronous, follow with msk_wait",
			schema: objectSchema([]string{"name", "kafka_connect_version", "subnets", "security_groups", "plugin_arn", "service_role_arn", "config"}, map[string]any{
				"name": stringSchema(""), "kafka_connect_version": stringSchema(""), "cluster_arn": stringSchema(""),
				"subnets": stringListSchema(""), "security_groups": stringListSchema(""), "plugin_arn": stringSchema(""),
				"plugin_revision":        map[string]any{"type": "integer", "minimum": 1},
				"worker_config_arn":      stringSchema(""),
				"worker_config_revision": map[string]any{"type": "integer", "minimum": 1},
				"service_role_arn":       stringSchema(""), "log_group": stringSchema(""), "config": stringMapSchema()}),
			run: d.createConnector, check: d.checkCreateConnector, preview: d.previewConnector},
		mskOp{d: d, name: "msk_delete_connector", write: true, describe: "delete an MSK Connect connector this room created (tagged piperoom-room); refused for any other connector (kafkaconnect:DeleteConnector)",
			schema: objectSchema([]string{"name"}, map[string]any{"name": stringSchema("")}), run: d.deleteConnector, check: d.checkNameParams, preview: d.previewDelete},
	}
}

// mskOp is one MSK op: the shared target check, tier and identity check
// around an op's own params check and run.
type mskOp struct {
	d        *MSKDriver
	name     string
	describe string
	write    bool
	schema   map[string]any
	check    func(raw json.RawMessage) error
	run      func(ctx context.Context, raw json.RawMessage) (ops.Result, error)
	preview  func(raw json.RawMessage) (events.PreviewResult, error)
}

func (o mskOp) Name() string                 { return o.name }
func (o mskOp) Target() string               { return KindAWS }
func (o mskOp) Layer() string                { return connectLayer }
func (o mskOp) Describe() string             { return o.describe }
func (o mskOp) ParamsSchema() map[string]any { return o.schema }

func (o mskOp) Tier() string {
	if o.write {
		return protocol.TierT2
	}
	return protocol.TierT0
}

// TierFor raises a write to T3 on a production target; it does no I/O and
// depends only on the class derived at registration.
func (o mskOp) TierFor(json.RawMessage) (string, error) {
	if !o.write {
		return protocol.TierT0, nil
	}
	return writeTier(o.d.class), nil
}

func (o mskOp) Validate(raw json.RawMessage, env ops.Env) error {
	if !targetIn(env, KindAWS, o.d.address) {
		return fmt.Errorf("%w: no aws target %s in this room", ErrTarget, o.d.address)
	}
	return o.check(raw)
}

func (o mskOp) Execute(ctx context.Context, raw json.RawMessage, env ops.Env) (ops.Result, error) {
	if err := o.Validate(raw, env); err != nil {
		return ops.Result{}, err
	}
	if err := o.d.checkIdentity(ctx); err != nil {
		return o.d.result("refused "+o.name+": "+err.Error(), nil), err
	}
	return o.run(ctx, raw)
}

// Preview is the step's preview for a write (design §2.5's Op.Preview
// signature, for when the catalogue calls it).
func (o mskOp) Preview(_ context.Context, raw json.RawMessage, env ops.Env) (events.PreviewResult, error) {
	if o.preview == nil {
		return events.PreviewResult{}, fmt.Errorf("managed: %s has no preview (a read)", o.name)
	}
	if err := o.Validate(raw, env); err != nil {
		return events.PreviewResult{}, err
	}
	p, err := o.preview(raw)
	if err != nil {
		return events.PreviewResult{}, err
	}
	p.Target = "aws:" + o.d.address
	return p, nil
}

// checkIdentity refuses a runner whose AWS identity is not in the target's
// account: otherwise a profile pointed at another account would run there
// with this target's class.
func (d *MSKDriver) checkIdentity(ctx context.Context) error {
	id, err := d.api.Identity(ctx)
	if err != nil {
		return fmt.Errorf("managed: reading the runner's AWS identity: %w", err)
	}
	if id.Account != d.account {
		return fmt.Errorf("%w: the target is account %s", ErrAccount, d.account)
	}
	return nil
}

func (d *MSKDriver) result(summary string, data any) ops.Result {
	return ops.Result{Summary: summary, Data: data, Receipt: events.SetupReceipt{Kind: "http", Target: "aws:" + d.address}}
}

// checkARN checks an ARN's shape and that it is in the target's account
// (and region, when the ARN has one).
func (d *MSKDriver) checkARN(arn string, shape func(string) error) error {
	if err := shape(arn); err != nil {
		return paramErr(err)
	}
	if mskconnect.AccountOfARN(arn) != d.account {
		return fmt.Errorf("%w: %s is not in account %s", ErrParams, clipText(arn, 120), d.account)
	}
	if r := mskconnect.RegionOfARN(arn); r != "" && r != d.region {
		return fmt.Errorf("%w: %s is not in region %s", ErrParams, clipText(arn, 120), d.region)
	}
	return nil
}

// ---- params ----

// ClusterParams name a cluster; empty means the target's cluster.
type ClusterParams struct {
	ClusterArn string `json:"cluster_arn,omitempty"`
}

// PluginsParams filter msk_list_plugins.
type PluginsParams struct {
	NamePrefix string `json:"name_prefix,omitempty"`
}

// NameParams name a connector.
type NameParams struct {
	Name string `json:"name"`
}

// VPCCheckParams are msk_vpc_check's.
type VPCCheckParams struct {
	Subnets        []string `json:"subnets"`
	SecurityGroups []string `json:"security_groups"`
	DatabaseIP     string   `json:"database_ip"`
	DatabasePort   int      `json:"database_port"`
}

// RoleParams name the service execution role.
type RoleParams struct {
	RoleArn string `json:"role_arn"`
}

// ObjectParams name the plugin archive.
type ObjectParams struct {
	Bucket       string `json:"bucket"`
	Key          string `json:"key"`
	Version      string `json:"version,omitempty"`
	ExpectedSize int64  `json:"expected_size,omitempty"`
}

// WaitParams are msk_wait's.
type WaitParams struct {
	Kind string `json:"kind"` // plugin | connector
	Arn  string `json:"arn"`
}

// CreatePluginParams are msk_create_plugin's.
type CreatePluginParams struct {
	Name         string `json:"name"`
	Bucket       string `json:"bucket"`
	Key          string `json:"key"`
	Version      string `json:"version,omitempty"`
	ContentType  string `json:"content_type"`
	ExpectedSize int64  `json:"expected_size,omitempty"`
}

// WorkerParams are msk_create_worker_config's.
type WorkerParams struct {
	Name    string            `json:"name"`
	Entries map[string]string `json:"entries"`
}

// CreateConnectorParams are msk_create_connector's. The bootstrap servers
// are not a param: they are the cluster's IAM listener, read from the
// cluster at execute time.
type CreateConnectorParams struct {
	Name                 string            `json:"name"`
	KafkaConnectVersion  string            `json:"kafka_connect_version"`
	ClusterArn           string            `json:"cluster_arn,omitempty"`
	Subnets              []string          `json:"subnets"`
	SecurityGroups       []string          `json:"security_groups"`
	PluginArn            string            `json:"plugin_arn"`
	PluginRevision       int64             `json:"plugin_revision,omitempty"`
	WorkerConfigArn      string            `json:"worker_config_arn,omitempty"`
	WorkerConfigRevision int64             `json:"worker_config_revision,omitempty"`
	ServiceRoleArn       string            `json:"service_role_arn"`
	LogGroup             string            `json:"log_group,omitempty"`
	Config               map[string]string `json:"config"`
}

func (d *MSKDriver) clusterOf(arn string) (string, error) {
	if arn == "" {
		arn = d.cluster
	}
	if arn == "" {
		return "", fmt.Errorf("%w: no cluster_arn and the aws target names no cluster", ErrParams)
	}
	return arn, d.checkARN(arn, mskconnect.CheckClusterARN)
}

func (d *MSKDriver) checkClusterParams(raw json.RawMessage) error {
	var p ClusterParams
	if err := decode(raw, &p); err != nil {
		return err
	}
	_, err := d.clusterOf(p.ClusterArn)
	return err
}

func (d *MSKDriver) checkPluginsParams(raw json.RawMessage) error {
	var p PluginsParams
	if err := decode(raw, &p); err != nil {
		return err
	}
	if p.NamePrefix != "" {
		return paramErr(mskconnect.CheckName(p.NamePrefix))
	}
	return nil
}

func (d *MSKDriver) checkNameParams(raw json.RawMessage) error {
	var p NameParams
	if err := decode(raw, &p); err != nil {
		return err
	}
	return paramErr(mskconnect.CheckName(p.Name))
}

func (d *MSKDriver) checkVPCParams(raw json.RawMessage) error {
	var p VPCCheckParams
	if err := decode(raw, &p); err != nil {
		return err
	}
	return checkVPC(p)
}

func (d *MSKDriver) checkRoleParams(raw json.RawMessage) error {
	var p RoleParams
	if err := decode(raw, &p); err != nil {
		return err
	}
	return d.checkARN(p.RoleArn, mskconnect.CheckRoleARN)
}

func (d *MSKDriver) checkObjectParams(raw json.RawMessage) error {
	var p ObjectParams
	if err := decode(raw, &p); err != nil {
		return err
	}
	return checkObject(p.Bucket, p.Key, p.Version, p.ExpectedSize)
}

func checkObject(bucket, key, version string, size int64) error {
	if err := mskconnect.CheckBucket(bucket); err != nil {
		return paramErr(err)
	}
	if err := mskconnect.CheckObjectKey(key); err != nil {
		return paramErr(err)
	}
	if size < 0 {
		return fmt.Errorf("%w: expected_size below zero", ErrParams)
	}
	return paramErr(mskconnect.CheckObjectVersion(version))
}

func (d *MSKDriver) checkWaitParams(raw json.RawMessage) error {
	var p WaitParams
	if err := decode(raw, &p); err != nil {
		return err
	}
	if p.Kind != "plugin" && p.Kind != "connector" {
		return fmt.Errorf("%w: kind %q, want plugin or connector", ErrParams, clipText(p.Kind, 16))
	}
	if err := d.checkARN(p.Arn, mskconnect.CheckResourceARN); err != nil {
		return err
	}
	want := map[string]string{"plugin": ":custom-plugin/", "connector": ":connector/"}[p.Kind]
	if !strings.Contains(p.Arn, want) {
		return fmt.Errorf("%w: %s is not a %s ARN", ErrParams, clipText(p.Arn, 120), p.Kind)
	}
	return nil
}

func (d *MSKDriver) checkCreatePlugin(raw json.RawMessage) error {
	var p CreatePluginParams
	if err := decode(raw, &p); err != nil {
		return err
	}
	if err := mskconnect.CheckName(p.Name); err != nil {
		return paramErr(err)
	}
	if p.ContentType != mskconnect.ContentZIP && p.ContentType != mskconnect.ContentJAR {
		return fmt.Errorf("%w: content_type %q, want ZIP or JAR", ErrParams, clipText(p.ContentType, 16))
	}
	return checkObject(p.Bucket, p.Key, p.Version, p.ExpectedSize)
}

func (d *MSKDriver) checkWorkerParams(raw json.RawMessage) error {
	var p WorkerParams
	if err := decode(raw, &p); err != nil {
		return err
	}
	if err := mskconnect.CheckName(p.Name); err != nil {
		return paramErr(err)
	}
	_, err := RenderWorkerProperties(p.Entries)
	return err
}

func (d *MSKDriver) checkCreateConnector(raw json.RawMessage) error {
	var p CreateConnectorParams
	if err := decode(raw, &p); err != nil {
		return err
	}
	if err := mskconnect.CheckName(p.Name); err != nil {
		return paramErr(err)
	}
	if err := mskconnect.CheckConnectVersion(p.KafkaConnectVersion); err != nil {
		return paramErr(err)
	}
	if _, err := d.clusterOf(p.ClusterArn); err != nil {
		return err
	}
	if len(p.Subnets) == 0 || len(p.Subnets) > 16 || len(p.SecurityGroups) == 0 || len(p.SecurityGroups) > 16 {
		return fmt.Errorf("%w: 1 to 16 subnets and 1 to 16 security groups", ErrParams)
	}
	for _, s := range p.Subnets {
		if err := mskconnect.CheckSubnet(s); err != nil {
			return paramErr(err)
		}
	}
	for _, g := range p.SecurityGroups {
		if err := mskconnect.CheckSecurityGroup(g); err != nil {
			return paramErr(err)
		}
	}
	if err := d.checkARN(p.PluginArn, mskconnect.CheckResourceARN); err != nil {
		return err
	}
	if p.WorkerConfigArn != "" {
		if err := d.checkARN(p.WorkerConfigArn, mskconnect.CheckResourceARN); err != nil {
			return err
		}
	}
	if err := d.checkARN(p.ServiceRoleArn, mskconnect.CheckRoleARN); err != nil {
		return err
	}
	if p.LogGroup != "" {
		if err := mskconnect.CheckLogGroup(p.LogGroup); err != nil {
			return paramErr(err)
		}
	}
	return checkConnectorConfig(p.Config)
}

// ---- reads ----

func (d *MSKDriver) describeCluster(ctx context.Context, raw json.RawMessage) (ops.Result, error) {
	var p ClusterParams
	_ = decode(raw, &p)
	arn, _ := d.clusterOf(p.ClusterArn)
	cl, err := d.api.DescribeCluster(ctx, arn)
	if err != nil {
		return d.result("", errData(err)), err
	}
	listener := "no IAM listener"
	if cl.BootstrapIAM != "" {
		listener = "IAM listener on"
	}
	return d.result(fmt.Sprintf("MSK cluster %s is %s, Kafka %s, %d subnets, %s", cl.Name, cl.State, cl.KafkaVersion, len(cl.Subnets), listener), cl), nil
}

func (d *MSKDriver) listPlugins(ctx context.Context, raw json.RawMessage) (ops.Result, error) {
	var p PluginsParams
	_ = decode(raw, &p)
	ps, err := d.api.ListPlugins(ctx, p.NamePrefix)
	if err != nil {
		return d.result("", errData(err)), err
	}
	if ps == nil {
		ps = []mskconnect.Plugin{}
	}
	return d.result(fmt.Sprintf("%d custom plugin(s)", len(ps)), map[string]any{"plugins": ps}), nil
}

// connectorData is msk_describe_connector's Data: "state" is MSK's own
// state, which the ladder's rung 1 judges (RUNNING only once the workers
// run); "tasks" is empty because MSK reports none.
type connectorData struct {
	Name           string            `json:"name"`
	Arn            string            `json:"arn"`
	State          string            `json:"state"`
	ConnectState   string            `json:"connect_state"`
	StateCode      string            `json:"state_code,omitempty"`
	StateMessage   string            `json:"state_message,omitempty"`
	CurrentVersion string            `json:"current_version,omitempty"`
	LogGroup       string            `json:"log_group,omitempty"`
	Config         map[string]string `json:"config,omitempty"`
	Tasks          []any             `json:"tasks"`
}

func (d *MSKDriver) describeConnector(ctx context.Context, raw json.RawMessage) (ops.Result, error) {
	var p NameParams
	_ = decode(raw, &p)
	arn, err := d.api.FindConnector(ctx, p.Name)
	if err != nil {
		return d.result("", errData(err)), err
	}
	cn, err := d.api.DescribeConnector(ctx, arn)
	if err != nil {
		return d.result("", errData(err)), err
	}
	data := connectorData{Name: cn.Name, Arn: cn.Arn, State: cn.State, ConnectState: mskconnect.MapState(cn.State), StateCode: cn.StateCode,
		StateMessage: cn.StateMessage, CurrentVersion: cn.CurrentVersion, LogGroup: cn.LogGroup, Config: cn.Config, Tasks: []any{}}
	s := fmt.Sprintf("MSK connector %s is %s", cn.Name, cn.State)
	if cn.StateCode != "" {
		s += " (" + cn.StateCode + ")"
	}
	return d.result(s, data), nil
}

func (d *MSKDriver) roleCheck(ctx context.Context, raw json.RawMessage) (ops.Result, error) {
	var p RoleParams
	_ = decode(raw, &p)
	role, err := d.api.GetRole(ctx, p.RoleArn)
	if err != nil {
		return d.result("", errData(err)), err
	}
	ds, err := d.api.SimulateRole(ctx, p.RoleArn, RoleActions)
	if err != nil {
		return d.result("", errData(err)), err
	}
	var denied []string
	for _, x := range ds {
		if x.Decision != "allowed" {
			denied = append(denied, x.Action)
		}
	}
	data := map[string]any{"role": role, "decisions": ds, "denied": denied, "ok": role.TrustsKafkaConnect && len(denied) == 0}
	switch {
	case !role.TrustsKafkaConnect:
		return d.result("role "+role.Name+" does not trust kafkaconnect.amazonaws.com", data), fmt.Errorf("%w: the role's trust policy does not name kafkaconnect.amazonaws.com", ErrCheckFailed)
	case len(denied) > 0:
		return d.result("role "+role.Name+" is denied "+strings.Join(denied, ", "), data), fmt.Errorf("%w: the role is denied %s", ErrCheckFailed, strings.Join(denied, ", "))
	}
	return d.result("role "+role.Name+" trusts MSK Connect and is allowed the connector's actions", data), nil
}

func (d *MSKDriver) pluginObjectCheck(ctx context.Context, raw json.RawMessage) (ops.Result, error) {
	var p ObjectParams
	_ = decode(raw, &p)
	return d.objectCheck(ctx, p.Bucket, p.Key, p.Version, p.ExpectedSize)
}

func (d *MSKDriver) objectCheck(ctx context.Context, bucket, key, version string, size int64) (ops.Result, error) {
	o, err := d.api.HeadObject(ctx, bucket, key, version)
	if err != nil {
		data := errData(err)
		data["hint"] = "the runner's identity needs s3:GetObject on the plugin object, and so does the MSK Connect service when it builds the plugin"
		return d.result("", data), err
	}
	matches := size == 0 || o.Size == size
	data := map[string]any{"object": o, "size_matches": matches, "digest": "unverified: S3 exposes no SHA-512 of the object (design §3.1)"}
	if !matches {
		return d.result(fmt.Sprintf("s3://%s/%s is %d bytes, the index says %d", bucket, key, o.Size, size), data),
			fmt.Errorf("%w: the object's size does not match the index", ErrCheckFailed)
	}
	return d.result(fmt.Sprintf("s3://%s/%s exists, %d bytes; size matches the index; digest unverified", bucket, key, o.Size), data), nil
}

// wait polls until a terminal state or until the op's deadline is too close
// for another poll; then it reports done false so the plan issues it again
// (the catalogue gives every op at most its default deadline today).
func (d *MSKDriver) wait(ctx context.Context, raw json.RawMessage) (ops.Result, error) {
	var p WaitParams
	_ = decode(raw, &p)
	polls := 0
	for {
		polls++
		var state, code, msg string
		if p.Kind == "plugin" {
			pl, err := d.api.DescribePlugin(ctx, p.Arn)
			if err != nil {
				return d.result("", errData(err)), err
			}
			state, code, msg = pl.State, pl.StateCode, pl.StateMessage
		} else {
			cn, err := d.api.DescribeConnector(ctx, p.Arn)
			if err != nil {
				return d.result("", errData(err)), err
			}
			state, code, msg = cn.State, cn.StateCode, cn.StateMessage
		}
		data := map[string]any{"kind": p.Kind, "arn": p.Arn, "state": state, "state_code": code, "state_message": msg, "polls": polls}
		switch state {
		case "ACTIVE", "RUNNING":
			data["done"] = true
			return d.result(fmt.Sprintf("the %s is %s", p.Kind, state), data), nil
		case "CREATE_FAILED", "UPDATE_FAILED", "FAILED", "DELETING":
			data["done"] = true
			return d.result(fmt.Sprintf("the %s is %s: %s", p.Kind, state, clipText(code+" "+msg, 200)), data),
				fmt.Errorf("%w: the %s is %s", ErrCheckFailed, p.Kind, state)
		}
		dl, ok := ctx.Deadline()
		if ok && time.Until(dl) < d.pollEvery+2*time.Second {
			data["done"] = false
			return d.result(fmt.Sprintf("the %s is still %s; run msk_wait again", p.Kind, state), data), nil
		}
		t := time.NewTimer(d.pollEvery)
		select {
		case <-ctx.Done():
			t.Stop()
			return d.result("", map[string]any{"state": state}), fmt.Errorf("managed: msk_wait: %w", ctx.Err())
		case <-t.C:
		}
	}
}

// ---- writes ----

func (d *MSKDriver) tags() map[string]string {
	return map[string]string{TagRoom: d.room, TagCreatedBy: createdBy}
}

func (d *MSKDriver) createPlugin(ctx context.Context, raw json.RawMessage) (ops.Result, error) {
	var p CreatePluginParams
	_ = decode(raw, &p)
	if res, err := d.objectCheck(ctx, p.Bucket, p.Key, p.Version, p.ExpectedSize); err != nil {
		res.Summary = "refused msk_create_plugin: the plugin archive check failed"
		return res, err
	}
	created, err := d.api.CreatePlugin(ctx, mskconnect.PluginSpec{
		Name: p.Name, ContentType: p.ContentType, BucketArn: "arn:aws:s3:::" + p.Bucket, FileKey: p.Key, ObjectVersion: p.Version,
		Description: "created by piperoom-setup", Tags: d.tags(),
	})
	if err != nil {
		return d.result("", errData(err)), err
	}
	return d.result(fmt.Sprintf("custom plugin %s is %s (revision %d); wait for ACTIVE with msk_wait", created.Name, created.State, created.Revision), created), nil
}

func (d *MSKDriver) createWorkerConfig(ctx context.Context, raw json.RawMessage) (ops.Result, error) {
	var p WorkerParams
	_ = decode(raw, &p)
	text, err := RenderWorkerProperties(p.Entries)
	if err != nil {
		return ops.Result{}, err
	}
	wc, err := d.api.CreateWorkerConfig(ctx, mskconnect.WorkerConfigSpec{Name: p.Name, Properties: text, Tags: d.tags()})
	if err != nil {
		return d.result("", errData(err)), err
	}
	return d.result(fmt.Sprintf("worker configuration %s is %s (revision %d)", wc.Name, wc.State, wc.Revision), map[string]any{"worker_config": wc, "properties": text}), nil
}

func (d *MSKDriver) createConnector(ctx context.Context, raw json.RawMessage) (ops.Result, error) {
	var p CreateConnectorParams
	_ = decode(raw, &p)
	clusterArn, _ := d.clusterOf(p.ClusterArn)
	cl, err := d.api.DescribeCluster(ctx, clusterArn)
	if err != nil {
		return d.result("", errData(err)), err
	}
	if cl.BootstrapIAM == "" {
		return d.result("refused msk_create_connector: the cluster has no IAM listener", map[string]any{"cluster": cl.Name}),
			fmt.Errorf("%w: cluster %s has no IAM bootstrap brokers (MSK Connect authenticates with IAM in v1)", ErrCheckFailed, cl.Name)
	}
	cfg, err := materialise(d.secrets, p.Config)
	if err != nil {
		return ops.Result{}, err
	}
	pluginRev := p.PluginRevision
	if pluginRev == 0 {
		pluginRev = 1
	}
	workerRev := p.WorkerConfigRevision
	if p.WorkerConfigArn != "" && workerRev == 0 {
		workerRev = 1
	}
	created, err := d.api.CreateConnector(ctx, mskconnect.ConnectorSpec{
		Name: p.Name, KafkaConnectVersion: p.KafkaConnectVersion, BootstrapServers: cl.BootstrapIAM,
		Subnets: p.Subnets, SecurityGroups: p.SecurityGroups, PluginArn: p.PluginArn, PluginRevision: pluginRev,
		WorkerConfigArn: p.WorkerConfigArn, WorkerConfigRevision: workerRev, ServiceRoleArn: p.ServiceRoleArn,
		Config: cfg, LogGroup: p.LogGroup, Tags: d.tags(),
	})
	if err != nil {
		return d.result("", errData(err)), err
	}
	// Data carries the config as the person wrote it: references, never the
	// materialised values.
	return d.result(fmt.Sprintf("MSK connector %s is %s; MSK provisions it for 5 to 15 minutes, wait with msk_wait", created.Name, created.State),
		map[string]any{"connector": created, "config": p.Config}), nil
}

func (d *MSKDriver) deleteConnector(ctx context.Context, raw json.RawMessage) (ops.Result, error) {
	var p NameParams
	_ = decode(raw, &p)
	arn, err := d.api.FindConnector(ctx, p.Name)
	if err != nil {
		return d.result("", errData(err)), err
	}
	tags, err := d.api.ConnectorTags(ctx, arn)
	if err != nil {
		return d.result("", errData(err)), err
	}
	if tags[TagRoom] != d.room {
		return d.result("refused msk_delete_connector: "+p.Name+" was not created by this room", map[string]any{"name": p.Name}),
			fmt.Errorf("%w: connector %s carries no %s tag of this room", ErrNotCreatedByRoom, p.Name, TagRoom)
	}
	cn, err := d.api.DescribeConnector(ctx, arn)
	if err != nil {
		return d.result("", errData(err)), err
	}
	out, err := d.api.DeleteConnector(ctx, arn, cn.CurrentVersion)
	if err != nil {
		return d.result("", errData(err)), err
	}
	return d.result(fmt.Sprintf("MSK connector %s is %s", p.Name, out.State), map[string]any{"name": p.Name, "arn": arn, "state": out.State}), nil
}

// ---- previews ----

func (d *MSKDriver) previewPlugin(raw json.RawMessage) (events.PreviewResult, error) {
	var p CreatePluginParams
	if err := decode(raw, &p); err != nil {
		return events.PreviewResult{}, err
	}
	return events.PreviewResult{
		Expected:   fmt.Sprintf("custom plugin %s from s3://%s/%s (%s); size checked against the index, digest unverified", p.Name, p.Bucket, p.Key, p.ContentType),
		Reversible: false,
	}, nil
}

func (d *MSKDriver) previewWorker(raw json.RawMessage) (events.PreviewResult, error) {
	var p WorkerParams
	if err := decode(raw, &p); err != nil {
		return events.PreviewResult{}, err
	}
	text, err := RenderWorkerProperties(p.Entries)
	if err != nil {
		return events.PreviewResult{}, err
	}
	return events.PreviewResult{Expected: text, Reversible: false}, nil
}

func (d *MSKDriver) previewConnector(raw json.RawMessage) (events.PreviewResult, error) {
	var p CreateConnectorParams
	if err := decode(raw, &p); err != nil {
		return events.PreviewResult{}, err
	}
	b, _ := json.Marshal(p.Config)
	return events.PreviewResult{
		Expected:   fmt.Sprintf("connector %s on Kafka Connect %s, 1 MCU x 1 worker, IAM auth, TLS; config %s", p.Name, p.KafkaConnectVersion, b),
		Reversible: true,
		Rollback:   fmt.Sprintf(`msk_delete_connector {"name":%q}`, p.Name),
	}, nil
}

func (d *MSKDriver) previewDelete(raw json.RawMessage) (events.PreviewResult, error) {
	var p NameParams
	if err := decode(raw, &p); err != nil {
		return events.PreviewResult{}, err
	}
	return events.PreviewResult{Current: "connector " + p.Name, Expected: "connector " + p.Name + " deleted (only if this room created it)", Reversible: false}, nil
}

// errData is the Data of a failed provider call: the error class, never the
// request.
func errData(err error) map[string]any {
	return map[string]any{"error_class": mskconnect.ErrorClass(err)}
}
