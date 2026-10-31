package mskconnect

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/aws-sdk-go-v2/service/iam"
	"github.com/aws/aws-sdk-go-v2/service/kafka"
	"github.com/aws/aws-sdk-go-v2/service/kafkaconnect"
	kctypes "github.com/aws/aws-sdk-go-v2/service/kafkaconnect/types"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"

	"piperoom/internal/actions"
)

// maxPages bounds every paginated list: a setup account has a handful of
// plugins and connectors, and an unbounded walk is a cost the person did not
// ask for.
const maxPages = 10

// Identity is the caller of sts.GetCallerIdentity. Arn can carry a
// person's name (an IAM user, an SSO session); it belongs in Data, never in
// a Summary or an error.
type Identity struct {
	Account string `json:"account"`
	Arn     string `json:"arn"`
	// Principal is the role or user name from the ARN, without the session.
	Principal string `json:"principal"`
}

// Cluster is an MSK cluster as setup needs it.
type Cluster struct {
	Arn            string   `json:"arn"`
	Name           string   `json:"name"`
	Type           string   `json:"type"`  // PROVISIONED | SERVERLESS
	State          string   `json:"state"` // ACTIVE, CREATING, ...
	KafkaVersion   string   `json:"kafka_version,omitempty"`
	Subnets        []string `json:"subnets"`
	SecurityGroups []string `json:"security_groups"`
	// Bootstrap strings per listener; empty when the listener is off.
	BootstrapIAM   string `json:"bootstrap_iam,omitempty"`
	BootstrapSCRAM string `json:"bootstrap_scram,omitempty"`
	BootstrapTLS   string `json:"bootstrap_tls,omitempty"`
}

// Plugin is a custom plugin.
type Plugin struct {
	Arn         string `json:"arn"`
	Name        string `json:"name"`
	State       string `json:"state"` // CREATING | CREATE_FAILED | ACTIVE | ...
	Revision    int64  `json:"revision"`
	ContentType string `json:"content_type,omitempty"`
	// StateCode and StateMessage are MSK Connect's reason for a failed
	// state; untrusted text.
	StateCode    string `json:"state_code,omitempty"`
	StateMessage string `json:"state_message,omitempty"`
}

// Connector is a connector's description. Config is redacted.
type Connector struct {
	Arn                 string            `json:"arn"`
	Name                string            `json:"name"`
	State               string            `json:"state"` // RUNNING | CREATING | UPDATING | DELETING | FAILED
	CurrentVersion      string            `json:"current_version,omitempty"`
	KafkaConnectVersion string            `json:"kafka_connect_version,omitempty"`
	StateCode           string            `json:"state_code,omitempty"`
	StateMessage        string            `json:"state_message,omitempty"`
	Config              map[string]string `json:"config,omitempty"`
	PluginArns          []string          `json:"plugin_arns,omitempty"`
	WorkerConfigArn     string            `json:"worker_config_arn,omitempty"`
	ServiceRoleArn      string            `json:"service_role_arn,omitempty"`
	LogGroup            string            `json:"log_group,omitempty"`
	Subnets             []string          `json:"subnets,omitempty"`
	SecurityGroups      []string          `json:"security_groups,omitempty"`
}

// Subnet is one subnet.
type Subnet struct {
	ID   string `json:"id"`
	VPC  string `json:"vpc"`
	CIDR string `json:"cidr"`
	AZ   string `json:"az,omitempty"`
}

// Rule is one security group rule.
type Rule struct {
	Protocol string   `json:"protocol"` // "-1" is every protocol
	FromPort int32    `json:"from_port"`
	ToPort   int32    `json:"to_port"`
	CIDRs    []string `json:"cidrs,omitempty"`
	Groups   []string `json:"groups,omitempty"`
}

// SecurityGroup is one group with its rules.
type SecurityGroup struct {
	ID      string `json:"id"`
	VPC     string `json:"vpc"`
	Ingress []Rule `json:"ingress"`
	Egress  []Rule `json:"egress"`
}

// Route is one route of a route table.
type Route struct {
	Destination string `json:"destination"`
	Target      string `json:"target"` // "local", igw-..., nat-..., tgw-..., pcx-...
	State       string `json:"state,omitempty"`
}

// RouteTable is one route table and the subnets it is associated with.
type RouteTable struct {
	ID      string   `json:"id"`
	VPC     string   `json:"vpc"`
	Main    bool     `json:"main"`
	Subnets []string `json:"subnets,omitempty"`
	Routes  []Route  `json:"routes"`
}

// Network is what DescribeNetwork read.
type Network struct {
	Subnets        []Subnet        `json:"subnets"`
	SecurityGroups []SecurityGroup `json:"security_groups"`
	RouteTables    []RouteTable    `json:"route_tables"`
}

// Role is a service execution role.
type Role struct {
	Arn  string `json:"arn"`
	Name string `json:"name"`
	// TrustsKafkaConnect says the trust policy names
	// kafkaconnect.amazonaws.com as a principal.
	TrustsKafkaConnect bool `json:"trusts_kafkaconnect"`
	// TrustProblem is why the trust policy does not trust MSK Connect
	// alone (see TrustProblem), "" when it does.
	TrustProblem string `json:"trust_problem,omitempty"`
}

// Decision is one action's result of iam.SimulatePrincipalPolicy.
type Decision struct {
	Action   string `json:"action"`
	Decision string `json:"decision"` // allowed | explicitDeny | implicitDeny
}

// Object is the plugin archive's metadata from s3.HeadObject.
type Object struct {
	Bucket    string `json:"bucket"`
	Key       string `json:"key"`
	VersionID string `json:"version_id,omitempty"`
	Size      int64  `json:"size"`
}

// Identity returns the caller of the configured credentials.
func (c *Client) Identity(ctx context.Context) (Identity, error) {
	out, err := call(ctx, c, "get caller identity", func(ctx context.Context) (*sts.GetCallerIdentityOutput, error) {
		return c.sts.GetCallerIdentity(ctx, &sts.GetCallerIdentityInput{})
	})
	if err != nil {
		return Identity{}, err
	}
	id := Identity{Account: str(out.Account), Arn: str(out.Arn)}
	id.Principal = principalOf(id.Arn)
	return id, nil
}

// principalOf returns the role or user name of an STS or IAM ARN:
// arn:aws:sts::1:assumed-role/Role/session -> Role; arn:aws:iam::1:user/u -> u.
func principalOf(arn string) string {
	i := strings.LastIndexByte(arn, ':')
	if i < 0 {
		return ""
	}
	res := strings.Split(arn[i+1:], "/")
	switch {
	case len(res) >= 2 && res[0] == "assumed-role":
		return res[1]
	case len(res) >= 2:
		return res[len(res)-1]
	}
	return ""
}

// DescribeCluster reads an MSK cluster and its bootstrap brokers.
func (c *Client) DescribeCluster(ctx context.Context, arn string) (Cluster, error) {
	if err := CheckClusterARN(arn); err != nil {
		return Cluster{}, err
	}
	out, err := call(ctx, c, "describe cluster", func(ctx context.Context) (*kafka.DescribeClusterV2Output, error) {
		return c.kf.DescribeClusterV2(ctx, &kafka.DescribeClusterV2Input{ClusterArn: aws.String(arn)})
	})
	if err != nil {
		return Cluster{}, err
	}
	if out.ClusterInfo == nil {
		return Cluster{}, fmt.Errorf("%w: describe cluster: the response has no cluster", ErrAWS)
	}
	ci := out.ClusterInfo
	cl := Cluster{Arn: str(ci.ClusterArn), Name: str(ci.ClusterName), Type: string(ci.ClusterType), State: string(ci.State)}
	if p := ci.Provisioned; p != nil {
		if p.BrokerNodeGroupInfo != nil {
			cl.Subnets = append(cl.Subnets, p.BrokerNodeGroupInfo.ClientSubnets...)
			cl.SecurityGroups = append(cl.SecurityGroups, p.BrokerNodeGroupInfo.SecurityGroups...)
		}
		if p.CurrentBrokerSoftwareInfo != nil {
			cl.KafkaVersion = str(p.CurrentBrokerSoftwareInfo.KafkaVersion)
		}
	}
	if s := ci.Serverless; s != nil {
		for _, v := range s.VpcConfigs {
			cl.Subnets = append(cl.Subnets, v.SubnetIds...)
			cl.SecurityGroups = append(cl.SecurityGroups, v.SecurityGroupIds...)
		}
	}
	bb, err := call(ctx, c, "get bootstrap brokers", func(ctx context.Context) (*kafka.GetBootstrapBrokersOutput, error) {
		return c.kf.GetBootstrapBrokers(ctx, &kafka.GetBootstrapBrokersInput{ClusterArn: aws.String(arn)})
	})
	if err != nil {
		return Cluster{}, err
	}
	cl.BootstrapIAM = str(bb.BootstrapBrokerStringSaslIam)
	cl.BootstrapSCRAM = str(bb.BootstrapBrokerStringSaslScram)
	cl.BootstrapTLS = str(bb.BootstrapBrokerStringTls)
	return cl, nil
}

// ListPlugins lists custom plugins whose name starts with prefix.
func (c *Client) ListPlugins(ctx context.Context, prefix string) ([]Plugin, error) {
	if prefix != "" {
		if err := CheckName(prefix); err != nil {
			return nil, err
		}
	}
	var out []Plugin
	var next *string
	for page := 0; page < maxPages; page++ {
		in := &kafkaconnect.ListCustomPluginsInput{MaxResults: aws.Int32(100), NextToken: next}
		if prefix != "" {
			in.NamePrefix = aws.String(prefix)
		}
		res, err := call(ctx, c, "list custom plugins", func(ctx context.Context) (*kafkaconnect.ListCustomPluginsOutput, error) {
			return c.kc.ListCustomPlugins(ctx, in)
		})
		if err != nil {
			return nil, err
		}
		for _, p := range res.CustomPlugins {
			pl := Plugin{Arn: str(p.CustomPluginArn), Name: str(p.Name), State: string(p.CustomPluginState)}
			if p.LatestRevision != nil {
				pl.Revision = p.LatestRevision.Revision
				pl.ContentType = string(p.LatestRevision.ContentType)
			}
			out = append(out, pl)
		}
		if res.NextToken == nil || *res.NextToken == "" {
			break
		}
		next = res.NextToken
	}
	return out, nil
}

// DescribePlugin reads one custom plugin.
func (c *Client) DescribePlugin(ctx context.Context, arn string) (Plugin, error) {
	if err := CheckResourceARN(arn); err != nil {
		return Plugin{}, err
	}
	out, err := call(ctx, c, "describe custom plugin", func(ctx context.Context) (*kafkaconnect.DescribeCustomPluginOutput, error) {
		return c.kc.DescribeCustomPlugin(ctx, &kafkaconnect.DescribeCustomPluginInput{CustomPluginArn: aws.String(arn)})
	})
	if err != nil {
		return Plugin{}, err
	}
	pl := Plugin{Arn: str(out.CustomPluginArn), Name: str(out.Name), State: string(out.CustomPluginState)}
	if out.LatestRevision != nil {
		pl.Revision = out.LatestRevision.Revision
		pl.ContentType = string(out.LatestRevision.ContentType)
	}
	pl.StateCode, pl.StateMessage = stateDesc(out.StateDescription)
	return pl, nil
}

func stateDesc(d *kctypes.StateDescription) (string, string) {
	if d == nil {
		return "", ""
	}
	return clip(str(d.Code), 64), clip(str(d.Message), 1024)
}

// FindConnector returns the ARN of the connector named name, ErrNotFound
// when there is none. ListConnectors filters by prefix; the exact name is
// matched here.
func (c *Client) FindConnector(ctx context.Context, name string) (string, error) {
	if err := CheckName(name); err != nil {
		return "", err
	}
	sums, err := c.listConnectors(ctx, name)
	if err != nil {
		return "", err
	}
	for _, s := range sums {
		if str(s.ConnectorName) == name {
			return str(s.ConnectorArn), nil
		}
	}
	return "", fmt.Errorf("%w: connector %q", ErrNotFound, name)
}

func (c *Client) listConnectors(ctx context.Context, prefix string) ([]kctypes.ConnectorSummary, error) {
	var out []kctypes.ConnectorSummary
	var next *string
	for page := 0; page < maxPages; page++ {
		in := &kafkaconnect.ListConnectorsInput{MaxResults: aws.Int32(100), NextToken: next}
		if prefix != "" {
			in.ConnectorNamePrefix = aws.String(prefix)
		}
		res, err := call(ctx, c, "list connectors", func(ctx context.Context) (*kafkaconnect.ListConnectorsOutput, error) {
			return c.kc.ListConnectors(ctx, in)
		})
		if err != nil {
			return nil, err
		}
		out = append(out, res.Connectors...)
		if res.NextToken == nil || *res.NextToken == "" {
			break
		}
		next = res.NextToken
	}
	return out, nil
}

// DescribeConnector reads one connector. Its configuration is redacted
// before it is returned (design §10 D6: MSK Connect returns the materialised
// database password here).
func (c *Client) DescribeConnector(ctx context.Context, arn string) (Connector, error) {
	if err := CheckResourceARN(arn); err != nil {
		return Connector{}, err
	}
	out, err := call(ctx, c, "describe connector", func(ctx context.Context) (*kafkaconnect.DescribeConnectorOutput, error) {
		return c.kc.DescribeConnector(ctx, &kafkaconnect.DescribeConnectorInput{ConnectorArn: aws.String(arn)})
	})
	if err != nil {
		return Connector{}, err
	}
	cn := Connector{
		Arn:                 str(out.ConnectorArn),
		Name:                str(out.ConnectorName),
		State:               string(out.ConnectorState),
		CurrentVersion:      str(out.CurrentVersion),
		KafkaConnectVersion: str(out.KafkaConnectVersion),
		ServiceRoleArn:      str(out.ServiceExecutionRoleArn),
	}
	if out.ConnectorConfiguration != nil {
		cn.Config = actions.RedactConfig(out.ConnectorConfiguration)
	}
	cn.StateCode, cn.StateMessage = stateDesc(out.StateDescription)
	for _, p := range out.Plugins {
		if p.CustomPlugin != nil {
			cn.PluginArns = append(cn.PluginArns, str(p.CustomPlugin.CustomPluginArn))
		}
	}
	if out.WorkerConfiguration != nil {
		cn.WorkerConfigArn = str(out.WorkerConfiguration.WorkerConfigurationArn)
	}
	if ld := out.LogDelivery; ld != nil && ld.WorkerLogDelivery != nil && ld.WorkerLogDelivery.CloudWatchLogs != nil {
		cn.LogGroup = str(ld.WorkerLogDelivery.CloudWatchLogs.LogGroup)
	}
	if kc := out.KafkaCluster; kc != nil && kc.ApacheKafkaCluster != nil && kc.ApacheKafkaCluster.Vpc != nil {
		cn.Subnets = append(cn.Subnets, kc.ApacheKafkaCluster.Vpc.Subnets...)
		cn.SecurityGroups = append(cn.SecurityGroups, kc.ApacheKafkaCluster.Vpc.SecurityGroups...)
	}
	return cn, nil
}

// ConnectorTags returns the tags of a connector (the guard's
// "created by this room" check reads them).
func (c *Client) ConnectorTags(ctx context.Context, arn string) (map[string]string, error) {
	if err := CheckResourceARN(arn); err != nil {
		return nil, err
	}
	out, err := call(ctx, c, "list tags", func(ctx context.Context) (*kafkaconnect.ListTagsForResourceOutput, error) {
		return c.kc.ListTagsForResource(ctx, &kafkaconnect.ListTagsForResourceInput{ResourceArn: aws.String(arn)})
	})
	if err != nil {
		return nil, err
	}
	tags := make(map[string]string, len(out.Tags))
	for k, v := range out.Tags {
		tags[k] = v
	}
	return tags, nil
}

// DescribeNetwork reads the subnets and security groups named, and the
// route tables associated with those subnets or main in their VPCs.
func (c *Client) DescribeNetwork(ctx context.Context, subnets, groups []string) (Network, error) {
	if len(subnets) == 0 || len(subnets) > 16 || len(groups) > 16 {
		return Network{}, fmt.Errorf("%w: 1 to 16 subnets and at most 16 security groups", ErrInvalid)
	}
	for _, s := range subnets {
		if err := CheckSubnet(s); err != nil {
			return Network{}, err
		}
	}
	for _, g := range groups {
		if err := CheckSecurityGroup(g); err != nil {
			return Network{}, err
		}
	}
	var n Network
	sn, err := call(ctx, c, "describe subnets", func(ctx context.Context) (*ec2.DescribeSubnetsOutput, error) {
		return c.ec2.DescribeSubnets(ctx, &ec2.DescribeSubnetsInput{SubnetIds: subnets})
	})
	if err != nil {
		return Network{}, err
	}
	vpcs := map[string]bool{}
	for _, s := range sn.Subnets {
		n.Subnets = append(n.Subnets, Subnet{ID: str(s.SubnetId), VPC: str(s.VpcId), CIDR: str(s.CidrBlock), AZ: str(s.AvailabilityZone)})
		vpcs[str(s.VpcId)] = true
	}
	if len(groups) > 0 {
		sg, err := call(ctx, c, "describe security groups", func(ctx context.Context) (*ec2.DescribeSecurityGroupsOutput, error) {
			return c.ec2.DescribeSecurityGroups(ctx, &ec2.DescribeSecurityGroupsInput{GroupIds: groups})
		})
		if err != nil {
			return Network{}, err
		}
		for _, g := range sg.SecurityGroups {
			n.SecurityGroups = append(n.SecurityGroups, SecurityGroup{
				ID: str(g.GroupId), VPC: str(g.VpcId),
				Ingress: rules(g.IpPermissions), Egress: rules(g.IpPermissionsEgress),
			})
		}
	}
	vpcList := make([]string, 0, len(vpcs))
	for v := range vpcs {
		if v != "" {
			vpcList = append(vpcList, v)
		}
	}
	sort.Strings(vpcList)
	if len(vpcList) == 0 {
		return n, nil
	}
	rt, err := call(ctx, c, "describe route tables", func(ctx context.Context) (*ec2.DescribeRouteTablesOutput, error) {
		return c.ec2.DescribeRouteTables(ctx, &ec2.DescribeRouteTablesInput{
			Filters: []ec2types.Filter{{Name: aws.String("vpc-id"), Values: vpcList}},
		})
	})
	if err != nil {
		return Network{}, err
	}
	for _, t := range rt.RouteTables {
		table := RouteTable{ID: str(t.RouteTableId), VPC: str(t.VpcId)}
		for _, a := range t.Associations {
			if a.Main != nil && *a.Main {
				table.Main = true
			}
			if s := str(a.SubnetId); s != "" {
				table.Subnets = append(table.Subnets, s)
			}
		}
		for _, r := range t.Routes {
			table.Routes = append(table.Routes, Route{Destination: routeDest(r), Target: routeTarget(r), State: string(r.State)})
		}
		n.RouteTables = append(n.RouteTables, table)
	}
	return n, nil
}

func rules(perms []ec2types.IpPermission) []Rule {
	out := make([]Rule, 0, len(perms))
	for _, p := range perms {
		r := Rule{Protocol: str(p.IpProtocol)}
		if p.FromPort != nil {
			r.FromPort = *p.FromPort
		}
		if p.ToPort != nil {
			r.ToPort = *p.ToPort
		}
		for _, ip := range p.IpRanges {
			r.CIDRs = append(r.CIDRs, str(ip.CidrIp))
		}
		for _, g := range p.UserIdGroupPairs {
			r.Groups = append(r.Groups, str(g.GroupId))
		}
		out = append(out, r)
	}
	return out
}

func routeDest(r ec2types.Route) string {
	switch {
	case r.DestinationCidrBlock != nil:
		return *r.DestinationCidrBlock
	case r.DestinationIpv6CidrBlock != nil:
		return *r.DestinationIpv6CidrBlock
	case r.DestinationPrefixListId != nil:
		return *r.DestinationPrefixListId
	}
	return ""
}

func routeTarget(r ec2types.Route) string {
	for _, p := range []*string{r.GatewayId, r.NatGatewayId, r.TransitGatewayId, r.VpcPeeringConnectionId, r.NetworkInterfaceId, r.InstanceId, r.LocalGatewayId, r.CarrierGatewayId, r.EgressOnlyInternetGatewayId} {
		if p != nil && *p != "" {
			return *p
		}
	}
	return ""
}

// GetRole reads a service execution role and whether it trusts MSK
// Connect.
func (c *Client) GetRole(ctx context.Context, arn string) (Role, error) {
	if err := CheckRoleARN(arn); err != nil {
		return Role{}, err
	}
	name := arn[strings.LastIndexByte(arn, '/')+1:]
	out, err := call(ctx, c, "get role", func(ctx context.Context) (*iam.GetRoleOutput, error) {
		return c.iam.GetRole(ctx, &iam.GetRoleInput{RoleName: aws.String(name)})
	})
	if err != nil {
		return Role{}, err
	}
	if out.Role == nil {
		return Role{}, fmt.Errorf("%w: get role: the response has no role", ErrAWS)
	}
	r := Role{Arn: str(out.Role.Arn), Name: str(out.Role.RoleName)}
	// IAM returns the trust policy URL-encoded (RFC 3986).
	doc, uerr := url.QueryUnescape(str(out.Role.AssumeRolePolicyDocument))
	if uerr != nil {
		doc = str(out.Role.AssumeRolePolicyDocument)
	}
	r.TrustsKafkaConnect = strings.Contains(doc, KafkaConnectService)
	r.TrustProblem = TrustProblem(doc)
	return r, nil
}

// SimulateRole asks IAM whether the role may perform actions (on any
// resource). The semantics of the simulation for the MSK IAM actions are
// not confirmed on a sandbox (design §3.1).
func (c *Client) SimulateRole(ctx context.Context, arn string, actionNames []string) ([]Decision, error) {
	if err := CheckRoleARN(arn); err != nil {
		return nil, err
	}
	if len(actionNames) == 0 || len(actionNames) > 16 {
		return nil, fmt.Errorf("%w: 1 to 16 actions", ErrInvalid)
	}
	out, err := call(ctx, c, "simulate principal policy", func(ctx context.Context) (*iam.SimulatePrincipalPolicyOutput, error) {
		return c.iam.SimulatePrincipalPolicy(ctx, &iam.SimulatePrincipalPolicyInput{PolicySourceArn: aws.String(arn), ActionNames: actionNames})
	})
	if err != nil {
		return nil, err
	}
	ds := make([]Decision, 0, len(out.EvaluationResults))
	for _, r := range out.EvaluationResults {
		ds = append(ds, Decision{Action: str(r.EvalActionName), Decision: string(r.EvalDecision)})
	}
	return ds, nil
}

// expectedOwnerRE is an AWS account id, the only ExpectedBucketOwner S3
// takes.
var expectedOwnerRE = regexp.MustCompile(`^[0-9]{12}$`)

// HeadObject reads the plugin archive's size and version. It never reads
// the object. expectedOwner is the account the bucket must belong to (S3's
// ExpectedBucketOwner, required): a bucket of the same name in another
// account answers 403 instead of handing that account's archive to the
// plugin check.
func (c *Client) HeadObject(ctx context.Context, bucket, key, version, expectedOwner string) (Object, error) {
	if !expectedOwnerRE.MatchString(expectedOwner) {
		return Object{}, fmt.Errorf("%w: the expected bucket owner must be a 12-digit account id", ErrInvalid)
	}
	if err := CheckBucket(bucket); err != nil {
		return Object{}, err
	}
	if err := CheckObjectKey(key); err != nil {
		return Object{}, err
	}
	if err := CheckObjectVersion(version); err != nil {
		return Object{}, err
	}
	in := &s3.HeadObjectInput{Bucket: aws.String(bucket), Key: aws.String(key), ExpectedBucketOwner: aws.String(expectedOwner)}
	if version != "" {
		in.VersionId = aws.String(version)
	}
	out, err := call(ctx, c, "head object", func(ctx context.Context) (*s3.HeadObjectOutput, error) {
		return c.s3.HeadObject(ctx, in)
	})
	if err != nil {
		return Object{}, err
	}
	o := Object{Bucket: bucket, Key: key, VersionID: str(out.VersionId)}
	if out.ContentLength != nil {
		o.Size = *out.ContentLength
	}
	return o, nil
}
