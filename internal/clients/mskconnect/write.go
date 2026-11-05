package mskconnect

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kafkaconnect"
	kctypes "github.com/aws/aws-sdk-go-v2/service/kafkaconnect/types"
)

// Content types of a custom plugin archive.
const (
	ContentZIP = "ZIP"
	ContentJAR = "JAR"
)

// maxProperties caps a worker configuration's properties text.
const maxProperties = 64 << 10

// maxConfigKeys caps a connector configuration.
const maxConfigKeys = 256

// PluginSpec creates a custom plugin from an archive the person uploaded.
type PluginSpec struct {
	Name          string
	ContentType   string // ContentZIP | ContentJAR
	BucketArn     string
	FileKey       string
	ObjectVersion string
	Description   string
	Tags          map[string]string
}

// WorkerConfigSpec creates a worker configuration. Properties is the
// connect-distributed.properties text; the client base64-encodes it, as the
// API requires.
type WorkerConfigSpec struct {
	Name       string
	Properties string
	Tags       map[string]string
}

// WorkerConfig is a created worker configuration.
type WorkerConfig struct {
	Arn      string `json:"arn"`
	Name     string `json:"name"`
	Revision int64  `json:"revision"`
	State    string `json:"state"`
}

// ConnectorSpec creates a connector with 1 MCU and 1 worker (design §3.1:
// capacity is not a param in v1), IAM client authentication and TLS in
// transit.
type ConnectorSpec struct {
	Name                 string
	KafkaConnectVersion  string
	BootstrapServers     string
	Subnets              []string
	SecurityGroups       []string
	PluginArn            string
	PluginRevision       int64
	WorkerConfigArn      string
	WorkerConfigRevision int64
	ServiceRoleArn       string
	// Config is the connector configuration with every ${secret:} reference
	// already materialised on the runner. It is sent once and is in no
	// return value, error or log.
	Config   map[string]string
	LogGroup string
	Tags     map[string]string
}

// Created is what a create call returned.
type Created struct {
	Arn   string `json:"arn"`
	Name  string `json:"name"`
	State string `json:"state"`
	// Revision is the plugin's revision (CreatePlugin only).
	Revision int64 `json:"revision,omitempty"`
}

func (s PluginSpec) check() error {
	if err := CheckName(s.Name); err != nil {
		return err
	}
	if s.ContentType != ContentZIP && s.ContentType != ContentJAR {
		return fmt.Errorf("%w: content type %q, want ZIP or JAR", ErrInvalid, clip(s.ContentType, 16))
	}
	if err := CheckBucketARN(s.BucketArn); err != nil {
		return err
	}
	if err := CheckObjectKey(s.FileKey); err != nil {
		return err
	}
	if err := CheckObjectVersion(s.ObjectVersion); err != nil {
		return err
	}
	if len(s.Description) > 256 {
		return fmt.Errorf("%w: description over 256 bytes", ErrInvalid)
	}
	return CheckTags(s.Tags)
}

// CreatePlugin creates a custom plugin. MSK Connect builds it
// asynchronously: the result is CREATING, and DescribePlugin polls it.
func (c *Client) CreatePlugin(ctx context.Context, s PluginSpec) (Created, error) {
	if err := s.check(); err != nil {
		return Created{}, err
	}
	in := &kafkaconnect.CreateCustomPluginInput{
		Name:        aws.String(s.Name),
		ContentType: kctypes.CustomPluginContentType(s.ContentType),
		Location: &kctypes.CustomPluginLocation{S3Location: &kctypes.S3Location{
			BucketArn: aws.String(s.BucketArn), FileKey: aws.String(s.FileKey),
		}},
		Tags: s.Tags,
	}
	if s.ObjectVersion != "" {
		in.Location.S3Location.ObjectVersion = aws.String(s.ObjectVersion)
	}
	if s.Description != "" {
		in.Description = aws.String(s.Description)
	}
	out, err := call(ctx, c, "create custom plugin", func(ctx context.Context) (*kafkaconnect.CreateCustomPluginOutput, error) {
		return c.kc.CreateCustomPlugin(ctx, in)
	})
	if err != nil {
		return Created{}, err
	}
	return Created{Arn: str(out.CustomPluginArn), Name: str(out.Name), State: string(out.CustomPluginState), Revision: out.Revision}, nil
}

// CreateWorkerConfig creates a worker configuration from properties text.
// The caller renders the text from its template under the key allow-list;
// this client only bounds it.
func (c *Client) CreateWorkerConfig(ctx context.Context, s WorkerConfigSpec) (WorkerConfig, error) {
	if err := CheckName(s.Name); err != nil {
		return WorkerConfig{}, err
	}
	if s.Properties == "" || len(s.Properties) > maxProperties {
		return WorkerConfig{}, fmt.Errorf("%w: properties text is empty or over %d bytes", ErrInvalid, maxProperties)
	}
	if err := CheckTags(s.Tags); err != nil {
		return WorkerConfig{}, err
	}
	in := &kafkaconnect.CreateWorkerConfigurationInput{
		Name:                  aws.String(s.Name),
		PropertiesFileContent: aws.String(base64.StdEncoding.EncodeToString([]byte(s.Properties))),
		Tags:                  s.Tags,
	}
	out, err := call(ctx, c, "create worker configuration", func(ctx context.Context) (*kafkaconnect.CreateWorkerConfigurationOutput, error) {
		return c.kc.CreateWorkerConfiguration(ctx, in)
	})
	if err != nil {
		return WorkerConfig{}, err
	}
	wc := WorkerConfig{Arn: str(out.WorkerConfigurationArn), Name: str(out.Name), State: string(out.WorkerConfigurationState)}
	if out.LatestRevision != nil {
		wc.Revision = out.LatestRevision.Revision
	}
	return wc, nil
}

func (s ConnectorSpec) check() error {
	if err := CheckName(s.Name); err != nil {
		return err
	}
	if err := CheckConnectVersion(s.KafkaConnectVersion); err != nil {
		return err
	}
	if s.BootstrapServers == "" || len(s.BootstrapServers) > 2048 {
		return fmt.Errorf("%w: bootstrap servers empty or over 2048 bytes", ErrInvalid)
	}
	if len(s.Subnets) == 0 || len(s.Subnets) > 16 || len(s.SecurityGroups) == 0 || len(s.SecurityGroups) > 16 {
		return fmt.Errorf("%w: 1 to 16 subnets and 1 to 16 security groups", ErrInvalid)
	}
	for _, v := range s.Subnets {
		if err := CheckSubnet(v); err != nil {
			return err
		}
	}
	for _, v := range s.SecurityGroups {
		if err := CheckSecurityGroup(v); err != nil {
			return err
		}
	}
	if err := CheckResourceARN(s.PluginArn); err != nil {
		return err
	}
	if s.WorkerConfigArn != "" {
		if err := CheckResourceARN(s.WorkerConfigArn); err != nil {
			return err
		}
	}
	if err := CheckRoleARN(s.ServiceRoleArn); err != nil {
		return err
	}
	if len(s.Config) == 0 || len(s.Config) > maxConfigKeys {
		return fmt.Errorf("%w: connector config has %d keys, want 1 to %d", ErrInvalid, len(s.Config), maxConfigKeys)
	}
	if s.LogGroup != "" {
		if err := CheckLogGroup(s.LogGroup); err != nil {
			return err
		}
	}
	return CheckTags(s.Tags)
}

// CreateConnector creates a connector. MSK Connect provisions it for 5 to
// 15 minutes: the result is CREATING, and DescribeConnector polls it.
func (c *Client) CreateConnector(ctx context.Context, s ConnectorSpec) (Created, error) {
	if err := s.check(); err != nil {
		return Created{}, err
	}
	in := &kafkaconnect.CreateConnectorInput{
		ConnectorName:       aws.String(s.Name),
		KafkaConnectVersion: aws.String(s.KafkaConnectVersion),
		Capacity: &kctypes.Capacity{ProvisionedCapacity: &kctypes.ProvisionedCapacity{
			McuCount: 1, WorkerCount: 1,
		}},
		KafkaCluster: &kctypes.KafkaCluster{ApacheKafkaCluster: &kctypes.ApacheKafkaCluster{
			BootstrapServers: aws.String(s.BootstrapServers),
			Vpc:              &kctypes.Vpc{Subnets: s.Subnets, SecurityGroups: s.SecurityGroups},
		}},
		KafkaClusterClientAuthentication: &kctypes.KafkaClusterClientAuthentication{AuthenticationType: kctypes.KafkaClusterClientAuthenticationTypeIam},
		KafkaClusterEncryptionInTransit:  &kctypes.KafkaClusterEncryptionInTransit{EncryptionType: kctypes.KafkaClusterEncryptionInTransitTypeTls},
		Plugins:                          []kctypes.Plugin{{CustomPlugin: &kctypes.CustomPlugin{CustomPluginArn: aws.String(s.PluginArn), Revision: s.PluginRevision}}},
		ServiceExecutionRoleArn:          aws.String(s.ServiceRoleArn),
		ConnectorConfiguration:           s.Config,
		Tags:                             s.Tags,
	}
	if s.WorkerConfigArn != "" {
		in.WorkerConfiguration = &kctypes.WorkerConfiguration{WorkerConfigurationArn: aws.String(s.WorkerConfigArn), Revision: s.WorkerConfigRevision}
	}
	if s.LogGroup != "" {
		in.LogDelivery = &kctypes.LogDelivery{WorkerLogDelivery: &kctypes.WorkerLogDelivery{
			CloudWatchLogs: &kctypes.CloudWatchLogsLogDelivery{Enabled: true, LogGroup: aws.String(s.LogGroup)},
		}}
	}
	out, err := call(ctx, c, "create connector", func(ctx context.Context) (*kafkaconnect.CreateConnectorOutput, error) {
		return c.kc.CreateConnector(ctx, in)
	})
	if err != nil {
		return Created{}, err
	}
	return Created{Arn: str(out.ConnectorArn), Name: str(out.ConnectorName), State: string(out.ConnectorState)}, nil
}

// DeleteConnector deletes a connector. currentVersion, when set, makes the
// delete conditional on the version the caller read (a ConflictException
// otherwise).
func (c *Client) DeleteConnector(ctx context.Context, arn, version string) (Created, error) {
	if err := CheckResourceARN(arn); err != nil {
		return Created{}, err
	}
	in := &kafkaconnect.DeleteConnectorInput{ConnectorArn: aws.String(arn)}
	if version != "" {
		if err := check(currentVersion, "current version", version); err != nil {
			return Created{}, err
		}
		in.CurrentVersion = aws.String(version)
	}
	out, err := call(ctx, c, "delete connector", func(ctx context.Context) (*kafkaconnect.DeleteConnectorOutput, error) {
		return c.kc.DeleteConnector(ctx, in)
	})
	if err != nil {
		return Created{}, err
	}
	return Created{Arn: str(out.ConnectorArn), State: string(out.ConnectorState)}, nil
}
