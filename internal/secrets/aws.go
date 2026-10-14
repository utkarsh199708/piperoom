package secrets

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/secretsmanager"
	smtypes "github.com/aws/aws-sdk-go-v2/service/secretsmanager/types"
	"github.com/aws/aws-sdk-go-v2/service/ssm"
	ssmtypes "github.com/aws/aws-sdk-go-v2/service/ssm/types"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/integrations/spec"
)

// AWSOptions configure the AWS Secrets Manager and SSM Parameter Store
// providers.
type AWSOptions struct {
	// Config is the shared AWS config (wiring.LoadAWS, or a per-workspace
	// AssumeRole config in hosted mode). Region must be set.
	Config aws.Config
	// Endpoint overrides the service endpoint (a VPC endpoint; tests).
	Endpoint string
}

// smAPI is the part of the Secrets Manager client the provider uses.
type smAPI interface {
	GetSecretValue(ctx context.Context, in *secretsmanager.GetSecretValueInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.GetSecretValueOutput, error)
	ListSecrets(ctx context.Context, in *secretsmanager.ListSecretsInput, optFns ...func(*secretsmanager.Options)) (*secretsmanager.ListSecretsOutput, error)
}

// ssmAPI is the part of the SSM client the provider uses.
type ssmAPI interface {
	GetParameter(ctx context.Context, in *ssm.GetParameterInput, optFns ...func(*ssm.Options)) (*ssm.GetParameterOutput, error)
	DescribeParameters(ctx context.Context, in *ssm.DescribeParametersInput, optFns ...func(*ssm.Options)) (*ssm.DescribeParametersOutput, error)
}

// AWSSecretsManagerProvider reads AWS Secrets Manager. Path is a secret
// name or ARN; Version is a version id or a staging label (AWSCURRENT,
// AWSPREVIOUS, ...). A JSON secret's fields are picked with #key.
type AWSSecretsManagerProvider struct {
	api    smAPI
	region string
}

// NewAWSSecretsManagerProvider builds the client from opts.Config.
func NewAWSSecretsManagerProvider(opts AWSOptions) (*AWSSecretsManagerProvider, error) {
	if opts.Config.Region == "" {
		return nil, fmt.Errorf("%w: AWS Secrets Manager needs a region", ErrBadProviderConfig)
	}
	c := secretsmanager.NewFromConfig(opts.Config, func(o *secretsmanager.Options) {
		if opts.Endpoint != "" {
			o.BaseEndpoint = aws.String(opts.Endpoint)
		}
	})
	return &AWSSecretsManagerProvider{api: c, region: opts.Config.Region}, nil
}

// GoString keeps %#v to the String form.
func (p *AWSSecretsManagerProvider) GoString() string { return p.String() }

// GoString keeps %#v to the String form.
func (p *AWSSSMProvider) GoString() string { return p.String() }

// String names the provider.
func (p *AWSSecretsManagerProvider) String() string {
	return "aws secrets manager provider " + p.region
}

// Close does nothing; the SDK client holds no goroutines.
func (p *AWSSecretsManagerProvider) Close() {}

// versionIDRe matches Secrets Manager version ids (UUIDs); anything else is a staging label.
var versionIDRe = regexp.MustCompile(`^[0-9a-fA-F]{8}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{4}-?[0-9a-fA-F]{12}$`)

// Resolve calls GetSecretValue.
func (p *AWSSecretsManagerProvider) Resolve(ctx context.Context, ref Ref) (Value, error) {
	ctx, cancel := withDefaultDeadline(ctx)
	defer cancel()
	in := &secretsmanager.GetSecretValueInput{SecretId: aws.String(ref.Path)}
	switch {
	case ref.Version == "":
	case versionIDRe.MatchString(ref.Version):
		in.VersionId = aws.String(ref.Version)
	default:
		in.VersionStage = aws.String(ref.Version)
	}
	out, err := p.api.GetSecretValue(ctx, in)
	if err != nil {
		return Value{}, fmt.Errorf("secrets: aws secrets manager: reading %s: %w", ref.Path, awsErr(err))
	}
	var data netsec.Secret
	switch {
	case out.SecretString != nil:
		data = netsec.Secret(*out.SecretString)
	case out.SecretBinary != nil:
		data = netsec.Secret(out.SecretBinary)
	default:
		return Value{}, fmt.Errorf("%w: %s has no value", ErrNotFound, ref.Path)
	}
	return Value{Data: data, Version: aws.ToString(out.VersionId)}, nil
}

// List returns secret names starting with prefix (ListSecrets name filter).
func (p *AWSSecretsManagerProvider) List(ctx context.Context, prefix string, limit int) ([]string, error) {
	ctx, cancel := withDefaultDeadline(ctx)
	defer cancel()
	if limit <= 0 {
		limit = 200
	}
	in := &secretsmanager.ListSecretsInput{MaxResults: aws.Int32(int32(min(limit, 100)))}
	if prefix != "" {
		in.Filters = []smtypes.Filter{{Key: smtypes.FilterNameStringTypeName, Values: []string{prefix}}}
	}
	out := []string{}
	for len(out) < limit {
		page, err := p.api.ListSecrets(ctx, in)
		if err != nil {
			err = awsErr(err)
			if errors.Is(err, ErrAccessDenied) {
				return nil, fmt.Errorf("%w: %w", ErrListUnsupported, err)
			}
			return nil, fmt.Errorf("secrets: aws secrets manager: listing: %w", err)
		}
		for _, s := range page.SecretList {
			if name := aws.ToString(s.Name); strings.HasPrefix(name, prefix) {
				out = append(out, name)
			}
		}
		if page.NextToken == nil || *page.NextToken == "" {
			break
		}
		in.NextToken = page.NextToken
	}
	sort.Strings(out)
	return capList(out, limit), nil
}

// Keys returns the fields of a JSON secret.
func (p *AWSSecretsManagerProvider) Keys(ctx context.Context, ref Ref) ([]string, error) {
	v, err := p.Resolve(ctx, ref.secretID())
	if err != nil {
		return nil, err
	}
	return KeysOf(v.Data), nil
}

// Check calls ListSecrets for reach and login, then reads the sample.
func (p *AWSSecretsManagerProvider) Check(ctx context.Context, sample *Ref) []spec.Check {
	return awsCheck(ctx, p, "AWS Secrets Manager", func(ctx context.Context) error {
		_, err := p.api.ListSecrets(ctx, &secretsmanager.ListSecretsInput{MaxResults: aws.Int32(1)})
		return err
	}, sample, "secretsmanager:ListSecrets")
}

// AWSSSMProvider reads SSM Parameter Store. Path "prod/db/password" is the
// parameter "/prod/db/password"; a single-segment path or an ARN is used as
// is. Version is a parameter version or label. SecureString values are
// decrypted.
type AWSSSMProvider struct {
	api    ssmAPI
	region string
}

// NewAWSSSMProvider builds the client from opts.Config.
func NewAWSSSMProvider(opts AWSOptions) (*AWSSSMProvider, error) {
	if opts.Config.Region == "" {
		return nil, fmt.Errorf("%w: AWS SSM Parameter Store needs a region", ErrBadProviderConfig)
	}
	c := ssm.NewFromConfig(opts.Config, func(o *ssm.Options) {
		if opts.Endpoint != "" {
			o.BaseEndpoint = aws.String(opts.Endpoint)
		}
	})
	return &AWSSSMProvider{api: c, region: opts.Config.Region}, nil
}

// String names the provider.
func (p *AWSSSMProvider) String() string { return "aws ssm parameter store provider " + p.region }

// Close does nothing.
func (p *AWSSSMProvider) Close() {}

// ssmName maps a reference path to a parameter name.
func ssmName(path string) string {
	if strings.HasPrefix(path, "arn:") || !strings.Contains(path, "/") {
		return path
	}
	return "/" + path
}

// Resolve calls GetParameter with decryption.
func (p *AWSSSMProvider) Resolve(ctx context.Context, ref Ref) (Value, error) {
	ctx, cancel := withDefaultDeadline(ctx)
	defer cancel()
	name := ssmName(ref.Path)
	if ref.Version != "" {
		name += ":" + ref.Version
	}
	out, err := p.api.GetParameter(ctx, &ssm.GetParameterInput{Name: aws.String(name), WithDecryption: aws.Bool(true)})
	if err != nil {
		return Value{}, fmt.Errorf("secrets: aws ssm: reading %s: %w", ref.Path, awsErr(err))
	}
	if out.Parameter == nil || out.Parameter.Value == nil {
		return Value{}, fmt.Errorf("%w: %s has no value", ErrNotFound, ref.Path)
	}
	return Value{Data: netsec.Secret(*out.Parameter.Value), Version: strconv.FormatInt(out.Parameter.Version, 10)}, nil
}

// List returns parameter names (without the leading slash) starting with
// prefix. It uses DescribeParameters, which returns metadata, never values.
func (p *AWSSSMProvider) List(ctx context.Context, prefix string, limit int) ([]string, error) {
	ctx, cancel := withDefaultDeadline(ctx)
	defer cancel()
	if limit <= 0 {
		limit = 200
	}
	in := &ssm.DescribeParametersInput{MaxResults: aws.Int32(50)}
	if prefix != "" {
		in.ParameterFilters = []ssmtypes.ParameterStringFilter{{
			Key: aws.String("Name"), Option: aws.String("BeginsWith"), Values: []string{ssmName(prefix)},
		}}
	}
	out := []string{}
	for len(out) < limit {
		page, err := p.api.DescribeParameters(ctx, in)
		if err != nil {
			err = awsErr(err)
			if errors.Is(err, ErrAccessDenied) {
				return nil, fmt.Errorf("%w: %w", ErrListUnsupported, err)
			}
			return nil, fmt.Errorf("secrets: aws ssm: listing: %w", err)
		}
		for _, m := range page.Parameters {
			out = append(out, strings.TrimPrefix(aws.ToString(m.Name), "/"))
		}
		if page.NextToken == nil || *page.NextToken == "" {
			break
		}
		in.NextToken = page.NextToken
	}
	sort.Strings(out)
	return capList(out, limit), nil
}

// Keys returns the fields of a JSON parameter.
func (p *AWSSSMProvider) Keys(ctx context.Context, ref Ref) ([]string, error) {
	v, err := p.Resolve(ctx, ref.secretID())
	if err != nil {
		return nil, err
	}
	return KeysOf(v.Data), nil
}

// Check calls DescribeParameters for reach and login, then reads the sample.
func (p *AWSSSMProvider) Check(ctx context.Context, sample *Ref) []spec.Check {
	return awsCheck(ctx, p, "AWS SSM Parameter Store", func(ctx context.Context) error {
		_, err := p.api.DescribeParameters(ctx, &ssm.DescribeParametersInput{MaxResults: aws.Int32(1)})
		return err
	}, sample, "ssm:DescribeParameters")
}

func awsCheck(ctx context.Context, p Provider, service string, probe func(context.Context) error, sample *Ref, listAction string) []spec.Check {
	reachTitle, loginTitle := "Can reach "+service, "AWS credentials are accepted"
	const tooTitle = "Piperoom has no more access than it needs"
	cctx, cancel := context.WithTimeout(ctx, checkTimeout)
	err := probe(cctx)
	cancel()
	var out []spec.Check
	if err != nil {
		err = awsErr(err)
		switch {
		case errors.Is(err, ErrUnreachable):
			return append(out, fail(CheckReach, reachTitle, errorDetail(err), "check the region, the VPC endpoint and outbound HTTPS"),
				skipped(CheckLogin, loginTitle, "not reachable"),
				skipped(CheckRead, "Can read the sample secret", "not reachable"),
				skipped(CheckTooMuch, tooTitle, "not reachable"))
		case errors.Is(err, ErrLogin):
			return append(out, pass(CheckReach, reachTitle, ""),
				fail(CheckLogin, loginTitle, errorDetail(err), "give Piperoom a role (IRSA, task or instance role) or fix the cross-account trust"),
				skipped(CheckRead, "Can read the sample secret", "login failed"),
				skipped(CheckTooMuch, tooTitle, "login failed"))
		case errors.Is(err, ErrAccessDenied):
			out = append(out, pass(CheckReach, reachTitle, ""), pass(CheckLogin, loginTitle, ""),
				warn(CheckListNames, "Piperoom can list names for the picker", "denied", "allow "+listAction+" (names only), or type references by hand"))
		default:
			out = append(out, pass(CheckReach, reachTitle, ""),
				warn(CheckLogin, loginTitle, errorDetail(err), ""))
		}
	} else {
		out = append(out, pass(CheckReach, reachTitle, ""), pass(CheckLogin, loginTitle, ""))
	}
	sc, _, _ := sampleCheck(ctx, p, sample)
	out = append(out, sc)
	return append(out, skipped(CheckTooMuch, tooTitle, "AWS cannot say whether an identity has extra rights without iam:SimulatePrincipalPolicy"))
}

// awsErr maps SDK errors to this package's sentinels, keeping the original.
func awsErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		return fmt.Errorf("%w: %w", ErrUnreachable, err)
	}
	var api smithy.APIError
	if errors.As(err, &api) {
		switch api.ErrorCode() {
		case "ResourceNotFoundException", "ParameterNotFound", "ParameterVersionNotFound", "ParameterVersionLabelNotFound":
			return fmt.Errorf("%w: %w", ErrNotFound, err)
		case "AccessDeniedException", "AccessDenied", "DecryptionFailure", "KMSAccessDeniedException":
			return fmt.Errorf("%w: %w", ErrAccessDenied, err)
		case "UnrecognizedClientException", "InvalidSignatureException", "ExpiredTokenException",
			"InvalidClientTokenId", "IncompleteSignature", "SignatureDoesNotMatch", "ExpiredToken":
			return fmt.Errorf("%w: %w", ErrLogin, err)
		case "ThrottlingException", "InternalServiceError", "InternalServerError", "ServiceUnavailable":
			return fmt.Errorf("%w: %w", ErrUnreachable, err)
		}
		if api.ErrorFault() == smithy.FaultServer {
			return fmt.Errorf("%w: %w", ErrUnreachable, err)
		}
		return err
	}
	var send *smithyhttp.RequestSendError
	if errors.As(err, &send) {
		return fmt.Errorf("%w: %w", ErrUnreachable, err)
	}
	return err
}
