package wiring

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"

	"piperoom/internal/agent"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/config"
)

// fakeAWS counts loader calls and credential fetches. Nothing touches the
// machine's real AWS chain.
type fakeAWS struct {
	loads     atomic.Int32
	retrieves atomic.Int32
	noCreds   bool
	loadErr   error
	credErr   error
}

func (f *fakeAWS) load(_ context.Context, region string) (aws.Config, error) {
	f.loads.Add(1)
	if f.loadErr != nil {
		return aws.Config{}, f.loadErr
	}
	c := aws.Config{Region: region}
	if !f.noCreds {
		c.Credentials = aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			f.retrieves.Add(1)
			if f.credErr != nil {
				return aws.Credentials{}, f.credErr
			}
			return aws.Credentials{AccessKeyID: "AKIDSHARED", SecretAccessKey: "shared-secret", SessionToken: "shared-token"}, nil
		})
	}
	return c, nil
}

func envOf(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func mskIAMConfig() config.Config {
	cfg := devA2Config()
	cfg.KafkaBrokers = "b-1.demo.kafka.ap-south-1.amazonaws.com:9098"
	cfg.KafkaSecurityProtocol = netsec.SASLSSL
	cfg.KafkaSASLMechanism = netsec.MechAWSMSKIAM
	cfg.AWSRegion = "ap-south-1"
	return cfg
}

func TestLoadAWSSkippedWhenNothingNeedsIt(t *testing.T) {
	f := &fakeAWS{}
	for _, cfg := range []config.Config{
		{},
		{ModelProvider: "anthropic", AWSRegion: "ap-south-1"},
		{ModelProvider: "vertex", KafkaSecurityProtocol: netsec.SASLSSL, KafkaSASLMechanism: netsec.MechSCRAMSHA512},
	} {
		got, err := loadAWS(context.Background(), cfg, envOf(nil), f.load)
		if err != nil || got != nil {
			t.Fatalf("loadAWS(%+v) = %v, %v; want nil, nil", cfg, got, err)
		}
	}
	if n := f.loads.Load(); n != 0 {
		t.Fatalf("loader called %d times, want 0", n)
	}
}

// The PR 1 done-when: with Bedrock and MSK IAM both on, the AWS chain is
// loaded exactly once and that one config reaches both the Bedrock endpoint
// and the Kafka SASL credentials.
func TestLoadAWSOnceSharedByBedrockAndKafka(t *testing.T) {
	// The real endpoint reads this env var; make sure its credential check runs.
	t.Setenv(bedrockTokenEnv, "")
	f := &fakeAWS{}
	cfg := mskIAMConfig()
	cfg.ModelProvider = "bedrock"
	cfg.Model = "us.anthropic.claude-opus-5-5-v1:0"

	awsCfg, err := loadAWS(context.Background(), cfg, envOf(nil), f.load)
	if err != nil {
		t.Fatal(err)
	}
	if awsCfg == nil || awsCfg.Region != "ap-south-1" {
		t.Fatalf("aws config = %+v, want region ap-south-1", awsCfg)
	}
	if got := f.retrieves.Load(); got != 1 {
		t.Fatalf("startup credential check: %d fetches, want 1", got)
	}

	// Bedrock: the endpoint must use the shared provider, not load its own.
	ep, err := NewAgentEndpoint(context.Background(), cfg, slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)), awsCfg)
	if err != nil {
		t.Fatal(err)
	}
	if ep.Provider() != agent.ProviderBedrock {
		t.Fatalf("provider = %s", ep.Provider())
	}
	if got := f.retrieves.Load(); got != 2 {
		t.Fatalf("after the endpoint: %d fetches from the shared provider, want 2", got)
	}

	// Kafka: the SASL credentials func reads the same provider.
	a2, err := buildA2Clients(cfg, awsCfg)
	if err != nil {
		t.Fatal(err)
	}
	a2.Kafka.Close()
	sec, err := kafkaSecurity(cfg, awsCfg)
	if err != nil {
		t.Fatal(err)
	}
	creds, err := sec.AWSCredentialsFunc(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if creds.AccessKey != "AKIDSHARED" || creds.SecretKey != "shared-secret" || creds.SessionToken != "shared-token" {
		t.Fatalf("kafka got other credentials than the shared config's")
	}
	if got := f.retrieves.Load(); got != 3 {
		t.Fatalf("after the kafka SASL func: %d fetches from the shared provider, want 3", got)
	}

	if n := f.loads.Load(); n != 1 {
		t.Fatalf("AWS config loaded %d times, want exactly 1", n)
	}
}

func TestLoadAWSErrors(t *testing.T) {
	boom := errors.New("no valid providers in chain")
	tests := []struct {
		name      string
		cfg       config.Config
		env       map[string]string
		f         *fakeAWS
		want      string
		wantLoads int32
		wrapped   error
	}{
		{name: "msk iam without region", cfg: func() config.Config { c := mskIAMConfig(); c.AWSRegion = " "; return c }(),
			f: &fakeAWS{}, want: "KAFKA_SASL_MECHANISM=AWS_MSK_IAM needs AWS_REGION"},
		{name: "bedrock without region", cfg: config.Config{ModelProvider: "Bedrock"},
			f: &fakeAWS{}, want: "PIPEROOM_MODEL_PROVIDER=bedrock needs AWS_REGION"},
		{name: "loader fails", cfg: mskIAMConfig(), f: &fakeAWS{loadErr: boom}, want: "load AWS config", wantLoads: 1, wrapped: boom},
		{name: "no credentials", cfg: mskIAMConfig(), f: &fakeAWS{noCreds: true}, want: "none were found", wantLoads: 1},
		{name: "retrieve fails", cfg: mskIAMConfig(), f: &fakeAWS{credErr: boom}, want: "retrieve AWS credentials", wantLoads: 1, wrapped: boom},
		{name: "bedrock api key does not excuse msk iam", cfg: mskIAMConfig(), env: map[string]string{bedrockTokenEnv: "k"},
			f: &fakeAWS{noCreds: true}, want: "none were found", wantLoads: 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := loadAWS(context.Background(), tt.cfg, envOf(tt.env), tt.f.load)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want one containing %q", err, tt.want)
			}
			if tt.wrapped != nil && !errors.Is(err, tt.wrapped) {
				t.Errorf("err = %v, want it to wrap %v", err, tt.wrapped)
			}
			if n := tt.f.loads.Load(); n != tt.wantLoads {
				t.Errorf("loader called %d times, want %d", n, tt.wantLoads)
			}
		})
	}
}

// A Bedrock API key replaces AWS credentials for Bedrock alone, as in the
// endpoint, so no credential fetch is required at start.
func TestLoadAWSBedrockAPIKeySkipsCredentialCheck(t *testing.T) {
	f := &fakeAWS{noCreds: true}
	cfg := config.Config{ModelProvider: "bedrock", AWSRegion: "ap-south-1"}
	got, err := loadAWS(context.Background(), cfg, envOf(map[string]string{bedrockTokenEnv: "k"}), f.load)
	if err != nil || got == nil {
		t.Fatalf("loadAWS = %v, %v; want a config", got, err)
	}
}

// A command that forgets LoadAWS gets a config error from Open, not a second
// load of the AWS chain.
func TestOpenMSKIAMWithoutSharedAWSConfig(t *testing.T) {
	cfg := mskIAMConfig()
	cfg.OwnersFile = writeEmptyOwners(t)
	_, err := Open(context.Background(), cfg, nil)
	if !errors.Is(err, errNoSharedAWS) {
		t.Fatalf("Open = %v, want errNoSharedAWS", err)
	}
}

// AWS_MSK_IAM over SASL_PLAINTEXT is refused by netsec at start-up, before
// any database ping.
func TestOpenMSKIAMWithoutTLS(t *testing.T) {
	f := &fakeAWS{}
	cfg := mskIAMConfig()
	cfg.OwnersFile = writeEmptyOwners(t)
	cfg.KafkaSecurityProtocol = netsec.SASLPlaintext
	awsCfg, err := loadAWS(context.Background(), cfg, envOf(nil), f.load)
	if err != nil {
		t.Fatal(err)
	}
	_, err = Open(context.Background(), cfg, awsCfg)
	if !errors.Is(err, netsec.ErrInvalid) || !strings.Contains(err.Error(), "needs security protocol SASL_SSL") {
		t.Fatalf("Open = %v, want netsec.ErrInvalid naming SASL_SSL", err)
	}
}

func TestMSKIAMCredentialsErrorAndDeadline(t *testing.T) {
	boom := errors.New("imds: no role")
	var hasDeadline bool
	awsCfg := &aws.Config{Credentials: aws.CredentialsProviderFunc(func(ctx context.Context) (aws.Credentials, error) {
		_, hasDeadline = ctx.Deadline()
		return aws.Credentials{}, boom
	})}
	_, err := mskIAMCredentials(awsCfg)(context.Background())
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v, want the provider error wrapped", err)
	}
	if !hasDeadline {
		t.Error("credential fetch ran without a deadline")
	}
}

func writeEmptyOwners(t *testing.T) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(p, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}
