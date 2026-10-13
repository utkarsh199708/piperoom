package agent

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
)

// With a shared AWS config the endpoint never loads its own: the loader is not
// called and the shared credentials provider is the one checked at startup.
func TestBedrockUsesTheSharedAWSConfig(t *testing.T) {
	var retrieved int
	shared := &aws.Config{
		Region: "ap-south-1",
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			retrieved++
			return aws.Credentials{AccessKeyID: "AKIDSHARED", SecretAccessKey: "s"}, nil
		}),
	}
	ld := providerLoaders{awsConfig: func(context.Context, string) (aws.Config, error) {
		t.Fatal("endpoint loaded its own AWS config although a shared one was given")
		return aws.Config{}, nil
	}}
	cfg := EndpointConfig{Provider: ProviderBedrock, AWSRegion: "ap-south-1", AWSConfig: shared}
	ep, err := newEndpoint(context.Background(), cfg, envOf(nil), ld, discardLogger())
	if err != nil {
		t.Fatal(err)
	}
	if ep.Provider() != ProviderBedrock {
		t.Fatalf("provider = %s", ep.Provider())
	}
	if retrieved != 1 {
		t.Fatalf("shared credentials retrieved %d times, want 1 (the startup check)", retrieved)
	}
	if shared.Region != "ap-south-1" {
		t.Fatalf("shared config mutated: region %q", shared.Region)
	}
}

// A shared config with no credentials is still a startup error for Bedrock.
func TestBedrockSharedAWSConfigWithoutCredentials(t *testing.T) {
	cfg := EndpointConfig{Provider: ProviderBedrock, AWSRegion: "ap-south-1", AWSConfig: &aws.Config{Region: "ap-south-1"}}
	if _, err := newEndpoint(context.Background(), cfg, envOf(nil), providerLoaders{}, discardLogger()); err == nil {
		t.Fatal("want a startup error without AWS credentials")
	}
}
