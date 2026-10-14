package secrets

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"

	"piperoom/internal/integrations/spec"
)

// fakeAWS speaks the awsJson1.1 protocol of Secrets Manager and SSM.
type fakeAWS struct {
	mu      sync.Mutex
	targets []string
	bodies  []map[string]any
	// handler returns status, error type ("" = success) and body.
	handle func(target string, in map[string]any) (int, string, any)
}

func (f *fakeAWS) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	target := r.Header.Get("X-Amz-Target")
	raw, _ := io.ReadAll(r.Body)
	var in map[string]any
	_ = json.Unmarshal(raw, &in)
	f.mu.Lock()
	f.targets = append(f.targets, target)
	f.bodies = append(f.bodies, in)
	h := f.handle
	f.mu.Unlock()
	status, errType, body := h(target, in)
	w.Header().Set("Content-Type", "application/x-amz-json-1.1")
	if errType != "" {
		w.Header().Set("X-Amzn-ErrorType", errType)
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{"__type": errType, "message": "fake " + errType})
		return
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func testAWSConfig() aws.Config {
	return aws.Config{
		Region: "eu-west-1",
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "AKIDTEST", SecretAccessKey: "test-secret-key"}, nil
		}),
		RetryMaxAttempts: 1,
	}
}

func newSMTest(t *testing.T, h func(string, map[string]any) (int, string, any)) (*fakeAWS, *AWSSecretsManagerProvider) {
	t.Helper()
	f := &fakeAWS{handle: h}
	srv := httptest.NewServer(f)
	t.Cleanup(srv.Close)
	p, err := NewAWSSecretsManagerProvider(AWSOptions{Config: testAWSConfig(), Endpoint: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	return f, p
}

func smHandler(target string, in map[string]any) (int, string, any) {
	switch target {
	case "secretsmanager.GetSecretValue":
		switch in["SecretId"] {
		case "prod/piperoom/orders-db":
			return 200, "", map[string]any{"Name": "prod/piperoom/orders-db",
				"SecretString": `{"username":"ro","password":"aws-pw"}`, "VersionId": "11111111-2222-3333-4444-555555555555"}
		case "binary":
			return 200, "", map[string]any{"SecretBinary": "YmluYXJ5LXB3", "VersionId": "v"}
		case "denied":
			return 400, "AccessDeniedException", nil
		case "throttled":
			return 400, "ThrottlingException", nil
		case "badcreds":
			return 400, "UnrecognizedClientException", nil
		case "kms":
			return 400, "DecryptionFailure", nil
		}
		return 400, "ResourceNotFoundException", nil
	case "secretsmanager.ListSecrets":
		if in["NextToken"] == nil {
			return 200, "", map[string]any{"SecretList": []any{map[string]any{"Name": "prod/piperoom/orders-db"}}, "NextToken": "p2"}
		}
		return 200, "", map[string]any{"SecretList": []any{map[string]any{"Name": "prod/piperoom/kafka"}}}
	}
	return 400, "UnknownOperationException", nil
}

func TestAWSSecretsManagerResolve(t *testing.T) {
	f, p := newSMTest(t, smHandler)
	ctx := context.Background()
	v, err := p.Resolve(ctx, Ref{Path: "prod/piperoom/orders-db", Key: "ignored"})
	if err != nil {
		t.Fatal(err)
	}
	if pw, _ := ExtractKey(v.Data, "password"); pw.Reveal() != "aws-pw" || v.Version != "11111111-2222-3333-4444-555555555555" {
		t.Fatalf("%q %q", pw.Reveal(), v.Version)
	}
	if v, err := p.Resolve(ctx, Ref{Path: "binary"}); err != nil || v.Data.Reveal() != "binary-pw" {
		t.Fatalf("binary %q %v", v.Data.Reveal(), err)
	}
	// Version: a UUID is a version id, anything else a staging label.
	_, _ = p.Resolve(ctx, Ref{Path: "prod/piperoom/orders-db", Version: "AWSPREVIOUS"})
	_, _ = p.Resolve(ctx, Ref{Path: "prod/piperoom/orders-db", Version: "11111111-2222-3333-4444-555555555555"})
	f.mu.Lock()
	n := len(f.bodies)
	stage, id := f.bodies[n-2], f.bodies[n-1]
	f.mu.Unlock()
	if stage["VersionStage"] != "AWSPREVIOUS" || stage["VersionId"] != nil || id["VersionId"] == nil {
		t.Fatalf("version params: %v / %v", stage, id)
	}

	for path, want := range map[string]error{
		"missing": ErrNotFound, "denied": ErrAccessDenied, "throttled": ErrUnreachable,
		"badcreds": ErrLogin, "kms": ErrAccessDenied,
	} {
		if _, err := p.Resolve(ctx, Ref{Path: path}); !errors.Is(err, want) {
			t.Errorf("%s: err = %v, want %v", path, err, want)
		}
	}
	keys, _ := p.Keys(ctx, Ref{Path: "prod/piperoom/orders-db"})
	if !reflect.DeepEqual(keys, []string{"password", "username"}) {
		t.Fatalf("keys = %v", keys)
	}
}

func TestAWSSecretsManagerUnreachable(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	url := srv.URL
	srv.Close()
	p, err := NewAWSSecretsManagerProvider(AWSOptions{Config: testAWSConfig(), Endpoint: url})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Resolve(context.Background(), Ref{Path: "x"}); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("closed endpoint err = %v", err)
	}
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(300 * time.Millisecond):
		case <-r.Context().Done():
		}
	}))
	defer slow.Close()
	p, _ = NewAWSSecretsManagerProvider(AWSOptions{Config: testAWSConfig(), Endpoint: slow.URL})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if _, err := p.Resolve(ctx, Ref{Path: "x"}); !errors.Is(err, ErrUnreachable) {
		t.Fatalf("timeout err = %v", err)
	}
	if _, err := NewAWSSecretsManagerProvider(AWSOptions{}); !errors.Is(err, ErrBadProviderConfig) {
		t.Fatalf("no region err = %v", err)
	}
}

func TestAWSSecretsManagerList(t *testing.T) {
	f, p := newSMTest(t, smHandler)
	got, err := p.List(context.Background(), "prod/piperoom/", 10)
	if err != nil || !reflect.DeepEqual(got, []string{"prod/piperoom/kafka", "prod/piperoom/orders-db"}) {
		t.Fatalf("List = %v %v", got, err)
	}
	f.mu.Lock()
	filters, _ := json.Marshal(f.bodies[0]["Filters"])
	f.mu.Unlock()
	if !strings.Contains(string(filters), `"Key":"name"`) || !strings.Contains(string(filters), "prod/piperoom/") {
		t.Fatalf("filters = %s", filters)
	}
	_, denied := newSMTest(t, func(string, map[string]any) (int, string, any) { return 400, "AccessDeniedException", nil })
	if _, err := denied.List(context.Background(), "", 10); !errors.Is(err, ErrListUnsupported) {
		t.Fatalf("denied list err = %v", err)
	}
}

func TestAWSSecretsManagerCheck(t *testing.T) {
	_, p := newSMTest(t, smHandler)
	checks := p.Check(context.Background(), &Ref{Path: "prod/piperoom/orders-db", Key: "password"})
	if spec.Failed(checks) {
		t.Fatalf("checks = %+v", checks)
	}
	if last := checks[len(checks)-1]; last.ID != CheckTooMuch || last.Status != spec.CheckSkipped || last.Detail == "" {
		t.Fatalf("least privilege = %+v", last)
	}
	for _, c := range checks {
		if strings.Contains(c.Detail, "aws-pw") {
			t.Fatal("check shows the value")
		}
	}
	_, bad := newSMTest(t, func(string, map[string]any) (int, string, any) { return 400, "UnrecognizedClientException", nil })
	checks = bad.Check(context.Background(), nil)
	if checks[1].ID != CheckLogin || checks[1].Status != spec.CheckFail {
		t.Fatalf("bad creds checks = %+v", checks)
	}
	_, denied := newSMTest(t, func(target string, in map[string]any) (int, string, any) {
		if target == "secretsmanager.ListSecrets" {
			return 400, "AccessDeniedException", nil
		}
		return smHandler(target, in)
	})
	checks = denied.Check(context.Background(), &Ref{Path: "prod/piperoom/orders-db"})
	if spec.Failed(checks) {
		t.Fatalf("list denied should only warn: %+v", checks)
	}
}

func ssmHandler(target string, in map[string]any) (int, string, any) {
	switch target {
	case "AmazonSSM.GetParameter":
		if in["WithDecryption"] != true {
			return 400, "ValidationException", nil
		}
		switch in["Name"] {
		case "/prod/db/password":
			return 200, "", map[string]any{"Parameter": map[string]any{"Name": "/prod/db/password", "Value": "ssm-pw", "Version": 7}}
		case "/prod/db/password:3":
			return 200, "", map[string]any{"Parameter": map[string]any{"Name": "/prod/db/password", "Value": "ssm-pw-3", "Version": 3}}
		case "plain":
			return 200, "", map[string]any{"Parameter": map[string]any{"Name": "plain", "Value": `{"a":"b"}`, "Version": 1}}
		case "/prod/db/password:99":
			return 400, "ParameterVersionNotFound", nil
		}
		return 400, "ParameterNotFound", nil
	case "AmazonSSM.DescribeParameters":
		return 200, "", map[string]any{"Parameters": []any{map[string]any{"Name": "/prod/db/password"}, map[string]any{"Name": "/prod/db/user"}}}
	}
	return 400, "UnknownOperationException", nil
}

func TestAWSSSM(t *testing.T) {
	f := &fakeAWS{handle: ssmHandler}
	srv := httptest.NewServer(f)
	defer srv.Close()
	p, err := NewAWSSSMProvider(AWSOptions{Config: testAWSConfig(), Endpoint: srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if v, err := p.Resolve(ctx, Ref{Path: "prod/db/password"}); err != nil || v.Data.Reveal() != "ssm-pw" || v.Version != "7" {
		t.Fatalf("%q %q %v", v.Data.Reveal(), v.Version, err)
	}
	if v, err := p.Resolve(ctx, Ref{Path: "prod/db/password", Version: "3"}); err != nil || v.Data.Reveal() != "ssm-pw-3" {
		t.Fatalf("pinned %q %v", v.Data.Reveal(), err)
	}
	if _, err := p.Resolve(ctx, Ref{Path: "prod/db/password", Version: "99"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("version err = %v", err)
	}
	if _, err := p.Resolve(ctx, Ref{Path: "prod/missing"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing err = %v", err)
	}
	if keys, _ := p.Keys(ctx, Ref{Path: "plain"}); !reflect.DeepEqual(keys, []string{"a"}) {
		t.Fatalf("keys = %v", keys)
	}
	got, err := p.List(ctx, "prod/db/", 10)
	if err != nil || !reflect.DeepEqual(got, []string{"prod/db/password", "prod/db/user"}) {
		t.Fatalf("List = %v %v", got, err)
	}
	f.mu.Lock()
	last := f.bodies[len(f.bodies)-1]
	f.mu.Unlock()
	if b, _ := json.Marshal(last); !strings.Contains(string(b), `"Values":["/prod/db/"]`) || strings.Contains(string(b), "WithDecryption") {
		t.Fatalf("describe body = %s", b)
	}
	if checks := p.Check(ctx, &Ref{Path: "prod/db/password"}); spec.Failed(checks) {
		t.Fatalf("checks = %+v", checks)
	}
	if ssmName("arn:aws:ssm:eu-west-1:1:parameter/x") != "arn:aws:ssm:eu-west-1:1:parameter/x" || ssmName("single") != "single" {
		t.Fatal("ssmName")
	}
}
