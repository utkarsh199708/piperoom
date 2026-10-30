package mskconnect

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/smithy-go"

	"piperoom/internal/clients"
	"piperoom/internal/setup/targets/managed/replay"
)

// cassetteSource is the Source line of the hand-written cassettes.
const cassetteSource = "hand-written from the AWS SDK for Go v2 shapes (kafkaconnect v1.33.0, kafka v1.56.0, ec2 v1.317.0, iam v1.56.0, s3 v1.106.0, sts v1.45.0); requests as the SDK sends them; not from a live account"

// TestCassettes replays every case over its cassette: the client's calls,
// error mapping and redaction against the recorded wire shapes, offline.
func TestCassettes(t *testing.T) {
	for _, tc := range mskCases {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join("testdata", "aws", tc.name+".json")
			r := replay.Replay(t, path, Scrub)
			c, err := New(testConfig(r))
			if err != nil {
				t.Fatal(err)
			}
			tc.run(t, context.Background(), c)
			if n := r.Remaining(); n != 0 {
				t.Errorf("%d recorded interactions unused", n)
			}
		})
	}
}

// Every cassette on disk belongs to a case, so none is stale by omission.
func TestEveryCassetteHasACase(t *testing.T) {
	files, err := filepath.Glob(filepath.Join("testdata", "aws", "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	names := map[string]bool{}
	for _, tc := range mskCases {
		names[tc.name] = true
	}
	for _, f := range files {
		if !names[strings.TrimSuffix(filepath.Base(f), ".json")] {
			t.Errorf("cassette %s has no case", f)
		}
	}
	if len(files) != len(mskCases) {
		t.Errorf("%d cassettes for %d cases", len(files), len(mskCases))
	}
}

// The board's coverage: 400, 403, 404, 409 and throttling.
func TestCassettesCoverTheErrorStatuses(t *testing.T) {
	want := map[int]bool{400: false, 403: false, 404: false, 409: false, 429: false}
	for _, tc := range mskCases {
		raw, err := os.ReadFile(filepath.Join("testdata", "aws", tc.name+".json"))
		if err != nil {
			t.Fatal(err)
		}
		for code := range want {
			if strings.Contains(string(raw), fmt.Sprintf(`"status": %d`, code)) {
				want[code] = true
			}
		}
	}
	for code, seen := range want {
		if !seen {
			t.Errorf("no cassette answers %d", code)
		}
	}
}

// The connector cassettes carry the database password only as REDACTED,
// request and response (the scrubber at record time).
func TestConnectorCassettesHoldNoPassword(t *testing.T) {
	for _, name := range []string{"create_connector_ok", "status_running_redacted", "describe_connector_failed"} {
		raw, err := os.ReadFile(filepath.Join("testdata", "aws", name+".json"))
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(raw), "pw-live-1") {
			t.Errorf("%s holds the password", name)
		}
		if !strings.Contains(string(raw), `database.password\":\"REDACTED`) && !strings.Contains(string(raw), `"database.password":"REDACTED"`) {
			t.Errorf("%s does not show the scrubbed password key", name)
		}
	}
}

func TestNewNeedsARegion(t *testing.T) {
	if _, err := New(aws.Config{}); !errors.Is(err, ErrInvalid) {
		t.Errorf("New without a region = %v", err)
	}
}

func TestValidationRefusesBeforeAnyRequest(t *testing.T) {
	canned := replay.NewCanned()
	c, err := New(testConfig(canned))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	bad := []struct {
		name string
		call func() error
	}{
		{"cluster ARN", func() error { _, err := c.DescribeCluster(ctx, "arn:aws:kafka:us-east-1:1:cluster/x"); return err }},
		{"connector ARN with a slash path", func() error { _, err := c.DescribeConnector(ctx, tConnector+"/../x"); return err }},
		{"connector name", func() error { _, err := c.FindConnector(ctx, "a b"); return err }},
		{"subnet", func() error { _, err := c.DescribeNetwork(ctx, []string{"subnet-x"}, nil); return err }},
		{"no subnet", func() error { _, err := c.DescribeNetwork(ctx, nil, nil); return err }},
		{"group", func() error { _, err := c.DescribeNetwork(ctx, []string{tSubnetA}, []string{"default"}); return err }},
		{"role", func() error { _, err := c.GetRole(ctx, "arn:aws:iam::000000000000:user/u"); return err }},
		{"object key", func() error { _, err := c.HeadObject(ctx, tBucket, "../etc/passwd", ""); return err }},
		{"bucket", func() error { _, err := c.HeadObject(ctx, "Bad_Bucket", tKey, ""); return err }},
		{"plugin content type", func() error {
			_, err := c.CreatePlugin(ctx, PluginSpec{Name: "p", ContentType: "TAR", BucketArn: tBucketARN, FileKey: tKey})
			return err
		}},
		{"connector without config", func() error {
			s := connectorSpec()
			s.Config = nil
			_, err := c.CreateConnector(ctx, s)
			return err
		}},
		{"connector bad version", func() error {
			s := connectorSpec()
			s.KafkaConnectVersion = "latest"
			_, err := c.CreateConnector(ctx, s)
			return err
		}},
		{"tag", func() error {
			s := connectorSpec()
			s.Tags = map[string]string{"bad key!": "x"}
			_, err := c.CreateConnector(ctx, s)
			return err
		}},
		{"worker config empty", func() error { _, err := c.CreateWorkerConfig(ctx, WorkerConfigSpec{Name: "w"}); return err }},
		{"delete version", func() error { _, err := c.DeleteConnector(ctx, tConnector, "v 1"); return err }},
	}
	for _, b := range bad {
		if err := b.call(); !errors.Is(err, ErrInvalid) {
			t.Errorf("%s: %v, want ErrInvalid", b.name, err)
		}
	}
}

func TestRestartIsNotSupported(t *testing.T) {
	c := &Client{}
	if err := c.RestartConnector(context.Background(), "x"); !errors.Is(err, clients.ErrNotSupported) {
		t.Errorf("RestartConnector = %v", err)
	}
	if err := c.RestartTask(context.Background(), "x", 0); !errors.Is(err, clients.ErrNotSupported) {
		t.Errorf("RestartTask = %v", err)
	}
}

func TestMapErrorTable(t *testing.T) {
	cases := []struct {
		code string
		want error
	}{
		{"BadRequestException", ErrBadRequest}, {"ValidationError", ErrBadRequest}, {"InvalidParameterValue", ErrBadRequest},
		{"ForbiddenException", ErrForbidden}, {"UnauthorizedException", ErrForbidden}, {"AccessDenied", ErrForbidden}, {"UnauthorizedOperation", ErrForbidden},
		{"NotFoundException", ErrNotFound}, {"NoSuchEntity", ErrNotFound}, {"InvalidGroup.NotFound", ErrNotFound},
		{"ConflictException", ErrConflict},
		{"TooManyRequestsException", ErrThrottled}, {"Throttling", ErrThrottled}, {"RequestLimitExceeded", ErrThrottled},
		{"InternalServerErrorException", ErrAWS},
	}
	for _, tc := range cases {
		err := mapError("x", &smithy.GenericAPIError{Code: tc.code, Message: "m"})
		if !errors.Is(err, tc.want) {
			t.Errorf("%s -> %v, want %v", tc.code, err, tc.want)
		}
	}
	if err := mapError("x", context.DeadlineExceeded); !errors.Is(err, ErrAWS) || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("deadline -> %v", err)
	}
	// A message with a credential in it is redacted.
	err := mapError("x", &smithy.GenericAPIError{Code: "AccessDenied", Message: "denied for postgres://app:hunter2@db/x"})
	if strings.Contains(err.Error(), "hunter2") {
		t.Errorf("message not redacted: %v", err)
	}
}

func TestMapStateAndPrincipal(t *testing.T) {
	for in, want := range map[string]string{"RUNNING": "RUNNING", "FAILED": "FAILED", "CREATING": "UNASSIGNED", "UPDATING": "UNASSIGNED", "DELETING": "STOPPED", "": "UNASSIGNED"} {
		if got := MapState(in); got != want {
			t.Errorf("MapState(%q) = %q, want %q", in, got, want)
		}
	}
	for in, want := range map[string]string{
		"arn:aws:sts::000000000000:assumed-role/PiperoomSetup/sess": "PiperoomSetup",
		"arn:aws:iam::000000000000:user/ops/alice":                  "alice",
		"arn:aws:iam::000000000000:root":                            "",
	} {
		if got := principalOf(in); got != want {
			t.Errorf("principalOf(%q) = %q, want %q", in, got, want)
		}
	}
	if AccountOfARN(tConnector) != tAccount || RegionOfARN(tConnector) != tRegion {
		t.Errorf("AccountOfARN/RegionOfARN of %s", tConnector)
	}
}
