package main

import (
	"os/exec"
	"slices"
	"strings"
	"testing"
)

// serverForbiddenDeps are packages the server binary must never link
// (review of PR #1114, finding 7). The managed-cloud and Strimzi setup
// drivers and their clients run on the runner with the person's own
// credentials (docs/design/setup-managed.md §2: the server never holds or
// carries a cloud credential), the runner and its command are a separate
// process, and the SQL Server driver is the runner's: the server only
// decodes the runner's results (internal/clients/sqlserver/sqlservertypes).
var serverForbiddenDeps = []string{
	"github.com/aws/aws-sdk-go-v2/service/kafkaconnect",
	"github.com/aws/aws-sdk-go-v2/service/kafka",
	"github.com/aws/aws-sdk-go-v2/service/ec2",
	"github.com/aws/aws-sdk-go-v2/service/iam",
	"github.com/microsoft/go-mssqldb",
	"piperoom/internal/clients/aiven",
	"piperoom/internal/clients/confluentcloud",
	"piperoom/internal/clients/mskconnect",
	"piperoom/internal/clients/sqlserver",
	"piperoom/internal/setup/runner",
	"piperoom/internal/setup/setupcmd",
	"piperoom/internal/setup/targets/managed",
	"piperoom/internal/setup/targets/strimzi",
}

// TestServerDoesNotLinkRunnerSideCode asks the go tool for the server's full
// build dependency list (test files excluded) and fails when a runner-side
// package appears, so an import that drags one in fails here rather than in
// a binary-size or credential review.
func TestServerDoesNotLinkRunnerSideCode(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps .: %v\n%s", err, out)
	}
	deps := make(map[string]bool)
	for _, line := range strings.Split(string(out), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			deps[line] = true
		}
	}
	// The probe must have listed the server's dependencies, or the check
	// below would pass on an empty answer.
	if !deps["piperoom/internal/room"] || !deps["piperoom/internal/httpapi"] {
		t.Fatalf("go list -deps . did not list the server's dependencies: %d lines", len(deps))
	}
	for _, forbidden := range serverForbiddenDeps {
		if deps[forbidden] {
			t.Errorf("cmd/server depends on %s", forbidden)
		}
	}
}

// The server links aws-sdk-go-v2/config, credentials, service/sts and
// service/s3 legitimately, so they cannot be in serverForbiddenDeps (review
// L11, checked with go list -deps ./cmd/server): config (and through it
// credentials and sts, for assumed roles and SSO) loads the Bedrock model
// client's credentials (internal/agent) and the MSK IAM SASL signer's
// (internal/wiring); service/s3 comes in only through
// github.com/snowflakedb/gosnowflake/v2's stage uploads. What the server
// must not do is use them itself: no piperoom package it links imports
// service/s3, service/sts or credentials, and only the two packages above
// import config. The runner-side AWS services (kafkaconnect, kafka, ec2,
// iam) stay forbidden outright above.
var (
	serverAWSForbiddenImports = []string{
		"github.com/aws/aws-sdk-go-v2/service/s3",
		"github.com/aws/aws-sdk-go-v2/service/sts",
		"github.com/aws/aws-sdk-go-v2/credentials",
	}
	serverAWSConfig          = "github.com/aws/aws-sdk-go-v2/config"
	serverAWSConfigImporters = []string{"piperoom/internal/agent", "piperoom/internal/wiring"}
)

// awsImportProblems reads go list lines of "<package> <import>..." and
// reports each piperoom package that imports an AWS package the server's
// own code must not use.
func awsImportProblems(lines []string) []string {
	var out []string
	for _, line := range lines {
		f := strings.Fields(line)
		if len(f) < 2 || !strings.HasPrefix(f[0], "piperoom/") {
			continue
		}
		for _, imp := range f[1:] {
			for _, bad := range serverAWSForbiddenImports {
				if imp == bad || strings.HasPrefix(imp, bad+"/") {
					out = append(out, f[0]+" imports "+imp)
				}
			}
			if (imp == serverAWSConfig || strings.HasPrefix(imp, serverAWSConfig+"/")) && !slices.Contains(serverAWSConfigImporters, f[0]) {
				out = append(out, f[0]+" imports "+imp)
			}
		}
	}
	return out
}

// TestServerCodeDoesNotUseRunnerAWSPackages (review L11): the server's own
// packages import no S3, STS or credentials package, and config only where
// the Bedrock and MSK IAM SASL clients load theirs.
func TestServerCodeDoesNotUseRunnerAWSPackages(t *testing.T) {
	out, err := exec.Command("go", "list", "-deps", "-f", "{{.ImportPath}}{{range .Imports}} {{.}}{{end}}", ".").CombinedOutput()
	if err != nil {
		t.Fatalf("go list -deps .: %v\n%s", err, out)
	}
	lines := strings.Split(string(out), "\n")
	if !slices.ContainsFunc(lines, func(l string) bool { return strings.HasPrefix(l, "piperoom/internal/wiring ") }) {
		t.Fatalf("go list -deps . did not list the server's packages: %d lines", len(lines))
	}
	for _, p := range awsImportProblems(lines) {
		t.Errorf("cmd/server: %s", p)
	}
}

// TestAWSImportProblems: the classifier above finds each kind of import it
// is there to catch, and passes the server's legitimate ones.
func TestAWSImportProblems(t *testing.T) {
	got := awsImportProblems([]string{
		"piperoom/internal/agent github.com/aws/aws-sdk-go-v2/aws github.com/aws/aws-sdk-go-v2/config",
		"piperoom/internal/wiring github.com/aws/aws-sdk-go-v2/config",
		"github.com/snowflakedb/gosnowflake/v2 github.com/aws/aws-sdk-go-v2/service/s3",
		"piperoom/internal/room github.com/aws/aws-sdk-go-v2/service/s3",
		"piperoom/internal/httpapi github.com/aws/aws-sdk-go-v2/service/sts",
		"piperoom/internal/setup/tools github.com/aws/aws-sdk-go-v2/credentials/stscreds",
		"piperoom/internal/setup/actions github.com/aws/aws-sdk-go-v2/config",
	})
	want := []string{
		"piperoom/internal/room imports github.com/aws/aws-sdk-go-v2/service/s3",
		"piperoom/internal/httpapi imports github.com/aws/aws-sdk-go-v2/service/sts",
		"piperoom/internal/setup/tools imports github.com/aws/aws-sdk-go-v2/credentials/stscreds",
		"piperoom/internal/setup/actions imports github.com/aws/aws-sdk-go-v2/config",
	}
	if !slices.Equal(got, want) {
		t.Errorf("problems = %q, want %q", got, want)
	}
}
