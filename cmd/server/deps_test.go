package main

import (
	"os/exec"
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
