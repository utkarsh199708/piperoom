package events

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// Q7 (Q0 item 6): the pull-request fix's event shapes.

func TestQ7Constants(t *testing.T) {
	if PullRequestUpdated != "pull_request.updated" {
		t.Errorf("PullRequestUpdated = %q", PullRequestUpdated)
	}
	if DeferredNotApplied != "not_applied" || DeferredNotDeployed != "not_deployed" {
		t.Errorf("deferred outcomes = %q, %q", DeferredNotApplied, DeferredNotDeployed)
	}
}

func TestQ7JSONShapes(t *testing.T) {
	merged := time.Date(2026, 10, 23, 14, 2, 0, 0, time.UTC)
	cases := []struct {
		name string
		v    any
		want string
	}{
		{"preview without diff marshals as before", PreviewResult{Target: "t", Current: "c", Expected: "e"},
			`{"target":"t","current":"c","expected":"e","reversible":false}`},
		{"action result without PR marshals as before", ActionExecutedPayload{ProposalID: "p_1", OK: true, Output: "ok"},
			`{"proposal_id":"p_1","step_index":0,"ok":true,"output":"ok"}`},
		{"action result with PR", ActionExecutedPayload{ProposalID: "p_1", OK: true, Output: "opened",
			PullRequest: &PullRequestRef{Repo: "connect-config", Number: 12, URL: "https://github.com/a/b/pull/12", Branch: "piperoom/r_1-p_1-0", HeadSHA: "abc"}},
			`{"proposal_id":"p_1","step_index":0,"ok":true,"output":"opened","pull_request":{"repo":"connect-config","number":12,"url":"https://github.com/a/b/pull/12","branch":"piperoom/r_1-p_1-0","head_sha":"abc"}}`},
		{"open PR update: ref fields inline, zero merge fields omitted", PullRequestUpdatedPayload{ProposalID: "p_1", StepIndex: 1,
			PullRequestRef: PullRequestRef{Repo: "r", Number: 3, URL: "u", Branch: "b", HeadSHA: "h"}, State: "open",
			Checks: PRChecks{State: "pending", Total: 2}},
			`{"proposal_id":"p_1","step_index":1,"repo":"r","number":3,"url":"u","branch":"b","head_sha":"h","state":"open","checks":{"state":"pending","total":2}}`},
		{"merged PR update", PullRequestUpdatedPayload{ProposalID: "p_1", PullRequestRef: PullRequestRef{Repo: "r", Number: 3, URL: "u", Branch: "b", HeadSHA: "h"},
			State: "merged", HeadChanged: true, MergeSHA: "m", MergedAt: merged, Checks: PRChecks{State: "failure", Total: 2, Failed: []string{"lint"}}},
			`{"proposal_id":"p_1","step_index":0,"repo":"r","number":3,"url":"u","branch":"b","head_sha":"h","state":"merged","head_changed":true,"merge_sha":"m","merged_at":"2026-10-23T14:02:00Z","checks":{"state":"failure","total":2,"failed":["lint"]}}`},
		{"diff preview", DiffPreview{Repo: "r", RepoURL: "https://github.com/a/r", Base: "main", BaseSHA: "s", Branch: "b", Layer: "kafka-connect",
			Files: []FileDiff{{Path: "Dockerfile", Status: "modified", Unified: "@@ -1 +1 @@\n-a\n+b\n", Added: 1, Removed: 1}}},
			`{"repo":"r","repo_url":"https://github.com/a/r","base":"main","base_sha":"s","branch":"b","layer":"kafka-connect","files":[{"path":"Dockerfile","status":"modified","unified":"@@ -1 +1 @@\n-a\n+b\n","added":1,"removed":1}]}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			b, err := json.Marshal(c.v)
			if err != nil {
				t.Fatal(err)
			}
			if string(b) != c.want {
				t.Fatalf("JSON\n got %s\nwant %s", b, c.want)
			}
		})
	}
}

func TestQ7PreviewDiffRoundTrip(t *testing.T) {
	in := PreviewResult{Target: "github.com/a/r @ main", Diff: &DiffPreview{Repo: "r", Hosts: []string{"repo1.maven.org"},
		Warnings: []string{"a download (curl)"}, Draft: true}}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), `"diff":{`) {
		t.Fatalf("diff missing: %s", b)
	}
	var out PreviewResult
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Diff == nil || out.Diff.Repo != "r" || !out.Diff.Draft || len(out.Diff.Hosts) != 1 || len(out.Diff.Warnings) != 1 {
		t.Fatalf("round trip = %+v", out.Diff)
	}
}
