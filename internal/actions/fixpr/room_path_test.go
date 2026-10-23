package fixpr

import (
	"context"
	"strings"
	"testing"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/events"
)

// The room's executor never sees *action: it gets an actions.Action from the
// registry, takes the step's layer from actions.StepLayer, puts the step in
// ctx with actions.WithStep, runs Validate and Preview through the contract
// interfaces and opens the PR through actions.PullRequestOpener. This test
// takes that path against the fake forge, so a local copy of a contract type
// (a PullRequestRef, a StepFrom key of the package's own) fails it.
func TestRoomShapedPath(t *testing.T) {
	e := newEnv(t)
	reg := actions.NewRegistry()
	if err := reg.Register(New(Deps{Reader: e.f, Writer: e.f, PublicURL: "https://piperoom.acme.dev"})); err != nil {
		t.Fatal(err)
	}
	a, ok := reg.Get(Name)
	if !ok {
		t.Fatal("not registered")
	}
	p := raw(t, e.good())

	layer, err := actions.StepLayer(a, e.pl, p)
	if err != nil || layer != "kafka-connect" {
		t.Fatalf("StepLayer = %q, %v", layer, err)
	}
	if _, ok := a.(actions.Deferred); !ok {
		t.Fatal("not actions.Deferred: a policy could approve it")
	}

	approvals := []string{"kafka-connect: Ana"}
	step := actions.StepRef{RoomID: "r_9", ProposalID: "p_7", StepIndex: 1, Title: "connect image", Approvals: approvals}
	ctx := actions.WithStep(e.ctx(), step)
	approvals[0] = "kafka-connect: Mallory" // WithStep copied the slice

	if err := a.Validate(ctx, p); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	pv, ok := a.(actions.Preview)
	if !ok {
		t.Fatal("not actions.Preview")
	}
	res, err := pv.Preview(ctx, p)
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	branch := clients.FixBranch("r_9", "p_7", 1)
	if res.Diff == nil || res.Diff.Branch != branch || res.Diff.Layer != layer || len(res.Diff.Files) != 2 {
		t.Fatalf("PreviewResult.Diff = %+v", res.Diff)
	}

	opener, ok := a.(actions.PullRequestOpener)
	if !ok {
		t.Fatal("not actions.PullRequestOpener: the executor would call Execute and record no PR")
	}
	out, ref, err := opener.ExecutePullRequest(ctx, p)
	if err != nil {
		t.Fatalf("ExecutePullRequest: %v", err)
	}
	pr, err := e.f.PullRequest(context.Background(), cfgRef, ref.Number)
	if err != nil {
		t.Fatalf("the PR the ref names: %v", err)
	}
	want := events.PullRequestRef{Repo: "connect-config", Number: pr.Number, URL: pr.URL, Branch: branch, HeadSHA: pr.HeadSHA}
	if ref != want || pr.HeadRef != branch || !strings.Contains(out, pr.URL) {
		t.Fatalf("ref = %+v, want %+v (out %q)", ref, want, out)
	}
	body := e.f.Opened()[0].Body
	if !strings.Contains(body, "proposal p_7, step 1") || !strings.Contains(body, "kafka-connect: Ana") || strings.Contains(body, "Mallory") {
		t.Fatalf("footer does not name the executor's step and approvers:\n%s", body)
	}

	// Without the executor's step in ctx the action refuses to run.
	if _, _, err := opener.ExecutePullRequest(e.ctx(), p); err == nil || !strings.Contains(err.Error(), "StepFrom") {
		t.Fatalf("no step: %v", err)
	}
}

// The step's repo reference carries the url's host from owners.yaml, so the
// forge client's per-call host check sees it.
func TestRefCarriesHost(t *testing.T) {
	e := newEnv(t)
	p, err := parse(raw(t, e.good()), []string{"github.com"})
	if err != nil {
		t.Fatal(err)
	}
	r, err := inScope(e.pl, p)
	if err != nil {
		t.Fatal(err)
	}
	if r.ref.Host != "github.com" || r.ref.Owner != "acme" || r.ref.Name != "connect-config" || r.ref.Base != "main" {
		t.Fatalf("ref = %+v", r.ref)
	}
}
