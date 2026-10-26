package room

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/forge"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/setup/plan"
	"piperoom/internal/tools"
)

// pipeline_register (U1-14; docs/design/setup-agent.md §3.6, §3.9, §3.10;
// docs/api.md "Registration"): the setup room's last step. It is an ordinary
// T2 setup action (layer "environment", the approver of §3.6) whose Execute,
// on the server rather than on the runner, writes the owners entry of the
// provisional pipeline and makes the server watch it:
//
//   - the step is gated on a green setup.verified for the current plan
//     (rungs 1 to 7 OK) with no executed step after it, at Create and again
//     before Execute (setupRegisterGate);
//   - the overlap check (owners.Overlap) refuses the proposal when no
//     overlapping pipeline's owner can ever approve it, and the Execute
//     re-check refuses unless the overlapping pipelines' owners are among
//     the recorded approvers (design §3.6);
//   - on a team server the requester's own approval is refused unless solo
//     (authorizeSetupLayer);
//   - with repos: set the owners entry goes out as one pull request through
//     the Q7c action (repo_open_pull_request); the room then waits for
//     pr_merged, exactly as Q7e follows any opened PR, and closes on the
//     merge. Without repos: the entry is written to
//     OWNERS_DIR/<pipeline id>.yaml, the owners set reloads and
//     pipeline.registered is emitted;
//   - the room closes with resolution "pipeline live" and room.labelled
//     gains cause "setup".

const (
	// registerActionName is the action (and only) name of the step.
	registerActionName = "pipeline_register"
	// pipelineLiveResolution is room.closed.resolution after a registration
	// (docs/api.md: "the room then closes with resolution `pipeline live`").
	pipelineLiveResolution = "pipeline live"
	// registerLabelCause is room.labelled.cause of a registered setup room.
	registerLabelCause = "setup"
	// registerPRDir is the repository directory the repos: registration
	// opens its one file in: owners/<pipeline id>.yaml. The tenant repo's
	// globs must cover the path (Q7c's rule, fail closed).
	registerPRDir = "owners"
	// registerTitlePrefix opens the PR title.
	registerTitlePrefix = "Register pipeline"
	// registerStepTimeout bounds the whole registration write (rule 4); the
	// executor's step bound applies on top.
	registerStepTimeout = 30 * time.Second
)

// registerRungs are the ladder rungs 1 to 7 that must be OK (design §3.4;
// plan.Ladder names them in ladder order). Rung 7 with no sink is reported
// green with its note by the ladder runner, so it never blocks here.
var registerRungs = []string{
	plan.RungConnector, plan.RungTasks, plan.RungCanaryTopic, plan.RungSnapshot,
	plan.RungCanaryEvent, plan.RungCanarySubject, plan.RungSinkRow,
}

// NewRegisterAction returns the pipeline_register action bound to the room
// service. cmd/server registers it in every registry a setup room can reach
// (the demo registry today; U1-10's SetupStack in real mode).
func NewRegisterAction(svc *Service) actions.Action { return &registerAction{svc: svc} }

// SetupForge wires the repos: registration path (design §3.10): the
// repo_open_pull_request action instance (fixpr.New, Q7c) the owners entry
// goes out through, and the forge reader whose BaseHead the PR's base_sha is
// read with. nil = no forge: a registration with repos: refuses.
type SetupForge struct {
	PR     actions.Action
	Reader clients.ForgeReader
}

// registerAction is the action. Its Execute runs on the service itself, so
// the action is a thin shell: everything lives in the service methods below,
// which the room's own tests call through the action or directly.
type registerAction struct{ svc *Service }

var (
	_ actions.Action            = (*registerAction)(nil)
	_ actions.Scoped            = (*registerAction)(nil)
	_ actions.PullRequestOpener = (*registerAction)(nil)
)

// Name is the action name a proposal step names.
func (a *registerAction) Name() string { return registerActionName }

// Layer is events.LayerEnvironment: the setup actions' layer (docs/api.md).
func (a *registerAction) Layer() string { return events.LayerEnvironment }

// Describe explains the action for the model and the approval card.
func (a *registerAction) Describe() string {
	return "Register the pipeline this setup built: write its owners entry (the provisional pipeline, the room's " +
		"creator as owner of every layer, connections: as file: references to the registered read credentials, " +
		"auto_approve off, and the setup: block) to OWNERS_DIR and reload, or open it as a pull request when " +
		"repos: is set. The pipeline is then watched by this server (a local target is watched locally and this " +
		"server skips it), and the room closes \"pipeline live\". Needs a green setup.verified for the current " +
		"plan (rungs 1 to 7) with no step executed after it."
}

// ParamsSchema is one closed empty object: everything the registration
// writes is derived from the room's own state (the provisional pipeline, the
// target classes, the recorded approvals), never from params.
func (a *registerAction) ParamsSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required":             []string{},
	}
}

// Validate checks the params' shape only: an empty object. The registration
// gates run in the room service (Create and before Execute); they are not
// this action's Validate, which also runs on paths with no room state.
func (a *registerAction) Validate(_ context.Context, params json.RawMessage) error {
	if err := checkRegisterParams(params); err != nil {
		return fmt.Errorf("room: %s: %w", registerActionName, err)
	}
	return nil
}

// checkRegisterParams refuses any params beyond an empty object: the write
// never takes a hint from the model.
func checkRegisterParams(params json.RawMessage) error {
	if len(strings.TrimSpace(string(params))) == 0 {
		return nil
	}
	var obj map[string]any
	if err := json.Unmarshal(params, &obj); err != nil || len(obj) > 0 {
		return errors.New("params must be an empty object: the registration writes only the room's own state")
	}
	return nil
}

// CheckScope is the no-I/O scope rule: the params name no pipeline resource,
// so every step is in scope (the room service calls it with the provisional
// pipeline, which is exactly what the registration writes).
func (a *registerAction) CheckScope(_ owners.Pipeline, _ json.RawMessage) error { return nil }

// ExecutePullRequest implements actions.PullRequestOpener: the executor
// prefers it and stores the returned reference on action.executed, which
// starts the room's follower for the repos: path (Q7e). For the OWNERS_DIR
// path the reference is nil and nothing is followed.
func (a *registerAction) ExecutePullRequest(ctx context.Context, params json.RawMessage) (string, events.PullRequestRef, error) {
	return a.svc.executeRegister(ctx, params)
}

// Execute is ExecutePullRequest without the reference (a caller that does
// not look for PullRequestOpener).
func (a *registerAction) Execute(ctx context.Context, params json.RawMessage) (string, error) {
	out, _, err := a.ExecutePullRequest(ctx, params)
	return out, err
}

// setupRegisterGate is the gate of a pipeline_register step, at Create and
// again before Execute (design §3.9): the latest setup.verified for the
// current plan has rungs 1 to 7 OK, and no action.executed followed it. The
// refusal is 409 not_verified (docs/api.md).
func setupRegisterGate(snap *RoomSnapshot) error {
	if snap == nil || snap.Setup == nil {
		return notVerified("pipeline_register needs a green setup.verified for the current plan")
	}
	setup := snap.Setup
	if setup.verifiedSeq == 0 {
		return notVerified("pipeline_register needs a green setup.verified for the current plan (the ladder has not run)")
	}
	if setup.execSeq > setup.verifiedSeq {
		return notVerified("a step executed after the latest setup.verified; run the verification ladder again before registering")
	}
	byName := make(map[string]bool, len(setup.Verified))
	for _, r := range setup.Verified {
		if r.OK {
			byName[r.Name] = true
		}
	}
	for _, name := range registerRungs {
		if !byName[name] {
			return notVerified(fmt.Sprintf("pipeline_register needs rung %q green in the latest setup.verified; run the verification ladder again", name))
		}
	}
	return nil
}

// notVerified is 409 not_verified (docs/api.md): pipeline_register without a
// green setup.verified for the current plan.
func notVerified(msg string) *Error {
	return &Error{Status: 409, Code: "not_verified", Msg: msg}
}

// registerOverlap is 409 overlap (docs/api.md): pipeline_register refused by
// the overlap check. The message names the overlap kinds and the requester's
// own tenant's pipelines only; a pipeline of another tenant appears as "a
// pipeline in another tenant" with its admins as the contact, never its id
// (design §3.6).
func registerOverlap(msg string) *Error {
	return &Error{Status: 409, Code: "overlap", Msg: msg}
}

// registerOverlapsOf returns the overlaps of the room's provisional pipeline
// against the current owners set, the overlapping pipelines of the room's own
// tenant, and whether any overlapping pipeline is in another tenant.
func (s *Service) registerOverlapsOf(snap *RoomSnapshot) (ovs []owners.Overlap, pipes []owners.Pipeline, foreign bool) {
	o := s.ownersNow()
	if o == nil || snap == nil || !snap.IsSetupRoom() {
		return nil, nil, false
	}
	prov, ok := snap.SetupProvisional()
	if !ok {
		return nil, nil, false
	}
	ovs = o.Overlap(prov, nil)
	seen := map[string]bool{}
	for _, ov := range ovs {
		if ov.PipelineID == "" {
			foreign = true
			continue
		}
		if seen[ov.PipelineID] {
			continue
		}
		if p, ok := o.Pipeline(ov.PipelineID); ok {
			seen[p.ID] = true
			pipes = append(pipes, p)
		}
	}
	return ovs, pipes, foreign
}

// registerCreateRefusal is the overlap refusal of the Create path: an overlap
// whose owners can never approve the proposal refuses it (409 overlap). A
// pipeline of another tenant's owners cannot approve here; an own-tenant
// overlapping pipeline that owns no layer has no approver either, so the
// Execute re-check could never hold for it. Overlaps whose owners CAN approve
// are allowed: the route already makes their layers the proposal's required
// layers (setupLayers, design §3.6), and the Execute re-check below holds
// them to it.
func (s *Service) registerCreateRefusal(snap *RoomSnapshot) error {
	ovs, pipes, foreign := s.registerOverlapsOf(snap)
	if len(ovs) == 0 {
		return nil
	}
	unapprovable := false
	for _, p := range pipes {
		owned := false
		for _, layer := range owners.KnownLayers() {
			if s.ownersNow().IsOwned(p.ID, layer) {
				owned = true
				break
			}
		}
		if !owned {
			unapprovable = true
			break
		}
	}
	if !unapprovable && !foreign {
		return nil
	}
	return registerOverlap(registerOverlapMessage(ovs, pipes, foreign))
}

// registerOverlapMessage is the refusal's text: the overlap kinds and the
// shared values (always the requester's own names), then the requester's own
// tenant's pipeline ids, or "a pipeline in another tenant" with the contact.
func registerOverlapMessage(ovs []owners.Overlap, pipes []owners.Pipeline, foreign bool) string {
	kinds := make([]string, 0, len(ovs))
	for _, ov := range ovs {
		kinds = append(kinds, ov.Kind+" "+ov.Value)
	}
	msg := "the pipeline overlaps an existing one (" + strings.Join(kinds, ", ") + ")"
	if len(pipes) > 0 {
		ids := make([]string, 0, len(pipes))
		for _, p := range pipes {
			ids = append(ids, p.ID)
		}
		msg += "; the overlapping pipelines are: " + strings.Join(ids, ", ")
	}
	if foreign {
		contact := "its tenant admins"
		for _, ov := range ovs {
			if len(ov.Contact) > 0 {
				contact = ov.Contact.Display()
				break
			}
		}
		msg += "; a pipeline in another tenant overlaps it too (contact: " + contact + ")"
	}
	return msg
}

// registerApprovalsHold reports whether the proposal's recorded approvals
// include, for every overlapping pipeline of the room's tenant, at least one
// of that pipeline's own owners (design §3.6: "an overlap is refused unless
// the overlapping pipeline's owners approved this proposal"). An admin or
// fallback grant does not count for the overlap rule. With no overlapping
// pipeline the rule holds.
//
// The approver's groups are the users.yaml groups (Users.Get) plus the group
// principal the approval itself was granted under: approval.given's
// identity.grant is the principal the approval route matched ("group:<name>"
// or "backup:group:<name>" when a group right gave it), which the server
// derived from the session's groups at approval time (appendApprovalLocked).
// So an approval an IdP group gave is matched from the log alone, with no
// contract change; an approver the server cannot place in any owning
// principal still fails closed.
func (s *Service) registerApprovalsHold(snap *RoomSnapshot, proposalID string, pipes []owners.Pipeline) bool {
	if len(pipes) == 0 {
		return true
	}
	prop, ok := openProposal(*snap, proposalID)
	if !ok || len(prop.approvalActors) == 0 {
		return false
	}
	groupsOf := func(layer, id string) []string {
		var groups []string
		if s.users != nil {
			if u, ok := s.users.Get(id); ok {
				groups = append(groups, u.Groups...)
			}
		}
		if g, ok := grantGroup(prop.approvalGrants[layer]); ok {
			groups = append(groups, g)
		}
		return groups
	}
	for _, p := range pipes {
		covered := false
		for layer, actor := range prop.approvalActors {
			if actor.Kind != events.ActorHuman {
				continue
			}
			groups := groupsOf(layer, actor.ID)
			for _, l := range owners.KnownLayers() {
				if _, ok := s.ownersNow().CanApprove(p.ID, l, actor.ID, groups); ok {
					covered = true
					break
				}
			}
			if covered {
				break
			}
		}
		if !covered {
			return false
		}
	}
	return true
}

// grantGroup returns the group of a group principal detail ("group:<name>"
// or "backup:group:<name>", as owners.CanApprove reports them); false for
// every other detail (a user principal, admin, fallback, solo, policy).
func grantGroup(detail string) (string, bool) {
	detail = strings.TrimPrefix(detail, owners.GrantBackupPrefix)
	if name, ok := strings.CutPrefix(detail, owners.KindGroup+":"); ok && name != "" {
		return name, true
	}
	return "", false
}

// registerPreconditions are the server-side conditions a registration needs
// beyond the ladder gate and the overlap rule, checked at Create (so the
// proposal is refused with the reason) and again before Execute writes
// anything:
//
//   - the pipeline id is not in the owners set already (409 pipeline_exists:
//     someone added it by hand since the room opened);
//   - without repos:, this server has an OWNERS_DIR and an owners reload
//     (Deps.ReloadOwners); with repos:, a forge (Deps.SetupForge). Checked
//     before the write: a file written and then not reloaded would refuse
//     the next attempt as "already exists";
//   - a pipeline this server will watch has read credentials registered for
//     at least one connection key: an entry with no connections: block makes
//     the server read the pipeline with its own process-wide settings
//     (owners.Pipeline.HasConnections), which design §3.7 rules out — the
//     person's click registers the credentials, and nothing is registered
//     without it. A local target is watched by the standalone server and
//     needs none here.
//
// The configuration refusals are 400 validation: the setup API has no code
// for a server that cannot register, and the message names what is missing.
func (s *Service) registerPreconditions(snap *RoomSnapshot) error {
	if snap == nil || snap.Setup == nil {
		return notSetupRoom()
	}
	if o := s.ownersNow(); o != nil {
		if _, ok := o.Pipeline(snap.PipelineID); ok {
			return pipelineExists(snap.PipelineID)
		}
	}
	if len(snap.Setup.req.Repos) > 0 {
		if s.setupForge == nil || s.setupForge.PR == nil {
			return validation("pipeline_register cannot open the registration pull request on this server: repos: is set but no forge is configured")
		}
	} else {
		if strings.TrimSpace(s.setupCfg.OwnersDir) == "" {
			return validation("pipeline_register cannot write the owners entry on this server: OWNERS_DIR is not configured (a standalone server writes next to the project)")
		}
		if s.reloadOwners == nil {
			return validation("pipeline_register cannot register on this server: it has no owners reload wired (the owners set would not see the new pipeline)")
		}
	}
	watchedBy, err := s.registerWatchedBy(snap.Setup)
	if err != nil {
		return err
	}
	if watchedBy != events.WatchedByServer {
		return nil
	}
	prov, ok := snap.SetupProvisional()
	if !ok {
		return notSetupRoom()
	}
	conns, err := s.registerConnections(snap.Setup, prov)
	if err != nil {
		return err
	}
	if len(conns) == 0 {
		return validation("pipeline_register needs the pipeline's read credentials registered with this server first (Register read credentials: POST /api/rooms/{id}/setup/credentials); a pipeline this server watches never reads the server's own connection settings")
	}
	return nil
}

// executeRegister is the registration's Execute. The room id, proposal id and
// step index come from the executor's context (actions.StepFrom). The file
// write and the reload are I/O and never run under the room's lock, exactly
// like the secrets write (setup_secrets.go); the event append runs under it.
func (s *Service) executeRegister(ctx context.Context, params json.RawMessage) (string, events.PullRequestRef, error) {
	if err := checkRegisterParams(params); err != nil {
		return "", events.PullRequestRef{}, err
	}
	step, ok := actions.StepFrom(ctx)
	if !ok || step.RoomID == "" || step.ProposalID == "" || step.StepIndex < 0 {
		return "", events.PullRequestRef{}, fmt.Errorf("room: %s: the executor gave no proposal step", registerActionName)
	}
	sctx, cancel := context.WithTimeout(ctx, registerStepTimeout)
	defer cancel()

	snap, err := s.readRoom(sctx, step.RoomID)
	if err != nil || snap == nil {
		return "", events.PullRequestRef{}, fmt.Errorf("room: %s: read the room: %w", registerActionName, err)
	}
	if !snap.IsSetupRoom() {
		return "", events.PullRequestRef{}, notSetupRoom()
	}
	if err := setupRegisterGate(snap); err != nil {
		return "", events.PullRequestRef{}, err
	}
	// The server-side preconditions again (Create checked them too): a
	// pipeline the owners set gained meanwhile, a server that cannot reload
	// or has no forge, or a server-watched pipeline with no registered
	// credentials refuses here, before anything is written.
	if err := s.registerPreconditions(snap); err != nil {
		return "", events.PullRequestRef{}, err
	}
	ovs, pipes, foreign := s.registerOverlapsOf(snap)
	if len(ovs) > 0 && !s.registerApprovalsHold(snap, step.ProposalID, pipes) {
		return "", events.PullRequestRef{}, registerOverlap(registerOverlapMessage(ovs, pipes, foreign))
	}

	entry, watchedBy, err := s.registerEntry(snap)
	if err != nil {
		return "", events.PullRequestRef{}, err
	}
	if len(snap.Setup.req.Repos) > 0 {
		return s.executeRegisterPR(sctx, snap, step, entry, watchedBy)
	}
	path, err := s.registerFile(sctx, snap, entry)
	if err != nil {
		return "", events.PullRequestRef{}, err
	}
	payload := events.PipelineRegisteredPayload{
		PipelineID: snap.PipelineID, Tenant: snap.SetupTenant(), Path: path, WatchedBy: watchedBy,
	}
	ev, err := s.appendRegistered(sctx, step.RoomID, step.ProposalID, payload)
	if err != nil {
		return "", events.PullRequestRef{}, err
	}
	s.log.Info("room: pipeline registered", "room", step.RoomID, "pipeline", snap.PipelineID, "path", path, "watched_by", watchedBy, "event", ev.Seq)
	return registerSummary(payload), events.PullRequestRef{}, nil
}

// executeRegisterPR is the repos: path (design §3.10): the owners entry goes
// out as one pull request through the Q7c action (repo_open_pull_request),
// the room's follower waits for pr_merged (Q7e), and the merge closes the
// room (registerPRMerged). The PR is opened under the tenant's repo — the
// provisional pipeline has no repos: yet — so the context carries the
// provisional pipeline with the tenant's repo copied in, and Q7c's
// validation (scope, text, secrets, base branch) runs unchanged.
func (s *Service) executeRegisterPR(ctx context.Context, snap *RoomSnapshot, step actions.StepRef, entry []byte, watchedBy string) (string, events.PullRequestRef, error) {
	if s.setupForge == nil || s.setupForge.PR == nil {
		return "", events.PullRequestRef{}, fmt.Errorf("room: %s: repos: is set but no forge is configured; the owners entry cannot go out as a pull request", registerActionName)
	}
	repo, ok := s.tenantRegisterRepo(snap)
	if !ok {
		return "", events.PullRequestRef{}, fmt.Errorf("room: %s: none of the room's repos is declared by a pipeline of its tenant", registerActionName)
	}
	prov, _ := snap.SetupProvisional()
	prov.Repos = []owners.Repo{repo}
	pctx := tools.WithPipeline(ctx, prov)

	rctx, cancel := context.WithTimeout(ctx, registerStepTimeout)
	head, err := s.setupForge.Reader.BaseHead(rctx, forge.RefOf(repo.Ref()))
	cancel()
	if err != nil {
		return "", events.PullRequestRef{}, fmt.Errorf("room: %s: read the base head of %s: %w", registerActionName, repo.ID, err)
	}
	title := fmt.Sprintf("%s %s", registerTitlePrefix, snap.PipelineID)
	body := fmt.Sprintf("The owners entry of pipeline %s, written by Piperoom setup room %s.", snap.PipelineID, step.RoomID)
	prParams, err := json.Marshal(map[string]any{
		"repo":     repo.ID,
		"base_sha": head,
		"title":    title,
		"body":     body,
		"changes": []map[string]any{{
			"path":    filepath.ToSlash(filepath.Join(registerPRDir, snap.PipelineID)) + ".yaml",
			"content": string(entry),
		}},
		"applied_when": map[string]any{"kind": "change"},
	})
	if err != nil {
		return "", events.PullRequestRef{}, fmt.Errorf("room: %s: build the pull request params: %w", registerActionName, err)
	}
	opener, ok := s.setupForge.PR.(actions.PullRequestOpener)
	if !ok {
		return "", events.PullRequestRef{}, fmt.Errorf("room: %s: the wired repo_open_pull_request does not open pull requests", registerActionName)
	}
	out, pr, err := opener.ExecutePullRequest(pctx, prParams)
	if err != nil {
		return "", events.PullRequestRef{}, err
	}
	payload := events.PipelineRegisteredPayload{
		PipelineID: snap.PipelineID, Tenant: snap.SetupTenant(), PullRequestURL: pr.URL, WatchedBy: watchedBy,
	}
	ev, err := s.appendRegistered(ctx, step.RoomID, step.ProposalID, payload)
	if err != nil {
		return "", events.PullRequestRef{}, err
	}
	s.log.Info("room: pipeline registration opened a pull request; the room waits for the merge", "room", step.RoomID, "pipeline", snap.PipelineID, "repo", repo.ID, "number", pr.Number, "event", ev.Seq)
	return out, pr, nil
}

// tenantRegisterRepo returns the first repo of the room's request the
// tenant's pipelines declare (owners.TenantRepo), in request order.
func (s *Service) tenantRegisterRepo(snap *RoomSnapshot) (owners.Repo, bool) {
	o := s.ownersNow()
	if o == nil {
		return owners.Repo{}, false
	}
	for _, id := range snap.Setup.req.Repos {
		if r, ok := o.TenantRepo(snap.SetupTenant(), id); ok {
			return r, true
		}
	}
	return owners.Repo{}, false
}

// appendRegistered emits pipeline.registered (system actor) under the room's
// lock. By is the room's creator (the requester) and ApprovedBy the
// proposal's recorded approvers; on a team server the approval guard
// (authorizeSetupLayer) has already made sure at least one is not the
// creator.
func (s *Service) appendRegistered(ctx context.Context, roomID, proposalID string, payload events.PipelineRegisteredPayload) (events.Event, error) {
	s.syncRoom(ctx, roomID)
	st, err := s.lockRoom(ctx, roomID)
	if err != nil {
		return events.Event{}, err
	}
	defer s.unlockRoom(st)
	if !s.roomOpen(st) {
		return events.Event{}, roomClosed("the room closed before the registration was recorded")
	}
	snap := &st.snap
	if !snap.IsSetupRoom() || snap.PipelineID != payload.PipelineID {
		return events.Event{}, notSetupRoom()
	}
	// Re-check the gate under the lock: the executor's check ran on an
	// unlocked read, and the room (a later executed step, a plan change)
	// may have moved.
	if err := setupRegisterGate(snap); err != nil {
		return events.Event{}, err
	}
	// The overlap re-check runs here too, against the owners set the
	// registration's own reload just published: a pipeline another room
	// registered between the executor's check and this append is seen, and
	// the recorded approvals must cover it as well (design §3.6).
	if ovs, pipes, foreign := s.registerOverlapsOf(snap); len(ovs) > 0 && !s.registerApprovalsHold(snap, proposalID, pipes) {
		return events.Event{}, registerOverlap(registerOverlapMessage(ovs, pipes, foreign))
	}
	payload.By = snap.Setup.creator
	if payload.By.ID == "" {
		payload.By = systemActor
	}
	if prop, ok := openProposal(*snap, proposalID); ok {
		for _, layer := range prop.RequiredLayers {
			if actor, ok := prop.approvalActors[layer]; ok {
				payload.ApprovedBy = append(payload.ApprovedBy, actor)
			}
		}
	}
	return s.emitLocked(ctx, st, roomID, events.PipelineRegistered, systemActor, payload)
}

// registerFile is the OWNERS_DIR path of a registration: the entry is written
// (writeRegisterFile), the owners set reloads (reloadAfterRegister), and the
// reloaded set is checked for the pipeline. The write and the reload are one
// step: when the reload fails, or skips the file, the file is removed again,
// so a failed registration leaves no entry behind and the next attempt is not
// refused as "already exists". The reload is the entry's validation: LoadDir
// skips a per-pipeline file that does not load (parse, shape or validation;
// the reason is logged and recorded in DirSkips) and keeps the rest, so a
// registration never records a pipeline the server did not load. owners.Load
// cannot be this check: a per-pipeline file must not carry the tenants: block
// its tenant: key needs (loadDir). The skip check comes first: a pipeline the
// main file gained meanwhile would make the id "loaded" while this file was
// skipped.
func (s *Service) registerFile(ctx context.Context, snap *RoomSnapshot, entry []byte) (string, error) {
	path, err := s.writeRegisterFile(ctx, snap, entry)
	if err != nil {
		return "", err
	}
	if err := s.reloadAfterRegister(ctx); err != nil {
		s.removeRegisterFile(path)
		return "", fmt.Errorf("%w (the written entry was removed again)", err)
	}
	o := s.ownersNow()
	for _, skip := range o.DirSkips() {
		if filepath.Clean(skip.File) == filepath.Clean(path) {
			s.removeRegisterFile(path)
			return "", fmt.Errorf("room: %s: the written entry was not loaded: %s (the file was removed)", registerActionName, skip.Reason)
		}
	}
	if _, ok := o.Pipeline(snap.PipelineID); !ok {
		s.removeRegisterFile(path)
		return "", fmt.Errorf("room: %s: the written entry was not loaded: the reloaded owners config does not list the pipeline (the file was removed)", registerActionName)
	}
	return path, nil
}

// removeRegisterFile removes the entry a failed registration wrote. A failure
// to remove is logged, never hidden: the file would refuse the next attempt.
func (s *Service) removeRegisterFile(path string) {
	if err := os.Remove(path); err != nil && !errors.Is(err, fs.ErrNotExist) {
		s.log.Warn("room: remove the owners entry of a failed registration", "path", path, "err", err)
	}
}

// reloadAfterRegister reloads the owners set after the registration file
// landed and tells the watcher (Deps.OnOwnersReloaded; cmd/server rebuilds
// the watcher, whose next tick then lists the new pipeline). The caller
// (registerFile) removes the file when the reload errors or skips it.
func (s *Service) reloadAfterRegister(ctx context.Context) error {
	if s.reloadOwners == nil {
		return fmt.Errorf("room: %s: this server has no OWNERS_DIR to reload (standalone servers write next to the project)", registerActionName)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.reloadOwners(); err != nil {
		return fmt.Errorf("room: %s: reload the owners config: %w", registerActionName, err)
	}
	if s.onOwnersReloaded != nil {
		s.onOwnersReloaded()
	}
	return nil
}

// registerEntry renders the owners entry of the room's provisional pipeline
// (design §3.9): the provisional pipeline plus tenant:, the per-pipeline
// owners: block (the creator owns every layer), connections: as file:
// references to the registered read credentials, auto_approve: [] and the
// setup: block. The part before connections: is rendered exactly as
// discover -write-owners renders the same pipeline (cmd/discover's
// RenderOwned; the byte-equality is pinned by a test), so the entry reads
// like any drafted file a human already knows.
func (s *Service) registerEntry(snap *RoomSnapshot) ([]byte, string, error) {
	setup := snap.Setup
	if setup == nil {
		return nil, "", notSetupRoom()
	}
	prov, ok := snap.SetupProvisional()
	if !ok {
		return nil, "", notSetupRoom()
	}
	watchedBy, err := s.registerWatchedBy(setup)
	if err != nil {
		return nil, "", err
	}
	conns, err := s.registerConnections(setup, prov)
	if err != nil {
		return nil, "", err
	}
	creatorID := setup.creator.ID
	if creatorID == "" {
		creatorID = "piperoom"
	}
	doc := registerOwnersNode(prov, creatorID, snap.ID, setup, watchedBy, conns)
	var b strings.Builder
	enc := yaml.NewEncoder(&b)
	enc.SetIndent(2)
	if err := enc.Encode(doc); err != nil {
		return nil, "", fmt.Errorf("room: %s: render the owners entry: %w", registerActionName, err)
	}
	if err := enc.Close(); err != nil {
		return nil, "", fmt.Errorf("room: %s: render the owners entry: %w", registerActionName, err)
	}
	return []byte(b.String()), watchedBy, nil
}

// registerWatchedBy decides who watches the pipeline (design §3.7): a setup
// whose highest target class is local is watched locally; everything else by
// the team server.
func (s *Service) registerWatchedBy(setup *SetupSnapshot) (string, error) {
	class := setupRoomClass(setup)
	switch class {
	case "", plan.ClassLocal, plan.ClassShared, plan.ClassProduction:
		if class == plan.ClassLocal {
			return events.WatchedByLocal, nil
		}
		return events.WatchedByServer, nil
	}
	return "", fmt.Errorf("room: %s: the room's target class %q is not a class", registerActionName, class)
}

// registerConnections maps the registered read credentials to the entry's
// connections: block (design §3.7): every name the credentials click
// registered with this server (setup.credentials_registered, the names the
// server holds a file for) that is an owners connection key
// (owners.ConnectionKeys) becomes a file: reference to
// <PIPEROOM_SECRETS_DIR>/<pipeline>/<name>. The keys are not limited to the
// ones the provisional pipeline's targets name: that set never holds
// warehouse_db_url, which the owners validation requires of every cdc block,
// so a block built from it alone could never load. A secret the driver set
// but never registered (setup.secret_set only) has no file here and is not
// referenced; a registered name that is not a connection key is the runner's
// own store name and is left out. A key with no registered credential is left
// out: the owners validation refuses a connections: block that misses a
// required key, so the reload skips such an entry and the registration fails
// with the reason; an entry with no key at all is refused before the write
// (registerPreconditions), because a pipeline without a connections: block
// reads the server's process-wide settings.
func (s *Service) registerConnections(setup *SetupSnapshot, prov owners.Pipeline) (map[string]string, error) {
	dir := strings.TrimSpace(s.setupCfg.SecretsDir)
	if dir == "" {
		return nil, fmt.Errorf("room: %s: PIPEROOM_SECRETS_DIR is not configured", registerActionName)
	}
	if !secretsPipelineID(prov.ID) {
		return nil, fmt.Errorf("room: %s: the pipeline id is not a valid pipeline id", registerActionName)
	}
	known := map[string]bool{}
	for _, key := range owners.ConnectionKeys() {
		known[key] = true
	}
	// The value is the plain absolute path the loader checks (checkRef:
	// "file: must be an absolute path"), never owners.FileRef: that is the
	// canonical comparison form ("file:" + a lower-cased, forward-slashed
	// path) CheckSharedRefs uses, and written into the entry it would make
	// every registered block fail validation.
	out := map[string]string{}
	for _, name := range setup.credentialNames {
		if !known[name] {
			continue
		}
		out[name] = filepath.Join(dir, prov.ID, name)
	}
	return out, nil
}

// writeRegisterFile writes the entry to OWNERS_DIR/<pipeline id>.yaml. The
// final path is created exclusively (O_EXCL): anything already there,
// whatever it is, refuses the write, so two registrations of one id cannot
// both succeed and a registration never overwrites; a symlink in the way is
// reported as such and never followed. The bytes are written to that file and
// synced, and the file is removed again when any step fails. A temporary file
// renamed into place would be atomic for a reader but not exclusive: rename
// replaces an existing file on every platform, so a check-then-rename could
// overwrite an entry another registration wrote in between. The reload that
// follows (registerFile) reads the complete file; a reader that catches a
// partial write skips it with a reason and sees it whole on its next reload.
// The entry's validation is that reload: owners.Load cannot judge a
// per-pipeline file, whose tenant: key needs the main file's tenants: block.
func (s *Service) writeRegisterFile(ctx context.Context, snap *RoomSnapshot, entry []byte) (string, error) {
	dir := strings.TrimSpace(s.setupCfg.OwnersDir)
	if dir == "" {
		return "", fmt.Errorf("room: %s: this server has no OWNERS_DIR configured (standalone servers write next to the project)", registerActionName)
	}
	id := snap.PipelineID
	if !secretsPipelineID(id) {
		return "", fmt.Errorf("room: %s: the pipeline id is not a valid pipeline id", registerActionName)
	}
	path := filepath.Join(dir, id+".yaml")
	if fi, err := os.Lstat(path); err == nil {
		if fi.Mode()&fs.ModeSymlink != 0 {
			return "", fmt.Errorf("room: %s: %s is a symbolic link; not writing through it", registerActionName, path)
		}
		return "", fmt.Errorf("room: %s: %s already exists", registerActionName, path)
	} else if !errors.Is(err, fs.ErrNotExist) {
		return "", fmt.Errorf("room: %s: check %s: %w", registerActionName, path, err)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("room: %s: create OWNERS_DIR: %w", registerActionName, err)
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		if errors.Is(err, fs.ErrExist) {
			return "", fmt.Errorf("room: %s: %s already exists", registerActionName, path)
		}
		return "", fmt.Errorf("room: %s: create %s: %w", registerActionName, path, err)
	}
	if _, err := f.Write(entry); err != nil {
		_ = f.Close()
		s.removeRegisterFile(path)
		return "", fmt.Errorf("room: %s: write %s: %w", registerActionName, path, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		s.removeRegisterFile(path)
		return "", fmt.Errorf("room: %s: sync %s: %w", registerActionName, path, err)
	}
	if err := f.Close(); err != nil {
		s.removeRegisterFile(path)
		return "", fmt.Errorf("room: %s: close %s: %w", registerActionName, path, err)
	}
	return path, nil
}

// registerSummary is the step's short result.
func registerSummary(p events.PipelineRegisteredPayload) string {
	if p.PullRequestURL != "" {
		return fmt.Sprintf("registered %s: pull request %s (watched by %s); the room waits for the merge", p.PipelineID, p.PullRequestURL, p.WatchedBy)
	}
	return fmt.Sprintf("registered %s at %s (watched by %s)", p.PipelineID, p.Path, p.WatchedBy)
}

// closeRegisteredSetup closes a setup room "pipeline live" once a
// pipeline_register proposal executed in full without repos: (the file path
// registered and reloaded). It runs after the proposal status is recorded
// (service_executor.go), like closeRolledBackSetup. A repos: registration
// stays open until the PR merges (registerPRMerged). proposalID is the
// register proposal, room.labelled's fix_proposal_id.
func (s *Service) closeRegisteredSetup(ctx context.Context, roomID, proposalID string) {
	snap, err := s.readRoom(ctx, roomID)
	if err != nil || snap == nil || !snap.IsSetupRoom() || snap.Setup == nil {
		return
	}
	if snap.Setup.WatchedBy == "" || snap.Setup.regPullRequestURL != "" {
		return
	}
	if _, err := s.closeRoom(ctx, roomID, systemActor, func(st *roomState) (string, error) {
		if !st.snap.IsSetupRoom() || st.snap.Setup == nil || st.snap.Setup.WatchedBy == "" {
			return "", notSetupRoom()
		}
		return pipelineLiveResolution, nil
	}); err != nil {
		var re *Error
		if !errors.As(err, &re) || re.Code != codeRoomClosed {
			s.log.Warn("room: closing the registered room failed", "room", roomID, "err", err)
			return
		}
	}
	if err := s.labelRegistered(ctx, roomID, proposalID); err != nil {
		s.log.Warn("room: labelling the registered room failed", "room", roomID, "err", err)
	}
	s.log.Info("room: setup registered; room closed", "room", roomID)
}

// registerPRMerged closes a setup room whose registration pull request merged
// (the repos: path waits for pr_merged, docs/api.md). The follower calls it
// after recording the merge (pr_follow.go). url is the merged PR's URL; the
// room closes only when it equals the registration PR's URL that the room's
// pipeline.registered recorded. An empty url never matches (fail closed: a
// merge the follower cannot name is not proof that the registration PR
// merged). Every other room and PR is a silent no-op: the follower sees
// merges of every followed PR, incident rooms included, so only a setup room
// that recorded a registration PR even reads on.
func (s *Service) registerPRMerged(ctx context.Context, roomID, proposalID, url string) {
	snap, err := s.readRoom(ctx, roomID)
	if err != nil || snap == nil || !snap.IsSetupRoom() || snap.Setup == nil || snap.Setup.regPullRequestURL == "" {
		return
	}
	if url == "" || snap.Setup.regPullRequestURL != url {
		return
	}
	// closeRoom re-checks under the lock; the read above only keeps the
	// follower's ordinary (non-registration) merges quiet.
	if _, err := s.closeRoom(ctx, roomID, systemActor, func(st *roomState) (string, error) {
		reg := st.snap.Setup
		if !st.snap.IsSetupRoom() || reg == nil || reg.regPullRequestURL == "" {
			return "", notSetupRoom()
		}
		if url == "" || reg.regPullRequestURL != url {
			return "", notSetupRoom()
		}
		return pipelineLiveResolution, nil
	}); err != nil {
		var re *Error
		if !errors.As(err, &re) || re.Code != codeRoomClosed {
			s.log.Warn("room: closing the room of a merged registration PR failed", "room", roomID, "err", err)
			return
		}
	}
	if err := s.labelRegistered(ctx, roomID, proposalID); err != nil {
		s.log.Warn("room: labelling the room of a merged registration PR failed", "room", roomID, "err", err)
	}
	s.log.Info("room: the registration pull request merged; the room closes", "room", roomID)
}

// labelRegistered appends room.labelled {cause: "setup"} for the closed
// room, as the system actor: the registration is the label's source, not a
// person (docs/api.md).
func (s *Service) labelRegistered(ctx context.Context, roomID, proposalID string) error {
	s.syncRoom(ctx, roomID)
	st, err := s.lockRoom(ctx, roomID)
	if err != nil {
		return err
	}
	defer s.unlockRoom(st)
	if s.roomOpen(st) {
		return roomClosed("the room is still open")
	}
	helped := true
	_, err = s.emitLocked(ctx, st, roomID, events.RoomLabelled, systemActor, events.RoomLabelledPayload{
		Cause: registerLabelCause, FixProposalID: proposalID, AgentHelped: &helped,
	})
	return err
}

// ---- Rendering the owners entry (byte-equal to discover -write-owners) ----

// registerOwnersNode builds the entry's document: a complete owners document
// (pipelines: with exactly one pipeline — the shape LoadDir requires of an
// OWNERS_DIR file), the pipeline mapping rendered exactly as discover's
// RenderOwned renders the same pipeline (the keys, their order, the flow
// lists and the comments — internal/discover's render.go pipelineNode), then
// tenant:, owners:, connections:, auto_approve: [] and the setup: block.
// roomID is the setup room's id, the setup: block's room_id (design §3.9).
// The discover part is duplicated here because RenderOwned writes the whole
// document (pipelines: plus layers:) while the per-pipeline file must not
// carry the top-level blocks; the byte-equality test pins the two renderings
// together.
func registerOwnersNode(prov owners.Pipeline, creatorID, roomID string, setup *SetupSnapshot, watchedBy string, conns map[string]string) *yaml.Node {
	m := &yaml.Node{Kind: yaml.MappingNode}
	title := str(prov.Title)
	title.LineComment = comment(titleTODO)
	sinkKey := str("sink_tables")
	if len(prov.SinkTables) > 0 {
		sinkKey.HeadComment = comment(fmt.Sprintf(sinkFoundTODOFmt, strings.Join(prov.SinkTables, ", ")))
	} else {
		sinkKey.HeadComment = comment(sinkMissingTODO)
	}
	m.Content = append(m.Content, str("id"), str(prov.ID), str("title"), title)
	if prov.SourceKind != "" {
		m.Content = append(m.Content, str("source_kind"), str(prov.SourceKind))
	}
	m.Content = append(m.Content,
		str("topic_prefix"), str(prov.TopicPrefix),
		str("source_tables"), flowList(prov.SourceTables),
		str("connectors"), flowList(prov.Connectors),
		str("consumer_groups"), flowList(prov.ConsumerGroups),
		sinkKey, flowList(nil),
		str("sink_ts_column"), &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: "", Style: yaml.DoubleQuotedStyle},
	)
	if prov.TenantName() != "default" {
		m.Content = append(m.Content, str("tenant"), str(prov.TenantName()))
	}
	m.Content = append(m.Content, str("owners"), registerLayersNode(prov, creatorID))
	if len(conns) > 0 {
		m.Content = append(m.Content, str("connections"), registerConnectionsNode(conns))
	}
	m.Content = append(m.Content,
		str("auto_approve"), flowList(nil),
		str("setup"), registerSetupNode(setup, roomID, watchedBy),
	)
	root := &yaml.Node{Kind: yaml.MappingNode, Content: []*yaml.Node{
		str("pipelines"), &yaml.Node{Kind: yaml.SequenceNode, Content: []*yaml.Node{m}},
	}}
	return &yaml.Node{Kind: yaml.DocumentNode, Content: []*yaml.Node{root}}
}

// registerLayersNode is the per-pipeline owners: block: the room's creator
// owns every layer of the pipeline (design §3.9).
func registerLayersNode(prov owners.Pipeline, creatorID string) *yaml.Node {
	m := &yaml.Node{Kind: yaml.MappingNode}
	for _, layer := range prov.Layers() {
		rights := &yaml.Node{Kind: yaml.MappingNode, Style: yaml.FlowStyle, Content: []*yaml.Node{
			str("approvers"), str("user:" + creatorID),
		}}
		m.Content = append(m.Content, str(layer), rights)
	}
	return m
}

// registerConnectionsNode renders connections: as {file: /abs/path} entries
// with the keys sorted (map iteration is random, so a stable file sorts
// them).
func registerConnectionsNode(conns map[string]string) *yaml.Node {
	keys := make([]string, 0, len(conns))
	for k := range conns {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	m := &yaml.Node{Kind: yaml.MappingNode}
	for _, k := range keys {
		ref := &yaml.Node{Kind: yaml.MappingNode, Style: yaml.FlowStyle, Content: []*yaml.Node{
			str("file"), str(conns[k]),
		}}
		m.Content = append(m.Content, str(k), ref)
	}
	return m
}

// registerSetupNode renders the setup: block (owners.SetupSpec; design
// §3.9: target, runner_label, room_id and watched_by).
func registerSetupNode(setup *SetupSnapshot, roomID, watchedBy string) *yaml.Node {
	m := &yaml.Node{Kind: yaml.MappingNode, Content: []*yaml.Node{
		str("target"), str(setup.req.Target),
		str("watched_by"), str(watchedBy),
	}}
	if setup.Runner != nil && setup.Runner.Label != "" {
		m.Content = append(m.Content, str("runner_label"), str(setup.Runner.Label))
	}
	m.Content = append(m.Content, str("room_id"), str(roomID))
	return m
}

// The discover renderer's fixed comments (internal/discover/render.go),
// duplicated so the byte-equality holds.
const (
	titleTODO        = "TODO: human-friendly title"
	sinkFoundTODOFmt = "TODO: sink table found in a sink connector config: %s; add it to sink_tables and set sink_ts_column (the column is not discoverable)"
	sinkMissingTODO  = "TODO: sink table not found; fill in sink_tables (schema.table) and sink_ts_column"
)

// str, flowList and comment are discover's renderer helpers, duplicated for
// the same reason.
func str(v string) *yaml.Node {
	n := &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: v}
	if strings.ContainsFunc(v, isControl) {
		n.Style = yaml.DoubleQuotedStyle
	}
	return n
}

func isControl(r rune) bool { return r < 0x20 || r == 0x7f }

func flowList(items []string) *yaml.Node {
	n := &yaml.Node{Kind: yaml.SequenceNode, Style: yaml.FlowStyle}
	for _, it := range items {
		n.Content = append(n.Content, str(it))
	}
	return n
}

// comment joins lines into a yaml.v3 comment, one "# " line each, with
// control characters removed (discover's comment).
func comment(lines ...string) string {
	out := make([]string, len(lines))
	for i, l := range lines {
		out[i] = "# " + strings.Map(func(r rune) rune {
			if isControl(r) {
				return ' '
			}
			return r
		}, l)
	}
	return strings.Join(out, "\n")
}
