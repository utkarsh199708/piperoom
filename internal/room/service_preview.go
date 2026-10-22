package room

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/redact"
	"piperoom/internal/topology"
)

const (
	// previewTotalTimeout bounds one proposal's whole preview; a step gets
	// previewStepTimeout of it. Six slow steps cannot add up past the total.
	previewTotalTimeout = 30 * time.Second
	previewStepTimeout  = 10 * time.Second
	// maxDownstream caps the node ids listed for one step.
	maxDownstream = 12
)

// previewProposal runs actions.Preview for every step of a new proposal and
// appends one proposal.previewed event as the system actor. A failure of any
// step is stored as that step's error: the preview is informational and never
// blocks an approval. Nothing here writes to a target system.
func (s *Service) previewProposal(parent context.Context, roomID, pipelineID, proposalID string, steps []events.ProposalStep) {
	ctx, cancel := context.WithTimeout(parent, previewTotalTimeout)
	defer cancel()

	acts := s.actionsFor(pipelineID)
	scoped := s.actionCtx(ctx, roomID, pipelineID)

	var graph *topology.Graph
	if s.topo != nil {
		tctx, tcancel := context.WithTimeout(ctx, previewStepTimeout)
		if g, err := s.topo.Build(tctx, pipelineID); err == nil {
			graph = &g
		}
		tcancel()
	}

	out := make([]events.StepPreview, len(steps))
	for i, step := range steps {
		out[i] = events.StepPreview{StepIndex: i}
		a, ok := acts.Get(step.Action)
		var pv actions.Preview
		if ok {
			pv, ok = a.(actions.Preview)
		}
		if !ok {
			out[i].Error = "no preview for this action"
			continue
		}
		sctx, scancel := context.WithTimeout(scoped, previewStepTimeout)
		res, err := pv.Preview(sctx, step.Params)
		scancel()
		if err != nil {
			out[i].Error = truncateDetail(sanitizeAgentText(redactText(err.Error())))
			continue
		}
		res.Target = sanitizeAgentText(redactText(res.Target))
		res.Current = sanitizeAgentText(redactText(res.Current))
		res.Expected = sanitizeAgentText(redactText(res.Expected))
		res.Rollback = sanitizeAgentText(redactText(res.Rollback))
		if res.DryRun != nil {
			dr := *res.DryRun
			dr.Text = sanitizeAgentText(redactText(dr.Text))
			res.DryRun = &dr
		}
		res.Downstream = downstreamOf(graph, step)
		out[i].Preview = &res
	}

	payload := events.ProposalPreviewedPayload{ProposalID: proposalID, Steps: out, At: s.now().UTC()}

	st := s.lockCachedRoom(roomID)
	if st == nil {
		return
	}
	defer s.unlockRoom(st)
	if !s.roomOpen(st) {
		return
	}
	open := false
	for _, p := range st.snap.OpenProposals {
		if p.ProposalID == proposalID {
			open = true
			break
		}
	}
	if !open {
		return // decided or withdrawn while previewing
	}
	if _, err := s.emitLocked(parent, st, roomID, events.ProposalPreviewed, systemActor, payload); err != nil {
		s.log.Warn("room: preview append failed", "room", roomID, "proposal", proposalID, "err", err)
		return
	}
	// The auto-approve countdown starts once the preview is in the log, and
	// only when it passed (autoapprove.go).
	s.maybeScheduleLocked(parent, st, roomID, proposalID)
}

func redactText(s string) string {
	t, _ := redact.Text(s)
	return t
}

// downstreamOf lists the node ids reachable over data edges from the node a
// step targets, in breadth-first order, at most maxDownstream. nil when there
// is no topology or no node matches (the UI says "downstream unknown").
func downstreamOf(g *topology.Graph, step events.ProposalStep) []string {
	if g == nil {
		return nil
	}
	var params struct {
		Connector string `json:"connector"`
		Subject   string `json:"subject"`
		Group     string `json:"group"`
	}
	if json.Unmarshal(step.Params, &params) != nil {
		return nil
	}
	var kind topology.NodeKind
	var name string
	switch {
	case params.Connector != "":
		kind, name = topology.KindConnector, params.Connector
	case params.Subject != "":
		topic := strings.TrimSuffix(strings.TrimSuffix(params.Subject, "-value"), "-key")
		kind, name = topology.KindTopic, topic
	case params.Group != "":
		kind, name = topology.KindConsumerGroup, params.Group
	default:
		return nil
	}
	start := ""
	for _, n := range g.Nodes {
		if n.Kind == kind && n.Name == name {
			start = n.ID
			break
		}
	}
	if start == "" {
		return nil
	}
	seen := map[string]bool{start: true}
	queue := []string{start}
	var out []string
	for len(queue) > 0 && len(out) < maxDownstream {
		cur := queue[0]
		queue = queue[1:]
		for _, e := range g.Edges {
			if e.From != cur || (e.Kind != "" && e.Kind != topology.EdgeData) || seen[e.To] {
				continue
			}
			seen[e.To] = true
			out = append(out, e.To)
			queue = append(queue, e.To)
			if len(out) == maxDownstream {
				break
			}
		}
	}
	return out
}
