package room

import (
	"context"
	"encoding/json"
	"strings"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/redact"
	"piperoom/internal/tools"
	"piperoom/internal/topology"
)

// previewBudget bounds one proposal's preview. The web shows "Preview
// unavailable" when no proposal.previewed arrives within 20 s (QA X2), so the
// whole preview, steps and topology together, finishes well inside that: the
// topology build runs beside the steps, the steps run side by side, and a
// step still running at its deadline is recorded as an error even when the
// action ignores its context.
type previewBudget struct {
	total time.Duration // the whole preview, before the append
	step  time.Duration // one step
	topo  time.Duration // the topology build (downstream lists only)
}

var defaultPreviewBudget = previewBudget{total: 12 * time.Second, step: 10 * time.Second, topo: 5 * time.Second}

const (
	// previewWriteTimeout bounds the append of proposal.previewed.
	previewWriteTimeout = 5 * time.Second
	// maxDownstream caps the node ids listed for one step.
	maxDownstream = 12
	// previewTimedOut is the error of a step that did not answer in time.
	previewTimedOut = "preview timed out"
)

// previewProposal runs actions.Preview for every step of a new proposal and
// appends one proposal.previewed event as the system actor. A failure of any
// step is stored as that step's error: the preview is informational and never
// blocks an approval. Nothing here writes to a target system.
func (s *Service) previewProposal(parent context.Context, roomID, pipelineID, proposalID string, steps []events.ProposalStep) {
	b := s.preview
	start := time.Now()
	ctx, cancel := context.WithTimeout(parent, b.total)
	defer cancel()

	acts := s.actionsFor(pipelineID)
	scoped := tools.WithPipelineID(ctx, pipelineID)
	if s.owners != nil {
		if p, ok := s.owners.Pipeline(pipelineID); ok {
			scoped = tools.WithPipeline(ctx, p)
		}
	}

	// The graph only adds downstream lists, so it never holds the preview
	// back: it builds beside the steps and is dropped if it is late.
	graphCh := make(chan *topology.Graph, 1)
	if s.topo != nil {
		tctx, tcancel := context.WithTimeout(ctx, b.topo)
		go func() {
			defer tcancel()
			if g, err := s.topo.Build(tctx, pipelineID); err == nil {
				graphCh <- &g
				return
			}
			graphCh <- nil
		}()
	} else {
		graphCh <- nil
	}

	out := make([]events.StepPreview, len(steps))
	type stepDone struct {
		i   int
		res actions.PreviewResult
		err error
	}
	// Buffered: a step that answers after its deadline never blocks.
	done := make(chan stepDone, len(steps))
	waiting := 0
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
		out[i].Error = previewTimedOut // until it answers
		waiting++
		go func(i int, params json.RawMessage) {
			sctx, scancel := context.WithTimeout(scoped, b.step)
			defer scancel()
			res, err := pv.Preview(sctx, params)
			done <- stepDone{i: i, res: res, err: err}
		}(i, step.Params)
	}
	stepTimer := time.NewTimer(b.step)
	defer stepTimer.Stop()
collect:
	for ; waiting > 0; waiting-- {
		select {
		case d := <-done:
			if d.err != nil {
				out[d.i].Error = truncateDetail(sanitizeAgentText(redactText(d.err.Error())))
				continue
			}
			res := d.res
			res.Target = sanitizeAgentText(redactText(res.Target))
			res.Current = sanitizeAgentText(redactText(res.Current))
			res.Expected = sanitizeAgentText(redactText(res.Expected))
			res.Rollback = sanitizeAgentText(redactText(res.Rollback))
			if res.DryRun != nil {
				dr := *res.DryRun
				dr.Text = sanitizeAgentText(redactText(dr.Text))
				res.DryRun = &dr
			}
			out[d.i].Error = ""
			out[d.i].Preview = &res
		case <-stepTimer.C:
			break collect
		case <-ctx.Done():
			break collect
		}
	}

	// Wait for the graph at most until its own deadline, measured from the
	// start, in case a builder ignores its context.
	var graph *topology.Graph
	topoTimer := time.NewTimer(time.Until(start.Add(b.topo)))
	defer topoTimer.Stop()
	select {
	case graph = <-graphCh:
	case <-topoTimer.C: // still building: no downstream lists this time
	}
	for i, step := range steps {
		if out[i].Preview != nil {
			out[i].Preview.Downstream = downstreamOf(graph, step)
		}
	}
	if parent.Err() != nil {
		return // the service is shutting down
	}
	s.appendPreview(parent, roomID, proposalID, events.ProposalPreviewedPayload{ProposalID: proposalID, Steps: out, At: s.now().UTC()})
}

// appendPreview writes proposal.previewed if the proposal is still open, with
// its own deadline (the preview's may be spent). The room is loaded when this
// replica's cache dropped it meanwhile. A failed append is logged: the web
// then says "Preview unavailable", which is true.
func (s *Service) appendPreview(parent context.Context, roomID, proposalID string, payload events.ProposalPreviewedPayload) {
	wctx, cancel := context.WithTimeout(parent, previewWriteTimeout)
	defer cancel()
	st, err := s.lockRoom(wctx, roomID)
	if err != nil {
		s.log.Warn("room: preview: load room failed", "room", roomID, "proposal", proposalID, "err", err)
		return
	}
	defer s.unlockRoom(st)
	if !s.roomOpen(st) || findOpenProposal(st.snap, proposalID) == nil {
		return // decided, withdrawn or closed while previewing
	}
	if _, err := s.emitLocked(wctx, st, roomID, events.ProposalPreviewed, systemActor, payload); err != nil {
		s.log.Warn("room: preview append failed", "room", roomID, "proposal", proposalID, "err", err)
	}
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
