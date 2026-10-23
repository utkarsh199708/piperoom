package room

import (
	"piperoom/internal/actions"
	"piperoom/internal/owners"
)

// The approving layer of a source-neutral source action depends on the room
// pipeline's source kind (Q8o, docs/design/mysql-source.md §4.1, §5.3): a
// source_grant_select step on a MySQL pipeline is approved by the mysql owner,
// on a Postgres pipeline by the postgres owner. The contract's marker
// actions.SourceLayered and helper actions.LayerFor (Q0 item 7) have not
// landed in internal/actions/action.go yet, so they are mirrored here under
// the design's names; when the contract lands, stepLayer becomes
// actions.LayerFor (Q8o's contract change request).

// sourceLayered mirrors actions.SourceLayered (design §5.3): an action on the
// pipeline's source database whose approving layer is the pipeline's source
// layer, not Layer(). Layer() of such an action answers "postgres".
type sourceLayered interface {
	actions.Action
	SourceLayered()
}

// stepLayer mirrors actions.LayerFor(a, sourceKind): the layer whose owner
// approves a step of a on a pipeline whose source_kind is sourceKind
// ("" = postgres). A sourceLayered action gets the source layer ("" for an
// unknown kind, which isKnownLayer refuses, so it fails closed); any other
// action its own Layer().
func stepLayer(a actions.Action, sourceKind string) string {
	if _, ok := a.(sourceLayered); ok {
		return sourceLayerOf(sourceKind)
	}
	return a.Layer()
}

// sourceLayerOf mirrors events.SourceLayer (design §5.1).
func sourceLayerOf(sourceKind string) string {
	switch sourceKind {
	case "", owners.SourceKindPostgres:
		return "postgres"
	case owners.SourceKindMySQL:
		return owners.LayerMySQL
	}
	return ""
}

// pipelineSourceKind returns the source_kind of the owners.yaml pipeline
// ("" when no owners config is loaded or the id matches no pipeline; it reads
// as postgres, today's behaviour).
func (s *Service) pipelineSourceKind(pipelineID string) string {
	if s.owners == nil || pipelineID == "" {
		return ""
	}
	p, ok := s.owners.Pipeline(pipelineID)
	if !ok {
		return ""
	}
	return p.SourceKindOrDefault()
}
