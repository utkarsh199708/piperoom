package room

import (
	"encoding/json"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/alertin"
	"piperoom/internal/events"
	"piperoom/internal/owners"
)

// pipelineKind returns the owners.yaml kind of the pipeline for
// agent.Alert.PipelineKind (P0 item 6): "cdc" or "batch". A pipeline
// owners.yaml does not know, or no owners config at all, gives "": the
// contract reads empty as cdc, and the runner can still tell "the file did not
// say" from "the file said cdc".
// pipelineLayers returns the layers of the pipeline, upstream first: the
// owners.yaml pipeline's Layers (its source_kind's source layer first, so
// "mysql" for a MySQL pipeline), or the layers of its kind for a pipeline
// owners.yaml does not know. New slice per call.
func (s *Service) pipelineLayers(pipelineID string) []string {
	if s.owners != nil {
		if p, ok := s.owners.Pipeline(pipelineID); ok {
			return p.Layers()
		}
	}
	return events.LayersOf(s.pipelineKind(pipelineID))
}

func (s *Service) pipelineKind(pipelineID string) string {
	if s.owners == nil {
		return ""
	}
	p, ok := s.owners.Pipeline(pipelineID)
	if !ok {
		return ""
	}
	return p.KindOrDefault()
}

// newAlert builds the agent.Alert of a room's run: the main run, the triage
// pass and the shadow run all come through here, so none of them forgets the
// pipeline kind (P4b filters the room's tools by it), the teams or the
// alert's time (X4). openedAt is the room's snapshot OpenedAt, read by the
// caller under the room's lock (the run's goroutine must not read st.snap).
func (s *Service) newAlert(roomID, title, pipelineID string, openedAt time.Time, raw json.RawMessage) agent.Alert {
	return agent.Alert{
		RoomID: roomID, Title: title, PipelineID: pipelineID, Raw: raw,
		PipelineKind: s.pipelineKind(pipelineID),
		Teams:        s.alertTeams(pipelineID),
		At:           openedAt,
		Staging:      s.stagingOn(pipelineID),
	}
}

// alertTeams is agent.Alert.Teams (X4): the owners.yaml teams of the
// pipeline's tenant, ids and owned services only; the Slack channel stays
// with the room service (page-team). nil without owners or teams.
func (s *Service) alertTeams(pipelineID string) []agent.Team {
	if s.owners == nil {
		return nil
	}
	var out []agent.Team
	for _, t := range s.owners.TeamsFor(pipelineID) {
		out = append(out, agent.Team{ID: t.ID, Owns: t.Owns})
	}
	return out
}

// knownLayers returns the layers of the pipelines owners.yaml lists (each
// pipeline's Layers: its kind's layers with its source_kind's source layer),
// in owners.KnownLayers order, never nil. With no owners config, or a file without pipelines, that is the cdc
// layers: every room then has a pipeline owners.yaml does not know, and such a
// room reads as cdc. LayersFor lists no layer outside this set, so a CDC-only
// install never offers orchestrator or transform. (P4a-4)
func (s *Service) knownLayers() []string {
	used := map[string]bool{}
	if s.owners != nil {
		for i := range s.owners.Pipelines {
			for _, l := range s.owners.Pipelines[i].Layers() {
				used[l] = true
			}
		}
	}
	if len(used) == 0 {
		for _, l := range events.LayersOf(events.PipelineKindCDC) {
			used[l] = true
		}
	}
	out := []string{}
	for _, l := range owners.KnownLayers() {
		if used[l] {
			out = append(out, l)
		}
	}
	return out
}

// PipelineForDAG returns the one batch pipeline in owners.yaml whose dags list
// dagID, or "" when none or several do, or there is no owners config. It is
// alertin.MatchDAG with one id, for the unsigned dev webhook, which reads one
// top-level dag_id: the dev webhook and the signed routes then resolve a
// dag_id the same way, including that a dag_id never selects a CDC pipeline.
// The id is untrusted: it is only compared, never logged.
func (s *Service) PipelineForDAG(dagID string) string {
	return alertin.MatchDAG(s.owners, dagID)
}
