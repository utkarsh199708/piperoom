package room

import (
	"encoding/json"
	"slices"
	"strings"

	"piperoom/internal/agent"
	"piperoom/internal/events"
)

// pipelineKind returns the owners.yaml kind of the pipeline for
// agent.Alert.PipelineKind (P0 item 6): "cdc" or "batch". A pipeline
// owners.yaml does not know, or no owners config at all, gives "": the
// contract reads empty as cdc, and the runner can still tell "the file did not
// say" from "the file said cdc".
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
// pipeline kind (P4b filters the room's tools by it).
func (s *Service) newAlert(roomID, title, pipelineID string, raw json.RawMessage) agent.Alert {
	return agent.Alert{
		RoomID: roomID, Title: title, PipelineID: pipelineID, Raw: raw,
		PipelineKind: s.pipelineKind(pipelineID),
	}
}

// knownLayers returns the layers of the pipeline kinds owners.yaml uses
// (events.LayersOf of each pipeline's kind), in events.Layers order, never
// nil. With no owners config, or a file without pipelines, that is the cdc
// layers: every room then has a pipeline owners.yaml does not know, and such a
// room reads as cdc. LayersFor lists no layer outside this set, so a CDC-only
// install never offers orchestrator or transform. (P4a-4)
func (s *Service) knownLayers() []string {
	kinds := map[string]bool{}
	if s.owners != nil {
		for i := range s.owners.Pipelines {
			kinds[s.owners.Pipelines[i].KindOrDefault()] = true
		}
	}
	if len(kinds) == 0 {
		kinds[events.PipelineKindCDC] = true
	}
	out := []string{}
	for _, l := range events.Layers {
		for k := range kinds {
			if slices.Contains(events.LayersOf(k), l) {
				out = append(out, l)
				break
			}
		}
	}
	return out
}

// PipelineForDAG returns the one owners.yaml pipeline whose dags list dagID,
// or "" when none does, more than one does, or there is no owners config. It
// is the single-id form of internal/alertin's matchDAG (exact, case-sensitive
// match after trimming spaces; a DAG two pipelines list is never guessed), for
// the unsigned dev webhook, which reads one top-level dag_id. The id is
// untrusted: it is only compared, never logged.
func (s *Service) PipelineForDAG(dagID string) string {
	dagID = strings.TrimSpace(dagID)
	if s.owners == nil || dagID == "" {
		return ""
	}
	match := ""
	for i := range s.owners.Pipelines {
		p := &s.owners.Pipelines[i]
		if !slices.Contains(p.DAGs, dagID) {
			continue
		}
		if match != "" && match != p.ID {
			return ""
		}
		match = p.ID
	}
	return match
}
