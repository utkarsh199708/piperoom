package room

import (
	"context"
	"encoding/json"
	"fmt"

	"piperoom/internal/events"
)

// Checklist limits (docs/api.md, POST /api/rooms/{id}/checklist).
const (
	maxChecklistNoteChars = 300
	maxChecklistIDChars   = 200
	runbookTool           = "runbook_lookup"
)

// ChecklistInput is the body of POST /api/rooms/{id}/checklist.
type ChecklistInput struct {
	StepID    string
	ToolUseID string
	Hit       int
	Item      int
	Done      bool
	Note      string
}

// ChecklistStep ticks or unticks one runbook item in an open room (POST
// /api/rooms/{id}/checklist). The tool result must be a runbook_lookup result
// of this room and the item must exist in its data; the same state again still
// appends (idempotent for the client, last event per key wins in the fold).
func (s *Service) ChecklistStep(ctx context.Context, user User, roomID string, in ChecklistInput) (events.Event, error) {
	if in.StepID == "" || in.ToolUseID == "" {
		return events.Event{}, validation("step_id and tool_use_id are required")
	}
	if err := checkLen("step_id", in.StepID, maxChecklistIDChars); err != nil {
		return events.Event{}, err
	}
	if err := checkLen("tool_use_id", in.ToolUseID, maxChecklistIDChars); err != nil {
		return events.Event{}, err
	}
	if in.Hit < 0 || in.Item < 0 {
		return events.Event{}, validation("hit and item must be 0 or more")
	}
	if err := checkLen("note", in.Note, maxChecklistNoteChars); err != nil {
		return events.Event{}, err
	}
	if err := rejectUnstorable(in.StepID, in.ToolUseID, in.Note); err != nil {
		return events.Event{}, err
	}
	// Shared store: fold what other replicas wrote, reading outside s.mu.
	s.syncRoom(ctx, roomID)
	s.mu.Lock()
	defer s.mu.Unlock()
	st, err := s.getStateLocked(ctx, roomID)
	if err != nil {
		return events.Event{}, err
	}
	if !s.roomOpen(st) {
		return events.Event{}, roomClosed("room is closed")
	}
	if err := s.checkRunbookItem(ctx, roomID, in); err != nil {
		return events.Event{}, err
	}
	e, err := s.emitLocked(ctx, st, roomID, events.ChecklistStepDone, s.humanActor(user), events.ChecklistStepDonePayload{
		StepID: in.StepID, ToolUseID: in.ToolUseID, Hit: in.Hit, Item: in.Item, Done: in.Done, Note: in.Note,
	})
	if err != nil {
		return events.Event{}, mapUnstorable(err)
	}
	return e, nil
}

// checkRunbookItem finds the agent.tool_result of in.ToolUseID in the room log
// and checks it is a runbook_lookup result with data.hits[Hit].items[Item].
func (s *Service) checkRunbookItem(ctx context.Context, roomID string, in ChecklistInput) error {
	evs, err := s.allEvents(ctx, roomID)
	if err != nil {
		return fmt.Errorf("room: read log for checklist: %w", err)
	}
	for _, e := range evs {
		if e.Type != events.AgentToolResult {
			continue
		}
		p, err := events.Decode[events.AgentToolResultPayload](e)
		if err != nil || p.ToolUseID != in.ToolUseID {
			continue
		}
		if p.Tool != runbookTool || p.IsError {
			return validation("tool_use_id is not a runbook_lookup result of this room")
		}
		var data struct {
			Hits []struct {
				Items []json.RawMessage `json:"items"`
			} `json:"hits"`
		}
		if err := json.Unmarshal(p.Data, &data); err != nil || in.Hit >= len(data.Hits) || in.Item >= len(data.Hits[in.Hit].Items) {
			return validation("hit and item must point at an item of that runbook_lookup result")
		}
		return nil
	}
	return validation("tool_use_id is not a runbook_lookup result of this room")
}
