package room

import (
	"context"
	"strings"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/integrations/spec"
)

// The room service implements spec.Notifier: an integration change made on
// the Integrations page is posted in every open room (docs/design/integrations.md
// §4.5, §7), so people watching an incident see that a connection changed
// under the agent. The message holds the kind title, the actor and field
// names, never a value.
var _ spec.Notifier = (*Service)(nil)

// integrationVerbs maps an audit action to the words of the room message.
var integrationVerbs = map[spec.AuditAction]string{
	spec.AuditCreated:       "connected",
	spec.AuditUpdated:       "updated",
	spec.AuditEnabled:       "enabled",
	spec.AuditDisabled:      "disabled",
	spec.AuditDeleted:       "removed",
	spec.AuditSecretRotated: "secret rotated",
}

// IntegrationMessage is the room text for an integration change, e.g.
// "Kafka Connect integration updated by priya (password, url)". ok is false
// for actions that change nothing a room cares about (a test).
func IntegrationMessage(e spec.AuditEntry, kindTitle string) (text string, ok bool) {
	verb, ok := integrationVerbs[e.Action]
	if !ok {
		return "", false
	}
	if kindTitle == "" {
		kindTitle = "An"
	}
	var b strings.Builder
	b.WriteString(kindTitle)
	b.WriteString(" integration ")
	b.WriteString(verb)
	if e.Actor != "" {
		b.WriteString(" by ")
		b.WriteString(e.Actor)
	}
	if len(e.Changed) > 0 {
		b.WriteString(" (")
		b.WriteString(strings.Join(e.Changed, ", "))
		b.WriteString(")")
	}
	return b.String(), true
}

// integrationNoticeTimeout bounds posting one change in all open rooms.
const integrationNoticeTimeout = 10 * time.Second

// IntegrationChanged implements spec.Notifier. It posts a system message in
// every open room. The room service serves the one workspace of a
// self-hosted install, so changes in any other workspace are ignored. A
// failed append is logged, never returned: the integration is already saved.
func (s *Service) IntegrationChanged(ctx context.Context, workspaceID string, e spec.AuditEntry, kindTitle string) {
	if workspaceID != spec.DefaultWorkspace {
		return
	}
	text, ok := IntegrationMessage(e, kindTitle)
	if !ok {
		return
	}
	// The message is about configuration, which is not secret, but it still
	// goes through the same cleaning as agent text before it is stored.
	payload, err := sanitizeAgentPayload(events.AgentMessagePayload{Text: text})
	if err != nil {
		s.log.Warn("room: integration notice not posted", "integration", e.IntegrationID, "err", err)
		return
	}
	// The change is already saved: a request the admin cancels must not skip
	// the rooms not yet told, so the notice gets its own bounded context.
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), integrationNoticeTimeout)
	defer cancel()
	// The lock is taken per room, not across the whole loop: a slow room
	// store must not block every steer, comment and approval for the length
	// of one admin save.
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	ids := make([]string, 0, len(s.rooms))
	for id, st := range s.rooms {
		if s.roomOpen(st) {
			ids = append(ids, id)
		}
	}
	s.mu.Unlock()
	for _, id := range ids {
		if ctx.Err() != nil {
			s.log.Warn("room: integration notice not posted", "room", id, "integration", e.IntegrationID, "err", ctx.Err())
			continue
		}
		s.notifyRoom(ctx, id, e.IntegrationID, payload)
	}
}

// notifyRoom posts payload in room id if it is still open.
func (s *Service) notifyRoom(ctx context.Context, id, integrationID string, payload any) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	st, ok := s.rooms[id]
	if !ok || !s.roomOpen(st) {
		return
	}
	if _, err := s.emitLocked(ctx, st, id, events.AgentMessage, systemActor, payload); err != nil {
		s.log.Warn("room: integration notice not posted", "room", id, "integration", integrationID, "err", err)
	}
}
