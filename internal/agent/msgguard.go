package agent

import (
	"context"
	"html"
	"regexp"
	"strings"
	"sync"
	"unicode"

	"piperoom/internal/events"
)

// A product QA run ended a room with an agent message that was the literal text
// "<br>", and announced the same recovery three times in 17 seconds. The guard
// below sits in front of the room sink of one run and drops both kinds of
// message, so a person never reads an empty line or a repeat as news.

// Why a message was not posted. The post_message tool shows these to the model.
const (
	dropReasonEmpty  = "it has no text: it is empty, or only whitespace and markup"
	dropReasonRepeat = "it repeats your previous message"
)

// messageDroppedError is returned by guardedSink.Emit for a message the guard
// dropped. It is not a failure: the runner logs it at debug level and the
// post_message tool turns it into a plain "message not posted" result.
type messageDroppedError struct{ reason string }

func (e *messageDroppedError) Error() string { return "agent: message not posted: " + e.reason }

// htmlTag matches an HTML-like tag or comment: <br>, <br/>, </p>, <div class="x">.
// A bare "<" or ">" in prose ("lag > 1000") is not a tag, since a tag needs a
// letter right after "<" and a closing ">" without another "<" in between.
var htmlTag = regexp.MustCompile(`(?s)<!--.*?-->|</?[A-Za-z][A-Za-z0-9:-]*(?:\s[^<>]*)?/?>`)

// hasContent reports whether text says anything once HTML-like tags and
// entities are taken away. "<br>", "<p></p>", "&nbsp;" and "&lt;br&gt;" (the
// same tag, escaped) have none; neither has whitespace, a control character or
// an invisible one. Tags are stripped before and after the entities are
// decoded, so an escaped tag cannot hide behind its entities.
func hasContent(text string) bool {
	s := htmlTag.ReplaceAllString(text, " ")
	s = html.UnescapeString(s)
	s = htmlTag.ReplaceAllString(s, " ")
	for _, r := range s {
		if unicode.IsSpace(r) || unicode.IsControl(r) || invisibleRune(r) {
			continue
		}
		return true
	}
	return false
}

// messageKey identifies a message for the repeat check: its text with every run
// of whitespace folded to one space, and the comment it answers. Two answers
// with the same words to different comments are not a repeat, since the second
// comment would stay unanswered.
func messageKey(p events.AgentMessagePayload) string {
	return p.ReplyTo + "\x00" + strings.Join(strings.Fields(p.Text), " ")
}

// messageGuard remembers the last message one run posted.
type messageGuard struct {
	mu   sync.Mutex
	last string
	has  bool
}

// verdict returns why p must not be posted, or "" when it may. The caller holds
// g.mu.
func (g *messageGuard) verdict(p events.AgentMessagePayload) string {
	if !hasContent(p.Text) {
		return dropReasonEmpty
	}
	if g.has && g.last == messageKey(p) {
		return dropReasonRepeat
	}
	return ""
}

// guardedSink wraps a Sink and keeps empty and repeated agent messages out of
// it. Every other event type goes straight through.
type guardedSink struct {
	inner Sink
	g     messageGuard
}

// newGuardedSink wraps inner. One guardedSink serves one run.
func newGuardedSink(inner Sink) *guardedSink { return &guardedSink{inner: inner} }

// Emit posts the event unless it is a message the guard drops, in which case it
// returns a *messageDroppedError and posts nothing. A message counts as "the
// previous one" only after the inner sink accepted it, so a failed write does
// not make its retry look like a repeat. The lock is held across the inner
// write on purpose: messages reach the log in the order the guard judged them.
func (s *guardedSink) Emit(ctx context.Context, typ events.Type, payload any) error {
	if typ != events.AgentMessage {
		return s.inner.Emit(ctx, typ, payload)
	}
	p, ok := agentMessagePayload(payload)
	if !ok {
		return s.inner.Emit(ctx, typ, payload)
	}
	s.g.mu.Lock()
	defer s.g.mu.Unlock()
	if reason := s.g.verdict(p); reason != "" {
		return &messageDroppedError{reason: reason}
	}
	if err := s.inner.Emit(ctx, typ, payload); err != nil {
		return err
	}
	s.g.last, s.g.has = messageKey(p), true
	return nil
}

// agentMessagePayload reads the payload of an agent.message the runner or a
// tool emits, which is always the struct or a pointer to it.
func agentMessagePayload(payload any) (events.AgentMessagePayload, bool) {
	switch p := payload.(type) {
	case events.AgentMessagePayload:
		return p, true
	case *events.AgentMessagePayload:
		if p != nil {
			return *p, true
		}
	}
	return events.AgentMessagePayload{}, false
}
