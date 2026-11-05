package slackbridge

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"piperoom/internal/events"
)

// Team pages (X4d, docs/api.md POST /api/rooms/{id}/page-team): the driver of
// a room whose cause is upstream asks the owning application team to look.
// Unlike the mirror, the post goes to the team's own channel (owners.yaml
// teams[].slack_channel), not to the room's thread, and it is sent while the
// driver's request waits, so a Slack failure reaches the driver and nothing is
// recorded. Outbound only: the team's reply is never read.
//
// Injection rule: the page is built from typed fields only. The hypothesis
// text, the change summary, the service and the driver's name are untrusted
// and go through clip (no mentions, no links). The only links are the room's
// (our PublicURL) and the change's URL after pageURL accepted it, each with a
// fixed label.

// teamChannelID is a Slack channel id (public C…, private G…), the same shape
// owners.yaml validates; checked again so a bad value never reaches Slack.
var teamChannelID = regexp.MustCompile(`^[CG][A-Z0-9]{6,23}$`)

// maxPageSteps caps the cited step numbers listed in one page.
const maxPageSteps = 20

// TeamPage is one page of an application team. Every string but Channel and
// Team is untrusted text.
type TeamPage struct {
	RoomID     string
	PipelineID string
	Channel    string // owners.yaml teams[].slack_channel
	Team       string // owners.yaml teams[].id
	By         events.Actor
	Scope      events.HypothesisScope
	Hypothesis string // the agent's hypothesis text
	Confidence string
	// Steps are the 1-based numbers of the cited steps, as the room shows
	// them; a cited step the log does not hold is left out.
	Steps []int
	// Change is the cited change; nil when the hypothesis cites none, or the
	// change log no longer holds it (ChangeID then still names it).
	Change   *TeamPageChange
	ChangeID int64
}

// TeamPageChange is the part of a clients.PipelineChange a page shows.
type TeamPageChange struct {
	Kind    string
	Service string
	Summary string
	URL     string // shown as a link only when pageURL accepts it
	At      time.Time
}

// ErrBadTeamChannel means the team's channel is not a Slack channel id.
var ErrBadTeamChannel = errors.New("slackbridge: team page: channel is not a Slack channel id")

// PageTeam posts p to the team's channel, once: the client retries only what
// Slack did not accept. The error names the Slack code, never the text.
func (b *Bridge) PageTeam(ctx context.Context, p TeamPage) error {
	if !teamChannelID.MatchString(p.Channel) {
		return ErrBadTeamChannel
	}
	m := newRenderer(b.publicURL).teamPage(p)
	m.Channel = p.Channel
	if _, err := b.client.post(ctx, m); err != nil {
		// Ids and the code only: the page text is never logged.
		b.log.Warn("slack team page failed", "room_id", p.RoomID, "team", p.Team, "code", errCode(err))
		return fmt.Errorf("slackbridge: page team %s: %w", p.Team, err)
	}
	return nil
}

// ErrorCode names a PageTeam failure for a person without quoting any text:
// Slack's error code when it gave one (e.g. not_in_channel), else a class.
func ErrorCode(err error) string {
	if errors.Is(err, ErrBadTeamChannel) {
		return "bad_channel"
	}
	return errCode(err)
}

func scopeWord(s events.HypothesisScope) string {
	switch s.Normalize() {
	case events.ScopeVendor:
		return "a vendor incident"
	case events.ScopeUpstreamApp:
		return "an application change outside the pipeline"
	}
	return "not established"
}

// teamPage renders p. Trusted markup is only our fixed words, numbers we
// formatted and the two links; the links sit in a context element without
// verbatim so Slack parses them, everything else is verbatim and escaped.
func (r *renderer) teamPage(p TeamPage) message {
	head := fmt.Sprintf(":pager: *Piperoom: team %s is asked to look*\n%s, driving room %s of pipeline %s, thinks the cause is %s.",
		inlineCode(p.Team, 80), clip(actorName(p.By), 100), inlineCode(p.RoomID, 100), inlineCode(p.PipelineID, 200), scopeWord(p.Scope))
	blocks := []block{
		section(head),
		section("*Hypothesis* (confidence: " + clip(p.Confidence, 20) + ")\n" + clip(p.Hypothesis, 1500)),
	}
	var links []textObj
	switch {
	case p.Change != nil:
		c := p.Change
		t := "*Cited change:* " + inlineCode(c.Kind, 40)
		if c.Service != "" {
			t += " of " + inlineCode(c.Service, 140)
		}
		if !c.At.IsZero() {
			t += " at " + c.At.UTC().Format(time.RFC3339)
		}
		t += "\n" + clip(c.Summary, 600)
		blocks = append(blocks, section(t))
		if u, ok := pageURL(c.URL); ok {
			links = append(links, textObj{Type: "mrkdwn", Text: "<" + u + "|Open the change>"})
		}
	case p.ChangeID > 0:
		blocks = append(blocks, section("*Cited change:* #"+strconv.FormatInt(p.ChangeID, 10)+" (no longer in the change log)"))
	}
	if len(p.Steps) > 0 {
		nums := make([]string, 0, min(len(p.Steps), maxPageSteps)+1)
		for i, n := range p.Steps {
			if i == maxPageSteps {
				nums = append(nums, fmt.Sprintf("+%d more", len(p.Steps)-maxPageSteps))
				break
			}
			nums = append(nums, strconv.Itoa(n))
		}
		word := "steps "
		if len(p.Steps) == 1 {
			word = "step "
		}
		blocks = append(blocks, section("*Evidence:* "+word+strings.Join(nums, ", ")+" of the room"))
	}
	foot := append([]textObj{md("Sent by Piperoom; replies here are not read by the room.")}, r.linkElement(p.RoomID)...)
	foot = append(foot, links...)
	blocks = append(blocks, block{Type: "context", Elements: foot})
	return message{
		Text:   fallback("Piperoom page for team "+p.Team+": ", p.Hypothesis),
		Blocks: blocks,
	}
}

// pageURL returns u ready for <u|label> markup when it is an absolute http(s)
// URL without user info, at most 1024 bytes, holding none of the characters
// that end Slack's link markup; & becomes &amp; as Slack's escaping asks.
// Anything else is not linked.
func pageURL(u string) (string, bool) {
	if u == "" || len(u) > 1024 || strings.ContainsAny(u, "<>| \t\r\n\"`") {
		return "", false
	}
	for _, r := range u {
		if r < 0x21 || r >= 0x7f {
			return "", false
		}
	}
	pu, err := url.Parse(u)
	if err != nil || (pu.Scheme != "http" && pu.Scheme != "https") || pu.Host == "" || pu.User != nil {
		return "", false
	}
	return strings.ReplaceAll(u, "&", "&amp;"), true
}
