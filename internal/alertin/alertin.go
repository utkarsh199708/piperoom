// Package alertin reads alert webhooks from real alerting systems
// (Prometheus Alertmanager, Grafana, PagerDuty, Datadog, and a generic JSON
// form), checks that they really come from the configured sender, and turns
// each one into an Alert that can open a room (agent.Alert, room.created).
//
// Each sender is checked with the strongest method it supports by itself:
//
//	Alertmanager  Authorization: Bearer <token>   (webhook_config.http_config.authorization)
//	Grafana       hex HMAC-SHA256 over "<unix ts>:<body>" in X-Grafana-Alerting-Signature,
//	              timestamp in X-Grafana-Alerting-Timestamp (must be within MaxSkew)
//	PagerDuty     X-PagerDuty-Signature: v1=<hex HMAC-SHA256(body)>[,v1=...]  (V3 webhooks),
//	              event.occurred_at must be within MaxEventAge
//	Datadog       shared token in a custom header (default X-Piperoom-Token),
//	              "date" ($DATE_POSIX) must be within MaxEventAge when present
//	Generic       X-Piperoom-Signature: t=<unix ts>,v1=<hex HMAC-SHA256("<ts>.<body>")>
//
// A source with no secret configured is disabled and every request for it
// fails. There is no HTTP server here: the room service (A5) reads the body,
// calls Config.Ingest and opens or reuses a room. Every Alert has a DedupKey
// that stays the same when the sender sends the same incident again, so the
// room service can return the room that is already open. That also makes a
// replayed, validly signed request harmless.
package alertin

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"piperoom/internal/agent"
	"piperoom/internal/events"
)

// Source names an alerting system.
type Source string

// Supported sources.
const (
	Alertmanager Source = "alertmanager"
	Grafana      Source = "grafana"
	PagerDuty    Source = "pagerduty"
	Datadog      Source = "datadog"
	Generic      Source = "generic"
	// Watcher is the built-in watcher (internal/watch). It builds its Alert
	// itself, so it has no HTTP route, no secret, and is not in Sources.
	Watcher Source = "watcher"
)

// Sources lists every source that arrives over HTTP. Watcher is left out on
// purpose: nothing may post as the watcher from outside the process.
var Sources = []Source{Alertmanager, Grafana, PagerDuty, Datadog, Generic}

// Alert statuses.
const (
	StatusFiring   = "firing"
	StatusResolved = "resolved"
)

// How Alert.PipelineID was found.
const (
	ViaLabel   = "label"   // the alert named it (pipeline / pipeline_id label, tag or field)
	ViaDAG     = "dag"     // exactly one owners.yaml pipeline lists the DAG of the alert's dag_id label or field
	ViaService = "service" // PagerDuty service ID in Config.PagerDutyServices
	ViaOwners  = "owners"  // exactly one pipeline in owners.yaml owns a connector/table/group the alert mentions
	ViaDefault = "default" // Config.DefaultPipeline
)

// Limits.
const (
	// MaxBodyBytes is the largest body Ingest and Parse accept. The HTTP layer
	// should cap its read at the same size (http.MaxBytesReader).
	MaxBodyBytes = 1 << 20
	// MaxTitleRunes caps the normalized title.
	MaxTitleRunes = 200
	// MaxMatchTextRunes caps each title, summary and description scanned for
	// pipeline names. Names sit near the start of alert text, and the scan costs
	// time in proportion to its length. A name past the cut is not matched.
	MaxMatchTextRunes = 2000
	// MaxLabelValues caps how many label, tag and field values one alert
	// contributes to pipeline matching. A 1 MB body can hold thousands of labels.
	// Explicit pipeline labels are read directly and are not affected by this cap.
	MaxLabelValues = 256
	// MaxLabelRunes caps each label value. Pipeline names are far shorter, so a
	// cut value does not turn into a false match.
	MaxLabelRunes = 256
	// DefaultSeverity is used when the source gives no usable severity.
	DefaultSeverity = "sev3"
)

// Errors returned by Ingest, Verify and Parse. Map them to HTTP like this:
// ErrUnauthenticated and ErrStale → 401, ErrTooLarge → 413, ErrIgnored → 202,
// any other error → 400.
var (
	ErrUnauthenticated = errors.New("alertin: unauthenticated")
	ErrStale           = errors.New("alertin: alert event too old")
	ErrTooLarge        = errors.New("alertin: body too large")
	// ErrIgnored marks a verified request that is not an alert (a PagerDuty
	// ping, an acknowledgement, ...). Resolved alerts are not errors; check
	// Alert.Status.
	ErrIgnored = errors.New("alertin: not an alert event")
)

// Alert is a verified, normalized alert.
type Alert struct {
	Source Source `json:"source"`
	Status string `json:"status"` // StatusFiring | StatusResolved
	// Title is one line with control characters removed, ≤ MaxTitleRunes.
	// It is still text written by whoever configured the alert; treat it as
	// untrusted data.
	Title    string `json:"title"`
	Severity string `json:"severity"` // sev1..sev4
	// PipelineID may be empty when nothing names or matches a pipeline.
	PipelineID  string `json:"pipeline_id"`
	PipelineVia string `json:"pipeline_via,omitempty"` // ViaLabel | ViaDAG | ViaService | ViaOwners | ViaDefault
	// DedupKey is stable across re-sends of the same incident: "<source>:<key>".
	DedupKey string `json:"dedup_key"`
	URL      string `json:"url,omitempty"` // link back to the alert at its source
	// OccurredAt is when the sender says the event happened; zero if unknown.
	OccurredAt time.Time `json:"occurred_at,omitzero"`
	// Raw is the request body exactly as received (docs/api.md: stored verbatim
	// in room.created.alert). Never put it in a prompt unwrapped.
	Raw json.RawMessage `json:"raw,omitempty"`
}

// Agent builds the agent's starting alert. The room service supplies the
// room ID and the rendered pipeline map.
func (a Alert) Agent(roomID, pipelineMap string) agent.Alert {
	return agent.Alert{
		RoomID:      roomID,
		Title:       a.Title,
		PipelineID:  a.PipelineID,
		Raw:         a.Raw,
		PipelineMap: pipelineMap,
	}
}

// RoomCreated builds the room.created payload.
func (a Alert) RoomCreated() events.RoomCreatedPayload {
	return events.RoomCreatedPayload{
		Title:      a.Title,
		Severity:   a.Severity,
		PipelineID: a.PipelineID,
		Alert:      a.Raw,
	}
}

var pipelineIDRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$`)

// ValidPipelineID reports whether id looks like an owners.yaml pipeline id
// (the room service and cmd/server use it too, so the shape lives in one place).
// Anything else in an alert label is ignored rather than trusted.
func ValidPipelineID(id string) bool {
	return pipelineIDRe.MatchString(id)
}

// severityAliases maps source vocabularies onto sev1..sev4.
var severityAliases = map[string]string{
	"sev1": "sev1", "p1": "sev1", "critical": "sev1", "page": "sev1", "emergency": "sev1", "fatal": "sev1",
	"sev2": "sev2", "p2": "sev2", "high": "sev2", "error": "sev2", "major": "sev2",
	"sev3": "sev3", "p3": "sev3", "warning": "sev3", "warn": "sev3", "medium": "sev3", "minor": "sev3",
	"sev4": "sev4", "p4": "sev4", "p5": "sev4", "low": "sev4", "info": "sev4", "informational": "sev4", "none": "sev4",
}

// normalizeSeverity maps a source severity onto sev1..sev4.
func normalizeSeverity(s string) (string, bool) {
	v, ok := severityAliases[strings.ToLower(strings.TrimSpace(s))]
	return v, ok
}

// firstSeverity returns the first candidate that maps to a known severity,
// or DefaultSeverity.
func firstSeverity(candidates ...string) string {
	for _, c := range candidates {
		if v, ok := normalizeSeverity(c); ok {
			return v
		}
	}
	return DefaultSeverity
}

// cleanTitle makes alert text safe to show as a one-line title: control
// characters (newlines included) become spaces, runs of spaces collapse, and
// the result is capped. Keeping it on one line stops an alert from faking
// extra lines (e.g. "Pipeline: ...") in the agent's opening message.
func cleanTitle(s string, fallback string) string {
	if !utf8.ValidString(s) {
		s = strings.ToValidUTF8(s, "")
	}
	var b strings.Builder
	space := false
	n := 0
	for _, r := range s {
		if unicode.IsControl(r) || unicode.IsSpace(r) || r == ' ' || r == ' ' {
			space = true
			continue
		}
		if n >= MaxTitleRunes {
			break
		}
		if space && b.Len() > 0 {
			b.WriteByte(' ')
			n++
			if n >= MaxTitleRunes {
				break
			}
		}
		space = false
		b.WriteRune(r)
		n++
	}
	if out := strings.TrimSpace(b.String()); out != "" {
		return out
	}
	return fallback
}

// checkSize rejects a body over MaxBodyBytes. It runs before parsing and before
// authentication, so an oversized request costs one length check.
func checkSize(body []byte) error {
	if len(body) > MaxBodyBytes {
		return fmt.Errorf("%w: %d bytes, limit %d", ErrTooLarge, len(body), MaxBodyBytes)
	}
	return nil
}

// truncateRunes returns s cut to at most n runes. The cut is on a rune
// boundary, so the result is valid UTF-8 when s is. encoding/json output always is.
func truncateRunes(s string, n int) string {
	i := 0
	for pos := range s {
		if i == n {
			return s[:pos]
		}
		i++
	}
	return s
}

// firstNonEmpty returns the first non-blank string, trimmed.
func firstNonEmpty(ss ...string) string {
	for _, s := range ss {
		if t := strings.TrimSpace(s); t != "" {
			return t
		}
	}
	return ""
}
