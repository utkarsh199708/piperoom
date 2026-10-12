package alertin

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// parsed is what a source parser returns: the alert plus what the pipeline
// resolver may use.
type parsed struct {
	alert    Alert
	explicit string   // pipeline the alert names itself
	service  string   // PagerDuty service ID
	values   []string // exact names (label values, tag values) matched against owners
	text     []string // free text (titles, summaries) scanned for owner names
}

// Parse turns a verified body into an Alert and resolves its pipeline. It
// does not authenticate; use Ingest for untrusted requests. It returns
// ErrIgnored for events that are not alerts.
func (c Config) Parse(src Source, body []byte) (Alert, error) {
	c = c.withDefaults()
	var (
		p   parsed
		err error
	)
	switch src {
	case Alertmanager, Grafana:
		p, err = parseAlertmanager(src, body)
	case PagerDuty:
		p, err = parsePagerDuty(body)
	case Datadog:
		p, err = parseDatadog(body)
	case Generic:
		p, err = parseGeneric(body)
	default:
		return Alert{}, fmt.Errorf("alertin: unknown source %q", src)
	}
	if err != nil {
		return Alert{}, err
	}
	a := p.alert
	a.Source = src
	a.Raw = json.RawMessage(append([]byte(nil), body...))
	a.PipelineID, a.PipelineVia = c.resolvePipeline(p)
	return a, nil
}

// resolvePipeline picks the pipeline in this order: the alert's own label,
// the PagerDuty service map, a unique owners.yaml match, the default.
func (c Config) resolvePipeline(p parsed) (string, string) {
	if ValidPipelineID(p.explicit) {
		return p.explicit, ViaLabel
	}
	if id := c.PagerDutyServices[p.service]; p.service != "" && ValidPipelineID(id) {
		return id, ViaService
	}
	if id := matchOwners(c.Owners, p.values, p.text); id != "" {
		return id, ViaOwners
	}
	if ValidPipelineID(c.DefaultPipeline) {
		return c.DefaultPipeline, ViaDefault
	}
	return "", ""
}

// amAlert is one alert inside an Alertmanager/Grafana group.
type amAlert struct {
	Status       string            `json:"status"`
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations"`
	GeneratorURL string            `json:"generatorURL"`
	Fingerprint  string            `json:"fingerprint"`
}

// amPayload is the Alertmanager webhook body (version 4). Grafana's webhook
// contact point sends the same shape plus title/message/state/orgId.
type amPayload struct {
	Version           string            `json:"version"`
	GroupKey          string            `json:"groupKey"`
	Status            string            `json:"status"`
	GroupLabels       map[string]string `json:"groupLabels"`
	CommonLabels      map[string]string `json:"commonLabels"`
	CommonAnnotations map[string]string `json:"commonAnnotations"`
	ExternalURL       string            `json:"externalURL"`
	Alerts            []amAlert         `json:"alerts"`

	// Grafana only.
	Title string `json:"title"`
	OrgID int64  `json:"orgId"`
}

func parseAlertmanager(src Source, body []byte) (parsed, error) {
	var p amPayload
	if err := json.Unmarshal(body, &p); err != nil {
		return parsed{}, fmt.Errorf("alertin: decode %s payload: %w", src, err)
	}
	if p.GroupKey == "" || len(p.Alerts) == 0 {
		return parsed{}, fmt.Errorf("alertin: %s payload has no groupKey or no alerts", src)
	}
	// Only firing alerts describe what is broken now; a group that is all
	// resolved still gets a title from its first alert.
	firing := make([]amAlert, 0, len(p.Alerts))
	for _, al := range p.Alerts {
		if al.Status == StatusFiring {
			firing = append(firing, al)
		}
	}
	if len(firing) == 0 {
		firing = p.Alerts[:1]
	}
	first := firing[0]
	label := func(name string) string {
		return firstNonEmpty(p.CommonLabels[name], p.GroupLabels[name], first.Labels[name])
	}

	status := StatusResolved
	if strings.EqualFold(p.Status, StatusFiring) {
		status = StatusFiring
	}
	title := firstNonEmpty(
		p.CommonAnnotations["summary"],
		first.Annotations["summary"],
		label("alertname"),
		p.Title,
	)
	key := p.GroupKey
	if src == Grafana {
		key = strconv.FormatInt(p.OrgID, 10) + "/" + p.GroupKey
	}

	out := parsed{
		alert: Alert{
			Status:   status,
			Title:    cleanTitle(title, string(src)+" alert"),
			Severity: firstSeverity(label("severity"), label("priority")),
			DedupKey: string(src) + ":" + key,
			URL:      firstNonEmpty(first.GeneratorURL, p.ExternalURL),
		},
		explicit: firstNonEmpty(label("pipeline"), label("pipeline_id")),
		text:     []string{title, p.CommonAnnotations["description"], first.Annotations["description"]},
	}
	for _, al := range firing {
		for _, v := range al.Labels {
			out.values = append(out.values, v)
		}
	}
	for _, v := range p.CommonLabels {
		out.values = append(out.values, v)
	}
	return out, nil
}

// pdPayload is a PagerDuty V3 webhook body for incident events.
type pdPayload struct {
	Event struct {
		ID         string    `json:"id"`
		EventType  string    `json:"event_type"`
		OccurredAt time.Time `json:"occurred_at"`
		Data       struct {
			ID       string `json:"id"`
			Title    string `json:"title"`
			Urgency  string `json:"urgency"`
			HTMLURL  string `json:"html_url"`
			Priority *struct {
				Summary string `json:"summary"`
			} `json:"priority"`
			Service struct {
				ID      string `json:"id"`
				Summary string `json:"summary"`
			} `json:"service"`
		} `json:"data"`
	} `json:"event"`
}

func parsePagerDuty(body []byte) (parsed, error) {
	// Decode the event type first so non-incident events (pagey.ping has no
	// occurred_at in every version) are ignored before stricter decoding.
	var head struct {
		Event struct {
			EventType string `json:"event_type"`
		} `json:"event"`
	}
	if err := json.Unmarshal(body, &head); err != nil {
		return parsed{}, fmt.Errorf("alertin: decode pagerduty payload: %w", err)
	}
	var status string
	switch head.Event.EventType {
	case "incident.triggered", "incident.reopened":
		status = StatusFiring
	case "incident.resolved":
		status = StatusResolved
	default:
		// pagey.ping, incident.acknowledged, incident.priority_updated, ...
		return parsed{}, fmt.Errorf("%w: pagerduty event %q", ErrIgnored, head.Event.EventType)
	}
	var p pdPayload
	if err := json.Unmarshal(body, &p); err != nil {
		return parsed{}, fmt.Errorf("alertin: decode pagerduty payload: %w", err)
	}
	d := p.Event.Data
	if d.ID == "" {
		return parsed{}, fmt.Errorf("alertin: pagerduty %s event has no incident id", head.Event.EventType)
	}
	if p.Event.OccurredAt.IsZero() {
		return parsed{}, fmt.Errorf("alertin: pagerduty %s event has no occurred_at", head.Event.EventType)
	}
	prio := ""
	if d.Priority != nil {
		prio = d.Priority.Summary
	}
	urgency := map[string]string{"high": "sev2", "low": "sev4"}[strings.ToLower(d.Urgency)]
	return parsed{
		alert: Alert{
			Status:     status,
			Title:      cleanTitle(d.Title, "pagerduty incident"),
			Severity:   firstSeverity(prio, urgency),
			DedupKey:   "pagerduty:" + d.ID,
			URL:        d.HTMLURL,
			OccurredAt: p.Event.OccurredAt,
		},
		service: d.Service.ID,
		values:  []string{d.Service.Summary},
		text:    []string{d.Title},
	}, nil
}

// ddPayload is the body produced by DatadogPayloadTemplate. Datadog bodies
// are user-defined, so only these keys are read.
type ddPayload struct {
	ID         string `json:"id"`
	Title      string `json:"title"`
	Transition string `json:"transition"`
	AlertType  string `json:"alert_type"`
	Priority   string `json:"priority"`
	CycleKey   string `json:"cycle_key"`
	AggregKey  string `json:"aggreg_key"`
	AlertID    string `json:"alert_id"`
	Tags       string `json:"tags"`
	Scope      string `json:"scope"`
	PipelineID string `json:"pipeline_id"`
	Date       string `json:"date"`
	Link       string `json:"link"`
}

// DatadogPayloadTemplate is the payload to paste into the Datadog webhook.
// The keys are what the parser reads; the $VARIABLES are Datadog's
// (https://docs.datadoghq.com/integrations/webhooks/#variables).
const DatadogPayloadTemplate = `{
  "id": "$ID",
  "title": "$EVENT_TITLE",
  "transition": "$ALERT_TRANSITION",
  "alert_type": "$ALERT_TYPE",
  "priority": "$ALERT_PRIORITY",
  "cycle_key": "$ALERT_CYCLE_KEY",
  "aggreg_key": "$AGGREG_KEY",
  "alert_id": "$ALERT_ID",
  "tags": "$TAGS",
  "scope": "$ALERT_SCOPE",
  "pipeline_id": "$TAGS[pipeline]",
  "date": "$DATE_POSIX",
  "link": "$LINK"
}`

func parseDatadog(body []byte) (parsed, error) {
	var p ddPayload
	if err := json.Unmarshal(body, &p); err != nil {
		return parsed{}, fmt.Errorf("alertin: decode datadog payload: %w", err)
	}
	if p.Transition == "" && p.AlertType == "" {
		return parsed{}, fmt.Errorf("%w: datadog payload has no transition", ErrIgnored)
	}
	status := StatusFiring
	if strings.EqualFold(p.Transition, "Recovered") || strings.EqualFold(p.AlertType, "success") {
		status = StatusResolved
	}
	key := firstNonEmpty(p.CycleKey, p.AggregKey, p.AlertID, p.ID)
	if key == "" {
		return parsed{}, fmt.Errorf("alertin: datadog payload has no cycle_key, aggreg_key, alert_id or id")
	}
	var occurred time.Time
	if p.Date != "" {
		sec, err := strconv.ParseInt(p.Date, 10, 64)
		if err != nil {
			return parsed{}, fmt.Errorf("alertin: datadog date %q is not epoch seconds", p.Date)
		}
		occurred = time.Unix(sec, 0)
	}
	alertType := map[string]string{"error": "sev2", "warning": "sev3", "info": "sev4"}[strings.ToLower(p.AlertType)]
	out := parsed{
		alert: Alert{
			Status:     status,
			Title:      cleanTitle(p.Title, "datadog alert"),
			Severity:   firstSeverity(p.Priority, alertType),
			DedupKey:   "datadog:" + key,
			URL:        p.Link,
			OccurredAt: occurred,
		},
		explicit: firstNonEmpty(p.PipelineID, tagValue(p.Tags, "pipeline")),
		text:     []string{p.Title},
	}
	for _, list := range []string{p.Tags, p.Scope} {
		for _, t := range strings.Split(list, ",") {
			if _, v, ok := strings.Cut(strings.TrimSpace(t), ":"); ok {
				out.values = append(out.values, v)
			}
		}
	}
	return out, nil
}

// tagValue finds key:value in a comma-separated Datadog tag list.
func tagValue(tags, key string) string {
	for _, t := range strings.Split(tags, ",") {
		k, v, ok := strings.Cut(strings.TrimSpace(t), ":")
		if ok && k == key {
			return v
		}
	}
	return ""
}

// parseGeneric reads the generic form: any JSON. When the body is an object,
// title, severity, pipeline_id, status, dedup_key and url are used if present
// (docs/api.md, POST /api/alerts/webhook).
func parseGeneric(body []byte) (parsed, error) {
	if !json.Valid(body) {
		return parsed{}, fmt.Errorf("alertin: generic payload is not JSON")
	}
	sum := sha256.Sum256(body)
	out := parsed{alert: Alert{
		Status:   StatusFiring,
		Title:    "generic alert",
		Severity: DefaultSeverity,
		DedupKey: "generic:sha256:" + hex.EncodeToString(sum[:16]),
	}}
	var obj map[string]any
	if err := json.Unmarshal(body, &obj); err != nil {
		// Valid JSON but not an object is allowed: keep the defaults.
		return out, nil
	}
	str := func(k string) string {
		s, _ := obj[k].(string)
		return s
	}
	title := str("title")
	out.alert.Title = cleanTitle(title, "generic alert")
	out.alert.Severity = firstSeverity(str("severity"))
	out.alert.URL = str("url")
	if strings.EqualFold(str("status"), StatusResolved) {
		out.alert.Status = StatusResolved
	}
	if k := firstNonEmpty(str("dedup_key")); k != "" {
		out.alert.DedupKey = "generic:" + k
	}
	out.explicit = str("pipeline_id")
	out.text = []string{title}
	for k, v := range obj {
		if s, ok := v.(string); ok && k != "title" {
			out.values = append(out.values, s)
		}
	}
	return out, nil
}
