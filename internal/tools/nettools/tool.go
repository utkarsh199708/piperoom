// Package nettools registers net_probe, the read-only probe of a room's
// configured endpoints: DNS, TCP and TLS (chain, SANs, expiry) per target, the
// brokers' advertised listeners, and Kafka Connect's own view of the source
// database. The model names a target label, never an address: every endpoint
// comes from config, the pipeline's connections: or its connectors.
package nettools

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/config"
	"piperoom/internal/owners"
	"piperoom/internal/redact"
	"piperoom/internal/tools"
)

// timeout is the tool's whole budget. Each step is bounded by stepTimeout
// (3 s); the steps share this budget, so a slow resolver cannot keep the tool
// past 10 s (AGENTS.md rule 4).
const timeout = 10 * time.Second

// maxChecks caps the checks one result carries (a long broker list with a
// listener check each), so the result cannot grow with the fleet.
const maxChecks = 48

// maxSummaryChecks caps the check lines of the Summary; Data keeps the rest.
const maxSummaryChecks = 20

// Deps holds what net_probe reads through. The tool takes no address and no
// credential from the model: the target source and the clients are the only
// way to an endpoint.
type Deps struct {
	// Connect reads the pipeline's connector configs, and validates one when
	// it is a ConfigValidator. Nil leaves the source target's Connect check out.
	Connect clients.ConnectClient
	// Kafka reads the brokers' metadata, whose host:port is the advertised
	// listener. Nil leaves the listener check out.
	Kafka clients.KafkaAdmin
	// Owners resolves the room's pipeline (tools.ScopeFor). Required in a room.
	Owners *owners.Owners
	// Config is the connection settings of the client set this tool is built
	// over: the process-wide settings, or one pipeline's resolved
	// connections: block (wiring builds one tool registry per set, so one set
	// never names another tenant's host).
	Config config.Config
	// Targets replaces where the targets come from. The setup runner passes
	// TargetsFromSetup; nil builds RoomTargets(Config, Connect).
	Targets TargetSource
	// Location is the network position the probe runs from. Nil =
	// ProcessLocation{} (the room service's own position).
	Location Location
	// StepTimeout bounds one DNS, TCP, TLS or Connect step; 0 = 3 s.
	StepTimeout time.Duration
}

// Register adds net_probe to reg.
func Register(reg *tools.Registry, d Deps) error {
	return reg.Register(&netProbe{d: d})
}

// netProbe is the read-only network probe: it opens TCP connections and reads
// certificates and metadata, and changes nothing.
type netProbe struct{ d Deps }

func (t *netProbe) Name() string     { return "net_probe" }
func (t *netProbe) Kind() tools.Kind { return tools.KindRead }

func (t *netProbe) InputSchema() map[string]any {
	return map[string]any{
		"type":                 "object",
		"additionalProperties": false,
		"required":             []string{"target"},
		"properties": map[string]any{
			"target": map[string]any{
				"type":        "string",
				"description": "which configured target to probe: source (the source database), kafka (the brokers), connect, schema-registry or warehouse. There is no parameter for an address: the target's endpoints come from Piperoom's own configuration.",
				"enum":        []string{LabelSource, LabelKafka, LabelConnect, LabelRegistry, LabelWarehouse},
			},
		},
	}
}

func (t *netProbe) Description() string {
	return "Probes one configured target from Piperoom's network position: DNS, TCP and TLS (chain, subject, SANs and expiry). " +
		"With target kafka it also reads the brokers' advertised listeners from cluster metadata and probes each one, so a listener that only works inside the cluster shows up; " +
		"with target source it asks Kafka Connect to validate the pipeline's running source connector config, so the database as Connect sees it is compared with what Piperoom reaches. " +
		"It takes a label, never an address, presents no client certificate, sends no credential and changes nothing. " +
		"Use it for connection refused, timeout, DNS, handshake or certificate errors, or when Connect and Piperoom disagree about whether a host is reachable."
}

// input is the whole tool input. A strict decode refuses any other key, so an
// address in the input is bad input, not an ignored field.
type input struct {
	Target string `json:"target"`
}

var labelRE = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,63}$`)

func decodeInput(raw json.RawMessage) (input, error) {
	var in input
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&in); err != nil {
		return in, fmt.Errorf("%w: %v", tools.ErrBadInput, err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return in, fmt.Errorf("%w: unexpected trailing input", tools.ErrBadInput)
	}
	in.Target = strings.ToLower(strings.TrimSpace(in.Target))
	if in.Target == "" {
		return in, fmt.Errorf("%w: target is required", tools.ErrBadInput)
	}
	if !labelRE.MatchString(in.Target) {
		return in, fmt.Errorf("%w: %q is not a target label", tools.ErrBadInput, clip(in.Target))
	}
	return in, nil
}

// probeData is Result.Data: the target, its endpoints and every check.
type probeData struct {
	Target    string   `json:"target"`
	Kind      string   `json:"kind"`
	Endpoints []string `json:"endpoints"`
	Checks    []Check  `json:"checks"`
	Note      string   `json:"note,omitempty"`
}

func (t *netProbe) Run(ctx context.Context, raw json.RawMessage) (tools.Result, error) {
	in, err := decodeInput(raw)
	if err != nil {
		return tools.Result{}, err
	}
	sc, err := tools.ScopeFor(ctx, t.d.Owners)
	if err != nil {
		return tools.Result{}, err
	}
	cctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	src := t.d.Targets
	if src == nil {
		src = RoomTargets(t.d.Config, t.d.Connect)
	}
	pl := sc.Pipeline()
	labels := src.Labels(pl)
	if !hasLabel(labels, in.Target) {
		return tools.Result{}, unknownLabel(labels, in.Target)
	}
	tgt, err := src.Target(cctx, pl, in.Target)
	if err != nil {
		return tools.Result{}, fmt.Errorf("net_probe: resolving target %q: %w", in.Target, err)
	}
	if len(tgt.Endpoints) == 0 {
		return tools.Result{}, fmt.Errorf("net_probe: target %q has no address configured%s", tgt.Label, noteSuffix(tgt.Note))
	}

	checks := Prober{Loc: t.d.Location, StepTimeout: t.d.StepTimeout}.Probe(cctx, tgt)
	switch tgt.Kind {
	case KindKafka:
		checks = append(checks, t.listenerChecks(cctx, checks, tgt)...)
	case KindSource:
		checks = append(checks, t.connectView(cctx, tgt))
	}

	data := probeData{Target: tgt.Label, Kind: tgt.Kind, Endpoints: addrs(tgt), Checks: checks, Note: tgt.Note}
	return t.result(data), nil
}

// hasLabel reports whether the source offers label (case-insensitively).
func hasLabel(labels []string, label string) bool {
	for _, l := range labels {
		if strings.EqualFold(l, label) {
			return true
		}
	}
	return false
}

// unknownLabel refuses a label the source does not have. The label is the only
// thing the model names; an unknown one is out of scope, never looked up.
func unknownLabel(labels []string, label string) error {
	if len(labels) == 0 {
		return fmt.Errorf("%w: this pipeline has no configured target", tools.ErrOutOfScope)
	}
	return fmt.Errorf("%w: %q is not one of this pipeline's targets (allowed: %s)",
		tools.ErrOutOfScope, clip(label), strings.Join(labels, ", "))
}

func addrs(t Target) []string {
	out := make([]string, 0, len(t.Endpoints))
	for _, e := range t.Endpoints {
		out = append(out, e.Addr)
	}
	return out
}

// noteSuffix appends a target's note to an error when it has one.
func noteSuffix(note string) string {
	if strings.TrimSpace(note) == "" {
		return ""
	}
	return " (" + note + ")"
}

// redactedText masks credentials in a server's error text and clips it: the
// text is untrusted and may quote a value that looks like a secret.
func redactedText(s string) string {
	if masked, _ := redact.Text(s); masked != "" {
		s = masked
	}
	return clip(s)
}

// result renders the Summary and applies the caps. A failed check is a finding,
// not a tool error: the room records the result and the model reads which step
// failed and how to fix it.
func (t *netProbe) result(d probeData) tools.Result {
	total := len(d.Checks)
	cut := false
	if total > maxChecks {
		d.Checks = d.Checks[:maxChecks]
		cut = true
	}
	summary := summarize(d, total)
	s, truncated := tools.Truncate(summary)
	return tools.Result{Summary: s, Data: d, Truncated: truncated || cut}
}

// summarize is the model-readable view: one header with the target and its
// endpoints, then one line per check, failures and warnings first in the order
// they were run. total is the check count before Data's cap, so the "more
// checks" line never hides a check Data does not carry either.
func summarize(d probeData, total int) string {
	var sb strings.Builder
	failed, warned := 0, 0
	for _, c := range d.Checks {
		switch c.Status {
		case StatusFail:
			failed++
		case StatusWarn:
			warned++
		}
	}
	fmt.Fprintf(&sb, "net_probe %s (%s): ", d.Target, strings.Join(d.Endpoints, ", "))
	switch {
	case failed > 0:
		fmt.Fprintf(&sb, "%d of %d checks failed", failed, total)
	case warned > 0:
		fmt.Fprintf(&sb, "ok with %d warning(s)", warned)
	default:
		sb.WriteString("all checks ok")
	}
	if failed+warned == 0 {
		sb.WriteString(": " + checkSummary(d.Checks))
	}
	for i, c := range d.Checks {
		if i >= maxSummaryChecks {
			fmt.Fprintf(&sb, "\n… %d more checks not shown (the data is capped at %d)", total-maxSummaryChecks, total)
			break
		}
		sb.WriteString("\n- " + checkLine(c))
	}
	if d.Note != "" {
		sb.WriteString("\nnote: " + d.Note)
	}
	return sb.String()
}

// checkSummary lists the steps that passed, e.g. "dns ok, tcp ok, tls ok".
func checkSummary(checks []Check) string {
	parts := make([]string, 0, len(checks))
	seen := map[string]int{}
	for _, c := range checks {
		seen[c.ID]++
	}
	for _, c := range checks {
		if c.Status == StatusSkip {
			continue
		}
		if seen[c.ID] > 1 {
			parts = append(parts, c.Addr+" "+c.ID+" "+c.Status)
			continue
		}
		parts = append(parts, c.ID+" "+c.Status)
	}
	if len(parts) == 0 {
		return "no step ran"
	}
	return strings.Join(parts, ", ")
}

// checkLine renders one check: the step, its endpoint, and the fact or the
// problem with its fix.
func checkLine(c Check) string {
	head := c.ID
	if c.Addr != "" {
		head += " " + c.Addr
	}
	var body string
	switch {
	case c.Status == StatusFail || c.Status == StatusWarn:
		body = c.Problem
		if body == "" {
			body = "failed"
		}
		if c.Fix != "" {
			body += "; fix: " + c.Fix
		}
	case c.Detail != "":
		body = c.Detail
	}
	if body == "" {
		return head + " " + c.Status
	}
	return head + " " + c.Status + ": " + body
}
