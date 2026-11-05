package agent

import (
	"regexp"
	"strings"
)

// KAN-410: the dead-slot eval failed because a retired connector's marker was
// only one fact on the pipeline map's connector line, and the model read the
// STOPPED state but proposed resuming the retired connector. The opening
// prompt therefore names the retirement evidence the map already carries, so
// the drop rule (prompt.go, Fixes) can fire instead of the resume checks.
//
// The marker is owners-config text rendered by the topology builder, but the
// map as a whole is untrusted data: this file only lifts the line, and
// firstUserMessage wraps it like the rest of the map.

const (
	// maxRetirementLines caps the lines lifted from one map. A pipeline with
	// more retired connectors than this still has them in the map below.
	maxRetirementLines = 4
	// maxRetirementLineBytes caps one lifted line, so a single long node line
	// cannot crowd the opening prompt.
	maxRetirementLineBytes = 300
)

// retirementMarker matches the word "retired" and its suffixed forms as
// owners config and the pipeline map render them: "retired 2026-10-01",
// "(retired 2026-10-01)", "retired_at: 2026-10-01", "retired=true". The
// leading boundary keeps "unretired" out; the owners key "connectors_retired"
// is matched by hasRetirementMarker.
var retirementMarker = regexp.MustCompile(`(?i)\bretired`)

// retirementEvidence returns the pipeline-map lines that mark a connector as
// retired, joined by newlines; "" when the map carries no such marker. A line
// counts only when it also names a connector: a "connect:" node line, a
// "connectors_retired:" entry, or any line carrying a "connect:" id. A legend
// sentence that merely uses the word "retired" is not evidence about a
// connector, so it is left in the map and not repeated here.
func retirementEvidence(pipelineMap string) string {
	connectors := pipelineMapConnectors(pipelineMap)
	var lines []string
	for _, line := range strings.Split(pipelineMap, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || !hasRetirementMarker(line) || !namesRetiredConnector(line, connectors) {
			continue
		}
		lines = append(lines, truncatePromptField(line, maxRetirementLineBytes))
		if len(lines) == maxRetirementLines {
			break
		}
	}
	return strings.Join(lines, "\n")
}

// pipelineMapConnectors lists the connector names of a rendered pipeline map.
// The retired marker may sit on a connector's node line or in a one-line
// summary ("pg-orders-connector (retired 2026-10-01)"), so both shapes are
// recognised against this list.
func pipelineMapConnectors(pipelineMap string) []string {
	var out []string
	for _, line := range strings.Split(pipelineMap, "\n") {
		if m := pipelineMapNode.FindStringSubmatch(strings.TrimSpace(line)); m != nil && m[1] == "connect" {
			out = append(out, m[2])
		}
	}
	return out
}

// namesRetiredConnector reports whether a line that carries a retirement
// marker also names a connector. The map's node format is pinned by
// pipelineMapNode (topology.Render); the other shapes are accepted so an
// owners-config summary line still counts.
func namesRetiredConnector(line string, connectors []string) bool {
	lower := strings.ToLower(line)
	if strings.Contains(lower, "connectors_retired") {
		return true
	}
	if m := pipelineMapNode.FindStringSubmatch(line); m != nil && m[1] == "connect" {
		return true
	}
	if strings.Contains(lower, "connect:") {
		return true
	}
	for _, name := range connectors {
		if strings.Contains(line, name) {
			return true
		}
	}
	return false
}

// hasRetirementMarker reports whether a line carries a retirement marker:
// the word "retired" or the owners-config key "connectors_retired", whose
// underscore keeps the word boundary above from matching.
func hasRetirementMarker(line string) bool {
	return retirementMarker.MatchString(line) || strings.Contains(strings.ToLower(line), "connectors_retired")
}
