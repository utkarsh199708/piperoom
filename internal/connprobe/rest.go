package connprobe

import (
	"context"
	"net/http"
	"sort"
	"strings"
)

// restReach runs the reach step of a REST probe: the guard check on the URL,
// then GET path. valid says whether a 200 body is what the service answers.
// It returns the reach check, the login check derived from the same answer
// (the zero Check when login could not be judged), and the 200 body.
func restReach(ctx context.Context, h *httpProbe, g *Guard, path string, valid func([]byte) (string, bool)) (reach, login Check, body []byte) {
	reach = step(func() Check {
		if err := h.checkURL(ctx, g); err != nil {
			return h.reachFail(err)
		}
		status, b, err := h.get(ctx, path)
		if err != nil {
			return h.reachFail(err)
		}
		switch {
		case isAuthStatus(status):
			login = h.loginFail(status, path)
			return Check{ID: "reach", Status: StatusOK, Detail: "server answered; login refused"}
		case status == http.StatusOK:
			detail, ok := valid(b)
			if !ok {
				return Check{ID: "reach", Status: StatusFail,
					Problem: "The answer to GET " + path + " is not what the " + h.svc.Name + " REST API returns.",
					Fix:     "Check that " + h.svc.URLKey + " points at " + h.svc.Name + "."}
			}
			login = h.loginOK()
			body = b
			return Check{ID: "reach", Status: StatusOK, Detail: detail}
		default:
			return h.statusFail("reach", status, path)
		}
	})
	return reach, login, body
}

// skipRestInto appends skip checks to r for ids after a blocking failure.
func skipRestInto(r *Result, why string, ids ...string) {
	for _, id := range ids {
		r.Checks = append(r.Checks, skipped(id, why))
	}
}

// uniqueSorted returns the non-empty, trimmed, distinct values, sorted.
func uniqueSorted(in []string) []string {
	seen := map[string]bool{}
	var out []string
	for _, s := range in {
		s = strings.TrimSpace(s)
		if s == "" || seen[s] {
			continue
		}
		seen[s] = true
		out = append(out, s)
	}
	sort.Strings(out)
	return out
}

// pipelineTopics returns the Debezium topics of the target's pipelines,
// "<topic_prefix>.<schema>.<table>", distinct and sorted. A pipeline without
// a topic prefix contributes none.
func pipelineTopics(t Target) []string {
	var all []string
	for _, p := range t.Pipelines {
		if strings.TrimSpace(p.TopicPrefix) == "" {
			continue
		}
		for _, st := range p.SourceTables {
			if st = strings.TrimSpace(st); st != "" {
				all = append(all, p.TopicPrefix+"."+st)
			}
		}
	}
	return uniqueSorted(all)
}

// pipelinePrefixes returns the distinct topic prefixes of the pipelines.
func pipelinePrefixes(t Target) []string {
	var all []string
	for _, p := range t.Pipelines {
		all = append(all, p.TopicPrefix)
	}
	return uniqueSorted(all)
}

// pipelineConnectors returns the distinct connectors of the pipelines.
func pipelineConnectors(t Target) []string {
	var all []string
	for _, p := range t.Pipelines {
		all = append(all, p.Connectors...)
	}
	return uniqueSorted(all)
}

// pipelineGroups returns the distinct consumer groups of the pipelines.
func pipelineGroups(t Target) []string {
	var all []string
	for _, p := range t.Pipelines {
		all = append(all, p.ConsumerGroups...)
	}
	return uniqueSorted(all)
}
