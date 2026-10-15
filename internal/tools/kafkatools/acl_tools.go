package kafkatools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"piperoom/internal/clients"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/tools"
)

// principalRe bounds the optional principal filter ("User:name", "User:CN=x@y").
var principalRe = regexp.MustCompile(`^[A-Za-z0-9:_.@/-]+$`)

const (
	maxPrincipalLen = 200
	maxACLPrincipal = 8 // principals rendered in one summary
)

// ---- kafka_acls ----

type kafkaACLs struct{ d Deps }

func (t *kafkaACLs) Name() string     { return "kafka_acls" }
func (t *kafkaACLs) Kind() tools.Kind { return tools.KindRead }
func (t *kafkaACLs) InputSchema() map[string]any {
	return objectSchema(map[string]any{
		"principal": strProp(`optional Kafka principal, e.g. "User:debezium"; narrows the answer and checks WRITE on the pipeline's topics`),
	})
}
func (t *kafkaACLs) Description() string {
	return "The Kafka ACLs on this pipeline's topics and consumer groups (and the cluster ACLs of those principals). " +
		"Use it for TopicAuthorizationFailed or GroupAuthorizationFailed. ACLs of other pipelines are never shown. " +
		"Note: an authentication failure (wrong or rotated password) is not an ACL problem."
}

func (t *kafkaACLs) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	p, err := parse[struct {
		Principal string `json:"principal"`
	}](input)
	if err != nil {
		return tools.Result{}, err
	}
	if p.Principal != "" && (len(p.Principal) > maxPrincipalLen || !principalRe.MatchString(p.Principal)) {
		return tools.Result{}, fmt.Errorf("%w: principal must match %s and be at most %d characters", tools.ErrBadInput, principalRe, maxPrincipalLen)
	}
	sc, err := t.d.scope(ctx)
	if err != nil {
		return tools.Result{}, err
	}
	topics, groups := sc.Topics(), sc.Pipeline().ConsumerGroups
	if len(topics) == 0 && len(groups) == 0 {
		return tools.Result{}, fmt.Errorf("%w: the room's pipeline has no topics or groups (an unscoped call cannot read ACLs)", tools.ErrBadInput)
	}
	ar, ok := t.d.Kafka.(clients.ACLReader)
	if !ok {
		return tools.Result{}, fmt.Errorf("kafkatools: kafka_acls: %w", clients.ErrNotSupported)
	}
	cctx, cancel := withTimeout(ctx)
	defer cancel()
	entries, err := ar.ACLs(cctx, topics, groups)
	switch {
	case errors.Is(err, kafka.ErrACLDescribeDenied):
		return res("cannot describe ACLs: the Piperoom principal lacks Describe on Cluster (docs/pilot/permissions.md)", map[string]any{"described": false}), nil
	case errors.Is(err, kafka.ErrNoAuthorizer):
		return res("no ACL authorizer is configured on the cluster: ACLs are not enforced, so TopicAuthorizationFailed cannot come from ACLs", map[string]any{"described": false, "authorizer": false}), nil
	case err != nil:
		return tools.Result{}, fmt.Errorf("kafkatools: kafka_acls: %w", err)
	}
	entries = pipelineACLs(entries, topics, groups, p.Principal)
	return res(aclSummary(entries, topics, p.Principal), map[string]any{"described": true, "acls": entries}), nil
}

// pipelineACLs keeps what concerns this pipeline: topic and group entries
// (the client already matched them to the names) and cluster entries only of
// principals that also hold a topic or group entry, or the one asked for. A
// cluster ACL of an unrelated principal is another tenant's data.
func pipelineACLs(in []clients.ACLEntry, topics, groups []string, principal string) []clients.ACLEntry {
	own := map[string]bool{}
	for _, e := range in {
		if e.ResourceType != "CLUSTER" {
			own[e.Principal] = true
		}
	}
	var out []clients.ACLEntry
	for _, e := range in {
		if principal != "" && e.Principal != principal && e.Principal != "User:*" {
			continue
		}
		if e.ResourceType == "CLUSTER" && !own[e.Principal] && e.Principal != principal {
			continue
		}
		out = append(out, e)
	}
	return out
}

func aclSummary(entries []clients.ACLEntry, topics []string, principal string) string {
	type key struct{ perm, typ, res, pat, host string }
	byPrincipal := map[string]map[key][]string{}
	for _, e := range entries {
		m := byPrincipal[e.Principal]
		if m == nil {
			m = map[key][]string{}
			byPrincipal[e.Principal] = m
		}
		k := key{e.Permission, strings.ToLower(e.ResourceType), e.Resource, e.Pattern, e.Host}
		m[k] = append(m[k], e.Operation)
	}
	names := make([]string, 0, len(byPrincipal))
	for n := range byPrincipal {
		names = append(names, n)
	}
	sort.Strings(names)

	var sb strings.Builder
	if len(names) == 0 {
		if principal != "" {
			fmt.Fprintf(&sb, "no ACLs for %s on this pipeline's topics, groups or the cluster", principal)
		} else {
			sb.WriteString("no ACLs on this pipeline's topics or groups (with an authorizer and allow.everyone.if.no.acl.found=false this denies everything)")
		}
	}
	for i, n := range names {
		if i >= maxACLPrincipal {
			appendLine(&sb, fmt.Sprintf("… %d more principals not shown", len(names)-i))
			break
		}
		keys := make([]key, 0, len(byPrincipal[n]))
		for k := range byPrincipal[n] {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(a, b int) bool {
			x, y := keys[a], keys[b]
			if x.typ+x.res+x.perm != y.typ+y.res+y.perm {
				return x.typ+x.res+x.perm < y.typ+y.res+y.perm
			}
			return x.host < y.host
		})
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			ops := byPrincipal[n][k]
			sort.Strings(ops)
			s := fmt.Sprintf("%s %s %s", k.perm, strings.Join(ops, ","), k.typ)
			if k.typ != "cluster" {
				s += fmt.Sprintf(" %s (%s)", k.res, k.pat)
			}
			if !anyHost(k.host) {
				s += " from " + k.host
			}
			parts = append(parts, s)
		}
		appendLine(&sb, n+": "+strings.Join(parts, "; "))
	}
	if principal != "" {
		for _, topic := range topics {
			allowed, only, denied := writeAccess(entries, principal, topic)
			switch {
			case !allowed:
				appendLine(&sb, fmt.Sprintf("missing: WRITE on topic %s for the connector principal %s", topic, principal))
			case len(only) > 0:
				appendLine(&sb, fmt.Sprintf("WRITE on topic %s for %s is allowed only from %s", topic, principal, strings.Join(only, ", ")))
			case len(denied) > 0:
				appendLine(&sb, fmt.Sprintf("WRITE on topic %s for %s is denied from %s", topic, principal, strings.Join(denied, ", ")))
			}
		}
	}
	return sb.String()
}

// anyHost reports whether an ACL host matches every client host.
func anyHost(h string) bool { return h == "*" || h == "" }

// writeAccess reports whether principal may WRITE to topic from some host:
// an ALLOW of WRITE or ALL that covers the topic and is not overridden by a
// DENY of either from the same host (or from every host). only lists the
// hosts it is limited to when no ALLOW is for every host; denied lists the
// hosts a DENY excludes from an every-host ALLOW.
func writeAccess(entries []clients.ACLEntry, principal, topic string) (allowed bool, only, denied []string) {
	allowHosts, denyHosts := map[string]bool{}, map[string]bool{}
	allowAll, denyAll := false, false
	for _, e := range entries {
		if e.ResourceType != "TOPIC" || (e.Principal != principal && e.Principal != "User:*") || (e.Operation != "WRITE" && e.Operation != "ALL") {
			continue
		}
		covers := e.Resource == "*" || (e.Pattern == "LITERAL" && e.Resource == topic) || (e.Pattern == "PREFIXED" && strings.HasPrefix(topic, e.Resource))
		if !covers {
			continue
		}
		switch {
		case e.Permission == "DENY" && anyHost(e.Host):
			denyAll = true
		case e.Permission == "DENY":
			denyHosts[e.Host] = true
		case anyHost(e.Host):
			allowAll = true
		default:
			allowHosts[e.Host] = true
		}
	}
	if denyAll {
		return false, nil, nil
	}
	if allowAll {
		for h := range denyHosts {
			denied = append(denied, h)
		}
		sort.Strings(denied)
		return true, nil, denied
	}
	for h := range allowHosts {
		if !denyHosts[h] {
			only = append(only, h)
		}
	}
	sort.Strings(only)
	return len(only) > 0, only, nil
}
