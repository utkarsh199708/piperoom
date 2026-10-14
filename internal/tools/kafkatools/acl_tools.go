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
	type key struct{ perm, typ, res, pat string }
	byPrincipal := map[string]map[key][]string{}
	for _, e := range entries {
		m := byPrincipal[e.Principal]
		if m == nil {
			m = map[key][]string{}
			byPrincipal[e.Principal] = m
		}
		k := key{e.Permission, strings.ToLower(e.ResourceType), e.Resource, e.Pattern}
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
			return x.typ+x.res+x.perm < y.typ+y.res+y.perm
		})
		parts := make([]string, 0, len(keys))
		for _, k := range keys {
			ops := byPrincipal[n][k]
			sort.Strings(ops)
			s := fmt.Sprintf("%s %s %s", k.perm, strings.Join(ops, ","), k.typ)
			if k.typ != "cluster" {
				s += fmt.Sprintf(" %s (%s)", k.res, k.pat)
			}
			parts = append(parts, s)
		}
		appendLine(&sb, n+": "+strings.Join(parts, "; "))
	}
	if principal != "" {
		for _, topic := range topics {
			if !canWrite(entries, principal, topic) {
				appendLine(&sb, fmt.Sprintf("missing: WRITE on topic %s for the connector principal %s", topic, principal))
			}
		}
	}
	return sb.String()
}

// canWrite reports whether principal may WRITE to topic: an ALLOW of WRITE or
// ALL that covers the topic, with no DENY of either.
func canWrite(entries []clients.ACLEntry, principal, topic string) bool {
	allowed := false
	for _, e := range entries {
		if e.ResourceType != "TOPIC" || (e.Principal != principal && e.Principal != "User:*") || (e.Operation != "WRITE" && e.Operation != "ALL") {
			continue
		}
		covers := e.Resource == "*" || (e.Pattern == "LITERAL" && e.Resource == topic) || (e.Pattern == "PREFIXED" && strings.HasPrefix(topic, e.Resource))
		if !covers {
			continue
		}
		if e.Permission == "DENY" {
			return false
		}
		allowed = true
	}
	return allowed
}
