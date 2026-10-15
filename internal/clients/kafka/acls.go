package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"piperoom/internal/clients"
)

var _ clients.ACLReader = (*Client)(nil)

var (
	// ErrACLDescribeDenied means the principal lacks Describe on Cluster.
	ErrACLDescribeDenied = errors.New("kafka: not allowed to describe ACLs (needs Describe on Cluster)")
	// ErrNoAuthorizer means the cluster runs without an ACL authorizer.
	ErrNoAuthorizer = errors.New("kafka: the cluster has no ACL authorizer")
)

// ACLs lists the ACLs that apply to the given topics and groups (literal,
// prefixed and wildcard patterns that match them) plus cluster ACLs, for any
// principal and host. Entries are filtered again here, so an ACL on another
// pipeline's resource is never returned even if the broker sent it.
func (c *Client) ACLs(ctx context.Context, topics, groups []string) ([]clients.ACLEntry, error) {
	// Topics()/Groups() with no names would match every ACL of that type, so a
	// type is added only when there is a name to match.
	b := kadm.NewACLs().Clusters().ResourcePatternType(kadm.ACLPatternMatch).Operations(kadm.OpAny)
	if len(topics) > 0 {
		b.Topics(topics...)
	}
	if len(groups) > 0 {
		b.Groups(groups...)
	}
	b.Allow().AllowHosts().Deny().DenyHosts() // empty = any principal, any host
	if err := b.ValidateDescribe(); err != nil {
		return nil, fmt.Errorf("kafka: describe ACLs filter: %w", err)
	}
	start := time.Now()
	results, err := c.clusterAdm().DescribeACLs(ctx, b)
	c.record(ctx, start, "kadm.DescribeACLs(topics, groups, cluster)", topicsParam(topics), fmt.Sprintf("groups=%d", len(groups)))
	if err != nil {
		return nil, aclErr(err)
	}
	var out []clients.ACLEntry
	seen := map[clients.ACLEntry]bool{}
	for _, r := range results {
		if r.Err != nil {
			return nil, aclErr(r.Err)
		}
		for _, d := range r.Described {
			e := clients.ACLEntry{
				Principal: d.Principal, Host: d.Host, ResourceType: d.Type.String(), Resource: d.Name,
				Pattern: d.Pattern.String(), Operation: d.Operation.String(), Permission: d.Permission.String(),
			}
			if !aclApplies(e, topics, groups) || seen[e] {
				continue
			}
			seen[e] = true
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		return strings.Join([]string{a.Principal, a.ResourceType, a.Resource, a.Operation, a.Permission}, "\x00") <
			strings.Join([]string{b.Principal, b.ResourceType, b.Resource, b.Operation, b.Permission}, "\x00")
	})
	return out, nil
}

func aclErr(err error) error {
	switch {
	case errors.Is(err, kerr.ClusterAuthorizationFailed):
		return fmt.Errorf("%w: %v", ErrACLDescribeDenied, err)
	case errors.Is(err, kerr.SecurityDisabled):
		return ErrNoAuthorizer
	}
	return fmt.Errorf("kafka: describe ACLs: %w", err)
}

// aclApplies reports whether e covers one of the topics or groups, or is a
// cluster ACL.
func aclApplies(e clients.ACLEntry, topics, groups []string) bool {
	var names []string
	switch e.ResourceType {
	case "CLUSTER":
		return true
	case "TOPIC":
		names = topics
	case "GROUP":
		names = groups
	default:
		return false
	}
	for _, n := range names {
		switch {
		case e.Resource == "*", e.Pattern == "LITERAL" && e.Resource == n, e.Pattern == "PREFIXED" && strings.HasPrefix(n, e.Resource):
			return true
		}
	}
	return false
}
