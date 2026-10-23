package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// ConnectWorkerGroup is one Kafka group whose protocol type is "connect": the
// distributed Connect cluster's coordinator group.
type ConnectWorkerGroup struct {
	GroupID string          `json:"group_id"`
	State   string          `json:"state"`
	Workers []ConnectWorker `json:"workers"`
}

// ConnectWorker is one member of a Connect worker group.
type ConnectWorker struct {
	MemberID string `json:"member_id"`
	// Host is the host the member joined from, as the group coordinator
	// reports it (an address, not necessarily routable from here).
	Host string `json:"host"`
	// URL is the REST URL the worker advertises in its join metadata, or ""
	// when the member sent none (or the metadata could not be decoded).
	URL string `json:"url,omitempty"`
}

// InternalTopic is one Connect internal topic: its shape and the two configs
// that decide whether Connect can use it.
type InternalTopic struct {
	Name string `json:"name"`
	// Role is config, offset or status when the name carries Connect's usual
	// suffix, else "" (a renamed topic configured explicitly).
	Role              string `json:"role,omitempty"`
	Partitions        int    `json:"partitions"`
	ReplicationFactor int    `json:"replication_factor"`
	CleanupPolicy     string `json:"cleanup_policy"`
	MinInSyncReplicas int    `json:"min_insync_replicas"`
}

// ConnectClusterReader reads the Connect side of a Kafka cluster: the worker
// groups and the internal topics they own.
//
// It is the Q0 clients.ConnectClusterReader shape (tasks/Q-board.md §3),
// declared here until that contract lands; this PR carries a contract change
// request with the exact clients.go text to move it. Until then the tool
// asserts for this interface, so every existing fake and the eval replay
// compile unchanged.
type ConnectClusterReader interface {
	// ConnectWorkerGroups lists the groups whose protocol type is "connect",
	// with the state and members of each.
	ConnectWorkerGroups(ctx context.Context) ([]ConnectWorkerGroup, error)
	// InternalTopics reads the requested topics' partition count, replication
	// factor, cleanup.policy and min.insync.replicas. Names the cluster does
	// not know are left out; a topic that disappears between the metadata and
	// config reads is reported (not silently dropped).
	InternalTopics(ctx context.Context, names []string) ([]InternalTopic, error)
}

// BrokerCounter reports the cluster's broker count. The connect_worker_info
// replication-factor flag needs it ("RF 1 on a multi-broker cluster"); the Q0
// contract shape has no field for it (contract change request in this PR).
type BrokerCounter interface {
	BrokerCount(ctx context.Context) (int, error)
}

var (
	_ ConnectClusterReader = (*Client)(nil)
	_ BrokerCounter        = (*Client)(nil)
)

// connectProtocolType is the group protocol Kafka Connect's distributed mode
// uses; every Connect worker joins a group with this type.
const connectProtocolType = "connect"

// connectClusterAdmin is the part of *kadm.Client that ConnectWorkerGroups,
// InternalTopics and BrokerCount use; a seam so unit tests run without a
// broker.
type connectClusterAdmin interface {
	ListGroups(ctx context.Context, filterStates ...string) (kadm.ListedGroups, error)
	DescribeGroups(ctx context.Context, groups ...string) (kadm.DescribedGroups, error)
	Metadata(ctx context.Context, topics ...string) (kadm.Metadata, error)
	DescribeTopicConfigs(ctx context.Context, topics ...string) (kadm.ResourceConfigs, error)
	BrokerMetadata(ctx context.Context) (kadm.Metadata, error)
}

// connectAdm returns the test seam when set, else the real kadm client.
func (c *Client) connectAdm() connectClusterAdmin {
	if c.connectFn != nil {
		return c.connectFn
	}
	return c.adm
}

// ConnectWorkerGroups lists the cluster's Connect groups and their workers. It
// is two calls: ListGroups to find the groups by protocol type, then one
// DescribeGroups for their members (the worker id and host) and each member's
// advertised REST URL. A group-level describe error fails the call: a partial
// worker list would read as "fewer workers than there are".
func (c *Client) ConnectWorkerGroups(ctx context.Context) ([]ConnectWorkerGroup, error) {
	adm := c.connectAdm()
	start := time.Now()
	lg, err := adm.ListGroups(ctx)
	c.record(ctx, start, "kadm.ListGroups()")
	if err != nil {
		return nil, fmt.Errorf("kafka: list connect groups: %w", err)
	}
	var ids []string
	for _, g := range lg.Sorted() {
		// Kafka reports the protocol type verbatim; Connect writes "connect"
		// in lower case. A blank type (Kafka before 2.6) is not a Connect
		// group we can vouch for.
		if strings.EqualFold(strings.TrimSpace(g.ProtocolType), connectProtocolType) {
			ids = append(ids, g.Group)
		}
	}
	if len(ids) == 0 {
		return nil, nil
	}
	sort.Strings(ids)

	start = time.Now()
	ds, err := adm.DescribeGroups(ctx, ids...)
	c.record(ctx, start, "kadm.DescribeGroups(groups)", fmt.Sprintf("groups=%d", len(ids)))
	if err != nil {
		return nil, fmt.Errorf("kafka: describe connect groups: %w", err)
	}
	out := make([]ConnectWorkerGroup, 0, len(ids))
	for _, id := range ids {
		d, ok := ds[id]
		if !ok {
			return nil, fmt.Errorf("kafka: describe connect group %s: no description returned", id)
		}
		if d.Err != nil {
			return nil, fmt.Errorf("kafka: describe connect group %s: %w", id, mapNotFound(d.Err))
		}
		g := ConnectWorkerGroup{GroupID: id, State: d.State, Workers: make([]ConnectWorker, 0, len(d.Members))}
		for _, m := range d.Members {
			g.Workers = append(g.Workers, connectWorker(m))
		}
		// kadm sorts by instance id first, but the contract's order must be
		// stable across clients: sort by member id.
		sort.Slice(g.Workers, func(i, j int) bool { return g.Workers[i].MemberID < g.Workers[j].MemberID })
		out = append(out, g)
	}
	return out, nil
}

// connectWorker maps one described member. The REST URL lives in the Connect
// member metadata, which kadm decodes into kmsg.ConnectMemberMetadata
// (GroupMemberMetadata.AsConnect, kmsg.ConnectMemberMetadata.URL — confirmed
// with go doc); a member with no decodable metadata keeps the host only.
func connectWorker(m kadm.DescribedGroupMember) ConnectWorker {
	w := ConnectWorker{MemberID: m.MemberID, Host: m.ClientHost}
	if md, ok := m.Join.AsConnect(); ok {
		w.URL = md.URL
	}
	return w
}

// InternalTopics reads the requested topics in two calls: metadata for the
// partition and replica counts, then the effective configs for cleanup.policy
// and min.insync.replicas (a broker default counts when the topic overrides
// neither). A topic the cluster does not know is left out of the result; every
// other failure is returned, so a caller never reads "no config" as "fine".
func (c *Client) InternalTopics(ctx context.Context, names []string) ([]InternalTopic, error) {
	if len(names) == 0 {
		return nil, nil
	}
	adm := c.connectAdm()
	start := time.Now()
	md, err := adm.Metadata(ctx, names...)
	c.record(ctx, start, "kadm.Metadata(topics)", topicsParam(names))
	if err != nil {
		return nil, fmt.Errorf("kafka: internal topic metadata: %w", err)
	}
	start = time.Now()
	cfgs, err := adm.DescribeTopicConfigs(ctx, names...)
	c.record(ctx, start, "kadm.DescribeTopicConfigs(topics)", topicsParam(names))
	if err != nil {
		return nil, fmt.Errorf("kafka: internal topic configs: %w", err)
	}

	// Config lookups are best-effort per topic here: a topic the metadata
	// read found but the config read does not know (deleted in between) is
	// reported by the caller as missing rather than failing the whole read.
	byName := make(map[string]kadm.ResourceConfig, len(cfgs))
	for _, rc := range cfgs {
		if rc.Err == nil {
			byName[rc.Name] = rc
		}
	}

	out := make([]InternalTopic, 0, len(names))
	seen := make(map[string]bool, len(names))
	for _, name := range names {
		if seen[name] {
			continue
		}
		seen[name] = true
		td, ok := md.Topics[name]
		if !ok {
			continue
		}
		if td.Err != nil {
			if errors.Is(td.Err, kerr.UnknownTopicOrPartition) {
				continue
			}
			return nil, fmt.Errorf("kafka: internal topic metadata %s: %w", name, mapNotFound(td.Err))
		}
		it := InternalTopic{
			Name:              name,
			Role:              internalTopicRole(name),
			Partitions:        len(td.Partitions),
			ReplicationFactor: td.Partitions.NumReplicas(),
		}
		if cfg, ok := byName[name]; ok {
			it.CleanupPolicy, it.MinInSyncReplicas = topicCleanup(cfg)
		} else if rc := findConfig(cfgs, name); rc != nil {
			// A real config failure (authorization, broker error) must not
			// turn into an empty policy the caller reads as "delete".
			if !errors.Is(rc.Err, kerr.UnknownTopicOrPartition) {
				return nil, fmt.Errorf("kafka: internal topic configs %s: %w", name, mapNotFound(rc.Err))
			}
			continue
		}
		out = append(out, it)
	}
	return out, nil
}

// findConfig returns the config row for name, if the response has one.
func findConfig(cfgs kadm.ResourceConfigs, name string) *kadm.ResourceConfig {
	for i := range cfgs {
		if cfgs[i].Name == name {
			return &cfgs[i]
		}
	}
	return nil
}

// topicCleanup reads only the two keys the Connect checks need, so no other
// (possibly sensitive) config value leaves the client.
func topicCleanup(rc kadm.ResourceConfig) (policy string, minISR int) {
	for i := range rc.Configs {
		switch rc.Configs[i].Key {
		case "cleanup.policy":
			policy = rc.Configs[i].MaybeValue()
		case "min.insync.replicas":
			if n, err := strconv.Atoi(rc.Configs[i].MaybeValue()); err == nil {
				minISR = n
			}
		}
	}
	return policy, minISR
}

// internalTopicRole names the Connect role a topic's usual suffix implies.
// A cluster that renamed its internal topics configures them explicitly, and
// their role is "" unless the name still carries the suffix.
func internalTopicRole(name string) string {
	switch {
	case strings.HasSuffix(name, "-configs"):
		return "config"
	case strings.HasSuffix(name, "-offsets"):
		return "offset"
	case strings.HasSuffix(name, "-status"):
		return "status"
	}
	return ""
}

// BrokerCount returns the number of brokers in the cluster, without asking
// for topics (kadm.BrokerMetadata).
func (c *Client) BrokerCount(ctx context.Context) (int, error) {
	start := time.Now()
	md, err := c.connectAdm().BrokerMetadata(ctx)
	c.record(ctx, start, "kadm.BrokerMetadata()")
	if err != nil {
		return 0, fmt.Errorf("kafka: broker metadata: %w", mapNotFound(err))
	}
	return len(md.Brokers), nil
}
