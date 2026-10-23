package kafka

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// The types and the interface below are the Q3a shapes from tasks/Q-board.md
// §3 (Q0 item 3). They belong in internal/clients/clients.go but are declared
// here until that contract lands, like connect.SourceOffsetReader; the PR
// carries a contract change request with the exact clients code. Fields
// marked "additive" are not in the board's shape and are part of that
// request.
var _ ConnectClusterReader = (*Client)(nil)

// ConnectClusterReader reads the Kafka side of Kafka Connect: the worker
// groups (protocol type "connect") and the worker's internal topics. It is
// optional; the Kafka client implements it.
type ConnectClusterReader interface {
	// ConnectWorkerGroups lists every group with protocol type "connect" and
	// describes its members. A group the broker cannot describe is returned
	// with Error set and no workers, so one bad group does not hide the rest.
	ConnectWorkerGroups(ctx context.Context) ([]ConnectWorkerGroup, error)
	// InternalTopics describes the named topics. With no names it finds the
	// candidates itself: every topic whose name ends in a Connect storage
	// suffix (see InternalTopicRole). A named topic the cluster does not know
	// is returned with Missing set. Discovery finds every Connect cluster's
	// topics; AttributeInternalTopics ties them to groups.
	InternalTopics(ctx context.Context, names []string) ([]InternalTopic, error)
}

// ConnectWorkerGroup is one Kafka Connect cluster as the group coordinator
// sees it.
type ConnectWorkerGroup struct {
	GroupID string          `json:"group_id"`
	State   string          `json:"state"`
	Workers []ConnectWorker `json:"workers"`
	// Error is why the group could not be described (additive).
	Error string `json:"error,omitempty"`
}

// ConnectWorker is one member of a Connect worker group.
type ConnectWorker struct {
	MemberID string `json:"member_id"`
	Host     string `json:"host"`
	// URL is the advertised REST URL from the member's join metadata
	// (kmsg.ConnectMemberMetadata.URL); "" when it cannot be read or is not
	// an http(s) URL.
	URL string `json:"url,omitempty"`
	// Leader is true for the group's leader, as the members' assignment
	// names it (additive).
	Leader bool `json:"leader,omitempty"`
	// Connectors are the connector names assigned to this worker, either the
	// connector instance or one of its tasks, from the member's assignment
	// (additive). The tool uses them to tell the room's Connect cluster from
	// others and never shows another pipeline's name.
	Connectors []string `json:"connectors,omitempty"`
}

// InternalTopic is one of a Connect cluster's storage topics. Values are
// effective: a broker default counts when the topic has no override.
type InternalTopic struct {
	Name              string `json:"name"`
	Role              string `json:"role,omitempty"` // config | offset | status | ""
	Partitions        int    `json:"partitions"`
	ReplicationFactor int    `json:"replication_factor"`
	CleanupPolicy     string `json:"cleanup_policy"`
	MinInSyncReplicas int    `json:"min_insync_replicas"`
	// Brokers is the cluster's broker count at read time, so a caller can
	// tell replication factor 1 on a multi-broker cluster from a one-broker
	// dev cluster (additive).
	Brokers int `json:"brokers"`
	// Missing is true for a named topic the cluster does not know (additive).
	Missing bool `json:"missing,omitempty"`
}

// Internal topic roles.
const (
	InternalRoleConfig = "config"
	InternalRoleOffset = "offset"
	InternalRoleStatus = "status"
)

// Bounds on what the reader keeps from untrusted member metadata.
const (
	maxConnectGroups       = 50
	maxWorkersPerGroup     = 200
	maxWorkerConnectors    = 200
	maxWorkerTextBytes     = 255
	maxInternalTopics      = 50
	connectProtocolType    = "connect"
	connectDescribeTimeout = 10 * time.Second
)

// connectAdmin is the part of *kadm.Client the Connect cluster reads use; a
// seam so unit tests run without a broker.
type connectAdmin interface {
	ListGroups(ctx context.Context, filterStates ...string) (kadm.ListedGroups, error)
	DescribeGroups(ctx context.Context, groups ...string) (kadm.DescribedGroups, error)
	Metadata(ctx context.Context, topics ...string) (kadm.Metadata, error)
	DescribeTopicConfigs(ctx context.Context, topics ...string) (kadm.ResourceConfigs, error)
}

func (c *Client) connectAdm() connectAdmin {
	if c.connectFn != nil {
		return c.connectFn
	}
	return c.adm
}

// ConnectWorkerGroups lists the groups whose protocol type is "connect" (one
// ListGroups, which every broker version answers with the protocol type) and
// describes them in one DescribeGroups. ListGroupsByType is not used: it
// filters by group type (classic, consumer), not protocol type, and needs
// Kafka 3.8.
func (c *Client) ConnectWorkerGroups(ctx context.Context) ([]ConnectWorkerGroup, error) {
	ctx, cancel := context.WithTimeout(ctx, connectDescribeTimeout)
	defer cancel()
	adm := c.connectAdm()
	start := time.Now()
	listed, err := adm.ListGroups(ctx)
	c.record(ctx, start, "kadm.ListGroups()")
	if err != nil && len(listed) == 0 {
		return nil, fmt.Errorf("kafka: list connect groups: %w", err)
	}
	var ids []string
	for _, g := range listed.Sorted() {
		if g.ProtocolType == connectProtocolType {
			ids = append(ids, g.Group)
		}
	}
	if len(ids) > maxConnectGroups {
		ids = ids[:maxConnectGroups]
	}
	if len(ids) == 0 {
		return []ConnectWorkerGroup{}, nil
	}
	start = time.Now()
	described, err := adm.DescribeGroups(ctx, ids...)
	// A count, not the ids: the receipt reaches the room, and the other
	// Connect clusters' group ids are not the room's to see.
	c.record(ctx, start, "kadm.DescribeGroups(groups)", fmt.Sprintf("groups=%d", len(ids)))
	if err != nil && len(described) == 0 {
		return nil, fmt.Errorf("kafka: describe connect groups: %w", err)
	}
	out := make([]ConnectWorkerGroup, 0, len(ids))
	for _, id := range ids {
		dg, ok := described[id]
		if !ok {
			out = append(out, ConnectWorkerGroup{GroupID: id, State: listed[id].State, Workers: []ConnectWorker{}, Error: "not described"})
			continue
		}
		out = append(out, workerGroup(dg))
	}
	return out, nil
}

// workerGroup maps one described group. Member metadata is written by the
// worker, so every string is cleaned and capped before it is kept.
func workerGroup(dg kadm.DescribedGroup) ConnectWorkerGroup {
	g := ConnectWorkerGroup{GroupID: dg.Group, State: workerText(dg.State), Workers: []ConnectWorker{}}
	if dg.Err != nil {
		g.Error = groupErrText(dg.Err)
		return g
	}
	for i, m := range dg.Members {
		if i >= maxWorkersPerGroup {
			break
		}
		w := ConnectWorker{
			MemberID: workerText(m.MemberID),
			Host:     workerText(strings.TrimPrefix(m.ClientHost, "/")),
		}
		if md, ok := m.Join.AsConnect(); ok {
			w.URL = workerURL(md.URL)
		}
		if as, ok := m.Assigned.AsConnect(); ok {
			w.Leader = as.Leader != "" && as.Leader == m.MemberID
			seen := map[string]bool{}
			for _, a := range as.Assignment {
				name := workerText(a.Connector)
				if name == "" || seen[name] || len(w.Connectors) >= maxWorkerConnectors {
					continue
				}
				seen[name] = true
				w.Connectors = append(w.Connectors, name)
			}
			sort.Strings(w.Connectors)
		}
		g.Workers = append(g.Workers, w)
	}
	sort.Slice(g.Workers, func(i, j int) bool { return g.Workers[i].MemberID < g.Workers[j].MemberID })
	return g
}

// groupErrText names a describe failure without echoing broker text.
func groupErrText(err error) string {
	var ke *kerr.Error
	if errors.As(err, &ke) {
		return ke.Message
	}
	return "describe failed"
}

// workerURL keeps an advertised REST URL only when it is a plain http(s) URL
// with a host and no credentials; anything else is not a URL a reader should
// follow.
func workerURL(s string) string {
	s = strings.TrimSpace(s)
	if s == "" || len(s) > maxWorkerTextBytes {
		return ""
	}
	u, err := url.Parse(s)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil {
		return ""
	}
	if strings.ContainsFunc(s, func(r rune) bool { return !unicode.IsPrint(r) || r == ' ' }) {
		return ""
	}
	return s
}

// workerText folds unprintable runes to spaces and caps the length, so a
// member id, host or connector name cannot forge a summary line.
func workerText(s string) string {
	s = strings.Map(func(r rune) rune {
		if (r != ' ' && !unicode.IsPrint(r)) || r == ' ' || r == ' ' {
			return ' '
		}
		return r
	}, s)
	if len(s) > maxWorkerTextBytes {
		cut := maxWorkerTextBytes
		for cut > 0 && !utf8RuneStart(s[cut]) {
			cut--
		}
		s = s[:cut]
	}
	return strings.TrimSpace(s)
}

func utf8RuneStart(b byte) bool { return b&0xC0 != 0x80 }

// InternalTopicRole returns the Connect storage role a topic name suggests:
// the last "-" or "_" separated word is configs/config, offsets/offset or
// status/statuses (Strimzi "<cluster>-configs", the Debezium images'
// "my_connect_configs"), or the name holds the word "connect" and one of
// those words anywhere ("_connect_configs_staging"). "" = not a candidate.
func InternalTopicRole(name string) string {
	words := strings.FieldsFunc(strings.ToLower(name), func(r rune) bool { return r == '-' || r == '_' })
	if len(words) == 0 {
		return ""
	}
	role := func(w string) string {
		switch w {
		case "configs", "config":
			return InternalRoleConfig
		case "offsets", "offset":
			return InternalRoleOffset
		case "status", "statuses":
			return InternalRoleStatus
		}
		return ""
	}
	// A name with a dot is a data topic ("<prefix>.<schema>.<table>"): a
	// table called "status" must not look like Connect's status topic.
	if strings.Contains(name, ".") {
		return ""
	}
	if r := role(words[len(words)-1]); r != "" && len(words) > 1 {
		return r
	}
	hasConnect := false
	for _, w := range words {
		if w == "connect" {
			hasConnect = true
		}
	}
	if !hasConnect {
		return ""
	}
	for _, w := range words {
		if r := role(w); r != "" {
			return r
		}
	}
	return ""
}

// genericConnectWords are the words Connect storage topic and group names
// share across clusters ("connect-configs", "my_connect_offsets", Strimzi's
// default group "connect-cluster"). They say nothing about which cluster a
// topic belongs to, so attribution ignores them.
var genericConnectWords = map[string]bool{"connect": true, "kafka": true, "my": true, "cluster": true}

// connectNameWords splits a topic or group name into the words that tell
// clusters apart: lower case, split on "-", "_" and ".", without the generic
// words and, for a topic, without its role words.
func connectNameWords(name string, topic bool) []string {
	var out []string
	for _, w := range strings.FieldsFunc(strings.ToLower(name), func(r rune) bool { return r == '-' || r == '_' || r == '.' }) {
		if genericConnectWords[w] {
			continue
		}
		if topic {
			switch w {
			case "configs", "config", "offsets", "offset", "status", "statuses":
				continue
			}
		}
		out = append(out, w)
	}
	return out
}

// containsRun reports whether short is a contiguous run of long.
func containsRun(long, short []string) bool {
	if len(short) == 0 || len(short) > len(long) {
		return false
	}
	for i := 0; i+len(short) <= len(long); i++ {
		if slices.Equal(long[i:i+len(short)], short) {
			return true
		}
	}
	return false
}

// AttributeInternalTopics ties discovered internal topics to the Connect
// worker groups on the cluster by Connect's naming habit: a cluster's
// storage topics are named after its group id ("<group>-configs" in
// Strimzi; "_connect_configs_staging" for the dev stack's group
// "piperoom-connect-staging").
//
//   - A topic whose distinguishing words (connectNameWords) equal a group's
//     belongs to that group; failing that, one whose words are a contiguous
//     run of a group's words, or the reverse. Two groups matching equally
//     well is ambiguous: the topic stays unattributed.
//   - A topic with only generic words ("connect-configs", "_connect_status",
//     "my_connect_offsets") belongs to the one group no specific topic was
//     tied to, if there is exactly one; with several such groups it could
//     be anyone's and stays unattributed.
//
// The result maps topic name to group id; an unattributed topic is absent.
// The caller shows a scoped room only its own groups' topics. Attribution
// is by name only: a wrong guess can hide a topic, and it can show another
// cluster's topic only when that cluster has no live group of its own (its
// specific topic shares the room group's words, or its generic topic falls
// to the room's group as the one unclaimed group).
func AttributeInternalTopics(topics, groupIDs []string) map[string]string {
	out := map[string]string{}
	groupWords := make([][]string, len(groupIDs))
	for i, g := range groupIDs {
		groupWords[i] = connectNameWords(g, false)
	}
	claimed := make([]bool, len(groupIDs))
	var generic []string
	for _, t := range topics {
		tw := connectNameWords(t, true)
		if len(tw) == 0 {
			generic = append(generic, t)
			continue
		}
		best, bestScore, tie := -1, 0, false
		for i, gw := range groupWords {
			score := 0
			switch {
			case slices.Equal(tw, gw):
				score = 2
			case containsRun(gw, tw) || containsRun(tw, gw):
				score = 1
			}
			switch {
			case score > bestScore:
				best, bestScore, tie = i, score, false
			case score > 0 && score == bestScore:
				tie = true
			}
		}
		if best >= 0 && !tie {
			out[t] = groupIDs[best]
			claimed[best] = true
		}
	}
	free := -1
	for i := range groupIDs {
		if claimed[i] {
			continue
		}
		if free != -1 {
			free = -2 // more than one: a generic name is ambiguous
			break
		}
		free = i
	}
	if free >= 0 {
		for _, t := range generic {
			out[t] = groupIDs[free]
		}
	}
	return out
}

// InternalTopics reads partitions and replication from one metadata request
// and cleanup.policy and min.insync.replicas from one DescribeTopicConfigs.
// With no names the metadata request lists every topic and keeps the
// InternalTopicRole candidates (at most 50).
func (c *Client) InternalTopics(ctx context.Context, names []string) ([]InternalTopic, error) {
	ctx, cancel := context.WithTimeout(ctx, connectDescribeTimeout)
	defer cancel()
	adm := c.connectAdm()
	discover := len(names) == 0
	start := time.Now()
	md, err := adm.Metadata(ctx, names...)
	if discover {
		c.record(ctx, start, "kadm.Metadata(all)")
	} else {
		c.record(ctx, start, "kadm.Metadata(topics)", topicsParam(names))
	}
	if err != nil {
		return nil, fmt.Errorf("kafka: internal topics metadata: %w", err)
	}
	if discover {
		for name := range md.Topics {
			if InternalTopicRole(name) != "" {
				names = append(names, name)
			}
		}
		sort.Strings(names)
	}
	if len(names) > maxInternalTopics {
		names = names[:maxInternalTopics]
	}
	brokers := len(md.Brokers)
	out := make([]InternalTopic, 0, len(names))
	var present []string
	for _, name := range names {
		it := InternalTopic{Name: name, Role: InternalTopicRole(name), Brokers: brokers}
		td, ok := md.Topics[name]
		switch {
		case !ok || errors.Is(td.Err, kerr.UnknownTopicOrPartition):
			it.Missing = true
		case td.Err != nil:
			return nil, fmt.Errorf("kafka: internal topic %s: %w", name, td.Err)
		default:
			it.Partitions = len(td.Partitions)
			it.ReplicationFactor = minReplicas(td.Partitions)
			present = append(present, name)
		}
		out = append(out, it)
	}
	if len(present) == 0 {
		return out, nil
	}
	start = time.Now()
	rcs, err := adm.DescribeTopicConfigs(ctx, present...)
	c.record(ctx, start, "kadm.DescribeTopicConfigs(topics)", topicsParam(present))
	if err != nil {
		return nil, fmt.Errorf("kafka: internal topic configs: %w", err)
	}
	byName := map[string]kadm.ResourceConfig{}
	for _, rc := range rcs {
		byName[rc.Name] = rc
	}
	for i := range out {
		rc, ok := byName[out[i].Name]
		if !ok || rc.Err != nil {
			continue // configs unknown: cleanup "" and min ISR 0
		}
		for j := range rc.Configs {
			switch rc.Configs[j].Key {
			case "cleanup.policy":
				out[i].CleanupPolicy = workerText(rc.Configs[j].MaybeValue())
			case "min.insync.replicas":
				if n, err := strconv.Atoi(rc.Configs[j].MaybeValue()); err == nil {
					out[i].MinInSyncReplicas = n
				}
			}
		}
	}
	return out, nil
}

// minReplicas is the smallest replica count over the partitions: one
// under-replicated partition is the one that loses data.
func minReplicas(ps kadm.PartitionDetails) int {
	n := -1
	for _, p := range ps {
		if n < 0 || len(p.Replicas) < n {
			n = len(p.Replicas)
		}
	}
	if n < 0 {
		return 0
	}
	return n
}
