package topology

import (
	"errors"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/dlq"
	"piperoom/internal/owners"
)

// dlqSpec is one dead-letter topic of a pipeline with everything that writes
// to it. It merges the two sources of docs/design/dlq.md section 3.1.1 so a
// topic that is both declared in owners.yaml and set on a connector is one node.
type dlqSpec struct {
	topic string
	// connectors and groups are the writers, in discovery order (declared
	// first, then connector configs, each in pipeline order).
	connectors []string
	groups     []string
	// configured is true when a sink connector's own config names the topic.
	// Only then does a missing topic warn: Connect should have created it.
	configured bool
}

// writerIDs returns the node ids of the writers: connectors first, then groups.
func (d dlqSpec) writerIDs() []string {
	ids := make([]string, 0, len(d.connectors)+len(d.groups))
	for _, c := range d.connectors {
		ids = append(ids, "connect:"+c)
	}
	for _, g := range d.groups {
		ids = append(ids, "group:"+g)
	}
	return ids
}

// writerNames returns the writer names sorted, for the writer fact.
func (d dlqSpec) writerNames() string {
	names := append(slices.Clone(d.connectors), d.groups...)
	sort.Strings(names)
	return strings.Join(names, ",")
}

// layer is the writer's layer; a topic with a connector writer is kafka-connect.
func (d dlqSpec) layer() string {
	if len(d.connectors) > 0 {
		return "kafka-connect"
	}
	return "consumer"
}

// discoverDLQs lists the pipeline's DLQ topics: declared ones first (file
// order), then topics named by a sink connector's config (pipeline order),
// de-duplicated by topic. A connector whose config could not be read
// contributes nothing: a missing side node is not a missing graph shape.
func discoverDLQs(p owners.Pipeline, info *collected) []dlqSpec {
	var out []dlqSpec
	idx := map[string]int{}
	get := func(topic string) *dlqSpec {
		i, ok := idx[topic]
		if !ok {
			i = len(out)
			idx[topic] = i
			out = append(out, dlqSpec{topic: topic})
		}
		return &out[i]
	}
	addConn := func(d *dlqSpec, c string) {
		if !slices.Contains(d.connectors, c) {
			d.connectors = append(d.connectors, c)
		}
	}
	for _, decl := range p.Watch().DLQTopics {
		d := get(decl.Topic)
		if slices.Contains(p.Connectors, decl.WrittenBy) {
			addConn(d, decl.WrittenBy)
		} else if !slices.Contains(d.groups, decl.WrittenBy) {
			d.groups = append(d.groups, decl.WrittenBy)
		}
	}
	for _, cn := range p.Connectors {
		snap, ok := info.connect[cn]
		if !ok || snap.err != nil {
			continue
		}
		ec := dlq.Parse(snap.config)
		if !ec.IsSink || ec.Topic == "" {
			continue
		}
		d := get(ec.Topic)
		addConn(d, cn)
		d.configured = true
	}
	return out
}

// dlqNode builds the side node for one DLQ topic from the TopicOffsets
// reading alone (docs/design/dlq.md section 3.3).
func (b *builder) dlqNode(d dlqSpec, t Thresholds, info *collected) Node {
	n := Node{
		ID:     "dlq:" + d.topic,
		Kind:   KindDLQTopic,
		Name:   d.topic,
		Layer:  d.layer(),
		Health: HealthUnknown,
	}
	n.Owner = b.layerOwner(n.Layer)
	writer := d.writerNames()

	err := info.topicErr[d.topic]
	switch {
	case errors.Is(err, clients.ErrNotFound):
		n.Health = HealthOK
		if d.configured {
			n.Health = HealthWarn
		}
		n.Facts = map[string]string{"records": "0", "exists": "false", "writer": writer}
	case err != nil:
		n.Facts = map[string]string{"error": "unreachable"}
	default:
		n.Health, n.Facts = dlqHealth(info.topicRecords[d.topic], info.topicNewest[d.topic], info.now, t)
		n.Facts["writer"] = writer
	}
	return n
}

// dlqHealth warns while the newest dead-letter record is younger than
// DLQRecentWarn: a fresh record means bad data is arriving now. An old pile is
// a fact for people, not a current fault.
func dlqHealth(records int64, newest, now time.Time, t Thresholds) (Health, map[string]string) {
	facts := map[string]string{"records": strconv.FormatInt(records, 10)}
	if newest.IsZero() {
		facts["newest_record"] = "never"
		return HealthOK, facts
	}
	age := now.Sub(newest)
	facts["newest_record"] = HumanAge(age) + " ago"
	if age < t.DLQRecentWarn {
		facts["recent"] = "true"
		return HealthWarn, facts
	}
	return HealthOK, facts
}

// addConnectorDLQFacts adds the error-handling facts to a connector node. Only
// the DLQ topic name and risk codes are copied, never another config value.
// A sev2 risk turns an ok connector into warn; failed or unknown stay as they are.
func (b *builder) addConnectorDLQFacts(n *Node, snap connectSnapshot, info *collected) {
	if snap.err != nil {
		return
	}
	ec := dlq.Parse(snap.config)
	var exists *bool
	if ec.Topic != "" {
		switch err := info.topicErr[ec.Topic]; {
		case err == nil:
			if _, queried := info.topicErr[ec.Topic]; queried {
				v := true
				exists = &v
			}
		case errors.Is(err, clients.ErrNotFound):
			v := false
			exists = &v
		}
	}
	risks := dlq.Risks(ec, exists)

	facts := map[string]string{}
	if ec.Tolerance == dlq.ToleranceAll {
		facts["errors_tolerance"] = "all"
	}
	switch {
	case ec.Topic != "":
		facts["dlq"] = ec.Topic
	case ec.IsSink && ec.Tolerance == dlq.ToleranceAll:
		facts["dlq"] = "none"
	}
	if len(risks) > 0 {
		facts["config_risk"] = strings.Join(dlq.Codes(risks), ",")
	}
	if len(facts) == 0 {
		return
	}
	if n.Facts == nil {
		n.Facts = map[string]string{}
	}
	for k, v := range facts {
		n.Facts[k] = v
	}
	if n.Health == HealthOK && dlq.WorstSev(risks) == dlq.SevSev2 {
		n.Health = HealthWarn
	}
}
