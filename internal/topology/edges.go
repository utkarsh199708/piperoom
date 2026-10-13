package topology

import (
	"fmt"
	"regexp"
	"slices"
	"strings"

	"piperoom/internal/dlq"
	"piperoom/internal/owners"
)

// The builder draws only edges it can prove from live config or metadata
// (docs/api.md "Topology"). A link it cannot prove but owners.yaml implies,
// because both ends are in the same pipeline, is drawn as MappingInferred so
// the agent treats it as a hypothesis. It never draws the old cross product
// (every table to every connector, every topic to every group) as proven:
// with two connectors or two groups that showed data flows that do not exist.

// flowPlan is the data-flow half of a graph: which topics exist, which are
// placeholders, and every data edge with how it is known.
type flowPlan struct {
	topics      []string        // CDC topics in discovery order (placeholders included)
	placeholder map[string]bool // topic names that stand in for unknown real names
	edges       []Edge          // data edges, in flow order
}

// isSinkConnector reports whether a readable connector is a sink: Connect
// says so in its status, its class ends in SinkConnector (the dlq package's
// rule, so both readings agree), or it has the sink-only topics keys.
func isSinkConnector(snap connectSnapshot) bool {
	if strings.EqualFold(snap.status.Type, "sink") || dlq.Parse(snap.config).IsSink {
		return true
	}
	return strings.TrimSpace(snap.config["topics"]) != "" || strings.TrimSpace(snap.config["topics.regex"]) != ""
}

// planFlow derives the topics and data edges of p from what collect read.
func (b *builder) planFlow(p owners.Pipeline, info *collected) flowPlan {
	fp := flowPlan{placeholder: map[string]bool{}}
	addTopic := func(t string, placeholder bool) {
		if !slices.Contains(fp.topics, t) {
			fp.topics = append(fp.topics, t)
		}
		if placeholder {
			fp.placeholder[t] = true
		}
	}

	// Pass 1: source connectors and their topics. A sink writes no CDC topic.
	var fromTables, toTopics []Edge
	sinks := map[string]bool{}
	for _, cn := range p.Connectors {
		cid := "connect:" + cn
		snap, ok := info.connect[cn]
		if !ok || snap.err != nil {
			// Config unreadable: keep today's shape (every table into the
			// connector, placeholder topics out of it) but say none of it is
			// proven.
			for _, tbl := range p.SourceTables {
				fromTables = append(fromTables, inferred(Edge{From: "pg:" + tbl, To: cid}))
			}
			for _, t := range fallbackTopics(p.SourceTables) {
				addTopic(t, true)
				toTopics = append(toTopics, inferred(Edge{From: cid, To: "topic:" + t}))
			}
			continue
		}
		if isSinkConnector(snap) {
			sinks[cn] = true
			continue
		}
		m := newStreamMatcher(snap.config, b.warnBadListEntry)
		for _, tbl := range p.SourceTables {
			if m.streams(tbl) {
				fromTables = append(fromTables, Edge{From: "pg:" + tbl, To: cid})
			}
		}
		ts, proven := routeTopics(snap.config, deriveTopics(snap.config, p.SourceTables, b.warnBadListEntry))
		for _, t := range ts {
			addTopic(t, false)
			e := Edge{From: cid, To: "topic:" + t}
			if !proven {
				e = inferred(e)
			}
			toTopics = append(toTopics, e)
		}
	}

	// Pass 2: readers of the CDC topics (sink connectors, consumer groups) and
	// what they write to the warehouse.
	var toReaders, toWarehouse []Edge
	for _, cn := range p.Connectors {
		if !sinks[cn] {
			continue
		}
		cid := "connect:" + cn
		read, ok := sinkInputs(info.connect[cn].config, fp.topics)
		for _, t := range fp.topics {
			switch {
			case !ok:
				toReaders = append(toReaders, inferred(Edge{From: "topic:" + t, To: cid}))
			case slices.Contains(read, t):
				toReaders = append(toReaders, Edge{From: "topic:" + t, To: cid})
			}
		}
		for _, s := range p.SinkTables {
			toWarehouse = append(toWarehouse, inferred(Edge{From: cid, To: "wh:" + s}))
		}
	}
	for _, gr := range p.ConsumerGroups {
		gid := "group:" + gr
		read := groupTopics(info.groups[gr], fp.topics)
		for _, t := range fp.topics {
			switch {
			case len(read) == 0:
				// Lag unreadable, or no offsets on any pipeline topic yet:
				// owners.yaml is all that links them.
				toReaders = append(toReaders, inferred(Edge{From: "topic:" + t, To: gid}))
			case slices.Contains(read, t):
				toReaders = append(toReaders, Edge{From: "topic:" + t, To: gid})
			}
		}
		// owners.yaml has no group -> sink table declaration yet, so this
		// link is never proven.
		for _, s := range p.SinkTables {
			toWarehouse = append(toWarehouse, inferred(Edge{From: gid, To: "wh:" + s}))
		}
	}

	fp.edges = slices.Concat(fromTables, toTopics, toReaders, toWarehouse)
	return fp
}

// groupTopics returns the pipeline topics a consumer group has committed
// offsets or an assignment on. Nil when its lag could not be read.
func groupTopics(snap groupSnapshot, topics []string) []string {
	if snap.err != nil {
		return nil
	}
	var out []string
	for _, pl := range snap.lag.Partitions {
		if slices.Contains(topics, pl.Topic) && !slices.Contains(out, pl.Topic) {
			out = append(out, pl.Topic)
		}
	}
	return out
}

// sinkInputs returns which of topics a sink connector reads, from its topics
// list or its topics.regex (Connect matches the whole name). ok is false when
// the config says neither, or the regex is not one Go can evaluate.
func sinkInputs(cfg map[string]string, topics []string) (read []string, ok bool) {
	if list := splitCSV(cfg["topics"]); len(list) > 0 {
		for _, t := range topics {
			if slices.Contains(list, t) {
				read = append(read, t)
			}
		}
		return read, true
	}
	expr := strings.TrimSpace(cfg["topics.regex"])
	if expr == "" {
		return nil, false
	}
	re, err := regexp.Compile("^(?:" + expr + ")$")
	if err != nil {
		return nil, false
	}
	for _, t := range topics {
		if re.MatchString(t) {
			read = append(read, t)
		}
	}
	return read, true
}

func inferred(e Edge) Edge {
	e.Mapping = MappingInferred
	return e
}

// ---- topic routing transforms ----

// Router transforms whose topic rewrite the builder evaluates. Both rewrite a
// topic only when their regex matches the whole name, then replace it with
// the expanded replacement (Java Matcher.matches + replaceFirst).
const (
	regexRouterClass       = "org.apache.kafka.connect.transforms.RegexRouter"
	logicalTableRouterType = "io.debezium.transforms.ByLogicalTableRouter"
)

// topicRouter is one evaluable routing transform.
type topicRouter struct {
	re   *regexp.Regexp
	tmpl string // Go Expand template
}

// routeTopics applies the connector's routing transforms to its derived
// topics, in chain order, and de-duplicates the result (a logical-table
// router merges shards into one topic). proven is false when the chain holds
// a transform that may rename topics but cannot be evaluated here (another
// router class, a predicate, or a regex RE2 cannot compile): the names are
// then the unrouted ones and must be treated as unconfirmed.
func routeTopics(cfg map[string]string, topics []string) (out []string, proven bool) {
	routers, proven := topicRouters(cfg)
	for _, t := range topics {
		for _, r := range routers {
			t = r.apply(t)
		}
		if !slices.Contains(out, t) {
			out = append(out, t)
		}
	}
	return out, proven
}

func (r topicRouter) apply(topic string) string {
	m := r.re.FindStringSubmatchIndex(topic)
	if m == nil {
		return topic
	}
	return string(r.re.ExpandString(nil, r.tmpl, topic, m))
}

// topicRouters reads the transforms chain. Only the routing transforms are
// looked at; other transforms do not change the topic name.
func topicRouters(cfg map[string]string) (routers []topicRouter, proven bool) {
	proven = true
	for _, alias := range splitCSV(cfg["transforms"]) {
		pre := "transforms." + alias + "."
		class := strings.TrimSpace(cfg[pre+"type"])
		var expr, repl string
		switch class {
		case regexRouterClass:
			expr, repl = cfg[pre+"regex"], cfg[pre+"replacement"]
		case logicalTableRouterType:
			expr, repl = cfg[pre+"topic.regex"], cfg[pre+"topic.replacement"]
		default:
			if mayRenameTopic(class) {
				proven = false
			}
			continue
		}
		if strings.TrimSpace(cfg[pre+"predicate"]) != "" {
			// Conditional routing depends on each record.
			proven = false
			continue
		}
		re, err := regexp.Compile("^(?:" + expr + ")$")
		if err != nil {
			proven = false
			continue
		}
		tmpl, err := javaReplacement(repl, re.NumSubexp())
		if err != nil {
			proven = false
			continue
		}
		routers = append(routers, topicRouter{re: re, tmpl: tmpl})
	}
	return routers, proven
}

// mayRenameTopic flags transform classes that route records to other topics
// (TimestampRouter, the outbox EventRouter, ExtractTopic, ...). It is a name
// heuristic on purpose: an unknown router must make the names unconfirmed,
// never silently proven.
func mayRenameTopic(class string) bool {
	c := strings.ToLower(class)
	return strings.Contains(c, "router") || strings.Contains(c, "topic")
}

// javaReplacement converts a java.util.regex replacement string to a Go
// Expand template, following Matcher.appendReplacement: "${name}" is a named
// group; "$" takes its first digit as a group number and each further digit
// only while the number stays within groups; a backslash quotes the next
// character; a literal "$" becomes "$$". A reference Java would refuse is an
// error, so the caller treats the routing as unproven.
func javaReplacement(s string, groups int) (string, error) {
	var b strings.Builder
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case c == '\\':
			if i+1 >= len(s) {
				return "", fmt.Errorf("trailing backslash")
			}
			i++
			if s[i] == '$' {
				b.WriteString("$$")
			} else {
				b.WriteByte(s[i])
			}
		case c == '$':
			j := i + 1
			if j < len(s) && s[j] == '{' {
				end := strings.IndexByte(s[j:], '}')
				if end < 0 {
					return "", fmt.Errorf("unclosed group name")
				}
				b.WriteString(s[i : j+end+1])
				i = j + end
				continue
			}
			if j >= len(s) || s[j] < '0' || s[j] > '9' {
				return "", fmt.Errorf("illegal group reference")
			}
			n := int(s[j] - '0')
			j++
			for j < len(s) && s[j] >= '0' && s[j] <= '9' && n*10+int(s[j]-'0') <= groups {
				n = n*10 + int(s[j]-'0')
				j++
			}
			if n > groups {
				return "", fmt.Errorf("no group %d", n)
			}
			fmt.Fprintf(&b, "${%d}", n)
			i = j - 1
		default:
			b.WriteByte(c)
		}
	}
	return b.String(), nil
}
