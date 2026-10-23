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
//
// Topic names come from connector config, which the map prints into the
// model's prompt, so a derived or routed name is used only when it is a legal
// Kafka topic name (dlq.ValidTopic): a name with a newline or a space could
// otherwise forge lines of the map.

// maxConfigRegex caps a regex read from connector config. A longer one is not
// evaluated, and what it would prove stays unconfirmed.
const maxConfigRegex = 4 << 10

// javaRegexSafe reports whether RE2 can be trusted to give java.util.regex's
// answer for a config pattern: not too long, and no POSIX bracket ("[:") or
// class intersection ("&&"), which the two engines read differently.
func javaRegexSafe(expr string) bool {
	return len(expr) <= maxConfigRegex && !strings.Contains(expr, "[:") && !strings.Contains(expr, "&&")
}

// compileConfigRegex compiles a config pattern for whole-name matching (whole,
// Java's Matcher.matches, with flags such as "(?i)" in front) and as written
// (bare, for a search), or reports that it cannot be trusted (javaRegexSafe,
// or RE2 rejects it). expr must compile on its own: "a)|(b" only parses once
// wrapped, and Java refuses it.
func compileConfigRegex(flags, expr string) (whole, bare *regexp.Regexp, ok bool) {
	if !javaRegexSafe(expr) {
		return nil, nil, false
	}
	bare, err := regexp.Compile(expr)
	if err != nil {
		return nil, nil, false
	}
	whole, err = regexp.Compile(flags + "^(?:" + expr + ")$")
	return whole, bare, err == nil
}

// flowPlan is the data-flow half of a graph: which topics exist, which are
// placeholders, and every data edge with how it is known.
type flowPlan struct {
	topics      []string        // CDC topics in discovery order (placeholders included)
	placeholder map[string]bool // topic names that stand in for unknown real names
	invalidName map[string]bool // placeholders for a configured name that is not a legal topic name
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

// isDebezium reports whether the builder may read a source connector's
// config the Debezium way. An unset class is read as Debezium.
func isDebezium(cfg map[string]string) bool {
	c := strings.TrimSpace(cfg["connector.class"])
	return c == "" || strings.HasPrefix(c, "io.debezium.connector.")
}

// defaultTopicNaming reports whether a Debezium connector names its topics
// <topic.prefix>.<schema>.<table>: the default delimiter and naming strategy.
func defaultTopicNaming(cfg map[string]string) bool {
	if d := cfg["topic.delimiter"]; d != "" && d != "." {
		return false
	}
	s := strings.TrimSpace(cfg["topic.naming.strategy"])
	return s == "" || s == "io.debezium.schema.SchemaTopicNamingStrategy"
}

// sourceMatchers compiles the include/exclude lists of every readable source
// connector once per Build.
func (b *builder) sourceMatchers(p owners.Pipeline, info *collected) map[string]streamMatcher {
	out := map[string]streamMatcher{}
	for _, cn := range p.Connectors {
		if snap, ok := info.connect[cn]; ok && snap.err == nil && !isSinkConnector(snap) {
			out[cn] = newStreamMatcher(snap.config, b.warnBadListEntry)
		}
	}
	return out
}

// sourceTopics returns the topics a source connector writes for the streamed
// tables: <topic.prefix>.<table>, routed. proven is false when the routing
// cannot be evaluated. placeholder is true when a derived or routed name is
// not a legal topic name: the table names then stand in, as for an unreadable
// config, so no untrusted name reaches the map.
func sourceTopics(cfg map[string]string, streamed []string) (topics []string, proven, placeholder bool) {
	prefix := cfg["topic.prefix"]
	if prefix == "" {
		return nil, true, false
	}
	names := make([]string, len(streamed))
	for i, tbl := range streamed {
		names[i] = prefix + "." + tbl
	}
	topics, proven = routeTopics(cfg, names)
	for _, t := range topics {
		if !dlq.ValidTopic(t) {
			return fallbackTopics(streamed), false, true
		}
	}
	return topics, proven, false
}

// planFlow derives the topics and data edges of p from what collect read.
func (b *builder) planFlow(p owners.Pipeline, info *collected) flowPlan {
	fp := flowPlan{placeholder: map[string]bool{}, invalidName: map[string]bool{}}
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
				fromTables = append(fromTables, inferred(Edge{From: sourceNodeID(p, tbl), To: cid}))
			}
			for _, t := range fallbackTopics(p.SourceTables) {
				addTopic(t, true)
				toTopics = append(toTopics, inferred(Edge{From: cid, To: "topic:" + t}))
			}
			continue
		}
		m, source := info.matchers[cn]
		if !source {
			sinks[cn] = true
			continue
		}
		// Debezium's list semantics prove which tables stream only for a
		// Debezium connector whose lists Go evaluates exactly.
		exact := m.exact() && isDebezium(snap.config)
		var streamed []string
		for _, tbl := range p.SourceTables {
			switch {
			case !exact:
				streamed = append(streamed, tbl)
				fromTables = append(fromTables, inferred(Edge{From: sourceNodeID(p, tbl), To: cid}))
			case m.streams(tbl):
				streamed = append(streamed, tbl)
				fromTables = append(fromTables, Edge{From: sourceNodeID(p, tbl), To: cid})
			}
		}
		ts, proven, placeholder := sourceTopics(snap.config, streamed)
		proven = proven && exact && defaultTopicNaming(snap.config)
		for _, t := range ts {
			addTopic(t, placeholder)
			if placeholder {
				fp.invalidName[t] = true
			}
			e := Edge{From: cid, To: "topic:" + t}
			if !proven {
				e = inferred(e)
			}
			toTopics = append(toTopics, e)
		}
	}

	// Pass 2: readers of the CDC topics (sink connectors, consumer groups) and
	// what they write to the warehouse. With sinks: the block is the only
	// declaration of what fills each table, so no connector or group is linked
	// to a sink except the writer the block names.
	explicitSinks := p.UsesSinkBlocks()
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
		if explicitSinks {
			continue
		}
		for _, s := range p.SinkTableNames() {
			toWarehouse = append(toWarehouse, inferred(Edge{From: cid, To: "wh:" + s}))
		}
	}
	for _, gr := range p.ConsumerGroupNames() {
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
		if explicitSinks {
			continue
		}
		// The legacy form has no group -> sink table declaration, so this link
		// is never proven. The sinks: form declares the writer per table below.
		for _, s := range p.SinkTableNames() {
			toWarehouse = append(toWarehouse, inferred(Edge{From: gid, To: "wh:" + s}))
		}
	}
	if explicitSinks {
		for _, ref := range p.SinkRefs() {
			if name, ok := ref.Sink.ConsumerGroupWriter(); ok {
				// Declared in owners.yaml, so the pairing is proven (graph.go
				// MappingMapped), unlike the legacy cross product. The ref key
				// keeps same-named sinks in different warehouses distinct.
				toWarehouse = append(toWarehouse, Edge{From: "group:" + name, To: "wh:" + ref.Key})
			}
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
// the config says neither, or the regex cannot be trusted (compileConfigRegex).
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
	re, _, ok := compileConfigRegex("", expr)
	if !ok {
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
// topic only when their regex matches the whole name, and then replace the
// first match found from the start (Java Matcher.matches, then replaceFirst,
// which resets the matcher and searches again).
const (
	regexRouterClass       = "org.apache.kafka.connect.transforms.RegexRouter"
	logicalTableRouterType = "io.debezium.transforms.ByLogicalTableRouter"
)

// topicRouter is one evaluable routing transform.
type topicRouter struct {
	whole *regexp.Regexp // anchored: Matcher.matches
	first *regexp.Regexp // unanchored: Matcher.replaceFirst
	tmpl  string         // Go Expand template
}

// routeTopics applies the connector's routing transforms to its derived
// topics, in chain order, and de-duplicates the result (a logical-table
// router merges shards into one topic). proven is false when the chain holds
// a transform that may rename topics but cannot be evaluated here (another
// router class, a predicate, an untrusted regex or a replacement Java would
// refuse): the names are then the unrouted ones and must be treated as
// unconfirmed.
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
	if !r.whole.MatchString(topic) {
		return topic
	}
	m := r.first.FindStringSubmatchIndex(topic)
	if m == nil {
		return topic // cannot happen: the whole name matched
	}
	return topic[:m[0]] + string(r.first.ExpandString(nil, r.tmpl, topic, m)) + topic[m[1]:]
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
		whole, first, ok := compileConfigRegex("", expr)
		if !ok {
			proven = false
			continue
		}
		tmpl, err := javaReplacement(repl, first)
		if err != nil {
			proven = false
			continue
		}
		routers = append(routers, topicRouter{whole: whole, first: first, tmpl: tmpl})
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

// groupNameRE is Java's rule for a named-group reference in a replacement.
var groupNameRE = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]*$`)

// javaReplacement converts a java.util.regex replacement string to a Go
// Expand template for re, following Matcher.appendReplacement: "${name}"
// names a group of re (letters and digits, starting with a letter; a number
// or an unknown name is refused); "$" takes its first digit as a group number
// and each further digit only while the number stays within re's groups; a
// backslash quotes the next character; a literal "$" becomes "$$". Anything
// Java would refuse is an error, so the caller treats the routing as unproven.
func javaReplacement(s string, re *regexp.Regexp) (string, error) {
	groups := re.NumSubexp()
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
				name := s[j+1 : j+end]
				if !groupNameRE.MatchString(name) || re.SubexpIndex(name) < 0 {
					return "", fmt.Errorf("no group named %q", name)
				}
				b.WriteString("${" + name + "}")
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
