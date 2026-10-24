package discover

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"sort"
	"strings"

	"piperoom/internal/clients"
)

const (
	pgSourceClass    = "io.debezium.connector.postgresql.PostgresConnector"
	mysqlSourceClass = "io.debezium.connector.mysql.MySqlConnector"
)

// sourceKindMySQL is owners.yaml's source_kind of a MySQL pipeline
// (owners.SourceKindMySQL, Q8d). The package does not import owners: the CLI
// stays light, and a test loads the rendered file with owners.Load.
const sourceKindMySQL = "mysql"

// sourceKinds maps a Debezium source class to its source_kind ("" = postgres,
// the default owners.yaml leaves out).
var sourceKinds = map[string]string{pgSourceClass: "", mysqlSourceClass: sourceKindMySQL}

// plainTable is a schema.table (db.table on MySQL) include entry with no regex metacharacters.
var plainTable = regexp.MustCompile(`^[A-Za-z0-9_]+\.[A-Za-z0-9_]+$`)

// sinkTable is a warehouse table name as written in a sink connector's
// table.name.format: exactly schema.table of plain identifiers, so a
// "${topic}" template, a bare table name (schema unknown) or hostile text is
// never taken as a name. A plain-identifier match also keeps the name safe to
// print in a comment.
var sinkTable = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*$`)

// sink is what discovery keeps of a sink connector: the topics it reads and,
// when its config names one literally, the table it writes. Only these two
// non-secret keys are read.
type sink struct {
	topics []string
	table  string // "" = not found
}

// draft is a pipeline under construction, before IDs are assigned.
type draft struct {
	connector  string
	sourceKind string // "" = postgres, or sourceKindMySQL
	prefix     string
	slot       string // "" on MySQL: it has no replication slot
	tables     []string
	topics     map[string]bool
	groups     []string
	sinkTables []string
	notes      []string
}

func (d *draft) note(format string, args ...any) {
	d.notes = append(d.notes, fmt.Sprintf(format, args...))
}

type runner struct {
	ctx      context.Context
	d        Deps
	o        Options
	res      Result
	warnings []string
}

func (r *runner) warn(format string, args ...any) {
	r.warnings = append(r.warnings, fmt.Sprintf(format, args...))
}

// call gives each client call its own deadline, so one slow endpoint cannot
// eat the whole run's budget.
func call[T any](r *runner, f func(context.Context) (T, error)) (T, error) {
	ctx, cancel := context.WithTimeout(r.ctx, r.o.CallTimeout)
	defer cancel()
	return f(ctx)
}

// Discover reads the cluster and drafts one pipeline per Debezium Postgres or
// MySQL source connector. Only failures to list connectors or topics are fatal;
// everything else becomes a skipped connector, a pipeline note or a warning,
// so a partly broken cluster still yields a useful draft.
func Discover(ctx context.Context, d Deps, o Options) (Result, error) {
	if o.CallTimeout <= 0 {
		o.CallTimeout = defaultCallTimeout
	}
	if o.MaxGroups <= 0 {
		o.MaxGroups = defaultMaxGroups
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	r := &runner{ctx: ctx, d: d, o: o}

	names, err := call(r, d.Connect.ListConnectors)
	if err != nil {
		return Result{}, fmt.Errorf("discover: list connectors: %w", err)
	}
	topicList, err := call(r, d.Kafka.Topics)
	if err != nil {
		return Result{}, fmt.Errorf("discover: list topics: %w", err)
	}
	topics := make(map[string]bool, len(topicList))
	for _, t := range topicList {
		topics[t] = true
	}

	var drafts []*draft
	sinks := map[string]sink{} // sink connector -> what it reads and writes
	for _, name := range sortedUnique(names) {
		dr, sk := r.connector(name, topics)
		if dr != nil {
			drafts = append(drafts, dr)
		}
		if len(sk.topics) > 0 {
			sinks[name] = sk
		}
	}
	r.warnSharedPrefixes(drafts)
	r.warnMixedKinds(drafts)

	if len(drafts) > 0 {
		r.checkSubjects(drafts)
		r.matchGroups(drafts)
		r.checkSlots(drafts)
		noteSinks(drafts, sinks)
	}
	if err := ctx.Err(); err != nil {
		return Result{}, fmt.Errorf("discover: %w", err)
	}

	r.res.Pipelines = assignIDs(drafts)
	sort.Slice(r.res.Skipped, func(i, j int) bool { return r.res.Skipped[i].Connector < r.res.Skipped[j].Connector })
	r.res.Warnings = sortedUnique(r.warnings)
	return r.res, nil
}

func (r *runner) skip(name, reason string) {
	r.res.Skipped = append(r.res.Skipped, Skipped{Connector: name, Reason: reason})
}

// connector reads one connector's config. It returns a draft for a Debezium
// Postgres or MySQL source, or what a sink connector reads and writes (only the
// non-secret "topics" and "table.name.format" keys are looked at).
func (r *runner) connector(name string, topics map[string]bool) (*draft, sink) {
	cfg, err := call(r, func(ctx context.Context) (map[string]string, error) { return r.d.Connect.Config(ctx, name) })
	switch {
	case errors.Is(err, clients.ErrNotFound):
		r.skip(name, "deleted during discovery")
		return nil, sink{}
	case err != nil:
		r.o.Logger.Warn("discover: connector config read failed", "connector", name, "err", err)
		r.skip(name, "config read failed")
		return nil, sink{}
	}
	class := cfg["connector.class"]
	kind, isSource := sourceKinds[class]
	if !isSource {
		if class == "" {
			r.skip(name, "not a Debezium Postgres or MySQL source (no connector.class)")
		} else {
			r.skip(name, fmt.Sprintf("not a Debezium Postgres or MySQL source (class %s)", class))
		}
		sk := sink{topics: splitList(cfg["topics"])}
		if t := strings.TrimSpace(cfg["table.name.format"]); sinkTable.MatchString(t) {
			sk.table = t
		}
		return nil, sk
	}

	dr := &draft{connector: name, sourceKind: kind, topics: map[string]bool{}}
	if kind == "" {
		dr.slot = cmp.Or(cfg["slot.name"], "debezium")
	}
	dr.prefix = cfg["topic.prefix"]
	if dr.prefix == "" {
		dr.prefix = cfg["database.server.name"]
		if dr.prefix == "" {
			r.skip(name, "no topic.prefix or database.server.name")
			return nil, sink{}
		}
		dr.note("topic prefix taken from database.server.name (Debezium 1.x key)")
	}

	derived := derivedTables(dr.prefix, topics)
	if inc := splitList(cfg["table.include.list"]); len(inc) > 0 {
		dr.tables = includeTables(dr, inc, derived)
	} else {
		dr.tables = guessTables(dr, derived, cfg)
	}
	dr.tables = sortedUnique(dr.tables)
	if len(dr.tables) == 0 {
		dr.note("TODO: no source tables found, fill in source_tables by hand")
	}
	for _, t := range dr.tables {
		if topic := dr.prefix + "." + t; topics[topic] {
			dr.topics[topic] = true
		}
	}

	var custom []string
	if cfg["transforms"] != "" {
		custom = append(custom, "transforms")
	}
	if v := cfg["topic.delimiter"]; v != "" && v != "." {
		custom = append(custom, "topic.delimiter")
	}
	if v := cfg["topic.naming.strategy"]; v != "" && v != "io.debezium.schema.SchemaTopicNamingStrategy" {
		custom = append(custom, "topic.naming.strategy")
	}
	if len(custom) > 0 {
		dr.note("custom topic naming (%s): check topic names by hand", strings.Join(custom, ", "))
	}
	return dr, sink{}
}

// derivedTables returns the schema.table names of the prefix's topics. The
// rest after "<prefix>." must be exactly two non-empty parts, which drops
// <prefix>.transaction and the topics of a longer prefix such as pg.eu.
func derivedTables(prefix string, topics map[string]bool) []string {
	var out []string
	for t := range topics {
		rest, ok := strings.CutPrefix(t, prefix+".")
		if !ok {
			continue
		}
		parts := strings.Split(rest, ".")
		if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
			out = append(out, rest)
		}
	}
	return sortedUnique(out)
}

// anchored compiles a Debezium list entry. Debezium matches the whole
// identifier and ignores case (Predicates.java uses Pattern.CASE_INSENSITIVE),
// so the RE2 form is (?i)^(?:entry)$.
func anchored(entry string) (*regexp.Regexp, error) {
	return regexp.Compile("(?i)^(?:" + entry + ")$")
}

func includeTables(dr *draft, entries, derived []string) []string {
	var out []string
	for _, e := range entries {
		re, err := anchored(e)
		if err != nil {
			dr.note("include entry %q is not RE2: add its tables by hand", e)
			continue
		}
		matched := false
		for _, t := range derived {
			if re.MatchString(t) {
				out = append(out, t)
				if strings.EqualFold(t, e) {
					matched = true
				}
			}
		}
		// A plain schema.table entry names a table even before its first
		// change event creates the topic.
		if plainTable.MatchString(e) && !matched {
			out = append(out, e)
			dr.note("no topic yet for %s", e)
		}
	}
	return out
}

// guessTables keeps the derived tables the connector's filters let through.
// The first part of a MySQL table is its database, filtered by
// database.include.list / database.exclude.list where Postgres has
// schema.include.list / schema.exclude.list.
func guessTables(dr *draft, derived []string, cfg map[string]string) []string {
	inKey, exKey := "schema.include.list", "schema.exclude.list"
	if dr.sourceKind == sourceKindMySQL {
		inKey, exKey = "database.include.list", "database.exclude.list"
	}
	tableEx := compileAll(dr, "exclude", splitList(cfg["table.exclude.list"]))
	schemaIn := compileAll(dr, "include", splitList(cfg[inKey]))
	schemaEx := compileAll(dr, "exclude", splitList(cfg[exKey]))
	var out []string
	for _, t := range derived {
		schema, _, _ := strings.Cut(t, ".")
		if anyMatch(tableEx, t) || anyMatch(schemaEx, schema) {
			continue
		}
		if len(schemaIn) > 0 && !anyMatch(schemaIn, schema) {
			continue
		}
		out = append(out, t)
	}
	if len(derived) > 0 {
		dr.note("source_tables guessed from existing topics")
	}
	return out
}

func compileAll(dr *draft, kind string, entries []string) []*regexp.Regexp {
	var out []*regexp.Regexp
	for _, e := range entries {
		re, err := anchored(e)
		if err != nil {
			dr.note("%s entry %q is not RE2: check its tables by hand", kind, e)
			continue
		}
		out = append(out, re)
	}
	return out
}

func anyMatch(res []*regexp.Regexp, s string) bool {
	for _, re := range res {
		if re.MatchString(s) {
			return true
		}
	}
	return false
}

// warnMixedKinds warns when the cluster has both Postgres and MySQL source
// connectors. The drafted pipelines carry no connections: blocks, so they
// would all share one client set, and one client set serves one source kind:
// the server (wiring.Open) refuses such a file. The warning lands in the
// file's footer and on stderr, and says what each kind needs.
func (r *runner) warnMixedKinds(drafts []*draft) {
	var pg, my []string
	for _, dr := range drafts {
		if dr.sourceKind == sourceKindMySQL {
			my = append(my, dr.connector)
		} else {
			pg = append(pg, dr.connector)
		}
	}
	if len(pg) == 0 || len(my) == 0 {
		return
	}
	r.warn("the cluster has Postgres source connectors (%s) and MySQL ones (%s); one server client set serves one source kind, so the server refuses this file as written: give each MySQL pipeline a connections: block whose source_db_url is its mysql:// URL (and keep SOURCE_DB_URL for the Postgres ones), or run one Piperoom deployment per source kind",
		strings.Join(pg, ", "), strings.Join(my, ", "))
}

func (r *runner) warnSharedPrefixes(drafts []*draft) {
	byPrefix := map[string][]string{}
	for _, dr := range drafts {
		byPrefix[dr.prefix] = append(byPrefix[dr.prefix], dr.connector)
	}
	for prefix, names := range byPrefix {
		if len(names) > 1 {
			r.warn("connectors %s share topic prefix %s", strings.Join(names, ", "), prefix)
		}
	}
}

func (r *runner) checkSubjects(drafts []*draft) {
	subjects, err := call(r, r.d.Registry.Subjects)
	if err != nil {
		r.o.Logger.Warn("discover: list subjects failed", "err", err)
		r.warn("schema registry subjects could not be listed: subject checks skipped")
		return
	}
	have := make(map[string]bool, len(subjects))
	for _, s := range subjects {
		have[s] = true
	}
	for _, dr := range drafts {
		for _, topic := range sortedKeys(dr.topics) {
			if !have[topic+"-value"] {
				dr.note("no subject %s-value (JSON converter or non-default subject name strategy?)", topic)
			}
		}
	}
}

// matchGroups reads each group's lag once and assigns the group to every
// pipeline whose topics it has committed offsets on.
func (r *runner) matchGroups(drafts []*draft) {
	groups, err := call(r, r.d.Kafka.ConsumerGroups)
	if err != nil {
		r.o.Logger.Warn("discover: list consumer groups failed", "err", err)
		r.warn("consumer groups could not be listed: consumer_groups left empty")
		return
	}
	groups = sortedUnique(groups)
	if len(groups) > r.o.MaxGroups {
		r.warn("%d consumer groups not checked (more than %d)", len(groups)-r.o.MaxGroups, r.o.MaxGroups)
		groups = groups[:r.o.MaxGroups]
	}
	for _, g := range groups {
		lag, err := call(r, func(ctx context.Context) (clients.GroupLag, error) { return r.d.Kafka.GroupLag(ctx, g) })
		if errors.Is(err, clients.ErrNotFound) {
			continue
		}
		if err != nil {
			r.o.Logger.Warn("discover: group lag read failed", "group", g, "err", err)
			r.warn("consumer group %s: lag could not be read", g)
			continue
		}
		for _, dr := range drafts {
			if !consumes(lag, dr.topics) {
				continue
			}
			dr.groups = append(dr.groups, g)
			if lag.State == "Empty" || lag.State == "Dead" {
				dr.note("group %s state %s at discovery: maybe retired", g, lag.State)
			}
		}
	}
}

// consumes reports a committed offset on one of the topics; -1 means the
// group never committed on that partition.
func consumes(lag clients.GroupLag, topics map[string]bool) bool {
	for _, p := range lag.Partitions {
		if topics[p.Topic] && p.Committed >= 0 {
			return true
		}
	}
	return false
}

// checkSlots notes a Postgres pipeline whose slot is missing or inactive. A
// MySQL pipeline has no slot and is not looked at; with only MySQL pipelines
// the slots are not even listed.
func (r *runner) checkSlots(drafts []*draft) {
	drafts = slices.DeleteFunc(slices.Clone(drafts), func(dr *draft) bool { return dr.slot == "" })
	if r.d.Source == nil || len(drafts) == 0 {
		return
	}
	slots, err := call(r, r.d.Source.ReplicationSlots)
	if err != nil {
		r.o.Logger.Warn("discover: list replication slots failed", "err", err)
		r.warn("replication slots could not be listed: slot checks skipped")
		return
	}
	byName := make(map[string]clients.ReplicationSlot, len(slots))
	for _, s := range slots {
		byName[s.Name] = s
	}
	for _, dr := range drafts {
		s, ok := byName[dr.slot]
		switch {
		case !ok:
			dr.note("slot %s not found in SOURCE_DB_URL (connector may point at another database)", dr.slot)
		case !s.Active:
			dr.note("slot %s inactive", dr.slot)
		}
	}
}

// noteSinks flags sink connectors that read a pipeline topic. owners
// "connectors" is the CheckScope list for connect_restart_*, so a human
// decides whether to add them. A sink that names its table literally also
// gives the pipeline a found sink table.
func noteSinks(drafts []*draft, sinks map[string]sink) {
	for _, name := range sortedKeys(sinks) {
		for _, t := range sinks[name].topics {
			for _, dr := range drafts {
				if !dr.topics[t] {
					continue
				}
				dr.note("sink connector %s reads %s: add it to connectors if connect_restart_* should cover it", name, t)
				if tbl := sinks[name].table; tbl != "" {
					dr.sinkTables = append(dr.sinkTables, tbl)
				}
			}
		}
	}
}

var nonSlug = regexp.MustCompile(`[^a-z0-9]+`)

// slug turns a connector name into a pipeline id: pg-orders-connector ->
// pg-orders.
func slug(name string) string {
	s := strings.Trim(nonSlug.ReplaceAllString(strings.ToLower(name), "-"), "-")
	s = strings.TrimSuffix(s, "-connector")
	if s == "" || s == "connector" {
		return "pipeline"
	}
	return s
}

// assignIDs gives each draft a unique id in connector-name order, then sorts
// pipelines by id, so the same cluster state always renders the same bytes.
func assignIDs(drafts []*draft) []Pipeline {
	used := map[string]bool{}
	out := make([]Pipeline, 0, len(drafts))
	for _, dr := range drafts {
		base := slug(dr.connector)
		id := base
		for n := 2; used[id]; n++ {
			id = fmt.Sprintf("%s-%d", base, n)
		}
		used[id] = true
		out = append(out, Pipeline{
			ID:             id,
			Connector:      dr.connector,
			SourceKind:     dr.sourceKind,
			TopicPrefix:    dr.prefix,
			SourceTables:   sortedUnique(dr.tables),
			Topics:         sortedKeys(dr.topics),
			ConsumerGroups: sortedUnique(dr.groups),
			SinkTables:     sortedUnique(dr.sinkTables),
			Notes:          sortedUnique(dr.notes),
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func sortedUnique(in []string) []string {
	out := slices.Clone(in)
	slices.Sort(out)
	return slices.Compact(out)
}

func sortedKeys[V any](m map[string]V) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}
