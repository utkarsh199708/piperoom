package kafkatools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/tools"
)

// ---- connect_worker_info (Q3a) ----

// Caps on what connect_worker_info shows. The reads are bounded by the
// clients; these bound the summary and Data.
const (
	workerInfoSummaryWorkers = 10  // worker lines per group in the summary
	workerInfoSummaryTopics  = 12  // internal topic lines in the summary
	workerInfoDataPlugins    = 200 // plugin rows in Data
	workerInfoDataLoggers    = 200 // logger rows in Data
	workerInfoSummaryLoggers = 10  // loggers named in the summary besides root
)

type workerInfo struct{ d Deps }

func (t *workerInfo) Name() string     { return "connect_worker_info" }
func (t *workerInfo) Kind() tools.Kind { return tools.KindRead }
func (t *workerInfo) InputSchema() map[string]any {
	return objectSchema(map[string]any{})
}
func (t *workerInfo) Description() string {
	return "Shows the Kafka Connect cluster that runs this pipeline's connectors as Kafka sees it: the worker group (protocol type connect), " +
		"each worker's host and advertised REST URL, the group leader; the config, offset and status internal topics with partitions, " +
		"replication and cleanup policy; the worker's plugins and log levels. Flags a config topic with more than one partition, an " +
		"internal topic that is not compacted, and replication factor 1 on a multi-broker cluster. Use when connectors lose their " +
		"config or offsets after a restart, workers keep rebalancing, a worker seems missing, or to check what the worker logs at."
}

// workerInfoWorker is one worker in Data. Connectors lists only this
// pipeline's connectors; the others are a count.
type workerInfoWorker struct {
	MemberID        string   `json:"member_id"`
	Host            string   `json:"host"`
	URL             string   `json:"url,omitempty"`
	Leader          bool     `json:"leader,omitempty"`
	Connectors      []string `json:"connectors,omitempty"`
	OtherConnectors int      `json:"other_connectors,omitempty"`
}

type workerInfoGroup struct {
	GroupID string             `json:"group_id"`
	State   string             `json:"state"`
	Error   string             `json:"error,omitempty"`
	Workers []workerInfoWorker `json:"workers"`
	// Why says how the group was tied to this room: "runs the pipeline's
	// connectors", "declared group", "only Connect group in the cluster" or
	// "unscoped".
	Why string `json:"why"`
}

type workerInfoLogger struct {
	Logger string `json:"logger"`
	Level  string `json:"level"`
}

type workerInfoData struct {
	Groups      []workerInfoGroup `json:"groups"`
	OtherGroups int               `json:"other_groups,omitempty"`
	GroupsError string            `json:"groups_error,omitempty"`
	// TopicsSource is "CONNECT_INTERNAL_TOPICS" or "name match".
	TopicsSource   string                `json:"topics_source"`
	InternalTopics []kafka.InternalTopic `json:"internal_topics"`
	TopicsError    string                `json:"topics_error,omitempty"`
	// TopicsNotShown counts name-matched topics a scoped room does not see:
	// another Connect cluster's, or ones no shown group's name ties to.
	TopicsNotShown int                `json:"topics_not_shown,omitempty"`
	Findings       []string           `json:"findings,omitempty"`
	KafkaVersion   string             `json:"kafka_version,omitempty"`
	PluginCount    int                `json:"plugin_count"`
	PluginsByType  map[string]int     `json:"plugins_by_type,omitempty"`
	Plugins        []connectPluginRow `json:"plugins,omitempty"`
	PluginsNote    string             `json:"plugins_note,omitempty"`
	Loggers        []workerInfoLogger `json:"loggers,omitempty"`
	LoggersNote    string             `json:"loggers_note,omitempty"`
}

func (t *workerInfo) Run(ctx context.Context, _ json.RawMessage) (tools.Result, error) {
	sc, err := t.d.scope(ctx)
	if err != nil {
		return tools.Result{}, err
	}
	kr, ok := t.d.Kafka.(kafka.ConnectClusterReader)
	if !ok {
		return tools.Result{}, fmt.Errorf("kafkatools: connect_worker_info: %w", clients.ErrNotSupported)
	}
	var data workerInfoData

	gctx, cancel := withTimeout(ctx)
	groups, gErr := kr.ConnectWorkerGroups(gctx)
	cancel()
	if gErr != nil {
		data.GroupsError = "could not list Connect worker groups: " + noteErr(gErr)
	} else {
		data.Groups, data.OtherGroups = scopeWorkerGroups(groups, sc)
	}

	names, roles := t.d.ConnectInternalTopics, map[string]string{}
	data.TopicsSource = "name match"
	if len(names) > 0 {
		data.TopicsSource = "CONNECT_INTERNAL_TOPICS"
		roles = configuredRoles(names)
	}
	tctx, cancel := withTimeout(ctx)
	topics, tErr := kr.InternalTopics(tctx, names)
	cancel()
	if tErr != nil {
		data.TopicsError = "could not read the internal topics: " + noteErr(tErr)
	} else {
		for i := range topics {
			if r, ok := roles[topics[i].Name]; ok {
				topics[i].Role = r
			}
		}
		data.InternalTopics = topics
	}
	if len(names) == 0 && !sc.Unscoped() {
		data.InternalTopics, data.TopicsNotShown = scopeInternalTopics(data.InternalTopics, groups, data.Groups)
	}
	if gErr != nil && tErr != nil {
		return tools.Result{}, fmt.Errorf("kafkatools: connect_worker_info: %w", errors.Join(gErr, tErr))
	}
	if data.InternalTopics == nil {
		data.InternalTopics = []kafka.InternalTopic{}
	}
	data.Findings = internalTopicFindings(data.InternalTopics, data.TopicsSource, data.TopicsNotShown)

	t.plugins(ctx, &data)
	t.loggers(ctx, &data)
	return res(renderWorkerInfo(data), data), nil
}

// noteErr keeps an error short and printable for Data and the summary.
func noteErr(err error) string { return pluginText(errText(err)) }

// scopeWorkerGroups keeps the room's Connect groups: a group one of whose
// workers runs a pipeline connector, or a group id the pipeline declares.
// When no group qualifies and the cluster has exactly one, that one is the
// room's Connect cluster (a stopped or rebalancing connector has no
// assignment yet). Every other group is a count only, and inside a shown
// group other pipelines' connector names are a count too.
func scopeWorkerGroups(groups []kafka.ConnectWorkerGroup, sc tools.Scope) ([]workerInfoGroup, int) {
	own := func(name string) bool { return sc.CheckConnector(name) == nil }
	var shown []workerInfoGroup
	for _, g := range groups {
		why := ""
		switch {
		case sc.Unscoped():
			why = "unscoped"
		case sc.CheckReadGroup(g.GroupID) == nil:
			why = "declared group"
		case runsOwn(g, own):
			why = "runs the pipeline's connectors"
		}
		if why != "" {
			shown = append(shown, showGroup(g, own, why))
		}
	}
	if len(shown) == 0 && len(groups) == 1 {
		return []workerInfoGroup{showGroup(groups[0], own, "only Connect group in the cluster")}, 0
	}
	if shown == nil {
		shown = []workerInfoGroup{}
	}
	return shown, len(groups) - len(shown)
}

func runsOwn(g kafka.ConnectWorkerGroup, own func(string) bool) bool {
	for _, w := range g.Workers {
		for _, c := range w.Connectors {
			if own(c) {
				return true
			}
		}
	}
	return false
}

func showGroup(g kafka.ConnectWorkerGroup, own func(string) bool, why string) workerInfoGroup {
	out := workerInfoGroup{GroupID: pluginText(g.GroupID), State: pluginText(g.State), Error: pluginText(g.Error), Why: why, Workers: []workerInfoWorker{}}
	for _, w := range g.Workers {
		row := workerInfoWorker{MemberID: pluginText(w.MemberID), Host: pluginText(w.Host), URL: w.URL, Leader: w.Leader}
		for _, c := range w.Connectors {
			if own(c) {
				row.Connectors = append(row.Connectors, pluginText(c))
			} else {
				row.OtherConnectors++
			}
		}
		out.Workers = append(out.Workers, row)
	}
	return out
}

// scopeInternalTopics keeps the name-matched topics that belong to a shown
// group (kafka.AttributeInternalTopics, over every Connect group on the
// cluster so another cluster's topics are claimed by their own group) and
// counts the rest. Name matching finds every Connect cluster's storage
// topics, and their names name the other clusters; a topic no group can be
// tied to (a stopped cluster's, or a name that fits two groups) is hidden
// too, since it may be anyone's. With the groups unreadable nothing can be
// tied, so every topic is a count.
func scopeInternalTopics(topics []kafka.InternalTopic, groups []kafka.ConnectWorkerGroup, shown []workerInfoGroup) ([]kafka.InternalTopic, int) {
	names := make([]string, len(topics))
	for i, it := range topics {
		names[i] = it.Name
	}
	ids := make([]string, len(groups))
	for i, g := range groups {
		ids[i] = g.GroupID
	}
	owner := kafka.AttributeInternalTopics(names, ids)
	isShown := map[string]bool{}
	for _, g := range shown {
		isShown[g.GroupID] = true
	}
	kept := []kafka.InternalTopic{}
	for _, it := range topics {
		if g, ok := owner[it.Name]; ok && isShown[pluginText(g)] {
			kept = append(kept, it)
		}
	}
	return kept, len(topics) - len(kept)
}

// ParseInternalTopics splits CONNECT_INTERNAL_TOPICS: a comma list of the
// config, offset and status topics, in that order. Empty entries are dropped.
// The wiring (Q3e) passes the result as Deps.ConnectInternalTopics.
func ParseInternalTopics(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// configuredRoles gives the operator's list its roles by position (config,
// offset, status), which beats a name guess: the operator named them.
func configuredRoles(names []string) map[string]string {
	order := []string{kafka.InternalRoleConfig, kafka.InternalRoleOffset, kafka.InternalRoleStatus}
	roles := map[string]string{}
	for i, n := range names {
		if i < len(order) {
			roles[n] = order[i]
		}
	}
	return roles
}

// internalTopicFindings applies the three rules of the Q3a row, plus a
// configured topic that does not exist. Each finding names the topic and why
// Connect cares.
func internalTopicFindings(topics []kafka.InternalTopic, source string, notShown int) []string {
	var out []string
	switch {
	case len(topics) > 0 || source != "name match":
	case notShown > 0:
		out = append(out, "no Connect internal topic is tied to this room's worker group by name; set CONNECT_INTERNAL_TOPICS to name them")
	default:
		out = append(out, "no topic looks like a Connect internal topic (…-configs, …-offsets, …-status); set CONNECT_INTERNAL_TOPICS to name them")
	}
	for _, it := range topics {
		name := pluginText(it.Name)
		if it.Missing {
			out = append(out, fmt.Sprintf("%s (from %s) does not exist on this cluster", name, source))
			continue
		}
		if it.Role == kafka.InternalRoleConfig && it.Partitions > 1 {
			out = append(out, fmt.Sprintf("config topic %s has %d partitions; Connect requires exactly 1, or workers can read connector configs out of order", name, it.Partitions))
		}
		if it.CleanupPolicy != "" && !strings.Contains(it.CleanupPolicy, "compact") {
			out = append(out, fmt.Sprintf("%s has cleanup.policy=%s; Connect's internal topics must be compacted, or configs, offsets and status are deleted after retention", name, it.CleanupPolicy))
		}
		if it.ReplicationFactor == 1 && it.Brokers > 1 {
			out = append(out, fmt.Sprintf("%s has replication factor 1 on a %d-broker cluster; losing one broker loses it", name, it.Brokers))
		}
	}
	return out
}

// plugins fills the plugin section from the optional PluginReader (Q1c) and
// the Kafka version from ConnectInfoReader. A failure is a note, not an
// error: the Kafka-side reads already answer the main question.
func (t *workerInfo) plugins(ctx context.Context, data *workerInfoData) {
	reader, ok := t.d.Connect.(connect.PluginReader)
	if !ok {
		data.PluginsNote = "this Connect client does not list plugins"
		return
	}
	cctx, cancel := withTimeout(ctx)
	defer cancel()
	if info, ok := (&connectPlugins{d: t.d}).serverInfo(cctx); ok {
		data.KafkaVersion = info.Version
	}
	plugins, err := reader.Plugins(cctx)
	if err != nil {
		data.PluginsNote = "could not list plugins: " + noteErr(err)
		return
	}
	rows := pluginRows(plugins)
	data.PluginCount = len(rows)
	data.PluginsByType = cappedPluginCounts(rows)
	if len(rows) > workerInfoDataPlugins {
		rows = rows[:workerInfoDataPlugins]
	}
	data.Plugins = rows
	if connect.LegacyPlugins(plugins) {
		data.PluginsNote = "pre-3.2 Connect: connector plugins only"
	}
}

// loggers fills the log levels from the optional LoggerReader. The levels
// are those of the worker the Connect client points at.
func (t *workerInfo) loggers(ctx context.Context, data *workerInfoData) {
	reader, ok := t.d.Connect.(connect.LoggerReader)
	if !ok {
		data.LoggersNote = "this Connect client does not read GET /admin/loggers"
		return
	}
	cctx, cancel := withTimeout(ctx)
	defer cancel()
	ls, err := reader.Loggers(cctx)
	switch {
	case errors.Is(err, clients.ErrNotSupported):
		data.LoggersNote = "this Connect has no GET /admin/loggers (Kafka Connect 2.4+)"
		return
	case err != nil:
		data.LoggersNote = "could not read log levels: " + noteErr(err)
		return
	}
	for i, l := range ls {
		if i >= workerInfoDataLoggers {
			break
		}
		data.Loggers = append(data.Loggers, workerInfoLogger{Logger: pluginText(l.Logger), Level: pluginText(l.Level)})
	}
}

// renderWorkerInfo writes the summary: groups and workers, internal topics,
// findings, plugins, log levels, then the not-shown count. Every line built
// from untrusted text starts with a fixed token so it cannot pass for one of
// the tool's own lines.
func renderWorkerInfo(d workerInfoData) string {
	var sb strings.Builder
	switch {
	case d.GroupsError != "":
		appendLine(&sb, d.GroupsError)
	case len(d.Groups) == 0 && d.OtherGroups == 0:
		appendLine(&sb, "No Connect worker group (protocol type connect) on this Kafka cluster")
	case len(d.Groups) == 0:
		appendLine(&sb, "No Connect worker group runs this pipeline's connectors right now")
	default:
		appendLine(&sb, fmt.Sprintf("Connect worker groups: %d", len(d.Groups)))
	}
	for _, g := range d.Groups {
		line := fmt.Sprintf("- group %s (%s): %s", g.GroupID, orStateUnknown(g.State), tools.Plural(len(g.Workers), "1 worker", fmt.Sprintf("%d workers", len(g.Workers))))
		if g.Error != "" {
			line += "; describe failed: " + g.Error
		}
		appendLine(&sb, line+"; "+g.Why)
		for i, w := range g.Workers {
			if i >= workerInfoSummaryWorkers {
				appendLine(&sb, fmt.Sprintf("  - %d more workers not shown (cap)", len(g.Workers)-i))
				break
			}
			appendLine(&sb, "  - "+workerLine(w))
		}
	}
	appendLine(&sb, fmt.Sprintf("Internal topics (%s):", d.TopicsSource))
	if d.TopicsError != "" {
		appendLine(&sb, "- "+d.TopicsError)
	}
	for i, it := range d.InternalTopics {
		if i >= workerInfoSummaryTopics {
			appendLine(&sb, fmt.Sprintf("- %d more internal topics not shown (cap)", len(d.InternalTopics)-i))
			break
		}
		appendLine(&sb, "- "+topicLine(it))
	}
	if len(d.Findings) > 0 {
		appendLine(&sb, "Findings:")
		for _, f := range d.Findings {
			appendLine(&sb, "- "+f)
		}
	}
	appendLine(&sb, pluginsLine(d))
	appendLine(&sb, loggersLine(d))
	appendLine(&sb, tools.NotShown(d.OtherGroups, "Connect worker group", "Connect worker groups"))
	appendLine(&sb, tools.NotShown(d.TopicsNotShown, "Connect internal topic", "Connect internal topics"))
	return sb.String()
}

func orStateUnknown(s string) string {
	if s == "" {
		return "state unknown"
	}
	return s
}

func workerLine(w workerInfoWorker) string {
	line := "worker " + orDash(w.Host)
	if w.URL != "" {
		line += " " + w.URL
	} else {
		line += " (REST URL not advertised)"
	}
	if w.Leader {
		line += " leader"
	}
	if len(w.Connectors) > 0 {
		line += "; runs " + strings.Join(w.Connectors, ", ")
	}
	if w.OtherConnectors > 0 {
		line += fmt.Sprintf("; %d other connectors", w.OtherConnectors)
	}
	return line
}

func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

func topicLine(it kafka.InternalTopic) string {
	role := it.Role
	if role == "" {
		role = "role?"
	}
	if it.Missing {
		return fmt.Sprintf("%s %s: missing", role, pluginText(it.Name))
	}
	line := fmt.Sprintf("%s %s: %d partitions, RF %d, cleanup %s", role, pluginText(it.Name), it.Partitions, it.ReplicationFactor, orUnknownWord(it.CleanupPolicy))
	if it.MinInSyncReplicas > 0 {
		line += fmt.Sprintf(", min ISR %d", it.MinInSyncReplicas)
	}
	return line
}

func orUnknownWord(s string) string {
	if s == "" {
		return "unknown"
	}
	return s
}

func pluginsLine(d workerInfoData) string {
	var parts []string
	if d.KafkaVersion != "" {
		parts = append(parts, "Kafka "+d.KafkaVersion)
	}
	if d.PluginCount > 0 {
		parts = append(parts, fmt.Sprintf("%d plugins (%s)", d.PluginCount, pluginCountLine(d.PluginsByType)))
	}
	if d.PluginsNote != "" {
		parts = append(parts, d.PluginsNote)
	}
	if len(parts) == 0 {
		parts = append(parts, "no plugins listed")
	}
	return "Plugins: " + strings.Join(parts, "; ") + " (connect_plugins lists them)"
}

// loggersLine names root's level and the loggers set to a different level,
// the ones an operator changed.
func loggersLine(d workerInfoData) string {
	if d.LoggersNote != "" {
		return "Log levels: " + d.LoggersNote
	}
	if len(d.Loggers) == 0 {
		return "Log levels: none reported"
	}
	root := ""
	for _, l := range d.Loggers {
		if l.Logger == "root" {
			root = l.Level
		}
	}
	var changed []string
	for _, l := range d.Loggers {
		if l.Logger != "root" && l.Level != root {
			changed = append(changed, l.Level+" "+l.Logger)
		}
	}
	sort.Strings(changed)
	line := "Log levels: root " + orUnknownWord(root)
	if n := len(changed); n > workerInfoSummaryLoggers {
		changed = append(changed[:workerInfoSummaryLoggers], fmt.Sprintf("%d more", n-workerInfoSummaryLoggers))
	}
	if len(changed) > 0 {
		line += "; " + strings.Join(changed, ", ")
	}
	return line
}
