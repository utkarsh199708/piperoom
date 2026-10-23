package kafkatools

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/tools"
)

// ---- connect_worker_info (Q3a) ----

// EnvConnectInternalTopics is the Q0 config variable that names the Connect
// internal topics when a cluster does not use Connect's default names
// (tasks/Q-board.md §3). config.Load does not carry it yet — this PR's contract
// change request asks for the field — so the tool falls back to the process
// environment; once the field lands, wiring passes it in Deps and the fallback
// comes out.
const EnvConnectInternalTopics = "CONNECT_INTERNAL_TOPICS"

// Caps on what connect_worker_info shows. Every name in it is untrusted
// cluster text, and a synthetic answer can hold thousands of groups, topics or
// loggers.
const (
	maxWorkerGroupRows   = 50
	maxWorkersPerGroup   = 50
	maxWorkerRows        = 200
	maxInternalTopicRows = 50
	maxLoggerRows        = 200
	maxLoggerLevelKeys   = 16
	maxNonInfoLoggers    = 10
	// workerSummaryBudget leaves room for the closing notes inside
	// tools.MaxSummaryBytes.
	workerSummaryBudget = 6000
	// maxTopicNameBytes is Kafka's own topic name limit.
	maxTopicNameBytes = 249
)

// connectInternalTopicSuffixes are Connect's default internal topic suffixes,
// appended to the worker group id.
var connectInternalTopicSuffixes = []string{"-configs", "-offsets", "-status"}

type connectWorkerInfo struct{ d Deps }

func (t *connectWorkerInfo) Name() string     { return "connect_worker_info" }
func (t *connectWorkerInfo) Kind() tools.Kind { return tools.KindRead }
func (t *connectWorkerInfo) InputSchema() map[string]any {
	return objectSchema(map[string]any{})
}
func (t *connectWorkerInfo) Description() string {
	return "Kafka Connect cluster view: the worker groups (groups whose protocol type is connect) and the workers in each, " +
		"with the REST URL each worker advertises; the internal topics (-configs, -offsets, -status) with partition count, replication factor, cleanup.policy and min.insync.replicas; " +
		"the worker's plugin inventory and logger levels. " +
		"Flags a config topic with more than one partition, an internal topic that is not compacted, and replication factor 1 on a multi-broker cluster, " +
		"and names an expected internal topic that does not exist."
}

func (t *connectWorkerInfo) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	if _, err := parse[struct{}](input); err != nil {
		return tools.Result{}, err
	}
	// The worker set is cluster-wide and holds no customer row, but the tool
	// stays scoped like every other read tool: a room call always has a
	// pipeline, and CLI/dev callers opt out explicitly.
	if _, err := t.d.scope(ctx); err != nil {
		return tools.Result{}, err
	}
	reader, ok := t.d.Kafka.(kafka.ConnectClusterReader)
	if !ok {
		return tools.Result{}, fmt.Errorf("%w: this Kafka client cannot read Connect worker groups", clients.ErrNotSupported)
	}

	cctx, cancel := withTimeout(ctx)
	defer cancel()

	groups, err := reader.ConnectWorkerGroups(cctx)
	if err != nil {
		return tools.Result{}, fmt.Errorf("kafkatools: connect_worker_info: %w", err)
	}
	requested, ignoredConfig, requestedOmitted := t.internalTopicNames(groups)
	var topics []kafka.InternalTopic
	if len(requested) > 0 {
		topics, err = reader.InternalTopics(cctx, requested)
		if err != nil {
			return tools.Result{}, fmt.Errorf("kafkatools: connect_worker_info: %w", err)
		}
	}
	// The broker count must be known before the flags are computed: it alone
	// decides the replication-factor-1 flag.
	brokers, brokerNote := t.brokerCount(cctx, topics)
	rep := newWorkerInfoReport(groups, topics, requested, brokers)
	rep.ignoredConfig, rep.requestedOmitted = ignoredConfig, requestedOmitted
	rep.brokerNote = brokerNote
	rep.pluginRows, rep.pluginNote = t.plugins(cctx)
	rep.pluginCounts = cappedPluginCounts(rep.pluginRows)
	if len(rep.pluginRows) > maxPluginRows {
		rep.pluginsOmitted = len(rep.pluginRows) - maxPluginRows
	}
	levels, levelNote := t.loggerLevels(cctx)
	rep.levelNote = levelNote
	rep.setLevels(levels)

	summary, cut := rep.summary()
	r := res(summary, rep.data())
	if cut || rep.truncated() {
		r.Truncated = true
	}
	return r, nil
}

// internalTopicNames returns the internal topics to read: the configured list
// (Deps first, then CONNECT_INTERNAL_TOPICS), else the default suffixes of
// every Connect worker group. Names that are not valid Kafka topic names are
// dropped and counted, so a typo degrades to "missing" rather than a broker
// error. The list is capped: a cluster with thousands of groups would
// otherwise turn one tool call into thousands of topic reads.
func (t *connectWorkerInfo) internalTopicNames(groups []kafka.ConnectWorkerGroup) (names []string, ignored, omitted int) {
	configured := t.d.ConnectInternalTopics
	if configured == nil {
		configured = splitInternalTopics(os.Getenv(EnvConnectInternalTopics))
	}
	if len(configured) > 0 {
		for _, name := range configured {
			if !validTopicName(name) {
				ignored++
				continue
			}
			if len(names) >= maxInternalTopicRows {
				omitted++
				continue
			}
			names = append(names, name)
		}
		return names, ignored, omitted
	}
	seen := map[string]bool{}
	for _, g := range groups {
		if g.GroupID == "" {
			continue
		}
		for _, suffix := range connectInternalTopicSuffixes {
			name := g.GroupID + suffix
			if seen[name] || !validTopicName(name) {
				continue
			}
			seen[name] = true
			if len(names) >= maxInternalTopicRows {
				omitted++
				continue
			}
			names = append(names, name)
		}
	}
	sort.Strings(names)
	return names, 0, omitted
}

// splitInternalTopics parses a comma-separated list: empty entries are
// dropped and order is kept, so the operator's order is what gets read.
func splitInternalTopics(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// validTopicName accepts a Kafka topic name: 1 to 249 bytes of [a-zA-Z0-9._-],
// and not the "." or ".." names Kafka reserves.
func validTopicName(name string) bool {
	if name == "" || len(name) > maxTopicNameBytes || name == "." || name == ".." {
		return false
	}
	for i := 0; i < len(name); i++ {
		c := name[i]
		if (c < 'a' || c > 'z') && (c < 'A' || c > 'Z') && (c < '0' || c > '9') && c != '.' && c != '_' && c != '-' {
			return false
		}
	}
	return true
}

// brokerCount reads the broker count, but only when a topic with replication
// factor 1 is present: that is the only flag it decides. A client without the
// optional read, or a failed read, leaves the count unknown with a note the
// summary shows — never an error, because the rest of the answer stands.
func (t *connectWorkerInfo) brokerCount(ctx context.Context, topics []kafka.InternalTopic) (int, string) {
	need := false
	for _, it := range topics {
		if it.ReplicationFactor == 1 {
			need = true
			break
		}
	}
	if !need {
		return 0, ""
	}
	bc, ok := t.d.Kafka.(kafka.BrokerCounter)
	if !ok {
		return 0, "broker count unknown (this Kafka client does not report it): the replication-factor-1 check is skipped"
	}
	n, err := bc.BrokerCount(ctx)
	if err != nil {
		return 0, "broker count unknown (the broker metadata read failed): the replication-factor-1 check is skipped"
	}
	return n, ""
}

// plugins reads the worker's plugin inventory when the Connect client has the
// optional PluginReader. A failed read is a note, not an error: the worker
// groups and topics still answer the question.
func (t *connectWorkerInfo) plugins(ctx context.Context) ([]connectPluginRow, string) {
	r, ok := t.d.Connect.(connect.PluginReader)
	if !ok {
		return nil, "plugins: not available (this Connect client cannot list plugins)"
	}
	plugins, err := r.Plugins(ctx)
	if err != nil {
		return nil, "plugins: unavailable (the worker's plugin endpoint did not answer)"
	}
	return pluginRows(plugins), ""
}

// loggerLevels reads the worker's logger levels when the Connect client has
// the optional LoggerLevelReader; the same degrade rule as plugins applies.
func (t *connectWorkerInfo) loggerLevels(ctx context.Context) (map[string]string, string) {
	r, ok := t.d.Connect.(connect.LoggerLevelReader)
	if !ok {
		return nil, "logger levels: not available (this Connect client cannot read /admin/loggers)"
	}
	levels, err := r.LoggerLevels(ctx)
	if err != nil {
		return nil, "logger levels: unavailable (the worker's /admin/loggers did not answer)"
	}
	return levels, ""
}

// ---- report ----

// workerInfoReport holds the sanitized sections connect_worker_info renders.
type workerInfoReport struct {
	groups         []workerGroupRow
	groupCount     int
	groupsOmitted  int
	workersOmitted int

	topics        []internalTopicRow
	requested     []string
	missing       []string
	topicsOmitted int

	brokers    int
	brokerNote string
	flags      []string

	pluginRows     []connectPluginRow
	pluginCounts   map[string]int
	pluginsOmitted int
	pluginNote     string

	loggers      map[string]string
	loggerCount  int
	loggerCounts map[string]int
	levelNote    string

	ignoredConfig    int
	requestedOmitted int
}

// workerGroupRow is one Connect group in Data.
type workerGroupRow struct {
	GroupID string      `json:"group_id"`
	State   string      `json:"state,omitempty"`
	Workers []workerRow `json:"workers"`
	// Omitted counts this group's workers past the caps; GroupCount still
	// counts every group.
	Omitted int `json:"omitted,omitempty"`
}

// workerRow is one Connect worker in Data. MemberID, Host and URL are
// sanitized (printable, capped) because they are untrusted cluster text.
type workerRow struct {
	MemberID string `json:"member_id"`
	Host     string `json:"host,omitempty"`
	URL      string `json:"url,omitempty"`
}

// internalTopicRow is one internal topic in Data. CleanupPolicy is untrusted
// topic config text, so it is sanitized too.
type internalTopicRow struct {
	Name              string `json:"name"`
	Role              string `json:"role,omitempty"`
	Partitions        int    `json:"partitions"`
	ReplicationFactor int    `json:"replication_factor"`
	CleanupPolicy     string `json:"cleanup_policy,omitempty"`
	MinInSyncReplicas int    `json:"min_insync_replicas,omitempty"`
}

// connectWorkerInfoData is the structured Data of connect_worker_info.
type connectWorkerInfoData struct {
	GroupCount     int              `json:"group_count"`
	GroupsOmitted  int              `json:"groups_omitted,omitempty"`
	WorkersOmitted int              `json:"workers_omitted,omitempty"`
	Groups         []workerGroupRow `json:"groups"`

	InternalTopics []internalTopicRow `json:"internal_topics"`
	TopicsOmitted  int                `json:"topics_omitted,omitempty"`
	TopicsNotRead  int                `json:"topics_not_read,omitempty"`
	MissingTopics  []string           `json:"missing_topics,omitempty"`
	IgnoredConfig  int                `json:"ignored_config_names,omitempty"`
	// BrokerCount is 0 when it was not needed (no topic has replication
	// factor 1) or could not be read; Flags say which flags the gap skipped.
	BrokerCount int      `json:"broker_count,omitempty"`
	Flags       []string `json:"flags"`

	PluginCount    int                `json:"plugin_count,omitempty"`
	PluginsByType  map[string]int     `json:"plugins_by_type,omitempty"`
	Plugins        []connectPluginRow `json:"plugins,omitempty"`
	PluginsOmitted int                `json:"plugins_omitted,omitempty"`
	LoggerCount    int                `json:"logger_count,omitempty"`
	LoggerLevels   map[string]int     `json:"logger_levels,omitempty"`
	Loggers        map[string]string  `json:"loggers,omitempty"`
}

// newWorkerInfoReport sanitizes the client's answer and computes the flags.
func newWorkerInfoReport(groups []kafka.ConnectWorkerGroup, topics []kafka.InternalTopic, requested []string, brokers int) *workerInfoReport {
	rep := &workerInfoReport{groupCount: len(groups), requested: requested, brokers: brokers}
	rep.groups, rep.groupsOmitted, rep.workersOmitted = workerGroupRows(groups)
	rep.topics, rep.topicsOmitted = internalTopicRows(topics)
	rep.flags, rep.missing = workerInfoFlags(rep.topics, requested, brokers)
	return rep
}

// workerGroupRows sanitizes the groups and applies the row caps; the returned
// counts say how much the caps cut.
func workerGroupRows(groups []kafka.ConnectWorkerGroup) ([]workerGroupRow, int, int) {
	rows := make([]workerGroupRow, 0, min(len(groups), maxWorkerGroupRows))
	groupsOmitted, workersOmitted, workersLeft := 0, 0, maxWorkerRows
	for i, g := range groups {
		if i >= maxWorkerGroupRows {
			groupsOmitted = len(groups) - i
			break
		}
		row := workerGroupRow{GroupID: pluginText(g.GroupID), State: pluginText(g.State)}
		for j, w := range g.Workers {
			if workersLeft <= 0 || j >= maxWorkersPerGroup {
				row.Omitted = len(g.Workers) - j
				workersOmitted += row.Omitted
				break
			}
			row.Workers = append(row.Workers, workerRow{
				MemberID: pluginText(w.MemberID),
				Host:     pluginText(w.Host),
				URL:      pluginText(w.URL),
			})
			workersLeft--
		}
		if row.Workers == nil {
			row.Workers = []workerRow{}
		}
		rows = append(rows, row)
	}
	// The client sorts, but the tool must not depend on it: sort here so two
	// calls over the same cluster render the same summary.
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].GroupID < rows[j].GroupID })
	for i := range rows {
		sort.SliceStable(rows[i].Workers, func(a, b int) bool {
			return rows[i].Workers[a].MemberID < rows[i].Workers[b].MemberID
		})
	}
	return rows, groupsOmitted, workersOmitted
}

// internalTopicRows sanitizes the topics and orders them config, offset,
// status, then by name, so two calls over the same cluster render the same
// summary.
func internalTopicRows(topics []kafka.InternalTopic) ([]internalTopicRow, int) {
	rows := make([]internalTopicRow, 0, min(len(topics), maxInternalTopicRows))
	for _, it := range topics {
		if len(rows) >= maxInternalTopicRows {
			break
		}
		rows = append(rows, internalTopicRow{
			Name:              pluginText(it.Name),
			Role:              pluginText(it.Role),
			Partitions:        it.Partitions,
			ReplicationFactor: it.ReplicationFactor,
			CleanupPolicy:     pluginText(it.CleanupPolicy),
			MinInSyncReplicas: it.MinInSyncReplicas,
		})
	}
	sort.SliceStable(rows, func(i, j int) bool {
		ri, rj := topicRoleRank(rows[i].Role), topicRoleRank(rows[j].Role)
		if ri != rj {
			return ri < rj
		}
		return rows[i].Name < rows[j].Name
	})
	return rows, len(topics) - len(rows)
}

// topicRoleRank orders roles as config, offset, status, then an unnamed role.
func topicRoleRank(role string) int {
	switch role {
	case "config":
		return 0
	case "offset":
		return 1
	case "status":
		return 2
	}
	return 3
}

// workerInfoFlags names what is wrong with the internal topics. It returns
// the flags and the requested names the cluster does not have.
func workerInfoFlags(topics []internalTopicRow, requested []string, brokers int) ([]string, []string) {
	flags := []string{}
	found := make(map[string]bool, len(topics))
	for _, it := range topics {
		found[it.Name] = true
		label := it.Name
		if it.Role != "" {
			label += " (" + it.Role + ")"
		}
		if it.Role == "config" && it.Partitions > 1 {
			flags = append(flags, fmt.Sprintf("%s has %d partitions; a Connect config topic must have exactly 1", label, it.Partitions))
		}
		if !hasCompactPolicy(it.CleanupPolicy) {
			if it.CleanupPolicy == "" {
				flags = append(flags, label+" reports no cleanup.policy; Connect internal topics need compact")
			} else {
				flags = append(flags, fmt.Sprintf("%s cleanup.policy is %q; Connect internal topics need compact", label, it.CleanupPolicy))
			}
		}
		if it.ReplicationFactor == 1 && brokers > 1 {
			flags = append(flags, fmt.Sprintf("%s has replication factor 1 on a %d-broker cluster", label, brokers))
		}
	}
	missing := []string{}
	for _, name := range requested {
		clean := pluginText(name)
		if !found[clean] {
			missing = append(missing, clean)
			flags = append(flags, clean+" does not exist (Connect creates its internal topics on first start)")
		}
	}
	return flags, missing
}

// hasCompactPolicy reports whether a cleanup.policy keeps compaction on; a
// "compact,delete" topic still compacts, so any compact token counts.
func hasCompactPolicy(policy string) bool {
	for _, part := range strings.Split(policy, ",") {
		if strings.EqualFold(strings.TrimSpace(part), "compact") {
			return true
		}
	}
	return false
}

// setLevels sanitizes the logger levels and counts them per level, capping
// both the logger rows and the distinct level keys (both are untrusted text).
func (r *workerInfoReport) setLevels(levels map[string]string) {
	r.loggerCount = len(levels)
	if len(levels) == 0 {
		return
	}
	names := make([]string, 0, len(levels))
	for name := range levels {
		names = append(names, name)
	}
	sort.Strings(names)
	r.loggers = make(map[string]string, min(len(names), maxLoggerRows))
	r.loggerCounts = map[string]int{}
	for _, name := range names {
		if level := pluginText(levels[name]); level != "" {
			r.loggerCounts[level]++
		} else {
			r.loggerCounts["(none)"]++
		}
	}
	// The rows keep every logger off INFO first: those are the ones a summary
	// line or a reader looks for (DEBUG explains a flood, WARN/ERROR a silent
	// one). INFO rows fill the rest of the cap.
	for _, wantInfo := range []bool{false, true} {
		for _, name := range names {
			if len(r.loggers) >= maxLoggerRows {
				break
			}
			level := pluginText(levels[name])
			if level == "" {
				level = "(none)"
			}
			if strings.EqualFold(level, "INFO") != wantInfo {
				continue
			}
			r.loggers[pluginText(name)] = level
		}
	}
	capLevelKeys(r.loggerCounts)
}

// capLevelKeys keeps at most maxLoggerLevelKeys distinct level keys: a hostile
// worker could otherwise mint one level per logger. The alphabetically first
// keys stay (the real levels among them), everything past the cap sums under
// "other".
func capLevelKeys(counts map[string]int) {
	if len(counts) <= maxLoggerLevelKeys {
		return
	}
	keys := make([]string, 0, len(counts))
	for level := range counts {
		keys = append(keys, level)
	}
	sort.Strings(keys)
	capped := make(map[string]int, maxLoggerLevelKeys)
	for i, level := range keys {
		if i < maxLoggerLevelKeys-1 {
			capped[level] = counts[level]
			continue
		}
		capped["other"] += counts[level]
	}
	for level := range counts {
		delete(counts, level)
	}
	for level, n := range capped {
		counts[level] = n
	}
}

// data is the structured answer.
func (r *workerInfoReport) data() connectWorkerInfoData {
	d := connectWorkerInfoData{
		GroupCount:     r.groupCount,
		GroupsOmitted:  r.groupsOmitted,
		WorkersOmitted: r.workersOmitted,
		Groups:         r.groups,
		InternalTopics: r.topics,
		TopicsOmitted:  r.topicsOmitted,
		TopicsNotRead:  r.requestedOmitted,
		MissingTopics:  r.missing,
		IgnoredConfig:  r.ignoredConfig,
		BrokerCount:    r.brokers,
		Flags:          r.flags,
		LoggerCount:    r.loggerCount,
		LoggerLevels:   r.loggerCounts,
		Loggers:        r.loggers,
	}
	if r.pluginRows != nil {
		d.PluginCount = len(r.pluginRows)
		d.PluginsByType = cappedPluginCounts(r.pluginRows)
		if len(r.pluginRows) > maxPluginRows {
			d.PluginsOmitted = len(r.pluginRows) - maxPluginRows
			d.Plugins = r.pluginRows[:maxPluginRows]
		} else {
			d.Plugins = r.pluginRows
		}
	}
	return d
}

// truncated reports whether a cap cut something the summary cannot mention.
func (r *workerInfoReport) truncated() bool {
	return r.groupsOmitted > 0 || r.workersOmitted > 0 || r.topicsOmitted > 0 ||
		r.pluginsOmitted > 0 || r.requestedOmitted > 0
}

// summary renders the report within workerSummaryBudget. It reports whether
// the budget cut a line.
func (r *workerInfoReport) summary() (string, bool) {
	var sb strings.Builder
	cut := false
	add := func(line string) bool {
		if line == "" {
			return true
		}
		if sb.Len()+len(line)+1 > workerSummaryBudget {
			cut = true
			return false
		}
		appendLine(&sb, line)
		return true
	}

	if len(r.groups) == 0 {
		add("no Connect worker groups found (no group in this cluster has protocol type connect)")
	} else {
		add(plural(r.groupCount, "Connect worker group"))
		for _, g := range r.groups {
			state := g.State
			if state == "" {
				state = "state unknown"
			}
			line := "- " + g.GroupID + ": " + state
			if len(g.Workers) == 0 {
				line += ", no workers"
			} else {
				parts := make([]string, 0, len(g.Workers))
				for _, w := range g.Workers {
					parts = append(parts, workerLine(w))
				}
				line += ", " + plural(len(g.Workers), "worker") + ": " + strings.Join(parts, ", ")
			}
			if g.Omitted > 0 {
				line += fmt.Sprintf(" (%d more workers not shown)", g.Omitted)
			}
			if !add(line) {
				break
			}
		}
		if r.groupsOmitted > 0 {
			add(fmt.Sprintf("%d more groups not shown (cap)", r.groupsOmitted))
		}
	}

	switch {
	case len(r.requested) == 0:
		add("internal topics: none (no connect group names one and " + EnvConnectInternalTopics + " is not set)")
	case len(r.topics) == 0:
		add(fmt.Sprintf("internal topics: none of the %d expected topics exist (Connect creates them on first start)", len(r.requested)))
	default:
		add(fmt.Sprintf("internal topics: %d of %d found", len(r.topics), len(r.requested)))
		for _, it := range r.topics {
			add(internalTopicLine(it))
		}
	}
	if r.topicsOmitted > 0 {
		add(fmt.Sprintf("%d more internal topics not shown (cap)", r.topicsOmitted))
	}
	if r.requestedOmitted > 0 {
		add(fmt.Sprintf("%d more expected internal topics not read (cap)", r.requestedOmitted))
	}
	if len(r.missing) > 0 {
		add("missing internal topics: " + strings.Join(r.missing, ", "))
	}
	if r.ignoredConfig > 0 {
		add(fmt.Sprintf("%d configured topic names ignored (not valid Kafka topic names)", r.ignoredConfig))
	}

	if len(r.flags) == 0 {
		add("flags: none")
	} else {
		add("flags:")
		for _, f := range r.flags {
			if !add("- " + f) {
				break
			}
		}
	}
	if r.brokerNote != "" {
		add(r.brokerNote)
	}

	switch {
	case r.pluginNote != "":
		add(r.pluginNote)
	case len(r.pluginRows) == 0:
		add("plugins: none reported by the worker")
	default:
		add(fmt.Sprintf("plugins: %d (%s)", len(r.pluginRows), pluginCountLine(r.pluginCounts)))
	}
	if r.pluginsOmitted > 0 {
		add(fmt.Sprintf("%d plugins not shown (cap)", r.pluginsOmitted))
	}

	switch {
	case r.levelNote != "":
		add(r.levelNote)
	case r.loggerCount == 0:
		add("logger levels: the worker reports no configured logger")
	default:
		add("logger levels: " + loggerLevelLine(r.loggerCounts) + loggerOffInfo(r.loggers))
	}
	return sb.String(), cut
}

// workerLine renders one worker as "member (host) url", omitting what is
// missing.
func workerLine(w workerRow) string {
	line := w.MemberID
	if line == "" {
		line = "(unknown member)"
	}
	if w.Host != "" {
		line += " (" + w.Host + ")"
	}
	if w.URL != "" {
		line += " " + w.URL
	}
	return line
}

// internalTopicLine renders one internal topic's shape and both configs.
func internalTopicLine(it internalTopicRow) string {
	line := "- " + it.Name
	if it.Role != "" {
		line += " (" + it.Role + ")"
	}
	line += fmt.Sprintf(": %d partitions, RF %d", it.Partitions, it.ReplicationFactor)
	if it.CleanupPolicy == "" {
		line += ", cleanup.policy unknown"
	} else {
		line += ", cleanup.policy " + it.CleanupPolicy
	}
	if it.MinInSyncReplicas > 0 {
		line += fmt.Sprintf(", min.insync.replicas %d", it.MinInSyncReplicas)
	}
	return line
}

// loggerLevelLine renders "140 loggers (138 INFO, 2 DEBUG)", levels sorted.
func loggerLevelLine(counts map[string]int) string {
	total := 0
	levels := make([]string, 0, len(counts))
	for level, n := range counts {
		total += n
		levels = append(levels, level)
	}
	sort.Strings(levels)
	parts := make([]string, 0, len(levels))
	for _, level := range levels {
		parts = append(parts, fmt.Sprintf("%d %s", counts[level], level))
	}
	return fmt.Sprintf("%d loggers (%s)", total, strings.Join(parts, ", "))
}

// loggerOffInfo names up to maxNonInfoLoggers loggers whose level is not
// INFO, sorted: DEBUG or TRACE explains a flood of log lines, WARN or ERROR a
// silenced one.
func loggerOffInfo(loggers map[string]string) string {
	var off []string
	for name, level := range loggers {
		if !strings.EqualFold(level, "INFO") && level != "(none)" {
			off = append(off, name+"="+level)
		}
	}
	if len(off) == 0 {
		return ""
	}
	sort.Strings(off)
	shown := off
	if len(shown) > maxNonInfoLoggers {
		shown = shown[:maxNonInfoLoggers]
	}
	out := "; not INFO: " + strings.Join(shown, ", ")
	if rest := len(off) - len(shown); rest > 0 {
		out += fmt.Sprintf(" (%d more in data)", rest)
	}
	return out
}
