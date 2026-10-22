package kafkatools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// ---- connect_task_balance (X6b) ----

// Caps on what connect_task_balance lists. The balance itself is computed
// over every task; only the rows shown are bounded.
const (
	balanceMaxWorkers      = 64  // worker rows in Data
	balanceSummaryWorkers  = 12  // worker lines in the summary
	balanceMaxOwnTasks     = 500 // the pipeline's task rows in Data, all workers
	balanceSummaryOwnTasks = 8   // the pipeline's tasks named per worker line
	balanceMaxSinks        = 10  // sink connectors whose config and partitions are read
	balanceMaxSinkTopics   = 20  // topics per sink whose partitions are counted
)

// unassignedLabel names clients.UnassignedWorker in text.
const unassignedLabel = "(unassigned)"

type taskBalance struct{ d Deps }

func (t *taskBalance) Name() string     { return "connect_task_balance" }
func (t *taskBalance) Kind() tools.Kind { return tools.KindRead }
func (t *taskBalance) InputSchema() map[string]any {
	return objectSchema(map[string]any{})
}
func (t *taskBalance) Description() string {
	return "Shows how Kafka Connect spreads tasks over its workers: per worker seen, this pipeline's connector tasks by name and state " +
		"and every other connector's tasks as a count. Flags an uneven spread, unassigned tasks, and sink connectors whose tasks.max " +
		"is above their topics' partition count (idle tasks). Use when throughput or lag is uneven, after a worker restart or rebalance, " +
		"or before tuning tasks.max."
}

// balanceWorker is one worker of the result. Other pipelines' tasks are a
// count only.
type balanceWorker struct {
	Worker        string                   `json:"worker"` // "" = unassigned
	Tasks         int                      `json:"tasks"`
	PipelineTasks []clients.TaskAssignment `json:"pipeline_tasks,omitempty"`
	OtherTasks    int                      `json:"other_tasks"`
}

// balanceSink is one of the pipeline's sink connectors: tasks.max against the
// partitions of its topics.
type balanceSink struct {
	Connector  string `json:"connector"`
	TasksMax   int    `json:"tasks_max,omitempty"`  // 0 = unknown
	Tasks      int    `json:"tasks"`                // tasks in Connect's status
	Partitions int    `json:"partitions,omitempty"` // 0 = not counted
	IdleTasks  int    `json:"idle_tasks"`
	Note       string `json:"note,omitempty"`
}

type balanceReport struct {
	WorkersSeen    int             `json:"workers_seen"`
	Tasks          int             `json:"tasks"` // assigned to a worker
	Unassigned     int             `json:"unassigned"`
	MaxPerWorker   int             `json:"max_per_worker"`
	MinPerWorker   int             `json:"min_per_worker"`
	Imbalanced     bool            `json:"imbalanced"`
	Workers        []balanceWorker `json:"workers"`
	WorkersOmitted int             `json:"workers_omitted,omitempty"`
	OwnOmitted     int             `json:"pipeline_tasks_omitted,omitempty"`
	Sinks          []balanceSink   `json:"sinks,omitempty"`
	SinksOmitted   int             `json:"sinks_omitted,omitempty"` // over balanceMaxSinks, not read
	// Missing are the pipeline's connectors Connect reports no status for.
	Missing  []string `json:"missing_connectors,omitempty"`
	Findings []string `json:"findings,omitempty"`
}

func (t *taskBalance) Run(ctx context.Context, _ json.RawMessage) (tools.Result, error) {
	sc, err := t.d.scope(ctx)
	if err != nil {
		return tools.Result{}, err
	}
	if t.d.Connect == nil {
		return tools.Result{}, errors.New("kafkatools: connect_task_balance: Kafka Connect is not configured")
	}
	wr, ok := t.d.Connect.(clients.WorkerTaskReader)
	if !ok {
		return tools.Result{}, fmt.Errorf("kafkatools: connect_task_balance: %w", clients.ErrNotSupported)
	}
	cctx, cancel := withTimeout(ctx)
	defer cancel()
	assigned, err := wr.WorkerTasks(cctx)
	if errors.Is(err, clients.ErrNotSupported) {
		return res("Kafka Connect does not report task assignments for the whole cluster (GET /connectors?expand=status needs Connect 2.3+); use connect_status per connector.",
			map[string]any{"supported": false}), nil
	}
	if err != nil {
		return tools.Result{}, fmt.Errorf("kafkatools: connect_task_balance: %w", err)
	}
	own := func(name string) bool { return sc.CheckConnector(name) == nil }
	rep := balanceOf(assigned, own)
	if !sc.Unscoped() {
		rep.Missing = missingConnectors(assigned, sc.Pipeline().Connectors)
	}
	sinks := ownSinks(assigned, own)
	rep.Sinks = t.sinks(cctx, sc, sinks)
	rep.SinksOmitted = len(sinks) - len(rep.Sinks)
	rep.Findings = balanceFindings(rep)
	return res(renderBalance(rep), rep), nil
}

// balanceOf groups the cluster's tasks per worker. Tasks of a connector own
// rejects are counted and never named. Only assigned tasks count toward the
// spread; a worker with no task (it runs only a connector instance) counts
// as a worker with 0.
func balanceOf(assigned map[string][]clients.TaskAssignment, own func(string) bool) balanceReport {
	var rep balanceReport
	var rows []balanceWorker
	for w, ts := range assigned {
		row := balanceWorker{Worker: w, Tasks: len(ts)}
		for _, a := range ts {
			if own(a.Connector) {
				row.PipelineTasks = append(row.PipelineTasks, a)
			} else {
				row.OtherTasks++
			}
		}
		if w == clients.UnassignedWorker {
			rep.Unassigned = len(ts)
		} else {
			rep.WorkersSeen++
			rep.Tasks += len(ts)
		}
		rows = append(rows, row)
	}
	// Busiest first; the unassigned row last; then by id for a stable order.
	sort.Slice(rows, func(i, j int) bool {
		ui, uj := rows[i].Worker == clients.UnassignedWorker, rows[j].Worker == clients.UnassignedWorker
		if ui != uj {
			return uj
		}
		if rows[i].Tasks != rows[j].Tasks {
			return rows[i].Tasks > rows[j].Tasks
		}
		return rows[i].Worker < rows[j].Worker
	})
	first := true
	for _, r := range rows {
		if r.Worker == clients.UnassignedWorker {
			continue
		}
		if first || r.Tasks > rep.MaxPerWorker {
			rep.MaxPerWorker = r.Tasks
		}
		if first || r.Tasks < rep.MinPerWorker {
			rep.MinPerWorker = r.Tasks
		}
		first = false
	}
	// Connect's assignors aim at an even spread within one task per worker.
	rep.Imbalanced = rep.WorkersSeen >= 2 && rep.MaxPerWorker-rep.MinPerWorker > 1
	if len(rows) > balanceMaxWorkers {
		rep.WorkersOmitted = len(rows) - balanceMaxWorkers
		for _, r := range rows[balanceMaxWorkers:] {
			rep.OwnOmitted += len(r.PipelineTasks)
		}
		rows = rows[:balanceMaxWorkers]
	}
	left := balanceMaxOwnTasks
	for i := range rows {
		if n := len(rows[i].PipelineTasks); n > left {
			rep.OwnOmitted += n - left
			rows[i].PipelineTasks = rows[i].PipelineTasks[:left]
		}
		left -= len(rows[i].PipelineTasks)
	}
	rep.Workers = rows
	return rep
}

// missingConnectors lists the pipeline's connectors with no task and no
// connector instance in Connect's status. A connector instance alone does
// not show in WorkerTasks, so a connector with no task is listed too.
func missingConnectors(assigned map[string][]clients.TaskAssignment, connectors []string) []string {
	seen := map[string]bool{}
	for _, ts := range assigned {
		for _, a := range ts {
			seen[a.Connector] = true
		}
	}
	var out []string
	for _, c := range connectors {
		if !seen[c] {
			out = append(out, c)
		}
	}
	sort.Strings(out)
	return out
}

// ownSinks returns the pipeline's sink connectors and their task counts.
func ownSinks(assigned map[string][]clients.TaskAssignment, own func(string) bool) map[string]int {
	out := map[string]int{}
	for _, ts := range assigned {
		for _, a := range ts {
			if a.Type == "sink" && own(a.Connector) {
				out[a.Connector]++
			}
		}
	}
	return out
}

// sinks reads tasks.max and the topic list of each own sink connector (the
// two keys only; no other config value is kept) and counts the partitions
// of its topics that are the pipeline's.
func (t *taskBalance) sinks(ctx context.Context, sc tools.Scope, tasks map[string]int) []balanceSink {
	names := make([]string, 0, len(tasks))
	for n := range tasks {
		names = append(names, n)
	}
	sort.Strings(names)
	if len(names) > balanceMaxSinks {
		names = names[:balanceMaxSinks]
	}
	var out []balanceSink
	for _, n := range names {
		s := balanceSink{Connector: n, Tasks: tasks[n]}
		cfg, err := t.d.Connect.Config(ctx, n)
		if err != nil {
			s.Note = "config not read: " + errText(err)
			out = append(out, s)
			continue
		}
		if v, err := strconv.Atoi(strings.TrimSpace(cfg["tasks.max"])); err == nil && v > 0 {
			s.TasksMax = v
		}
		s.Partitions, s.Note = t.partitions(ctx, sc, cfg)
		// A sink connector usually starts tasks.max tasks; the tasks Connect
		// reports are what runs, so they decide when there are any.
		running := s.Tasks
		if running == 0 {
			running = s.TasksMax
		}
		if s.Partitions > 0 && running > s.Partitions {
			s.IdleTasks = running - s.Partitions
		}
		out = append(out, s)
	}
	return out
}

// partitions counts the partitions of a sink's topics. It returns 0 and a
// note when a topic cannot be counted: a topics.regex, a topic outside the
// pipeline (never named), no Kafka client or a read error.
func (t *taskBalance) partitions(ctx context.Context, sc tools.Scope, cfg map[string]string) (int, string) {
	var topics []string
	for _, tp := range strings.Split(cfg["topics"], ",") {
		if tp = strings.TrimSpace(tp); tp != "" {
			topics = append(topics, tp)
		}
	}
	switch {
	case len(topics) == 0 && strings.TrimSpace(cfg["topics.regex"]) != "":
		return 0, "topics.regex: partitions not counted"
	case len(topics) == 0:
		return 0, "no topics in config"
	case len(topics) > balanceMaxSinkTopics:
		return 0, fmt.Sprintf("%d topics: more than %d, partitions not counted", len(topics), balanceMaxSinkTopics)
	case t.d.Kafka == nil:
		return 0, "Kafka is not configured: partitions not counted"
	}
	outside := 0
	for _, tp := range topics {
		if !sc.TopicInScope(tp) {
			outside++
		}
	}
	if outside > 0 {
		return 0, fmt.Sprintf("reads %d %s outside this pipeline: partitions not counted", outside, tools.Plural(outside, "topic", "topics"))
	}
	total := 0
	for _, tp := range topics {
		parts, err := t.d.Kafka.TopicOffsets(ctx, tp)
		if err != nil {
			return 0, "partitions not read: " + errText(err)
		}
		total += len(parts)
	}
	return total, ""
}

// errText is an error for a note: one line, cut.
func errText(err error) string {
	s := strings.Join(strings.Fields(err.Error()), " ")
	if len(s) > 160 {
		s = s[:160] + "…"
	}
	return s
}

func balanceFindings(rep balanceReport) []string {
	var out []string
	if rep.Imbalanced {
		busiest := rep.Workers[0]
		out = append(out, fmt.Sprintf("imbalance: worker %s runs %d of %d tasks; %d workers seen hold %d to %d each (an even spread is about %d)",
			busiest.Worker, busiest.Tasks, rep.Tasks, rep.WorkersSeen, rep.MinPerWorker, rep.MaxPerWorker, (rep.Tasks+rep.WorkersSeen-1)/rep.WorkersSeen))
	}
	if rep.Unassigned > 0 {
		out = append(out, fmt.Sprintf("%d %s unassigned (a rebalance in progress, or no worker can run them)", rep.Unassigned, tools.Plural(rep.Unassigned, "task", "tasks")))
	}
	for _, s := range rep.Sinks {
		if s.IdleTasks > 0 {
			tm := "unset"
			if s.TasksMax > 0 {
				tm = strconv.Itoa(s.TasksMax)
			}
			out = append(out, fmt.Sprintf("sink %s: %d tasks (tasks.max %s) on %d partitions: %d %s idle (a partition is read by one task at a time)",
				s.Connector, s.IdleTasks+s.Partitions, tm, s.Partitions, s.IdleTasks, tools.Plural(s.IdleTasks, "task", "tasks")))
		}
	}
	if len(rep.Missing) > 0 {
		out = append(out, "no task in Connect's status for: "+strings.Join(rep.Missing, ", "))
	}
	return out
}

func renderBalance(rep balanceReport) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "Connect: %d %s seen, %d %s assigned", rep.WorkersSeen, tools.Plural(rep.WorkersSeen, "worker", "workers"), rep.Tasks, tools.Plural(rep.Tasks, "task", "tasks"))
	if rep.Unassigned > 0 {
		fmt.Fprintf(&sb, ", %d unassigned", rep.Unassigned)
	}
	sb.WriteString(" (a worker that runs nothing does not show).")
	if len(rep.Findings) == 0 {
		appendLine(&sb, "No imbalance, unassigned task or idle sink task found.")
	}
	for _, f := range rep.Findings {
		appendLine(&sb, "Finding: "+f)
	}
	for i, w := range rep.Workers {
		if i == balanceSummaryWorkers {
			appendLine(&sb, fmt.Sprintf("%d more workers in Data", len(rep.Workers)-i+rep.WorkersOmitted))
			break
		}
		name := w.Worker
		if name == clients.UnassignedWorker {
			name = unassignedLabel
		}
		line := fmt.Sprintf("%s: %d %s", name, w.Tasks, tools.Plural(w.Tasks, "task", "tasks"))
		var parts []string
		for j, a := range w.PipelineTasks {
			if j == balanceSummaryOwnTasks {
				parts = append(parts, fmt.Sprintf("%d more", len(w.PipelineTasks)-j))
				break
			}
			parts = append(parts, fmt.Sprintf("%s task %d %s", a.Connector, a.Task, a.State))
		}
		if len(parts) > 0 {
			line += "; this pipeline: " + strings.Join(parts, ", ")
		}
		if w.OtherTasks > 0 {
			line += fmt.Sprintf("; %d of other pipelines", w.OtherTasks)
		}
		appendLine(&sb, line)
	}
	for _, s := range rep.Sinks {
		if s.Note != "" {
			appendLine(&sb, fmt.Sprintf("sink %s: %s", s.Connector, s.Note))
		}
	}
	if rep.SinksOmitted > 0 {
		appendLine(&sb, fmt.Sprintf("%d more sink connectors not checked for idle tasks (at most %d per call)", rep.SinksOmitted, balanceMaxSinks))
	}
	return sb.String()
}
