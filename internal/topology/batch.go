package topology

import (
	"context"
	"errors"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
)

// Batch pipelines (tasks/P4-pipeline-kinds-batch.md, P4a): the map is
// DAG -> task -> model -> sink_table, drawn from the orchestrator reader (DAG
// runs and the latest run's task instances), the transform reader (model
// lineage and the latest run results) and the warehouse (sink freshness).
//
// What is proven and what is not:
//   - dag -> task is mapped: the tasks are the latest run's task instances;
//   - parent -> child model is mapped: the manifest's lineage;
//   - task -> model, dag -> model and model -> sink are inferred: neither
//     reader says which task runs which model or which model loads which
//     table, so they come from names (a task id segment "<model>" or
//     "<model>_run"; a sink table named like the model) or, failing a name,
//     from owners.yaml putting both ends in one pipeline. They are marked
//     (unconfirmed), never drawn as proven.
//
// A reader the pipeline needs (the orchestrator when it lists dags, the
// transform reader when it lists models) that is nil or answers
// clients.ErrNotSupported leaves its nodes out and sets Graph.Partial. An
// unreachable reader still draws the owners.yaml nodes, with HealthUnknown.

// batchRunsLimit is how many recent runs of a DAG the builder reads: the
// newest gives the DAG's state and its tasks, the rest its cadence.
const batchRunsLimit = 5

// lineageReads caps the ModelLineage calls one Build makes at once.
const lineageReads = 4

// WithOrchestrator gives the builder the orchestrator reader (Airflow) that
// draws a batch pipeline's dag and task nodes. If it also implements
// clients.DAGScheduleReader, a paused DAG warns.
func WithOrchestrator(r clients.OrchestratorReader) Option {
	return func(b *builder) { b.orch = r }
}

// WithTransform gives the builder the transform reader (dbt) that draws a
// batch pipeline's model nodes.
func WithTransform(r clients.TransformReader) Option {
	return func(b *builder) { b.transform = r }
}

type dagSnapshot struct {
	runs     []clients.DAGRun // newest first
	err      error
	tasks    []clients.TaskInstance // of runs[0]
	tasksErr error
	paused   bool
}

type lineageSnapshot struct {
	l   clients.Lineage
	err error
}

// batchCollected holds everything the batch health phase gathers.
type batchCollected struct {
	dags       map[string]dagSnapshot
	lineage    map[string]lineageSnapshot
	results    clients.RunResults
	resultsErr error
	freshness  map[string]freshSnapshot
	now        time.Time
}

// collectBatch reads every reader in parallel under ctx's budget. It calls
// only the readers the pipeline needs, and never a nil one.
func (b *builder) collectBatch(ctx context.Context, p owners.Pipeline) *batchCollected {
	c := &batchCollected{
		dags:      map[string]dagSnapshot{},
		lineage:   map[string]lineageSnapshot{},
		freshness: map[string]freshSnapshot{},
		now:       b.now(),
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	run := func(f func()) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f()
		}()
	}
	if b.orch != nil {
		sched, _ := b.orch.(clients.DAGScheduleReader)
		for _, d := range p.DAGs {
			run(func() {
				s := b.readDAG(ctx, d, sched)
				mu.Lock()
				c.dags[d] = s
				mu.Unlock()
			})
		}
	}
	if b.transform != nil && len(p.Models) > 0 {
		run(func() {
			rr, err := b.transform.RunResults(ctx)
			mu.Lock()
			c.results, c.resultsErr = rr, err
			mu.Unlock()
		})
		// A dbt artifacts reader parses the whole manifest (up to 16 MB) on
		// every call, so a pipeline with many models reads a few at a time.
		sem := make(chan struct{}, lineageReads)
		for _, m := range p.Models {
			name, ok := owners.ModelName(m)
			if !ok {
				continue // a pipeline built in code with an invalid entry
			}
			run(func() {
				var l clients.Lineage
				var err error
				select {
				case sem <- struct{}{}:
					// Read by the entry as written: a unique_id pins the read
					// to its package, so a same-named model in another package
					// does not widen the lineage.
					l, err = b.transform.ModelLineage(ctx, m)
					<-sem
				case <-ctx.Done():
					err = ctx.Err()
				}
				mu.Lock()
				c.lineage[name] = lineageSnapshot{l: l, err: err}
				mu.Unlock()
			})
		}
	}
	if b.wh != nil {
		for _, s := range p.SinkTables {
			run(func() {
				ts, err := b.wh.Freshness(ctx, s, p.SinkTSColumn)
				mu.Lock()
				c.freshness[s] = freshSnapshot{ts: ts, err: err}
				mu.Unlock()
			})
		}
	}
	wg.Wait()
	return c
}

// readDAG reads one DAG's recent runs, whether it is paused (when the reader
// can tell) and the newest run's task instances.
func (b *builder) readDAG(ctx context.Context, dagID string, sched clients.DAGScheduleReader) dagSnapshot {
	var s dagSnapshot
	s.runs, s.err = b.orch.DAGRuns(ctx, dagID, batchRunsLimit)
	if s.err != nil {
		return s
	}
	if sched != nil {
		// The schedule only adds a fact; without it the DAG node stands.
		if d, err := sched.DAG(ctx, dagID); err == nil {
			s.paused = d.Paused
		}
	}
	if len(s.runs) > 0 {
		s.tasks, s.tasksErr = b.orch.TaskInstances(ctx, dagID, s.runs[0].ID)
	}
	return s
}

// buildBatch returns the graph of a batch pipeline. Like Build it never
// fails: what a reader cannot answer is unknown health or a Partial graph.
func (b *builder) buildBatch(ctx context.Context, p owners.Pipeline) Graph {
	cctx, cancel := context.WithTimeout(ctx, b.t.HealthBudget)
	defer cancel()
	info := b.collectBatch(cctx, p)

	g := Graph{PipelineID: p.ID}
	// A reader is needed only for what the pipeline lists: a models-only
	// pipeline is complete without an orchestrator.
	g.Partial = (len(p.DAGs) > 0 && b.orch == nil) || (len(p.Models) > 0 && b.transform == nil)

	type taskRef struct{ id, taskID string }
	var dagIDs []string
	var tasks []taskRef
	var cadence time.Duration
	for _, d := range p.DAGs {
		snap, ok := info.dags[d]
		// owners.yaml checks the alphabet, not the length: an id the
		// orchestrator could not hold is not drawn either.
		if !ok || errors.Is(snap.err, clients.ErrNotSupported) || !airflowIDRE.MatchString(d) {
			g.Partial = true
			continue
		}
		dn := b.dagNode(d, snap, info.now)
		taskNodes, skipped := b.taskNodes(d, snap.tasks)
		if skipped > 0 {
			if dn.Facts == nil {
				dn.Facts = map[string]string{}
			}
			dn.Facts["tasks_skipped"] = strconv.Itoa(skipped)
		}
		g.Nodes = append(g.Nodes, dn)
		dagIDs = append(dagIDs, dn.ID)
		if c, ok := runCadence(snap.runs); ok && c > cadence {
			cadence = c
		}
		if errors.Is(snap.tasksErr, clients.ErrNotSupported) {
			g.Partial = true
		}
		for _, tn := range taskNodes {
			g.Nodes = append(g.Nodes, tn)
			g.Edges = append(g.Edges, Edge{From: dn.ID, To: tn.ID})
			tasks = append(tasks, taskRef{id: tn.ID, taskID: strings.TrimPrefix(tn.Name, d+".")})
		}
	}

	models, modelEdges, partial := b.modelNodes(p, info)
	g.Partial = g.Partial || partial
	hasParent, hasChild := map[string]bool{}, map[string]bool{}
	for _, e := range modelEdges {
		hasChild[strings.TrimPrefix(e.From, "model:")] = true
		hasParent[strings.TrimPrefix(e.To, "model:")] = true
	}
	g.Nodes = append(g.Nodes, models...)

	// Which task runs which model is not in either reader: a task named for
	// the model, else every DAG of the pipeline for a root model.
	named := map[string]bool{}
	for _, t := range tasks {
		for _, m := range models {
			if taskNamesModel(t.taskID, m.Name) {
				g.Edges = append(g.Edges, inferred(Edge{From: t.id, To: m.ID}))
				named[m.Name] = true
			}
		}
	}
	for _, m := range models {
		if named[m.Name] || hasParent[m.Name] {
			continue
		}
		for _, d := range dagIDs {
			g.Edges = append(g.Edges, inferred(Edge{From: d, To: m.ID}))
		}
	}
	g.Edges = append(g.Edges, modelEdges...)

	th, cadenceFact := b.batchThresholds(cadence)
	for _, s := range p.SinkTables {
		n := b.batchSinkNode(p, s, info, th)
		n.Facts["cadence"] = cadenceFact
		g.Nodes = append(g.Nodes, n)
		for _, from := range batchSinkInputs(s, models, hasChild, dagIDs) {
			g.Edges = append(g.Edges, inferred(Edge{From: from, To: n.ID}))
		}
	}
	return g
}

// batchSinkInputs names what loads sink table s: the model named like its table,
// else every leaf model, else (no model drawn) every DAG.
func batchSinkInputs(s string, models []Node, hasChild map[string]bool, dagIDs []string) []string {
	table := s[strings.LastIndex(s, ".")+1:]
	for _, m := range models {
		if strings.EqualFold(m.Name, table) {
			return []string{m.ID}
		}
	}
	var leaves []string
	for _, m := range models {
		if !hasChild[m.Name] {
			leaves = append(leaves, m.ID)
		}
	}
	if len(leaves) > 0 {
		return leaves
	}
	return dagIDs
}

// taskNamesModel reports whether a task id names a model: one of its
// dot-separated parts (task groups) is the model or "<model>_run", as dbt
// operators commonly name the task that builds a model.
func taskNamesModel(taskID, model string) bool {
	for _, part := range strings.Split(taskID, ".") {
		if part == model || part == model+"_run" {
			return true
		}
	}
	return false
}

// batchThresholds returns the sink thresholds for a batch pipeline whose
// slowest DAG runs every cadence (0 = unknown: Thresholds.BatchCadence is
// assumed): a sink warns when one run is late and fails when a whole run
// is missing, each with the CDC slack on top. The fact says which cadence.
func (b *builder) batchThresholds(cadence time.Duration) (Thresholds, string) {
	fact := HumanAge(cadence)
	if cadence <= 0 {
		cadence = b.t.BatchCadence
		fact = HumanAge(cadence) + " (assumed)"
	}
	th := b.t
	th.SinkStaleWarn = cadence + b.t.SinkStaleWarn
	th.SinkStaleFail = 2*cadence + b.t.SinkStaleFail
	return th, fact
}

// runCadence returns the shortest positive gap between the logical dates of
// a DAG's scheduled runs; manual and backfill runs do not show the schedule.
func runCadence(runs []clients.DAGRun) (time.Duration, bool) {
	var ds []time.Time
	for _, r := range runs {
		if r.RunType == "scheduled" && !r.LogicalDate.IsZero() {
			ds = append(ds, r.LogicalDate)
		}
	}
	slices.SortFunc(ds, time.Time.Compare)
	var best time.Duration
	for i := 1; i < len(ds); i++ {
		if d := ds[i].Sub(ds[i-1]); d > 0 && (best == 0 || d < best) {
			best = d
		}
	}
	return best, best > 0
}

func (b *builder) dagNode(dagID string, s dagSnapshot, now time.Time) Node {
	n := Node{
		ID:     "dag:" + dagID,
		Kind:   KindDAG,
		Name:   dagID,
		Layer:  "orchestrator",
		Owner:  b.layerOwner("orchestrator"),
		Health: HealthUnknown,
	}
	switch {
	case errors.Is(s.err, clients.ErrNotFound):
		// The orchestrator has no such DAG: nothing will run.
		n.Health, n.Facts = HealthFail, map[string]string{"missing": "true"}
		return n
	case s.err != nil:
		n.Facts = map[string]string{"error": "unreachable"}
		return n
	}
	n.Facts = map[string]string{}
	if len(s.runs) == 0 {
		n.Health, n.Facts["last_run"] = HealthWarn, "never"
	} else {
		r := s.runs[0]
		n.Health = runHealth(r.State)
		n.Facts["last_run_state"] = factWord(r.State)
		n.Facts["last_run_type"] = factWord(r.RunType)
		if !r.LogicalDate.IsZero() {
			n.Facts["last_run_logical_date"] = FormatTS(r.LogicalDate)
		}
		n.Facts["last_run_started"] = "not yet"
		if !r.StartedAt.IsZero() {
			n.Facts["last_run_started"] = HumanAge(now.Sub(r.StartedAt)) + " ago"
			if !r.EndedAt.IsZero() {
				n.Facts["last_run_took"] = HumanAge(r.EndedAt.Sub(r.StartedAt))
			}
		}
		if len(s.runs) > 1 {
			// A queued or running run hides the failed one before it.
			n.Facts["previous_run_state"] = factWord(s.runs[1].State)
		}
	}
	if c, ok := runCadence(s.runs); ok {
		n.Facts["cadence"] = HumanAge(c)
	}
	if s.paused {
		// A paused DAG schedules nothing: a late batch with no failed run.
		n.Facts["paused"] = "true"
		n.Health = worse(n.Health, HealthWarn)
	}
	switch {
	case s.tasksErr == nil, errors.Is(s.tasksErr, clients.ErrNotSupported):
	case errors.Is(s.tasksErr, clients.ErrNotFound):
		n.Facts["tasks"] = "run not found"
	default:
		n.Facts["tasks"] = "unreachable"
	}
	return n
}

// taskNodes draws one node per task of the latest run, in the order they
// started (never-started last). The instances of a mapped task share a node
// that shows its worst instance.
//
// A task whose id is not an Airflow id (airflowIDRE) is not drawn: the id is
// server text that becomes a node id and name in the lane and the model's
// map. skipped counts those instances.
func (b *builder) taskNodes(dagID string, tis []clients.TaskInstance) (out []Node, skipped int) {
	type agg struct {
		rep    clients.TaskInstance
		health Health
		start  time.Time
		count  int
		mapped bool
	}
	byID := map[string]*agg{}
	var order []string
	for _, ti := range tis {
		if !airflowIDRE.MatchString(ti.TaskID) {
			skipped++
			continue
		}
		h := taskHealth(ti.State)
		a, ok := byID[ti.TaskID]
		if !ok {
			a = &agg{rep: ti, health: h}
			byID[ti.TaskID] = a
			order = append(order, ti.TaskID)
		} else if rank(h) > rank(a.health) {
			a.rep, a.health = ti, h
		}
		a.count++
		a.mapped = a.mapped || ti.MapIndex >= 0
		if !ti.StartedAt.IsZero() && (a.start.IsZero() || ti.StartedAt.Before(a.start)) {
			a.start = ti.StartedAt
		}
	}
	slices.SortStableFunc(order, func(x, y string) int {
		sx, sy := byID[x].start, byID[y].start
		switch {
		case sx.IsZero() != sy.IsZero():
			if sx.IsZero() {
				return 1
			}
			return -1
		case !sx.Equal(sy):
			return sx.Compare(sy)
		}
		return strings.Compare(x, y)
	})
	out = make([]Node, 0, len(order))
	for _, id := range order {
		a := byID[id]
		facts := map[string]string{"state": factWord(a.rep.State)}
		if a.rep.Try > 0 {
			facts["try"] = strconv.Itoa(a.rep.Try)
		}
		if a.rep.Operator != "" {
			facts["operator"] = factWord(a.rep.Operator)
		}
		if a.mapped {
			facts["mapped_instances"] = strconv.Itoa(a.count)
		}
		out = append(out, Node{
			ID:     "task:" + dagID + "." + id,
			Kind:   KindTask,
			Name:   dagID + "." + id,
			Layer:  "orchestrator",
			Owner:  b.layerOwner("orchestrator"),
			Health: a.health,
			Facts:  facts,
		})
	}
	return out, skipped
}

// modelNodes draws the pipeline's models, parents before children (ties in
// owners.yaml order), with the parent -> child edges between them. partial
// is true when the reader cannot read lineage, so models are left out.
func (b *builder) modelNodes(p owners.Pipeline, info *batchCollected) (nodes []Node, edges []Edge, partial bool) {
	var names []string
	for _, m := range p.Models {
		name, ok := owners.ModelName(m)
		if !ok {
			continue // an invalid entry (only a pipeline built in code can carry one)
		}
		s, ok := info.lineage[name]
		if !ok || errors.Is(s.err, clients.ErrNotSupported) {
			partial = partial || ok // !ok: no reader, counted by buildBatch
			continue
		}
		names = append(names, name)
	}
	pos := map[string]int{}
	for i, m := range names {
		pos[m] = i
	}
	seen := map[[2]string]bool{}
	var pairs [][2]string
	link := func(parent, child string) {
		if _, ok := pos[parent]; !ok || parent == child || seen[[2]string{parent, child}] {
			return
		}
		seen[[2]string{parent, child}] = true
		pairs = append(pairs, [2]string{parent, child})
	}
	for _, m := range names {
		l := info.lineage[m].l
		for _, ref := range l.Parents {
			if n, ok := owners.ModelName(ref); ok {
				link(n, m)
			}
		}
		for _, ref := range l.Children {
			if n, ok := owners.ModelName(ref); ok {
				if _, in := pos[n]; in {
					link(m, n)
				}
			}
		}
	}
	for _, i := range parentsFirst(len(names), pairs, pos) {
		nodes = append(nodes, b.modelNode(names[i], info))
	}
	for _, pr := range pairs {
		edges = append(edges, Edge{From: "model:" + pr[0], To: "model:" + pr[1]})
	}
	return nodes, edges, partial
}

// parentsFirst orders n items so every pair's parent comes before its child,
// taking the lowest index whenever several are ready; a cycle (a manifest
// dbt would refuse) cannot stop it: the lowest remaining index goes next.
func parentsFirst(n int, pairs [][2]string, pos map[string]int) []int {
	indeg := make([]int, n)
	next := make([][]int, n)
	for _, pr := range pairs {
		f, t := pos[pr[0]], pos[pr[1]]
		next[f] = append(next[f], t)
		indeg[t]++
	}
	done := make([]bool, n)
	out := make([]int, 0, n)
	for len(out) < n {
		pick := -1
		for i := 0; i < n && pick < 0; i++ {
			if !done[i] && indeg[i] <= 0 {
				pick = i
			}
		}
		for i := 0; i < n && pick < 0; i++ {
			if !done[i] {
				pick = i
			}
		}
		done[pick] = true
		out = append(out, pick)
		for _, t := range next[pick] {
			indeg[t]--
		}
	}
	return out
}

func (b *builder) modelNode(name string, info *batchCollected) Node {
	n := Node{
		ID:     "model:" + name,
		Kind:   KindModel,
		Name:   name,
		Layer:  "transform",
		Owner:  b.layerOwner("transform"),
		Health: HealthUnknown,
	}
	switch err := info.lineage[name].err; {
	case errors.Is(err, clients.ErrNotFound):
		// owners.yaml names a model the manifest does not have.
		n.Health, n.Facts = HealthFail, map[string]string{"missing": "true"}
		return n
	case err != nil:
		n.Facts = map[string]string{"error": "unreachable"}
		return n
	}
	switch err := info.resultsErr; {
	case errors.Is(err, clients.ErrNotFound):
		n.Facts = map[string]string{"run_results": "none"}
		return n
	case errors.Is(err, clients.ErrNotSupported):
		n.Facts = map[string]string{"run_results": "not supported"}
		return n
	case err != nil:
		n.Facts = map[string]string{"run_results": "unreachable"}
		return n
	}
	for _, r := range info.results.Results {
		if m, ok := owners.ModelName(r.Model); !ok || m != name {
			continue
		}
		n.Health = resultHealth(r.Status)
		n.Facts = map[string]string{"last_run_status": factWord(r.Status)}
		if !info.results.GeneratedAt.IsZero() {
			n.Facts["last_run_at"] = HumanAge(info.now.Sub(info.results.GeneratedAt)) + " ago"
		}
		if r.Rows >= 0 {
			n.Facts["rows"] = strconv.FormatInt(r.Rows, 10)
		}
		return n
	}
	n.Facts = map[string]string{"last_run_status": "not in the latest run"}
	return n
}

// batchSinkNode is sinkNode with the batch thresholds; a builder without a
// warehouse client says so instead of calling a nil one.
func (b *builder) batchSinkNode(p owners.Pipeline, s string, info *batchCollected, th Thresholds) Node {
	if b.wh == nil {
		return Node{
			ID: "wh:" + s, Kind: KindSinkTable, Name: s, Layer: "warehouse",
			Owner: b.layerOwner("warehouse"), Health: HealthUnknown,
			Facts: map[string]string{"error": "not configured", "ts_column": p.SinkTSColumn},
		}
	}
	return b.sinkNode(p, s, info.freshness[s], info.now, th)
}

// runHealth judges a DAG run state (Airflow: queued | running | success |
// failed). A running run is not a failure yet.
func runHealth(state string) Health {
	switch state {
	case "success", "running", "queued":
		return HealthOK
	case "failed":
		return HealthFail
	}
	return HealthUnknown
}

// taskHealth judges an Airflow task instance state. upstream_failed and
// up_for_retry warn: the first is a consequence of another task's failure,
// the second may still pass.
func taskHealth(state string) Health {
	switch state {
	case "success", "skipped", "running", "queued", "scheduled", "deferred", "up_for_reschedule", "restarting":
		return HealthOK
	case "up_for_retry", "upstream_failed":
		return HealthWarn
	case "failed":
		return HealthFail
	}
	return HealthUnknown
}

// resultHealth judges a dbt result status. skipped warns: dbt skips a model
// whose parent failed.
func resultHealth(status string) Health {
	switch status {
	case "success", "pass":
		return HealthOK
	case "warn", "skipped":
		return HealthWarn
	case "error", "fail":
		return HealthFail
	}
	return HealthUnknown
}

// rank orders health from best to worst for picking a mapped task's worst
// instance and for raising a node's health.
func rank(h Health) int {
	switch h {
	case HealthOK:
		return 0
	case HealthWarn:
		return 2
	case HealthFail:
		return 3
	}
	return 1 // unknown
}

func worse(a, b Health) Health {
	if rank(b) > rank(a) {
		return b
	}
	return a
}

// airflowIDRE is Airflow's own dag_id and task_id rule (KEY_REGEX, at most
// 250 characters), ASCII only: Go's \w is ASCII.
var airflowIDRE = regexp.MustCompile(`^[\w.-]{1,250}$`)

// factWordRE is a state, run type or operator class as the batch systems
// write them.
var factWordRE = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,64}$`)

// factWord passes a short machine word from a batch system into a fact and
// nothing else: those values come from a fixed set, so any other text is
// shown as "(other)" instead of reaching the room and the model's prompt.
func factWord(s string) string {
	switch {
	case s == "":
		return "none"
	case factWordRE.MatchString(s):
		return s
	}
	return "(other)"
}
