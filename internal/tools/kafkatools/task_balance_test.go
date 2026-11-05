package kafkatools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// workerConnect is fakeConnect plus WorkerTasks.
type workerConnect struct {
	*fakeConnect
	tasks     map[string][]clients.TaskAssignment
	err       error
	taskCalls atomic.Int32
}

func (w *workerConnect) WorkerTasks(context.Context) (map[string][]clients.TaskAssignment, error) {
	w.taskCalls.Add(1)
	return w.tasks, w.err
}

// balanceOwners: the room's pipeline "orders" (a source and a sink) and
// another team's "billing".
func balanceOwners() *owners.Owners {
	return &owners.Owners{Pipelines: []owners.Pipeline{
		{ID: "orders", TopicPrefix: "pg", SourceTables: []string{"public.orders"},
			Connectors: []string{"pg-orders-connector", "orders-sink"}},
		{ID: "billing", TopicPrefix: "bill", SourceTables: []string{"public.invoices"},
			Connectors: []string{"bill-secret-connector"}},
	}}
}

func tasksOf(conn, typ string, n int) []clients.TaskAssignment {
	out := make([]clients.TaskAssignment, n)
	for i := range out {
		out[i] = clients.TaskAssignment{Connector: conn, Type: typ, Task: i, State: "RUNNING"}
	}
	return out
}

type balanceFixture struct {
	wc      *workerConnect
	configs atomic.Int32
	offsets atomic.Int32
	tool    tools.Tool
}

func newBalance(t *testing.T, tasks map[string][]clients.TaskAssignment, cfg map[string]string, partitions int) *balanceFixture {
	t.Helper()
	f := &balanceFixture{}
	fc := &fakeConnect{
		configFn: func(_ context.Context, n string) (map[string]string, error) {
			f.configs.Add(1)
			if n != "orders-sink" {
				t.Errorf("config read of %q", n)
			}
			return cfg, nil
		},
	}
	guardWrites(t, fc)
	f.wc = &workerConnect{fakeConnect: fc, tasks: tasks}
	fk := &fakeKafka{offsetsFn: func(_ context.Context, topic string) ([]clients.PartitionOffsets, error) {
		f.offsets.Add(1)
		if topic != "pg.public.orders" {
			t.Errorf("offsets of %q", topic)
		}
		return make([]clients.PartitionOffsets, partitions), nil
	}}
	reg := tools.NewReadOnlyRegistry()
	if err := Register(reg, Deps{Connect: f.wc, Kafka: fk, Owners: balanceOwners()}); err != nil {
		t.Fatal(err)
	}
	tl, ok := reg.Get("connect_task_balance")
	if !ok {
		t.Fatal("connect_task_balance not registered for a WorkerTaskReader")
	}
	f.tool = tl
	return f
}

func balanceCtx() context.Context { return tools.WithPipelineID(context.Background(), "orders") }

func runBalance(t *testing.T, f *balanceFixture) (tools.Result, balanceReport) {
	t.Helper()
	r, err := f.tool.Run(balanceCtx(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	rep, ok := r.Data.(balanceReport)
	if !ok {
		t.Fatalf("Data = %T", r.Data)
	}
	return r, rep
}

// TC1: 6 tasks on one of 3 workers is an imbalance finding; 2/2/2 is none.
func TestTaskBalanceImbalance(t *testing.T) {
	skewed := map[string][]clients.TaskAssignment{
		"w1:8083": append(tasksOf("pg-orders-connector", "source", 1), tasksOf("bill-secret-connector", "source", 5)...),
		"w2:8083": {}, // runs a connector instance only
		"w3:8083": {},
	}
	r, rep := runBalance(t, newBalance(t, skewed, nil, 0))
	if !rep.Imbalanced || rep.WorkersSeen != 3 || rep.Tasks != 6 || rep.MaxPerWorker != 6 || rep.MinPerWorker != 0 {
		t.Fatalf("report = %+v", rep)
	}
	if !strings.Contains(r.Summary, "imbalance: worker w1:8083 runs 6 of 6 tasks") || !strings.Contains(r.Summary, "about 2") {
		t.Fatalf("summary:\n%s", r.Summary)
	}
	if !strings.Contains(r.Summary, "pg-orders-connector task 0 RUNNING") || !strings.Contains(r.Summary, "5 of other pipelines") {
		t.Fatalf("summary:\n%s", r.Summary)
	}

	even := map[string][]clients.TaskAssignment{
		"w1:8083": tasksOf("pg-orders-connector", "source", 2),
		"w2:8083": tasksOf("bill-secret-connector", "source", 2),
		"w3:8083": {{Connector: "pg-orders-connector", Type: "source", Task: 2, State: "RUNNING"}, {Connector: "bill-secret-connector", Type: "source", Task: 2, State: "RUNNING"}},
	}
	r, rep = runBalance(t, newBalance(t, even, nil, 0))
	if rep.Imbalanced || strings.Contains(r.Summary, "imbalance") {
		t.Fatalf("even spread: %+v\n%s", rep, r.Summary)
	}
}

// TC2: a sink with tasks.max 8 on a 3-partition topic has 5 idle tasks.
func TestTaskBalanceIdleSinkTasks(t *testing.T) {
	tasks := map[string][]clients.TaskAssignment{
		"w1:8083": append(tasksOf("orders-sink", "sink", 8)[:4], tasksOf("pg-orders-connector", "source", 1)...),
		"w2:8083": tasksOf("orders-sink", "sink", 8)[4:],
	}
	f := newBalance(t, tasks, map[string]string{"tasks.max": "8", "topics": "pg.public.orders", "connection.password": "hunter2"}, 3)
	r, rep := runBalance(t, f)
	if len(rep.Sinks) != 1 || rep.Sinks[0].IdleTasks != 5 || rep.Sinks[0].Partitions != 3 || rep.Sinks[0].TasksMax != 8 {
		t.Fatalf("sinks = %+v", rep.Sinks)
	}
	if !strings.Contains(r.Summary, "5 tasks idle") {
		t.Fatalf("summary:\n%s", r.Summary)
	}
	b, _ := json.Marshal(r.Data)
	if strings.Contains(r.Summary+string(b), "hunter2") {
		t.Fatal("a config value other than tasks.max/topics leaked")
	}
	if f.configs.Load() != 1 || f.offsets.Load() != 1 {
		t.Fatalf("config reads %d, offset reads %d", f.configs.Load(), f.offsets.Load())
	}

	// Partitions >= tasks: no idle task.
	_, rep = runBalance(t, newBalance(t, tasks, map[string]string{"tasks.max": "8", "topics": "pg.public.orders"}, 12))
	if rep.Sinks[0].IdleTasks != 0 || len(rep.Findings) != 0 {
		t.Fatalf("12 partitions: %+v", rep)
	}
	// A sink reading another pipeline's topic: not counted, the topic not named, no offsets read.
	f = newBalance(t, tasks, map[string]string{"tasks.max": "8", "topics": "pg.public.orders,bill.public.invoices"}, 3)
	r, rep = runBalance(t, f)
	if rep.Sinks[0].Partitions != 0 || f.offsets.Load() != 0 || strings.Contains(r.Summary, "invoices") || !strings.Contains(r.Summary, "1 topic outside this pipeline") {
		t.Fatalf("outside topic: %+v\n%s", rep.Sinks, r.Summary)
	}
	// topics.regex is not resolved.
	_, rep = runBalance(t, newBalance(t, tasks, map[string]string{"tasks.max": "8", "topics.regex": "pg\\..*"}, 3))
	if rep.Sinks[0].Note == "" || rep.Sinks[0].IdleTasks != 0 {
		t.Fatalf("regex: %+v", rep.Sinks)
	}
}

// E3, E6: another pipeline's connector is never named, typed or given a
// state, in Summary or Data; its tasks are a count per worker.
func TestTaskBalanceHidesOtherPipelines(t *testing.T) {
	tasks := map[string][]clients.TaskAssignment{
		"w1:8083":                {{Connector: "bill-secret-connector", Type: "sink", Task: 0, State: "FAILED"}},
		"w2:8083":                tasksOf("pg-orders-connector", "source", 1),
		clients.UnassignedWorker: {{Connector: "bill-secret-connector", Type: "sink", Task: 1, State: "UNASSIGNED"}},
	}
	f := newBalance(t, tasks, nil, 0)
	r, rep := runBalance(t, f)
	b, err := json.Marshal(r.Data)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range []string{"bill-secret-connector", "FAILED", "UNASSIGNED"} {
		if strings.Contains(r.Summary, s) || strings.Contains(string(b), s) {
			t.Errorf("%q shown:\n%s\n%s", s, r.Summary, b)
		}
	}
	if rep.Unassigned != 1 || !strings.Contains(r.Summary, "1 task unassigned") || !strings.Contains(r.Summary, "(unassigned): 1 task; 1 of other pipelines") {
		t.Fatalf("unassigned: %+v\n%s", rep, r.Summary)
	}
	// orders-sink has no task in Connect's status.
	if len(rep.Missing) != 1 || rep.Missing[0] != "orders-sink" {
		t.Fatalf("missing = %v", rep.Missing)
	}
	if f.configs.Load() != 0 {
		t.Fatal("another pipeline's sink config was read")
	}
}

// E2, E3: no scope, or a pipeline the owners file does not know, is refused
// before any Connect call.
func TestTaskBalanceScopeBeforeIO(t *testing.T) {
	f := newBalance(t, map[string][]clients.TaskAssignment{}, nil, 0)
	for _, ctx := range []context.Context{
		context.Background(),
		tools.WithPipelineID(context.Background(), "nope"),
	} {
		if _, err := f.tool.Run(ctx, json.RawMessage(`{}`)); err == nil {
			t.Fatal("ran without a room scope")
		}
	}
	if n := f.wc.taskCalls.Load(); n != 0 {
		t.Fatalf("%d WorkerTasks calls before the scope check", n)
	}
}

// E5: an older Connect is a clear fact, other errors are returned wrapped;
// a Connect client without WorkerTasks gets no tool.
func TestTaskBalanceErrors(t *testing.T) {
	f := newBalance(t, nil, nil, 0)
	f.wc.err = clients.ErrNotSupported
	r, err := f.tool.Run(balanceCtx(), json.RawMessage(`{}`))
	if err != nil || !strings.Contains(r.Summary, "Connect 2.3+") {
		t.Fatalf("not supported: %v %q", err, r.Summary)
	}
	boom := errors.New("connect: GET /connectors?expand=status: status 500")
	f.wc.err = boom
	if _, err := f.tool.Run(balanceCtx(), json.RawMessage(`{}`)); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}

	reg := tools.NewReadOnlyRegistry()
	if err := Register(reg, Deps{Connect: &fakeConnect{}, Owners: balanceOwners()}); err != nil {
		t.Fatal(err)
	}
	if _, ok := reg.Get("connect_task_balance"); ok {
		t.Fatal("registered for a Connect client without WorkerTasks")
	}
}

// E4: the rows shown are capped; the balance counts every task.
func TestTaskBalanceCaps(t *testing.T) {
	tasks := map[string][]clients.TaskAssignment{}
	for i := 0; i < balanceMaxWorkers+6; i++ {
		tasks[fmt.Sprintf("w%03d:8083", i)] = tasksOf("pg-orders-connector", "source", 10)
	}
	r, rep := runBalance(t, newBalance(t, tasks, nil, 0))
	if len(rep.Workers) != balanceMaxWorkers || rep.WorkersOmitted != 6 || rep.WorkersSeen != balanceMaxWorkers+6 {
		t.Fatalf("workers %d omitted %d seen %d", len(rep.Workers), rep.WorkersOmitted, rep.WorkersSeen)
	}
	own := 0
	for _, w := range rep.Workers {
		own += len(w.PipelineTasks)
	}
	if own != balanceMaxOwnTasks || rep.OwnOmitted != (balanceMaxWorkers+6)*10-balanceMaxOwnTasks {
		t.Fatalf("own rows %d, omitted %d", own, rep.OwnOmitted)
	}
	if len(r.Summary) > tools.MaxSummaryBytes {
		t.Fatalf("summary %d bytes", len(r.Summary))
	}
}
