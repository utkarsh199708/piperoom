package room

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/store/memstore"
)

const (
	kindCDCPipeline   = "  - id: orders\n    topic_prefix: pg\n    source_tables: [public.orders]\n    connectors: [c1]\n"
	kindBatchPipeline = "  - id: revenue\n    kind: batch\n    dags: [revenue_daily, shared_dag]\n    models: [fct_revenue]\n"
	kindBatchOther    = "  - id: billing\n    kind: batch\n    dags: [billing_daily, shared_dag]\n"
)

// loadOwnersFile loads an owners.yaml written verbatim (no pipeline added).
func loadOwnersFile(t *testing.T, src string) *owners.Owners {
	t.Helper()
	path := filepath.Join(t.TempDir(), "owners.yaml")
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	o, err := owners.Load(path)
	if err != nil {
		t.Fatalf("owners.Load: %v", err)
	}
	return o
}

func kindService(t *testing.T, own *owners.Owners, solo string, run agent.Runner) *Service {
	t.Helper()
	if run == nil {
		run = func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
			<-ctx.Done()
			return nil
		}
	}
	svc := New(Deps{
		Store: memstore.New(), Users: mustUsers([]User{
			{ID: "arjun", Name: "Arjun", Layers: []string{"kafka"}},
			{ID: "priya", Name: "Priya"},
		}),
		Owners: own, SoloUser: solo, Run: run,
	})
	t.Cleanup(svc.Close)
	return svc
}

func TestPipelinesForReportsKind(t *testing.T) {
	own := loadOwnersFile(t, "pipelines:\n"+kindCDCPipeline+kindBatchPipeline+
		"  - id: explicit\n    kind: cdc\n    topic_prefix: pg2\n    source_tables: [public.x]\n    connectors: [c2]\n")
	svc := kindService(t, own, "priya", nil)
	got := svc.PipelinesFor(User{ID: "priya"})
	want := []PipelineInfo{
		{ID: "orders", Kind: "cdc"}, // no kind: in the file reads as cdc
		{ID: "revenue", Kind: "batch"},
		{ID: "explicit", Kind: "cdc"},
	}
	if !slices.Equal(got, want) {
		t.Fatalf("PipelinesFor = %+v, want %+v", got, want)
	}
	b, err := json.Marshal(got[1])
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != `{"id":"revenue","title":"","shadow":false,"kind":"batch"}` {
		t.Fatalf("JSON = %s", b)
	}
}

func TestNewAlertFillsPipelineKind(t *testing.T) {
	own := loadOwnersFile(t, "pipelines:\n"+kindCDCPipeline+kindBatchPipeline)
	svc := kindService(t, own, "", nil)
	for pid, want := range map[string]string{
		"orders":  events.PipelineKindCDC,
		"revenue": events.PipelineKindBatch,
		// A pipeline owners.yaml does not know stays empty: the contract
		// reads it as cdc, and the runner can tell it was not stated.
		"ghost": "",
		"":      "",
	} {
		if got := svc.newAlert("r", "t", pid, time.Time{}, nil).PipelineKind; got != want {
			t.Errorf("PipelineKind(%q) = %q, want %q", pid, got, want)
		}
	}
	// No owners config at all (unit tests, nothing loaded): empty, no panic.
	bare := kindService(t, nil, "", nil)
	if got := bare.newAlert("r", "t", "orders", time.Time{}, nil).PipelineKind; got != "" {
		t.Fatalf("no owners: PipelineKind = %q, want empty", got)
	}
}

// The agent's run receives the kind for rooms on each kind (end to end through
// the alert path that every route shares).
func TestAgentRunGetsPipelineKind(t *testing.T) {
	own := loadOwnersFile(t, "pipelines:\n"+kindCDCPipeline+kindBatchPipeline)
	var mu sync.Mutex
	got := map[string]agent.Alert{}
	seen := make(chan struct{}, 8)
	svc := kindService(t, own, "", func(ctx context.Context, a agent.Alert, _ agent.Deps, _ agent.Options) error {
		mu.Lock()
		got[a.PipelineID] = a
		mu.Unlock()
		seen <- struct{}{}
		<-ctx.Done()
		return nil
	})
	opened := map[string]time.Time{}
	for _, pid := range []string{"orders", "revenue", "ghost"} {
		raw := json.RawMessage(`{"title":"t","pipeline_id":"` + pid + `"}`)
		snap, _, err := svc.CreateRoomFromAlert(context.Background(), "t", "sev2", pid, raw)
		if err != nil {
			t.Fatalf("CreateRoomFromAlert(%s): %v", pid, err)
		}
		opened[pid] = snap.OpenedAt
	}
	for i := 0; i < 3; i++ {
		select {
		case <-seen:
		case <-time.After(5 * time.Second):
			t.Fatalf("only %d of 3 agent runs started", i)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	for pid, want := range map[string]string{"orders": "cdc", "revenue": "batch", "ghost": ""} {
		if a, ok := got[pid]; !ok || a.PipelineKind != want {
			t.Errorf("run for %s: PipelineKind = %q (started %v), want %q", pid, a.PipelineKind, ok, want)
		}
		// X4c: the run ranks changes against the alert's time, the room's
		// opened_at, not its own start.
		if a := got[pid]; a.At.IsZero() || !a.At.Equal(opened[pid]) {
			t.Errorf("run for %s: At = %v, want the room's opened_at %v", pid, a.At, opened[pid])
		}
	}
}

func TestLayersForScopedByKinds(t *testing.T) {
	cdc := events.LayersOf(events.PipelineKindCDC)
	t.Run("cdc only", func(t *testing.T) {
		svc := kindService(t, loadOwnersFile(t, "pipelines:\n"+kindCDCPipeline+"layers:\n  kafka: arjun\n"), "priya", nil)
		if got := svc.LayersFor(User{ID: "priya"}); !slices.Equal(got, cdc) {
			t.Fatalf("solo = %v, want %v", got, cdc)
		}
		if got := svc.LayersFor(User{ID: "arjun"}); !slices.Equal(got, []string{"kafka"}) {
			t.Fatalf("arjun = %v", got)
		}
	})
	t.Run("no owners config reads as cdc", func(t *testing.T) {
		svc := kindService(t, nil, "priya", nil)
		if got := svc.LayersFor(User{ID: "priya"}); !slices.Equal(got, cdc) {
			t.Fatalf("solo = %v, want %v", got, cdc)
		}
	})
	t.Run("cdc and batch", func(t *testing.T) {
		svc := kindService(t, loadOwnersFile(t, "pipelines:\n"+kindCDCPipeline+kindBatchPipeline+
			"layers:\n  kafka: arjun\n  orchestrator: priya\n  transform: group:analytics\n"), "", nil)
		if got := svc.LayersFor(User{ID: "priya"}); !slices.Equal(got, []string{"orchestrator"}) {
			t.Fatalf("priya = %v", got)
		}
		if got := svc.LayersFor(User{ID: "zoe", Groups: []string{"analytics"}}); !slices.Equal(got, []string{"transform"}) {
			t.Fatalf("group member = %v", got)
		}
		solo := kindService(t, loadOwnersFile(t, "pipelines:\n"+kindCDCPipeline+kindBatchPipeline), "priya", nil)
		if got := solo.LayersFor(User{ID: "priya"}); !slices.Equal(got, events.Layers) {
			t.Fatalf("solo = %v, want every layer %v", got, events.Layers)
		}
	})
	t.Run("batch only", func(t *testing.T) {
		svc := kindService(t, loadOwnersFile(t, "pipelines:\n"+kindBatchPipeline+"layers:\n  orchestrator: priya\n"), "priya", nil)
		want := []string{"warehouse", "orchestrator", "transform"} // events.Layers order
		if got := svc.LayersFor(User{ID: "priya"}); !slices.Equal(got, want) {
			t.Fatalf("solo = %v, want %v", got, want)
		}
		// arjun's users.yaml kafka is not a layer of any pipeline here.
		other := kindService(t, loadOwnersFile(t, "pipelines:\n"+kindBatchPipeline+"layers:\n  orchestrator: priya\n"), "", nil)
		if got := other.LayersFor(User{ID: "arjun"}); len(got) != 0 {
			t.Fatalf("arjun = %v, want none", got)
		}
	})
}

func TestPipelineForDAG(t *testing.T) {
	svc := kindService(t, loadOwnersFile(t, "pipelines:\n"+kindCDCPipeline+kindBatchPipeline+kindBatchOther), "", nil)
	for dag, want := range map[string]string{
		"revenue_daily":   "revenue",
		" billing_daily ": "billing", // trimmed, as alertin.MatchDAG
		"shared_dag":      "",        // two pipelines list it: no guess
		"Revenue_Daily":   "",        // case-sensitive
		"unknown":         "",
		"":                "",
	} {
		if got := svc.PipelineForDAG(dag); got != want {
			t.Errorf("PipelineForDAG(%q) = %q, want %q", dag, got, want)
		}
	}
	if got := kindService(t, nil, "", nil).PipelineForDAG("revenue_daily"); got != "" {
		t.Fatalf("no owners: %q", got)
	}
	// owners.Load refuses dags on a cdc pipeline, but an Owners built in code
	// is not checked: a dag_id must still never pick a CDC pipeline.
	inCode := &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders", DAGs: []string{"orders_dag"}}}}
	if got := kindService(t, inCode, "", nil).PipelineForDAG("orders_dag"); got != "" {
		t.Fatalf("cdc pipeline with dags: %q, want none", got)
	}
}
