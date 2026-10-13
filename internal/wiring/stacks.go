package wiring

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"

	"piperoom/internal/actions"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
	"piperoom/internal/tools/runbooks"
	"piperoom/internal/topology"
)

// Per-pipeline client sets (docs/design/tenancy.md section 5).
//
// Every pipeline with a connections: block gets its own clients, built once at
// start from its own references, and its own tool registry, action registry
// and topology builder over those clients only. The pipelines without one share
// the process-wide set. A tool cannot reach another pipeline's client because
// it holds none: isolation is by construction, keyed by pipeline id in Stacks,
// not by a check inside each tool.

// ProcessWideStack is the Name of the set built from the process-wide
// settings; NoStack is the Name of the empty set.
const (
	ProcessWideStack = "process-wide"
	NoStack          = "none"
)

// awsProcessIdentityRef is the connection reference of MSK IAM: the process
// has one AWS identity, so two tenants using it share a credential.
const awsProcessIdentityRef = "aws:process-identity"

// Stack is one client set and everything built over it.
type Stack struct {
	// Name is ProcessWideStack, "pipeline <id>", or NoStack.
	Name string
	// Pipelines are the ids this set serves, in owners order.
	Pipelines []string
	// Clients holds the clients and the owners view (only Pipelines). Nil
	// for the empty set.
	Clients *Wired
	// Tools are the agent's read tools over Clients.
	Tools *tools.Registry
	// Actions are the write actions over Clients.
	Actions *actions.Registry
	// Topology builds graphs and traces over Clients.
	Topology topology.Builder
}

// Stacks holds every client set of the process. It is also a
// topology.Builder: Build and Trace go to the set of the given pipeline.
type Stacks struct {
	byPipeline map[string]*Stack
	// fallback serves a pipeline id owners.yaml does not define, and an empty
	// one: the process-wide set when it exists (its tools refuse such an id,
	// as before tenancy), else the empty set.
	fallback *Stack
	all      []*Stack
	once     sync.Once
}

var _ topology.Builder = (*Stacks)(nil)

// StackOptions tunes OpenStacks.
type StackOptions struct {
	// Only, when set, opens the sets of these pipelines alone
	// (cmd/investigate). An id owners.yaml does not define is an error.
	Only []string
}

// stackDeps are OpenStacks' collaborators; tests replace them so no stack,
// variable or file is needed.
type stackDeps struct {
	open    func(ctx context.Context, cfg config.Config, awsCfg *aws.Config, o *owners.Owners) (*Wired, error)
	src     connSource
	loadAWS func(ctx context.Context, cfg config.Config) (*aws.Config, error)
}

// OpenStacks builds the client sets of own: one per pipeline with a
// connections: block, and the process-wide set from cfg when at least one
// pipeline has none (or own has no pipeline). awsCfg is LoadAWS's result; when
// it is nil and a pipeline's connections use MSK IAM, the AWS chain is loaded
// here. Any failure closes what was opened and returns an error that names the
// pipeline and the reference, never a value. The caller must Close the result.
func OpenStacks(ctx context.Context, cfg config.Config, awsCfg *aws.Config, own *owners.Owners, opts StackOptions) (*Stacks, error) {
	return openStacks(ctx, cfg, awsCfg, own, opts, stackDeps{
		open: openClients,
		src:  osConnSource,
		loadAWS: func(ctx context.Context, c config.Config) (*aws.Config, error) {
			return loadAWS(ctx, c, os.Getenv, defaultAWSLoader)
		},
	})
}

func openStacks(ctx context.Context, cfg config.Config, awsCfg *aws.Config, own *owners.Owners, opts StackOptions, d stackDeps) (*Stacks, error) {
	if own == nil {
		return nil, errors.New("wiring: open stacks: no owners config")
	}
	selected := own.Pipelines
	if opts.Only != nil {
		selected = nil
		for _, id := range opts.Only {
			p, ok := own.Pipeline(id)
			if !ok {
				return nil, fmt.Errorf("wiring: pipeline %q is not in the owners file", id)
			}
			selected = append(selected, p)
		}
	}

	// Resolve every reference before anything is opened, so a missing
	// variable or file fails the start without a half-built process.
	var procIDs, ownIDs []string
	pcfg := map[string]config.Config{}
	extra := map[string][]string{}
	needAWS := false
	for _, p := range selected {
		if !p.HasConnections() {
			procIDs = append(procIDs, p.ID)
			if usesMSKIAM(cfg) {
				extra[p.ID] = []string{awsProcessIdentityRef}
			}
			continue
		}
		pc, err := pipelineConfig(p, d.src)
		if err != nil {
			return nil, err
		}
		if _, err := warehouseKind(pc); err != nil {
			return nil, fmt.Errorf("wiring: pipeline %q: %w", p.ID, err)
		}
		// Settings that are not connections (the AWS region) stay process-wide.
		pc.AWSRegion = cfg.AWSRegion
		if usesMSKIAM(pc) {
			extra[p.ID] = []string{awsProcessIdentityRef}
			needAWS = true
		}
		ownIDs = append(ownIDs, p.ID)
		pcfg[p.ID] = pc
	}
	if err := own.CheckSharedRefs(extra); err != nil {
		return nil, fmt.Errorf("wiring: %w", err)
	}
	if needAWS && awsCfg == nil {
		c := cfg
		c.KafkaSASLMechanism = netsec.MechAWSMSKIAM
		c.ModelProvider = ""
		ac, err := d.loadAWS(ctx, c)
		if err != nil {
			return nil, fmt.Errorf("wiring: MSK IAM in a pipeline's connections: %w", err)
		}
		awsCfg = ac
	}

	s := &Stacks{byPipeline: map[string]*Stack{}}
	add := func(name string, ids []string, c config.Config) error {
		w, err := d.open(ctx, c, awsCfg, own.Only(ids...))
		if err != nil {
			return err
		}
		st, err := newStack(name, ids, w)
		if err != nil {
			w.Close()
			return err
		}
		s.all = append(s.all, st)
		for _, id := range ids {
			s.byPipeline[id] = st
		}
		return nil
	}
	if len(procIDs) > 0 || (len(own.Pipelines) == 0 && opts.Only == nil) {
		if err := add(ProcessWideStack, procIDs, cfg); err != nil {
			return nil, err
		}
		s.fallback = s.all[0]
	}
	for _, id := range ownIDs {
		if err := add("pipeline "+id, []string{id}, pcfg[id]); err != nil {
			s.Close()
			return nil, fmt.Errorf("wiring: pipeline %q: %w", id, err)
		}
	}
	if s.fallback == nil {
		empty, err := emptyStack()
		if err != nil {
			s.Close()
			return nil, err
		}
		s.fallback = empty
	}
	return s, nil
}

// newStack registers the tools and actions of one client set.
func newStack(name string, ids []string, w *Wired) (*Stack, error) {
	reg, areg := tools.NewRegistry(), actions.NewRegistry()
	if err := RegisterTools(reg, areg, w); err != nil {
		return nil, err
	}
	return &Stack{Name: name, Pipelines: ids, Clients: w, Tools: reg, Actions: areg, Topology: w.Builder}, nil
}

// emptyStack serves pipelines no set covers: the runbook tool only (it needs no
// client), no action, and a topology that refuses.
func emptyStack() (*Stack, error) {
	reg := tools.NewRegistry()
	if err := runbooks.Register(reg); err != nil {
		return nil, fmt.Errorf("wiring: register runbooks: %w", err)
	}
	return &Stack{Name: NoStack, Tools: reg, Actions: actions.NewRegistry(), Topology: noTopology{}}, nil
}

// For returns the set of the pipeline. It never returns nil.
func (s *Stacks) For(pipelineID string) *Stack {
	if st, ok := s.byPipeline[pipelineID]; ok {
		return st
	}
	return s.fallback
}

// All returns every opened set (not the empty one), process-wide first.
func (s *Stacks) All() []*Stack { return append([]*Stack(nil), s.all...) }

// Build implements topology.Builder with the pipeline's own set.
func (s *Stacks) Build(ctx context.Context, pipelineID string) (topology.Graph, error) {
	return s.For(pipelineID).Topology.Build(ctx, pipelineID)
}

// Render implements topology.Builder.
func (s *Stacks) Render(g topology.Graph) string {
	return s.For(g.PipelineID).Topology.Render(g)
}

// Trace implements topology.Builder with the pipeline's own set.
func (s *Stacks) Trace(ctx context.Context, pipelineID, table, key string) ([]events.TraceStage, error) {
	return s.For(pipelineID).Topology.Trace(ctx, pipelineID, table, key)
}

// LayerOwner implements topology.Builder with the pipeline's own set.
func (s *Stacks) LayerOwner(pipelineID, layer string) (string, bool) {
	return s.For(pipelineID).Topology.LayerOwner(pipelineID, layer)
}

// Close closes every set. It is safe to call more than once and on nil.
func (s *Stacks) Close() {
	if s == nil {
		return
	}
	s.once.Do(func() {
		for i := len(s.all) - 1; i >= 0; i-- {
			s.all[i].Clients.Close()
		}
	})
}

// errNoClientSet is the topology answer for a pipeline no client set serves.
var errNoClientSet = errors.New("wiring: no client set serves this pipeline")

// noTopology is the empty set's builder.
type noTopology struct{}

func (noTopology) Build(_ context.Context, pipelineID string) (topology.Graph, error) {
	return topology.Graph{}, fmt.Errorf("pipeline %q: %w", pipelineID, errNoClientSet)
}

func (noTopology) Render(topology.Graph) string { return "" }

func (noTopology) Trace(_ context.Context, pipelineID, _, _ string) ([]events.TraceStage, error) {
	return nil, fmt.Errorf("pipeline %q: %w", pipelineID, errNoClientSet)
}

func (noTopology) LayerOwner(string, string) (string, bool) { return "", false }
