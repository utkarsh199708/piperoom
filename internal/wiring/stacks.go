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
	// Actions are the write actions over Clients: over read-only clients
	// unless write mode is on (registerActions).
	Actions *actions.Registry
	// Topology builds graphs and traces over Clients.
	Topology topology.Builder

	// staging is the pipeline's staging client set that Actions hold
	// (staging.go); nil without staging: or with write mode off.
	staging *actions.Staging
}

// Stacks holds every client set of the process. It is also a
// topology.Builder: Build and Trace go to the set of the given pipeline.
type Stacks struct {
	byPipeline map[string]*Stack
	// empty serves every pipeline id owners.yaml does not define, and the
	// empty id: no client at all, whatever sets exist. A room can never reach
	// a client by naming a pipeline nobody configured.
	empty *Stack
	all   []*Stack
	// closers release what the sets do not own: the staging clients.
	closers []func()
	once    sync.Once
}

var _ topology.Builder = (*Stacks)(nil)

// StackOptions tunes OpenStacks.
type StackOptions struct {
	// Only, when set, opens the sets of these pipelines alone
	// (cmd/investigate). An id owners.yaml does not define is an error.
	Only []string
	// Prepare, when set, is called for every opened client set after its
	// clients exist and before its tools and actions are registered, with the
	// set's own Wired (its Owners holds only the set's pipelines). The server
	// uses it to give each set what only the server owns, for example the room
	// store reader of similar_incidents (#100: w.Rooms = ...). It must not
	// replace the clients.
	Prepare func(w *Wired)
	// ReadOnly, when true, opens every set read-only whatever
	// PIPEROOM_WRITE_MODE says: no write client is built and the actions are
	// registered over read-only clients. It can only narrow: writes need
	// PIPEROOM_WRITE_MODE=on and ReadOnly false. cmd/investigate sets it
	// unless -approve-all.
	ReadOnly bool
}

// stackDeps are OpenStacks' collaborators; tests replace them so no stack,
// variable or file is needed.
type stackDeps struct {
	open    func(ctx context.Context, cfg config.Config, awsCfg *aws.Config, o *owners.Owners, write bool) (*Wired, error)
	src     connSource
	loadAWS func(ctx context.Context, cfg config.Config) (*aws.Config, error)
	// staging builds a pipeline's staging clients (staging.go); only called
	// with write mode on.
	staging stagingOpener
}

// OpenStacks builds the client sets of own: one per pipeline with a
// connections: block, and the process-wide set from cfg when at least one
// pipeline has none. awsCfg is LoadAWS's result; when it is nil and a
// pipeline's connections use MSK IAM, the AWS chain is loaded here. When any
// opened pipeline has a connections: block, the libpq PG* variables and home
// files are refused (checkLibpqDefaults). PIPEROOM_WRITE_MODE (cfg.WriteMode,
// and opts.ReadOnly) decides for every set whether write clients are built:
// a bad value, or a SOURCE_DB_WRITE_URL equal to SOURCE_DB_URL, stops the
// start before anything is opened. Any failure closes what was opened and
// returns an error that names the pipeline and the reference, never a value.
// A pipeline's staging: block (X8) is resolved and checked against every
// production Connect and Schema Registry endpoint in both write modes, and
// its clients are built only with write mode on (staging.go); every
// require_staging: entry must name a registered actions.Stager of the
// pipeline's set. The caller must Close the result.
func OpenStacks(ctx context.Context, cfg config.Config, awsCfg *aws.Config, own *owners.Owners, opts StackOptions) (*Stacks, error) {
	return openStacks(ctx, cfg, awsCfg, own, opts, stackDeps{
		open: openClients,
		src:  osConnSource,
		loadAWS: func(ctx context.Context, c config.Config) (*aws.Config, error) {
			return loadAWS(ctx, c, os.Getenv, defaultAWSLoader)
		},
		staging: openStagingClients,
	})
}

func openStacks(ctx context.Context, cfg config.Config, awsCfg *aws.Config, own *owners.Owners, opts StackOptions, d stackDeps) (*Stacks, error) {
	if own == nil {
		return nil, errors.New("wiring: open stacks: no owners config")
	}
	// Write mode is one switch for the process: read here once, never from a
	// pipeline's own settings (pipelineConfig leaves it empty).
	write, err := openWriteMode(cfg, opts.ReadOnly)
	if err != nil {
		return nil, err
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
	// scfg holds the resolved staging: blocks (X8), by pipeline id.
	scfg := map[string]config.Config{}
	// extra holds the references only resolution can see: the process AWS
	// identity of MSK IAM, and the resolved paths of TLS keys and
	// certificates, whichever variable or file named them.
	extra := map[string][]string{}
	needAWS := false
	for _, p := range selected {
		if p.HasStaging() {
			sc, err := stagingConfig(p, d.src)
			if err != nil {
				return nil, err
			}
			// Staging's key files and AWS identity count for the shared
			// reference rule like the pipeline's own.
			extra[p.ID] = append(extra[p.ID], tlsPathRefs(sc)...)
			if usesMSKIAM(sc) {
				extra[p.ID] = append(extra[p.ID], awsProcessIdentityRef)
				needAWS = needAWS || write
			}
			scfg[p.ID] = sc
		}
		if !p.HasConnections() {
			procIDs = append(procIDs, p.ID)
			extra[p.ID] = append(extra[p.ID], tlsPathRefs(cfg)...)
			if usesMSKIAM(cfg) {
				extra[p.ID] = append(extra[p.ID], awsProcessIdentityRef)
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
		// The pipeline's own write URL is checked in both write modes, like
		// SOURCE_DB_WRITE_URL, so the mistake stops the start before anyone
		// turns write mode on. Off, the set's settings drop it: no writer can
		// be built from it and the set does not keep the secret.
		if err := checkPipelineWriteURL(p.ID, pc); err != nil {
			return nil, err
		}
		if !write {
			pc.SourceDBWriteURL = ""
		}
		// Settings that are not connections (the AWS region) stay process-wide.
		pc.AWSRegion = cfg.AWSRegion
		extra[p.ID] = append(extra[p.ID], tlsPathRefs(pc)...)
		if usesMSKIAM(pc) {
			extra[p.ID] = append(extra[p.ID], awsProcessIdentityRef)
			needAWS = true
		}
		ownIDs = append(ownIDs, p.ID)
		pcfg[p.ID] = pc
	}
	if len(ownIDs) > 0 {
		// pgx falls back to libpq's variables and home files for anything a
		// DSN leaves out, so one tenant's DSN could log in with them.
		if err := checkLibpqDefaults(d.src); err != nil {
			return nil, err
		}
	}
	if err := own.CheckSharedRefs(extra); err != nil {
		return nil, fmt.Errorf("wiring: %w", err)
	}
	if len(scfg) > 0 {
		if err := own.CheckStagingOverlap(stagingEndpoints(cfg, own, pcfg, scfg, d.src)); err != nil {
			return nil, fmt.Errorf("wiring: %w", err)
		}
	}
	if !write {
		// Off, no staging client is built and nothing keeps the secrets.
		scfg = nil
	}
	if needAWS && awsCfg == nil {
		c := cfg
		c.KafkaSASLMechanism = netsec.MechAWSMSKIAM
		// MSK IAM alone: no model role is named as a user of this load.
		c.ModelProvider, c.TriagePassProvider, c.SummaryProvider, c.JudgeProvider = "", "", "", ""
		ac, err := d.loadAWS(ctx, c)
		if err != nil {
			return nil, fmt.Errorf("wiring: MSK IAM in a pipeline's connections: %w", err)
		}
		awsCfg = ac
	}

	empty, err := emptyStack()
	if err != nil {
		return nil, err
	}
	s := &Stacks{byPipeline: map[string]*Stack{}, empty: empty}
	add := func(name string, ids []string, c config.Config) error {
		w, err := d.open(ctx, c, awsCfg, own.Only(ids...), write)
		if err != nil {
			return err
		}
		st, err := newStack(name, ids, w, opts.Prepare)
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
	if len(procIDs) > 0 {
		if err := add(ProcessWideStack, procIDs, cfg); err != nil {
			return nil, err
		}
	}
	for _, id := range ownIDs {
		if err := add("pipeline "+id, []string{id}, pcfg[id]); err != nil {
			s.Close()
			return nil, fmt.Errorf("wiring: pipeline %q: %w", id, err)
		}
	}
	for _, p := range selected {
		if sc, ok := scfg[p.ID]; ok {
			if err := s.addStaging(p.ID, sc, awsCfg, d.staging); err != nil {
				s.Close()
				return nil, err
			}
		}
		// Checked against the pipeline's own registry, which holds every
		// action in both write modes (only the clients change).
		if len(p.RequireStaging) > 0 {
			if err := own.Only(p.ID).ValidateRequireStaging(stagerNames(s.For(p.ID).Actions)); err != nil {
				s.Close()
				return nil, fmt.Errorf("wiring: %w", err)
			}
		}
	}
	return s, nil
}

// newStack registers the tools and actions of one client set, after prepare
// (StackOptions.Prepare) has seen its Wired.
func newStack(name string, ids []string, w *Wired, prepare func(*Wired)) (*Stack, error) {
	if prepare != nil {
		prepare(w)
	}
	reg, areg := tools.NewReadOnlyRegistry(), actions.NewRegistry()
	if err := RegisterTools(reg, areg, w); err != nil {
		return nil, err
	}
	return &Stack{Name: name, Pipelines: ids, Clients: w, Tools: reg, Actions: areg, Topology: w.Builder}, nil
}

// emptyStack serves pipelines no set covers: the runbook tool only (it needs no
// client), no action, and a topology that refuses.
func emptyStack() (*Stack, error) {
	reg := tools.NewReadOnlyRegistry()
	if err := runbooks.Register(reg); err != nil {
		return nil, fmt.Errorf("wiring: register runbooks: %w", err)
	}
	return &Stack{Name: NoStack, Tools: reg, Actions: actions.NewRegistry(), Topology: noTopology{}}, nil
}

// For returns the set of the pipeline, or the empty set for an id no set
// serves (unknown, empty, or not opened by StackOptions.Only). It never
// returns nil.
func (s *Stacks) For(pipelineID string) *Stack {
	if st, ok := s.byPipeline[pipelineID]; ok {
		return st
	}
	return s.empty
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
		for i := len(s.closers) - 1; i >= 0; i-- {
			s.closers[i]()
		}
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
