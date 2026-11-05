package wiring

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"

	"piperoom/internal/actions"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/clients/schemareg"
	"piperoom/internal/config"
	"piperoom/internal/owners"
)

// Staging client sets (X8a; docs/api.md "X8 staging").
//
// A pipeline's owners.yaml staging: {connections: ...} block names a second
// Connect and Schema Registry (and optionally Kafka) to try a fix on before
// production. OpenStacks resolves it like connections: (same references,
// same errors that never carry a value) in both write modes, so a missing
// variable or a staging URL that is production stops the start before
// anyone turns write mode on. Only with write mode on does it build the
// clients, from the staging block alone (never from the process-wide or
// connections: settings), and hand them to that pipeline's actions as
// actions.Deps.Staging. Off, or without staging:, Deps.Staging is nil and
// every Stage answers events.StageNoStaging.
//
// The action registry is per client set, but staging is per pipeline: a
// pipeline with staging: in the process-wide set gets a view of that set
// (same clients, tools and topology) whose action registry alone holds its
// staging clients, so another pipeline of the set can never stage on them.

// stagingOpener builds a staging client set from resolved staging settings
// and returns its closer. Tests replace it.
type stagingOpener func(cfg config.Config, awsCfg *aws.Config) (*actions.Staging, func(), error)

// openStagingClients is the real stagingOpener. It reuses the production
// constructors and their netsec mapping (login, TLS, MSK IAM), so staging is
// reached exactly as production would be. A system the block does not name
// stays a nil field. The Kafka client is the reader client only: staging
// Connect may share the production cluster, and no X8 action writes Kafka.
// No I/O happens here (kgo dials lazily).
func openStagingClients(cfg config.Config, awsCfg *aws.Config) (*actions.Staging, func(), error) {
	st := &actions.Staging{}
	var registry *schemareg.Client
	if strings.TrimSpace(cfg.SchemaRegistryURL) != "" {
		r, err := newRegistryClient(cfg)
		if err != nil {
			return nil, nil, err
		}
		registry, st.Registry = r, r
	}
	if strings.TrimSpace(cfg.ConnectURL) != "" {
		c, err := newConnectClient(cfg)
		if err != nil {
			return nil, nil, err
		}
		st.Connect = c
	}
	closeAll := func() {}
	if brokers := splitBrokers(cfg.KafkaBrokers); len(brokers) > 0 {
		sec, err := kafkaSecurity(cfg, awsCfg)
		if err != nil {
			return nil, nil, err
		}
		kc, err := kafka.NewSecure(brokers, registry, sec)
		if err != nil {
			return nil, nil, fmt.Errorf("wiring: kafka client: %w", err)
		}
		st.Kafka = kc
		closeAll = kc.Close
	}
	return st, closeAll, nil
}

// stagingConfig resolves a pipeline's staging.connections: block into an
// otherwise empty Config. Errors name the pipeline, the key and the
// variable or path, never a value.
func stagingConfig(p owners.Pipeline, src connSource) (config.Config, error) {
	return resolveConnections(p.ID, "staging.connections", p.Staging.Connections, src)
}

// stagingEndpoints gathers the resolved URLs CheckStagingOverlap compares:
// the process-wide settings, every pipeline's own connections: (pcfg for the
// opened ones; the others are resolved here, and one whose reference cannot
// be read in this process is left to owners.Load's reference check), and
// the resolved staging blocks.
func stagingEndpoints(cfg config.Config, own *owners.Owners, pcfg, scfg map[string]config.Config, src connSource) owners.StagingEndpoints {
	urls := func(c config.Config) map[string]string {
		return map[string]string{"connect_url": c.ConnectURL, "schema_registry_url": c.SchemaRegistryURL}
	}
	ep := owners.StagingEndpoints{
		Process:    urls(cfg),
		Production: map[string]map[string]string{},
		Staging:    map[string]map[string]string{},
	}
	for id, sc := range scfg {
		ep.Staging[id] = urls(sc)
	}
	for _, p := range own.Pipelines {
		if !p.HasConnections() {
			continue // it uses the process-wide settings, compared above
		}
		if pc, ok := pcfg[p.ID]; ok {
			ep.Production[p.ID] = urls(pc)
			continue
		}
		vals := map[string]string{}
		for _, k := range []string{"connect_url", "schema_registry_url"} {
			if r, ok := p.Connections[k]; ok {
				if v, err := resolveRef(k, r, src); err == nil {
					vals[k] = v
				}
			}
		}
		ep.Production[p.ID] = vals
	}
	return ep
}

// addStaging opens the staging clients of pipeline id and gives them to the
// actions of its set: in place when the set serves id alone, else through a
// per-pipeline view of the set (see the comment at the top). The clients'
// closer joins s, so Close releases them. Called with write mode on only.
func (s *Stacks) addStaging(id string, sc config.Config, awsCfg *aws.Config, open stagingOpener) error {
	base, ok := s.byPipeline[id]
	if !ok || base.Clients == nil {
		return fmt.Errorf("wiring: pipeline %q: staging: no client set serves the pipeline", id)
	}
	stg, closeStg, err := open(sc, awsCfg)
	if err != nil {
		return fmt.Errorf("wiring: pipeline %q: staging: %w", id, err)
	}
	s.closers = append(s.closers, closeStg)
	areg := actions.NewRegistry()
	if err := registerActions(areg, base.Clients, stg); err != nil {
		return fmt.Errorf("wiring: pipeline %q: staging: register actions: %w", id, err)
	}
	if len(base.Pipelines) == 1 {
		base.Actions, base.staging = areg, stg
		return nil
	}
	view := *base
	view.Pipelines = []string{id}
	view.Actions, view.staging = areg, stg
	s.byPipeline[id] = &view
	return nil
}

// stagerNames are the names of the actions in areg that implement
// actions.Stager: what a require_staging: entry may name.
func stagerNames(areg *actions.Registry) []string {
	var out []string
	for _, a := range areg.All() {
		if _, ok := a.(actions.Stager); ok {
			out = append(out, a.Name())
		}
	}
	return out
}

// HasStaging reports whether the set's actions hold a staging client set:
// the pipeline declares staging: and write mode is on (the room snapshot's
// mode.staging and agent.Alert.Staging).
func (s *Stack) HasStaging() bool { return s != nil && s.staging != nil }
