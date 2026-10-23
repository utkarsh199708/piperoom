// Package catalog is the server's description of the setup runner's
// operation catalogue (docs/design/setup-agent.md §2.5): for every op the
// server may offer, its name, tier floor, target kind, owners layer, one-line
// description and JSON Schema. The setup tools (internal/setup/tools), the
// setup action adapters (internal/setup/actions) and setup_plan read it; the
// runner's catalogue (internal/setup/ops and the target drivers) is the one
// that executes.
//
// Why a description rather than the runner's registry itself: an
// ops.Registry opens an os.Root at a project directory on the runner's
// machine, and the server has neither. The description is a static table
// (Builtin) pinned to the runner's ops by TestBuiltinMatchesRunner, which
// builds the real runner catalogue in a temporary directory and compares
// every field, so the two cannot drift apart silently.
//
// The package holds no I/O and no mutable global state: Builtin returns a new
// Catalogue on every call.
package catalog

import (
	"encoding/json"
	"errors"
	"fmt"
	"regexp"

	"piperoom/internal/setup/plan"
)

// How an op reaches the runner. A setup op is offered by this wave's setup
// packages (a KindRead tool for a T0 op, an action adapter for a T1+ op); a
// client op is a method of the remote clients (internal/setup/remoteclients),
// offered through the Q tools and Q actions under their own names. Both may
// be steps of a setup plan.
const (
	ViaSetup  = "setup"
	ViaClient = "client"
)

// Spec describes one op.
type Spec struct {
	// Name is the op's catalogue name, snake_case ("compose_up").
	Name string
	// Tier is the op's floor, protocol.TierT0..TierT3.
	Tier string
	// Target is the events.SetupTarget kind the op reaches ("compose",
	// "postgres", "connect", ...), or "" for the runner's own project
	// directory and host.
	Target string
	// Layer is the owners layer the op belongs to ("environment",
	// "kafka-connect", "postgres", ...).
	Layer string
	// Describe is one line for the model, the plan and the approval card.
	Describe string
	// Schema is the op's params JSON Schema exactly as the runner's op
	// declares it (a JSON object, in the subset Check understands). Required
	// for a ViaSetup op; a ViaClient op's params are its client method's.
	Schema json.RawMessage
	// Via is ViaSetup or ViaClient.
	Via string
	// TableKeys are the params keys whose value is a source table or a list
	// of them; a setup action's CheckScope refuses one that is not in the
	// room's pipeline.
	TableKeys []string
	// HostKeys are the params keys whose value is a host, host:port or URL;
	// a setup action's CheckScope refuses one that is not a host of the
	// room's pipeline.
	HostKeys []string
}

// SchemaMap decodes Schema into a new map, so a caller can neither change the
// catalogue's copy nor share one between tools. A spec without a schema
// answers the empty object schema.
func (s Spec) SchemaMap() map[string]any {
	out := map[string]any{}
	if len(s.Schema) > 0 {
		// New checked that Schema decodes; a failure here cannot happen for
		// a spec that came out of a Catalogue.
		_ = json.Unmarshal(s.Schema, &out)
	}
	return out
}

// Check validates params against the spec's schema (see the package-level
// Check).
func (s Spec) Check(params json.RawMessage) error {
	if err := Check(s.SchemaMap(), params); err != nil {
		return fmt.Errorf("%s: %w", s.Name, err)
	}
	return nil
}

// Catalogue is a set of op specs in a stable order. It implements
// plan.Catalogue (Floor), so setup_plan resolves a plan against it.
type Catalogue struct {
	byName map[string]Spec
	order  []string
}

var _ plan.Catalogue = (*Catalogue)(nil)

// ErrSpec reports a spec New refused.
var ErrSpec = errors.New("catalog: invalid op spec")

// opNameRe is the ops package's naming rule: snake_case.
var opNameRe = regexp.MustCompile(`^[a-z][a-z0-9_]{0,63}$`)

// New builds a Catalogue. It refuses an empty or non-snake_case name, a
// duplicate, an unknown tier or Via, and a ViaSetup spec without a schema, or
// whose schema is not a JSON object in the checked subset (every keyword the
// server cannot check is refused here, so no rule is silently skipped).
func New(specs ...Spec) (*Catalogue, error) {
	c := &Catalogue{byName: make(map[string]Spec, len(specs))}
	for _, s := range specs {
		if !opNameRe.MatchString(s.Name) {
			return nil, fmt.Errorf("%w: name %q", ErrSpec, s.Name)
		}
		if _, dup := c.byName[s.Name]; dup {
			return nil, fmt.Errorf("%w: duplicate op %q", ErrSpec, s.Name)
		}
		if !plan.ValidTier(s.Tier) {
			return nil, fmt.Errorf("%w: %s: tier %q", ErrSpec, s.Name, s.Tier)
		}
		switch s.Via {
		case ViaSetup:
			if len(s.Schema) == 0 {
				return nil, fmt.Errorf("%w: %s: a setup op needs its schema", ErrSpec, s.Name)
			}
		case ViaClient:
		default:
			return nil, fmt.Errorf("%w: %s: via %q", ErrSpec, s.Name, s.Via)
		}
		if len(s.Schema) > 0 {
			var schema map[string]any
			if err := json.Unmarshal(s.Schema, &schema); err != nil {
				return nil, fmt.Errorf("%w: %s: schema: %w", ErrSpec, s.Name, err)
			}
			if t, _ := schema["type"].(string); t != "object" {
				return nil, fmt.Errorf("%w: %s: schema type must be object", ErrSpec, s.Name)
			}
			if err := checkSchema(schema, s.Name); err != nil {
				return nil, fmt.Errorf("%w: %w", ErrSpec, err)
			}
		}
		s.Schema = append(json.RawMessage(nil), s.Schema...)
		s.TableKeys = append([]string(nil), s.TableKeys...)
		s.HostKeys = append([]string(nil), s.HostKeys...)
		c.byName[s.Name] = s
		c.order = append(c.order, s.Name)
	}
	return c, nil
}

// Floor returns the op's tier floor, and false for an op not in the catalogue
// (plan.Catalogue).
func (c *Catalogue) Floor(op string) (string, bool) {
	s, ok := c.Spec(op)
	if !ok {
		return "", false
	}
	return s.Tier, true
}

// Spec returns the spec named op.
func (c *Catalogue) Spec(op string) (Spec, bool) {
	if c == nil {
		return Spec{}, false
	}
	s, ok := c.byName[op]
	if !ok {
		return Spec{}, false
	}
	return copySpec(s), true
}

// Specs returns every spec in catalogue order.
func (c *Catalogue) Specs() []Spec {
	if c == nil {
		return nil
	}
	out := make([]Spec, 0, len(c.order))
	for _, n := range c.order {
		out = append(out, copySpec(c.byName[n]))
	}
	return out
}

// Reads returns the ViaSetup ops with a T0 floor: the ops the setup tools
// forward as KindRead tools.
func (c *Catalogue) Reads() []Spec {
	return c.filter(func(s Spec) bool { return s.Via == ViaSetup && s.Tier == plan.TierT0 })
}

// Writes returns the ViaSetup ops with a T1 or higher floor: the ops the setup
// action adapters forward as proposal steps.
func (c *Catalogue) Writes() []Spec {
	return c.filter(func(s Spec) bool { return s.Via == ViaSetup && s.Tier != plan.TierT0 })
}

// With returns a new Catalogue holding c's specs followed by more; a name
// already in c is kept from c (the setup spec of an op wins over a client
// spec of the same name, which carries no schema), so a union of the two
// sources needs no de-duplication by the caller. more is checked as New
// checks it.
func (c *Catalogue) With(more ...Spec) (*Catalogue, error) {
	all := c.Specs()
	seen := make(map[string]bool, len(all))
	for _, s := range all {
		seen[s.Name] = true
	}
	for _, s := range more {
		if seen[s.Name] {
			continue
		}
		seen[s.Name] = true
		all = append(all, s)
	}
	return New(all...)
}

func (c *Catalogue) filter(keep func(Spec) bool) []Spec {
	var out []Spec
	for _, s := range c.Specs() {
		if keep(s) {
			out = append(out, s)
		}
	}
	return out
}

func copySpec(s Spec) Spec {
	s.Schema = append(json.RawMessage(nil), s.Schema...)
	s.TableKeys = append([]string(nil), s.TableKeys...)
	s.HostKeys = append([]string(nil), s.HostKeys...)
	return s
}
