package managed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"piperoom/internal/clients"
	"piperoom/internal/clients/aiven"
	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
)

// AivenAPI is what the Aiven ops need from internal/clients/aiven;
// *aiven.Client implements it.
type AivenAPI interface {
	Service(ctx context.Context) (aiven.Service, error)
	ServiceNamed(ctx context.Context, name string) (aiven.Service, error)
	Plugins(ctx context.Context) ([]aiven.Plugin, error)
	Status(ctx context.Context, name string) (clients.ConnectorStatus, error)
	CreateConnector(ctx context.Context, cfg map[string]string) (string, error)
	CreateIntegration(ctx context.Context, source string) (string, error)
}

var _ AivenAPI = (*aiven.Client)(nil)

// AivenOptions configures the Aiven driver.
type AivenOptions struct {
	// Client is built with the setup application user's token, scoped to
	// the target's project and Kafka Connect service.
	Client AivenAPI
	// Target is the room's SetupTarget of kind "aiven": <project>/<service>
	// (the Kafka Connect service).
	Target       events.SetupTarget
	Class        string
	ProdPatterns []string
	Secrets      Materialiser
}

// AivenDriver holds the Aiven target's names and class.
type AivenDriver struct {
	api     AivenAPI
	project string
	service string
	class   string
	secrets Materialiser
}

// Class is the target's derived class.
func (d *AivenDriver) Class() string { return d.class }

// RegisterAiven adds the Aiven ops to r.
func RegisterAiven(r *ops.Registry, o AivenOptions) (*AivenDriver, error) {
	if r == nil || o.Client == nil {
		return nil, errors.New("managed: a registry and an Aiven client are required")
	}
	if o.Target.Kind != KindAiven {
		return nil, fmt.Errorf("managed: the Aiven target has kind %q, want %q", o.Target.Kind, KindAiven)
	}
	parts := strings.Split(o.Target.Address, "/")
	if len(parts) != 2 || aiven.CheckProject(parts[0]) != nil || aiven.CheckService(parts[1]) != nil {
		return nil, fmt.Errorf("managed: the aiven target address %q is not <project>/<service>", clipText(o.Target.Address, 80))
	}
	d := &AivenDriver{api: o.Client, project: parts[0], service: parts[1], secrets: o.Secrets}
	person := o.Class
	if person == "" {
		person = o.Target.Class
	}
	class, err := deriveClass(person, o.ProdPatterns, d.project)
	if err != nil {
		return nil, err
	}
	d.class = class
	addr := d.project + "/" + d.service
	for _, op := range []ops.Op{
		simpleOp{kind: KindAiven, addr: addr, name: "aiven_service_get", describe: "read an Aiven service of the project: type, state, plan, cloud, user names (never its URIs or passwords)",
			schema: objectSchema(nil, map[string]any{"service": stringSchema("")}), check: checkAivenService, run: d.serviceGet, class: d.Class},
		simpleOp{kind: KindAiven, addr: addr, name: "aiven_list_plugins", describe: "list the connector plugins available on the Kafka Connect service",
			schema: objectSchema(nil, map[string]any{}), check: noParams, run: d.listPlugins, class: d.Class},
		simpleOp{kind: KindAiven, addr: addr, name: ReadAivenConnector, describe: "read an Aiven connector's status: state and tasks (the ladder's rungs 1 and 2)",
			schema: objectSchema([]string{"name"}, map[string]any{"name": stringSchema("")}), check: checkAivenName, run: d.status, class: d.Class},
		simpleOp{kind: KindAiven, addr: addr, name: "aiven_create_connector", write: true, describe: "create a connector on the Kafka Connect service after checking its class is an available plugin; ${secret:} values materialised on the runner",
			schema: objectSchema([]string{"name", "config"}, map[string]any{"name": stringSchema(""), "config": stringMapSchema()}),
			check:  checkAivenCreate, run: d.createConnector, class: d.Class},
		simpleOp{kind: KindAiven, addr: addr, name: "aiven_integration_create", write: true, describe: "join a Kafka service of the project to the Kafka Connect service (integration type kafka_connect)",
			schema: objectSchema([]string{"source_service"}, map[string]any{"source_service": stringSchema("")}),
			check:  checkAivenIntegration, run: d.createIntegration, class: d.Class},
	} {
		if err := r.Register(op); err != nil {
			return nil, fmt.Errorf("managed: registering %s: %w", op.Name(), err)
		}
	}
	return d, nil
}

// AivenServiceParams name a service; empty means the target's Connect
// service.
type AivenServiceParams struct {
	Service string `json:"service,omitempty"`
}

// AivenCreateParams are aiven_create_connector's.
type AivenCreateParams struct {
	Name   string            `json:"name"`
	Config map[string]string `json:"config"`
}

// AivenIntegrationParams are aiven_integration_create's.
type AivenIntegrationParams struct {
	SourceService string `json:"source_service"`
}

func checkAivenService(raw json.RawMessage) error {
	var p AivenServiceParams
	if err := decode(raw, &p); err != nil {
		return err
	}
	if p.Service != "" {
		return paramErr(aiven.CheckService(p.Service))
	}
	return nil
}

func checkAivenName(raw json.RawMessage) error {
	var p NameParams
	if err := decode(raw, &p); err != nil {
		return err
	}
	return paramErr(aiven.CheckName(p.Name))
}

func checkAivenCreate(raw json.RawMessage) error {
	var p AivenCreateParams
	if err := decode(raw, &p); err != nil {
		return err
	}
	if err := aiven.CheckName(p.Name); err != nil {
		return paramErr(err)
	}
	if v, ok := p.Config["name"]; ok && v != p.Name {
		return fmt.Errorf("%w: config name %q is not the connector name %q", ErrParams, clipText(v, 64), p.Name)
	}
	if p.Config["connector.class"] == "" {
		return fmt.Errorf("%w: config needs connector.class", ErrParams)
	}
	return checkConnectorConfig(p.Config)
}

func checkAivenIntegration(raw json.RawMessage) error {
	var p AivenIntegrationParams
	if err := decode(raw, &p); err != nil {
		return err
	}
	return paramErr(aiven.CheckService(p.SourceService))
}

func avErr(err error) map[string]any {
	return map[string]any{"error_class": aiven.ErrorClass(err)}
}

func (d *AivenDriver) serviceGet(ctx context.Context, raw json.RawMessage) (ops.Result, error) {
	var p AivenServiceParams
	_ = decode(raw, &p)
	name := p.Service
	if name == "" {
		name = d.service
	}
	s, err := d.api.ServiceNamed(ctx, name)
	if err != nil {
		return ops.Result{Data: avErr(err)}, err
	}
	return ops.Result{Summary: fmt.Sprintf("Aiven service %s (%s) is %s, plan %s, %s", s.Name, s.Type, s.State, s.Plan, s.Cloud), Data: s}, nil
}

func (d *AivenDriver) listPlugins(ctx context.Context, _ json.RawMessage) (ops.Result, error) {
	ps, err := d.api.Plugins(ctx)
	if err != nil {
		return ops.Result{Data: avErr(err)}, err
	}
	return ops.Result{Summary: fmt.Sprintf("%d plugin(s) available on %s", len(ps), d.service), Data: map[string]any{"plugins": ps}}, nil
}

func (d *AivenDriver) status(ctx context.Context, raw json.RawMessage) (ops.Result, error) {
	var p NameParams
	_ = decode(raw, &p)
	st, err := d.api.Status(ctx, p.Name)
	if err != nil {
		return ops.Result{Data: avErr(err)}, err
	}
	running := 0
	for _, t := range st.Tasks {
		if t.State == "RUNNING" {
			running++
		}
	}
	return ops.Result{Summary: fmt.Sprintf("connector %s is %s, %d/%d task(s) RUNNING", st.Name, st.State, running, len(st.Tasks)), Data: st}, nil
}

func (d *AivenDriver) createConnector(ctx context.Context, raw json.RawMessage) (ops.Result, error) {
	var p AivenCreateParams
	_ = decode(raw, &p)
	class := p.Config["connector.class"]
	ps, err := d.api.Plugins(ctx)
	if err != nil {
		return ops.Result{Data: avErr(err)}, err
	}
	var have []string
	found := false
	for _, pl := range ps {
		if pl.Class == class {
			found = true
		}
		if pl.Type == "source" {
			have = append(have, pl.Class)
		}
	}
	if !found {
		return ops.Result{Summary: fmt.Sprintf("refused aiven_create_connector: %s is not an available plugin on %s; available sources: %s", class, d.service, clipText(strings.Join(have, ", "), 300)),
				Data: map[string]any{"class": class, "available_sources": have, "created": false}},
			fmt.Errorf("%w: %s is not available on the service", ErrCheckFailed, class)
	}
	cfg, err := materialise(d.secrets, p.Config)
	if err != nil {
		return ops.Result{}, err
	}
	cfg["name"] = p.Name
	name, err := d.api.CreateConnector(ctx, cfg)
	if err != nil {
		return ops.Result{Data: avErr(err)}, err
	}
	return ops.Result{Summary: fmt.Sprintf("connector %s created on %s; read its status with %s", name, d.service, ReadAivenConnector),
		Data: map[string]any{"connector": name, "config": p.Config, "created": true}}, nil
}

func (d *AivenDriver) createIntegration(ctx context.Context, raw json.RawMessage) (ops.Result, error) {
	var p AivenIntegrationParams
	_ = decode(raw, &p)
	src, err := d.api.ServiceNamed(ctx, p.SourceService)
	if err != nil {
		return ops.Result{Data: avErr(err)}, err
	}
	if src.Type != "kafka" {
		return ops.Result{Summary: "refused aiven_integration_create: " + p.SourceService + " is a " + src.Type + " service, not kafka", Data: map[string]any{"type": src.Type}},
			fmt.Errorf("%w: %s is not a Kafka service", ErrCheckFailed, p.SourceService)
	}
	id, err := d.api.CreateIntegration(ctx, p.SourceService)
	if err != nil {
		return ops.Result{Data: avErr(err)}, err
	}
	return ops.Result{Summary: fmt.Sprintf("integration %s -> %s (kafka_connect) created", p.SourceService, d.service), Data: map[string]any{"integration_id": id}}, nil
}
