package managed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"piperoom/internal/clients"
	"piperoom/internal/clients/confluentcloud"
	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

// ConfluentAPI is what the Confluent ops need from
// internal/clients/confluentcloud; *confluentcloud.Client implements it.
type ConfluentAPI interface {
	Plugins(ctx context.Context) ([]confluentcloud.Plugin, error)
	Validate(ctx context.Context, plugin string, cfg map[string]string) (confluentcloud.Validation, error)
	CreateConnector(ctx context.Context, name string, cfg map[string]string) (confluentcloud.Created, error)
	Status(ctx context.Context, name string) (clients.ConnectorStatus, error)
	Cluster(ctx context.Context) (confluentcloud.Cluster, error)
}

// ACLAPI creates Kafka ACLs on the cluster with the setup account's
// cluster API key; *confluentcloud.ACLClient implements it.
type ACLAPI interface {
	CreateACL(ctx context.Context, acl confluentcloud.ACL) error
}

var (
	_ ConfluentAPI = (*confluentcloud.Client)(nil)
	_ ACLAPI       = (*confluentcloud.ACLClient)(nil)
)

// ConfluentOptions configures the Confluent driver.
type ConfluentOptions struct {
	// Client is the Connect v1 client built with the setup account's Cloud
	// API key, scoped to the target's environment and cluster.
	Client ConfluentAPI
	// ACL is the cluster REST client with the setup account's cluster API
	// key; nil leaves ccloud_acl_create failing with ErrNoACLKey.
	ACL ACLAPI
	// Target is the room's SetupTarget of kind "confluent":
	// <environment id>/<cluster id>.
	Target       events.SetupTarget
	Class        string
	ProdPatterns []string
	Secrets      Materialiser
	// Principal returns the service account the person entered on the
	// runner's terminal for an ACL template (ACLTemplateWatcher: the read
	// account they created from the card; ACLTemplateConnector: the managed
	// connector's own account), as "sa-..." or "User:sa-...". The model's
	// principal param must be that account: ccloud_acl_create never grants
	// a principal the person did not name on the runner. nil, or an error,
	// refuses the op with ErrNoPrincipal.
	Principal func(template string) (string, error)
}

// ErrNoACLKey: the runner holds no cluster API key for ACLs.
var ErrNoACLKey = errors.New("managed: no cluster API key on the runner (${secret:ccloud_cluster_key} and _secret)")

// ErrNoPrincipal: the person has not entered the template's service account
// on the runner, so no ACL row is created for any principal.
var ErrNoPrincipal = errors.New("managed: no service account for this ACL template on the runner")

// ErrPrincipal: the principal param is not the service account the person
// entered on the runner for the template.
var ErrPrincipal = errors.New("managed: the principal is not the service account entered on the runner for this template")

// minACLPrefix is the shortest topic prefix an ACL template grants on. A
// PREFIXED row on one or two characters covers most of a cluster's topics;
// the server binds the prefix to the pipeline's topic_prefix as well.
const minACLPrefix = 3

// ConfluentDriver holds the Confluent target's ids and class.
type ConfluentDriver struct {
	api     ConfluentAPI
	acl     ACLAPI
	env     string
	cluster string
	class   string
	secrets Materialiser
	// principal is ConfluentOptions.Principal.
	principal func(template string) (string, error)
}

// Class is the target's derived class.
func (d *ConfluentDriver) Class() string { return d.class }

// RegisterConfluent adds the Confluent ops to r.
func RegisterConfluent(r *ops.Registry, o ConfluentOptions) (*ConfluentDriver, error) {
	if r == nil || o.Client == nil {
		return nil, errors.New("managed: a registry and a Confluent client are required")
	}
	if o.Target.Kind != KindConfluent {
		return nil, fmt.Errorf("managed: the Confluent target has kind %q, want %q", o.Target.Kind, KindConfluent)
	}
	parts := strings.Split(o.Target.Address, "/")
	if len(parts) != 2 || confluentcloud.CheckEnvironment(parts[0]) != nil || confluentcloud.CheckCluster(parts[1]) != nil {
		return nil, fmt.Errorf("managed: the confluent target address %q is not <environment id>/<cluster id>", clipText(o.Target.Address, 80))
	}
	d := &ConfluentDriver{api: o.Client, acl: o.ACL, env: parts[0], cluster: parts[1], secrets: o.Secrets, principal: o.Principal}
	person := o.Class
	if person == "" {
		person = o.Target.Class
	}
	class, err := deriveClass(person, o.ProdPatterns, d.env)
	if err != nil {
		return nil, err
	}
	// Design §4 names the environment or the cluster id; either match
	// makes the target production.
	if class != protocol.ClassProduction {
		if c2, err := deriveClass(person, o.ProdPatterns, d.cluster); err == nil {
			class = c2
		}
	}
	d.class = class
	for _, op := range d.ops() {
		if err := r.Register(op); err != nil {
			return nil, fmt.Errorf("managed: registering %s: %w", op.Name(), err)
		}
	}
	return d, nil
}

func (d *ConfluentDriver) address() string { return d.env + "/" + d.cluster }

func (d *ConfluentDriver) ops() []ops.Op {
	nameSchema := objectSchema([]string{"name"}, map[string]any{"name": stringSchema("")})
	return []ops.Op{
		simpleOp{kind: KindConfluent, addr: d.address(), name: "ccloud_list_plugins", describe: "list the managed connector plugins of the Confluent Cloud cluster (Connect v1 connector-plugins)",
			schema: objectSchema(nil, map[string]any{}), check: noParams, run: d.listPlugins, class: d.Class},
		simpleOp{kind: KindConfluent, addr: d.address(), name: ReadConfluentConnector, describe: "read a Confluent Cloud connector's status: state, tasks and their messages (Connect v1 status); the ladder's rungs 1 and 2",
			schema: nameSchema, check: checkCCName, run: d.status, class: d.Class},
		simpleOp{kind: KindConfluent, addr: d.address(), name: "ccloud_cluster", describe: "read the Confluent Cloud Kafka cluster: phase, cloud, region, bootstrap and REST endpoints (CMK v2)",
			schema: objectSchema(nil, map[string]any{}), check: noParams, run: d.readCluster, class: d.Class},
		simpleOp{kind: KindConfluent, addr: d.address(), name: "ccloud_create_connector", write: true, describe: "validate the connector config against the managed plugin, then create the connector only if validation has no errors (Connect v1 validate, create)",
			schema: objectSchema([]string{"name", "plugin", "config"}, map[string]any{"name": stringSchema(""), "plugin": stringSchema(""), "config": stringMapSchema()}),
			check:  checkCCCreate, run: d.createConnector, class: d.Class},
		simpleOp{kind: KindConfluent, addr: d.address(), name: "ccloud_acl_create", write: true, describe: "create the fixed ACL rows of a template (watcher: read the pipeline's topics and the connector's group; connector: write the pipeline's topics) for one service account (Kafka REST v3 acls)",
			schema: objectSchema([]string{"template", "principal", "topic_prefix"}, map[string]any{
				"template":  map[string]any{"type": "string", "enum": []string{ACLTemplateWatcher, ACLTemplateConnector}},
				"principal": stringSchema(""), "topic_prefix": stringSchema(""), "group": stringSchema("")}),
			check: checkACLParams, run: d.createACLs, class: d.Class},
	}
}

// simpleOp is a provider op without an identity check (Confluent, Aiven):
// the target check, the tier and the op's params check and run.
type simpleOp struct {
	kind, addr string
	name       string
	describe   string
	write      bool
	schema     map[string]any
	check      func(raw json.RawMessage) error
	run        func(ctx context.Context, raw json.RawMessage) (ops.Result, error)
	class      func() string
}

func (o simpleOp) Name() string                 { return o.name }
func (o simpleOp) Target() string               { return o.kind }
func (o simpleOp) Layer() string                { return connectLayer }
func (o simpleOp) Describe() string             { return o.describe }
func (o simpleOp) ParamsSchema() map[string]any { return o.schema }

// GuardForbidden is ops.ClassGuarder over the driver's derived class.
func (o simpleOp) GuardForbidden(class string) bool { return guardForbidden(o.name, class, o.class()) }

func (o simpleOp) Tier() string {
	if o.write {
		return protocol.TierT2
	}
	return protocol.TierT0
}

// TierFor raises a write to T3 on a production target, with no I/O.
func (o simpleOp) TierFor(json.RawMessage) (string, error) {
	if !o.write {
		return protocol.TierT0, nil
	}
	return writeTier(o.class()), nil
}

func (o simpleOp) Validate(raw json.RawMessage, env ops.Env) error {
	if !targetIn(env, o.kind, o.addr) {
		return fmt.Errorf("%w: no %s target %s in this room", ErrTarget, o.kind, o.addr)
	}
	return o.check(raw)
}

func (o simpleOp) Execute(ctx context.Context, raw json.RawMessage, env ops.Env) (ops.Result, error) {
	if err := o.Validate(raw, env); err != nil {
		return ops.Result{}, err
	}
	res, err := o.run(ctx, raw)
	res.Receipt.Kind = "http"
	res.Receipt.Target = o.kind + ":" + o.addr
	return res, err
}

func noParams(raw json.RawMessage) error {
	var p struct{}
	return decode(raw, &p)
}

func checkCCName(raw json.RawMessage) error {
	var p NameParams
	if err := decode(raw, &p); err != nil {
		return err
	}
	return paramErr(confluentcloud.CheckName(p.Name))
}

// CCCreateParams are ccloud_create_connector's.
type CCCreateParams struct {
	Name   string            `json:"name"`
	Plugin string            `json:"plugin"`
	Config map[string]string `json:"config"`
}

func checkCCCreate(raw json.RawMessage) error {
	var p CCCreateParams
	if err := decode(raw, &p); err != nil {
		return err
	}
	if err := confluentcloud.CheckName(p.Name); err != nil {
		return paramErr(err)
	}
	if err := confluentcloud.CheckPlugin(p.Plugin); err != nil {
		return paramErr(err)
	}
	if v, ok := p.Config["connector.class"]; ok && v != p.Plugin {
		return fmt.Errorf("%w: config connector.class %q is not the plugin %q", ErrParams, clipText(v, 64), p.Plugin)
	}
	if v, ok := p.Config["name"]; ok && v != p.Name {
		return fmt.Errorf("%w: config name %q is not the connector name %q", ErrParams, clipText(v, 64), p.Name)
	}
	return checkConnectorConfig(p.Config)
}

// ACL templates of ccloud_acl_create.
const (
	ACLTemplateWatcher   = "watcher"
	ACLTemplateConnector = "connector"
)

// ACLParams are ccloud_acl_create's. Group is the connector's consumer
// group, required for the watcher template.
type ACLParams struct {
	Template    string `json:"template"`
	Principal   string `json:"principal"`
	TopicPrefix string `json:"topic_prefix"`
	Group       string `json:"group,omitempty"`
}

func checkACLParams(raw json.RawMessage) error {
	var p ACLParams
	if err := decode(raw, &p); err != nil {
		return err
	}
	_, err := ACLRows(p)
	return err
}

// ACLRows returns the fixed rows of a template. The watcher's rows are
// design §2.2's: DESCRIBE and READ on the topics with the prefix,
// DESCRIBE_CONFIGS on them, DESCRIBE on the connector's group. The
// connector's rows let a source connector's service account write its
// topics: CREATE, WRITE and DESCRIBE on the prefix and DESCRIBE on the
// cluster; they are not confirmed against Confluent's per-plugin ACL list.
func ACLRows(p ACLParams) ([]confluentcloud.ACL, error) {
	if err := confluentcloud.CheckPrincipal(p.Principal); err != nil {
		return nil, paramErr(err)
	}
	if err := confluentcloud.CheckResource(p.TopicPrefix); err != nil {
		return nil, paramErr(err)
	}
	if len(p.TopicPrefix) < minACLPrefix || strings.HasPrefix(p.TopicPrefix, "_") {
		// "_" starts the cluster's internal topics (_schemas,
		// _confluent-...); no template grants on them.
		return nil, fmt.Errorf("%w: topic_prefix %q must be at least %d characters and must not start with _", ErrParams, clipText(p.TopicPrefix, 32), minACLPrefix)
	}
	row := func(rt, name, pattern, op string) confluentcloud.ACL {
		return confluentcloud.ACL{ResourceType: rt, ResourceName: name, PatternType: pattern, Principal: p.Principal, Host: "*", Operation: op, Permission: "ALLOW"}
	}
	switch p.Template {
	case ACLTemplateWatcher:
		if err := confluentcloud.CheckResource(p.Group); err != nil {
			return nil, fmt.Errorf("%w: the watcher template needs the connector's group: %v", ErrParams, err)
		}
		return []confluentcloud.ACL{
			row("TOPIC", p.TopicPrefix, "PREFIXED", "DESCRIBE"),
			row("TOPIC", p.TopicPrefix, "PREFIXED", "READ"),
			row("TOPIC", p.TopicPrefix, "PREFIXED", "DESCRIBE_CONFIGS"),
			row("GROUP", p.Group, "LITERAL", "DESCRIBE"),
		}, nil
	case ACLTemplateConnector:
		if p.Group != "" {
			return nil, fmt.Errorf("%w: the connector template takes no group", ErrParams)
		}
		return []confluentcloud.ACL{
			row("TOPIC", p.TopicPrefix, "PREFIXED", "CREATE"),
			row("TOPIC", p.TopicPrefix, "PREFIXED", "WRITE"),
			row("TOPIC", p.TopicPrefix, "PREFIXED", "DESCRIBE"),
			row("CLUSTER", "kafka-cluster", "LITERAL", "DESCRIBE"),
		}, nil
	}
	return nil, fmt.Errorf("%w: template %q, want watcher or connector", ErrParams, clipText(p.Template, 16))
}

func ccErr(err error) map[string]any {
	return map[string]any{"error_class": confluentcloud.ErrorClass(err)}
}

func (d *ConfluentDriver) listPlugins(ctx context.Context, _ json.RawMessage) (ops.Result, error) {
	ps, err := d.api.Plugins(ctx)
	if err != nil {
		return ops.Result{Data: ccErr(err)}, err
	}
	var sources []string
	for _, p := range ps {
		if p.Type == "source" {
			sources = append(sources, p.Class)
		}
	}
	return ops.Result{Summary: fmt.Sprintf("%d managed plugin(s); sources: %s", len(ps), clipText(strings.Join(sources, ", "), 300)), Data: map[string]any{"plugins": ps}}, nil
}

func (d *ConfluentDriver) status(ctx context.Context, raw json.RawMessage) (ops.Result, error) {
	var p NameParams
	_ = decode(raw, &p)
	st, err := d.api.Status(ctx, p.Name)
	if err != nil {
		return ops.Result{Data: ccErr(err)}, err
	}
	running := 0
	for _, t := range st.Tasks {
		if t.State == "RUNNING" {
			running++
		}
	}
	s := fmt.Sprintf("connector %s is %s, %d/%d task(s) RUNNING", st.Name, st.State, running, len(st.Tasks))
	if st.State != "RUNNING" && egressHint(st) {
		s += "; the trace reads like the database is not reachable from Confluent's egress: allow-list the egress ranges or use a private link (a person's change)"
	}
	return ops.Result{Summary: s, Data: st}, nil
}

// egressHint reports a connection failure in a status trace (design §3.2:
// the plan's connect phase asks the person to allow-list the egress ranges).
func egressHint(st clients.ConnectorStatus) bool {
	texts := []string{st.Trace}
	for _, t := range st.Tasks {
		texts = append(texts, t.Trace)
	}
	for _, t := range texts {
		lt := strings.ToLower(t)
		if strings.Contains(lt, "connection refused") || strings.Contains(lt, "timed out") || strings.Contains(lt, "timeout") {
			return true
		}
	}
	return false
}

func (d *ConfluentDriver) readCluster(ctx context.Context, _ json.RawMessage) (ops.Result, error) {
	cl, err := d.api.Cluster(ctx)
	if err != nil {
		return ops.Result{Data: ccErr(err)}, err
	}
	return ops.Result{Summary: fmt.Sprintf("cluster %s (%s) is %s on %s %s", cl.Name, cl.ID, cl.Phase, cl.Cloud, cl.Region), Data: cl}, nil
}

func (d *ConfluentDriver) createConnector(ctx context.Context, raw json.RawMessage) (ops.Result, error) {
	var p CCCreateParams
	_ = decode(raw, &p)
	cfg, err := materialise(d.secrets, p.Config)
	if err != nil {
		return ops.Result{}, err
	}
	cfg["connector.class"] = p.Plugin
	cfg["name"] = p.Name
	v, err := d.api.Validate(ctx, p.Plugin, cfg)
	if err != nil {
		return ops.Result{Summary: "validation of " + p.Name + " failed", Data: ccErr(err)}, err
	}
	if v.ErrorCount > 0 || len(v.Errors) > 0 {
		var names []string
		for _, e := range v.Errors {
			names = append(names, e.Name)
		}
		return ops.Result{Summary: fmt.Sprintf("refused ccloud_create_connector: validation found %d error(s) in %s; nothing was created", v.ErrorCount, clipText(strings.Join(names, ", "), 200)),
				Data: map[string]any{"validation": v, "created": false}},
			fmt.Errorf("%w: %d error(s)", ErrNotValid, v.ErrorCount)
	}
	created, err := d.api.CreateConnector(ctx, p.Name, cfg)
	if err != nil {
		return ops.Result{Data: ccErr(err)}, err
	}
	return ops.Result{Summary: fmt.Sprintf("connector %s created after a clean validation; read its status with %s", created.Name, ReadConfluentConnector),
		Data: map[string]any{"connector": created.Name, "type": created.Type, "config": p.Config, "created": true}}, nil
}

// checkPrincipal binds the principal param to the service account the
// person entered on the runner for the template (ConfluentOptions.Principal).
// The model names the principal, but it cannot choose it: a row for any
// other account is refused before the first ACL call.
func (d *ConfluentDriver) checkPrincipal(p ACLParams) error {
	if d.principal == nil {
		return fmt.Errorf("%w (%s)", ErrNoPrincipal, p.Template)
	}
	want, err := d.principal(p.Template)
	if err != nil {
		return fmt.Errorf("%w (%s): %w", ErrNoPrincipal, p.Template, err)
	}
	want = strings.TrimSpace(want)
	if !strings.HasPrefix(want, "User:") {
		want = "User:" + want
	}
	if confluentcloud.CheckPrincipal(want) != nil {
		return fmt.Errorf("%w (%s): the entered value is not a service account id (sa-...)", ErrNoPrincipal, p.Template)
	}
	if p.Principal != want {
		return fmt.Errorf("%w (%s)", ErrPrincipal, p.Template)
	}
	return nil
}

func (d *ConfluentDriver) createACLs(ctx context.Context, raw json.RawMessage) (ops.Result, error) {
	var p ACLParams
	_ = decode(raw, &p)
	rows, _ := ACLRows(p)
	if d.acl == nil {
		return ops.Result{Summary: "refused ccloud_acl_create: " + ErrNoACLKey.Error()}, ErrNoACLKey
	}
	if err := d.checkPrincipal(p); err != nil {
		return ops.Result{Summary: "refused ccloud_acl_create: " + err.Error()}, err
	}
	var done []confluentcloud.ACL
	for _, row := range rows {
		if err := d.acl.CreateACL(ctx, row); err != nil {
			data := ccErr(err)
			data["created"] = done
			return ops.Result{Summary: fmt.Sprintf("ccloud_acl_create stopped after %d of %d rows", len(done), len(rows)), Data: data}, err
		}
		done = append(done, row)
	}
	return ops.Result{Summary: fmt.Sprintf("%d ACL rows of the %s template for %s on %s*", len(done), p.Template, p.Principal, p.TopicPrefix), Data: map[string]any{"created": done}}, nil
}
