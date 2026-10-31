// Package strimzi is the Strimzi target of the setup runner (rows U12a, U12b;
// docs/design/setup-strimzi.md): the runner is an in-cluster Job in the
// target namespace and talks to the Kubernetes API over net/http with its
// projected ServiceAccount token (kube.go). There is no kubectl and no
// client-go, and no op runs a program (an AST test pins it).
//
// Every object the runner touches was pre-created by the Helm chart with a
// name fixed at chart time (§3, §4): the Strimzi CRs as shells paused with
// strimzi.io/pause-reconciliation, the db-credentials Secret and the logging
// ConfigMap. The runner never creates anything. Its Role grants get, patch
// and delete by resourceNames only, and every op here refuses a name outside
// the chart-fixed set before any request (and RBAC refuses it after); before
// a patch or a delete it also checks the object is the pipeline release's
// (checkOwned), so a same-named object someone else made is never touched.
//
// Ops: k8s_get, k8s_events, k8s_logs and strimzi_build_status (T0, one
// namespace, read-only), k8s_apply {kind, template, params} (T2: a template
// renders a spec, the field allow-list validates it with no I/O, the preview
// is the JSON patch with dryRun=All, the write is the three-step JSON patch of
// §5) and k8s_secret_apply (T2) with its preview preview_k8s_secret_apply
// (T0), and the rollback ops of §8, k8s_delete and k8s_unapply (T2,
// rollback.go). The namespace and the Kafka cluster come
// from Env.Targets only: a kubernetes target "<context>/<namespace>" and a
// kafka target "strimzi://<name>".
package strimzi

import (
	"errors"
	"fmt"
	"regexp"
	"strings"
	"sync"

	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/plan"
	"piperoom/internal/setup/protocol"
)

// Target kinds and constants of the driver.
const (
	targetKubernetes = "kubernetes"
	targetKafka      = "kafka"
	strimziScheme    = "strimzi://"
	layer            = "kafka-connect"
	// PauseAnnotation is the annotation every chart shell carries until the
	// runner's last request of a k8s_apply removes it.
	PauseAnnotation = "strimzi.io/pause-reconciliation"
	// apiGroupPath is the Strimzi v1 API (Strimzi 1.0.0 and newer, §2).
	apiGroupPath = "/apis/kafka.strimzi.io/v1"
	// maxReplicas caps spec.replicas, and therefore the pod names the Role
	// lists (§4).
	maxReplicas = 3
)

// Errors callers branch on with errors.Is.
var (
	// ErrParams: params do not decode or fail a bound.
	ErrParams = ops.ErrParams
	// ErrName: a name outside the chart-fixed set.
	ErrName = errors.New("strimzi: not one of the chart's names")
	// ErrKind: a kind or template outside the allow-list.
	ErrKind = errors.New("strimzi: kind or template not allowed")
	// ErrField: a rendered field outside the allow-list (the error names its path).
	ErrField = errors.New("strimzi: field not allowed")
	// ErrTarget: the room registered no kubernetes or kafka target.
	ErrTarget = ops.ErrTarget
	// ErrReadBack: the stored object differs from the rendered one after the
	// write; the object stays paused.
	ErrReadBack = errors.New("strimzi: the stored spec differs from the rendered spec; the object stays paused")
	// ErrNoSecrets: k8s_secret_apply has no secret store to materialise from.
	ErrNoSecrets = errors.New("strimzi: no secret store on this runner")
)

// nameRE is a Kubernetes object name segment the chart uses (DNS-1123 label).
var nameRE = regexp.MustCompile(`^[a-z0-9]([a-z0-9-]{0,51}[a-z0-9])?$`)

// Options configures a Driver. Pipeline and Connectors are the chart values
// the Job was started with (setup.runner.pipeline, setup.runner.connectors):
// they fix every name the runner may reach.
type Options struct {
	Pipeline   string
	Connectors []string
	// Class is the class of the Kubernetes target; "" means shared (a
	// cluster is never local, §7).
	Class string
	// API is the Kubernetes API client; required (InClusterConfig + NewAPI
	// in the Job, an httptest server in tests).
	API *API
	// PluginHosts are the hosts a build.plugins artifact may come from
	// (PIPEROOM_SETUP_PLUGIN_HOSTS); empty means repo1.maven.org.
	PluginHosts []string
	// Secret resolves a ${secret:name} reference from the runner's secret
	// store for k8s_secret_apply. nil makes that op refuse with ErrNoSecrets.
	Secret func(name string) (string, bool)
}

// Driver holds the target's options and names.
type Driver struct {
	api        *API
	pipeline   string
	connectors []string
	secret     func(string) (string, bool)
	// class is the Kubernetes target's class: Options.Class at
	// registration, raised (never lowered) by every guard check to the
	// runner's class for the request (classGuard), so a target the room
	// raises to production later gets the production refusals at once.
	classMu sync.Mutex
	class   string
	plugins []string
	// written are the credential values k8s_secret_apply put in the
	// db-credentials Secret: the Connect workers resolve them, and a pod log
	// that echoes one is masked (podLog).
	writtenMu sync.Mutex
	written   []string
}

// rememberWritten records credential values the runner wrote.
func (d *Driver) rememberWritten(vs []string) {
	d.writtenMu.Lock()
	defer d.writtenMu.Unlock()
	for _, v := range vs {
		if len(v) >= 4 && !contains(d.written, v) {
			d.written = append(d.written, v)
		}
	}
}

func (d *Driver) writtenValues() []string {
	d.writtenMu.Lock()
	defer d.writtenMu.Unlock()
	return append([]string(nil), d.written...)
}

// pluginHosts are the allowed build artifact hosts.
func (d *Driver) pluginHosts() []string {
	if len(d.plugins) == 0 {
		return []string{"repo1.maven.org"}
	}
	return d.plugins
}

// Register adds the Strimzi ops to r.
func Register(r *ops.Registry, o Options) (*Driver, error) {
	if r == nil {
		return nil, errors.New("strimzi: a registry is required")
	}
	if o.API == nil {
		return nil, errors.New("strimzi: an API client is required")
	}
	if !nameRE.MatchString(o.Pipeline) || len(o.Pipeline) > 40 {
		return nil, fmt.Errorf("%w: pipeline %q is not a short DNS label", ErrParams, clipText(o.Pipeline, 64))
	}
	if len(o.Connectors) == 0 || len(o.Connectors) > 16 {
		return nil, fmt.Errorf("%w: 1 to 16 connector names are required (the chart's setup.runner.connectors)", ErrParams)
	}
	for _, c := range o.Connectors {
		if !nameRE.MatchString(c) {
			return nil, fmt.Errorf("%w: connector %q is not a DNS label", ErrParams, clipText(c, 64))
		}
	}
	switch o.Class {
	case "", protocol.ClassShared, protocol.ClassProduction:
	case protocol.ClassLocal:
		return nil, fmt.Errorf("%w: a Kubernetes target is never local (design §7)", ErrParams)
	default:
		return nil, fmt.Errorf("%w: unknown class %q", ErrParams, clipText(o.Class, 32))
	}
	d := &Driver{api: o.API, pipeline: o.Pipeline, connectors: append([]string(nil), o.Connectors...), class: o.Class, secret: o.Secret, plugins: o.PluginHosts}
	if d.class == "" {
		d.class = protocol.ClassShared
	}
	for _, op := range d.ops() {
		if err := r.Register(op); err != nil {
			return nil, fmt.Errorf("strimzi: registering %s: %w", op.Name(), err)
		}
	}
	return d, nil
}

func (d *Driver) ops() []ops.Op {
	return []ops.Op{
		k8sGetOp{d: d}, k8sEventsOp{d: d}, k8sLogsOp{d: d}, buildStatusOp{d: d},
		k8sApplyOp{d: d}, previewApplyOp{d: d}, k8sSecretApplyOp{d: d}, previewSecretApplyOp{d: d},
		k8sDeleteOp{d: d}, k8sUnapplyOp{d: d},
	}
}

// currentClass is the target's class now.
func (d *Driver) currentClass() string {
	d.classMu.Lock()
	defer d.classMu.Unlock()
	return d.class
}

// production reports whether the target is production now.
func (d *Driver) production() bool { return d.currentClass() == protocol.ClassProduction }

// classGuard is every Strimzi op's ops.ClassGuarder: it raises the driver's
// class to the runner's class for the request (an unknown class changes
// nothing; a class only rises), then asks the server's guard table
// (plan.Forbidden) on the result. k8s_unapply's production row depends on
// the kind it unapplies, which this check cannot see: Execute refuses those
// kinds itself on the raised class (rollback.go), as it does for every other
// production refusal.
func (d *Driver) classGuard(op, class string) bool {
	d.classMu.Lock()
	if r, ok := plan.ClassRank(class); ok {
		if cur, _ := plan.ClassRank(d.class); r > cur {
			d.class = class
		}
	}
	cur := d.class
	d.classMu.Unlock()
	if op == plan.OpK8sUnapply {
		return false
	}
	_, forbidden := plan.Forbidden(plan.GuardRequest{Op: op, Class: cur})
	return forbidden
}

// GuardForbidden is ops.ClassGuarder for each Strimzi op.
func (o k8sGetOp) GuardForbidden(c string) bool             { return o.d.classGuard(o.Name(), c) }
func (o k8sEventsOp) GuardForbidden(c string) bool          { return o.d.classGuard(o.Name(), c) }
func (o k8sLogsOp) GuardForbidden(c string) bool            { return o.d.classGuard(o.Name(), c) }
func (o buildStatusOp) GuardForbidden(c string) bool        { return o.d.classGuard(o.Name(), c) }
func (o k8sApplyOp) GuardForbidden(c string) bool           { return o.d.classGuard(o.Name(), c) }
func (o previewApplyOp) GuardForbidden(c string) bool       { return o.d.classGuard(o.Name(), c) }
func (o k8sSecretApplyOp) GuardForbidden(c string) bool     { return o.d.classGuard(o.Name(), c) }
func (o previewSecretApplyOp) GuardForbidden(c string) bool { return o.d.classGuard(o.Name(), c) }
func (o k8sDeleteOp) GuardForbidden(c string) bool          { return o.d.classGuard(o.Name(), c) }
func (o k8sUnapplyOp) GuardForbidden(c string) bool         { return o.d.classGuard(o.Name(), c) }

// ---- names (§3, §4) ----

func (d *Driver) connectName() string     { return d.pipeline + "-connect" }
func (d *Driver) canaryTopicName() string { return d.pipeline + "-canary" }
func (d *Driver) dbSecretName() string    { return d.pipeline + "-db-credentials" }
func (d *Driver) loggingName() string     { return d.pipeline + "-connect-logging" }
func (d *Driver) metricsName() string     { return d.pipeline + "-connect-metrics" }
func (d *Driver) pushSecretName() string  { return d.pipeline + "-registry-push" }
func (d *Driver) readUserName() string    { return d.pipeline + "-piperoom-ro" }

// podNames are the Connect pods (spec.replicas ≤ 3) and the build pod.
func (d *Driver) podNames() []string {
	c := d.connectName() + "-connect"
	return []string{c + "-0", c + "-1", c + "-2", c + "-build"}
}

// kinds maps a k8s_get kind to its API plural and whether it is a Strimzi CR.
var kinds = map[string]struct {
	plural  string
	strimzi bool
}{
	"Kafka":          {"kafkas", true},
	"KafkaConnect":   {"kafkaconnects", true},
	"KafkaConnector": {"kafkaconnectors", true},
	"KafkaTopic":     {"kafkatopics", true},
	"KafkaUser":      {"kafkausers", true},
	"Pod":            {"pods", false},
}

// allowedNames are the chart-fixed names of one kind; kafka is the room's
// Kafka cluster.
func (d *Driver) allowedNames(kind, kafka string) []string {
	switch kind {
	case "Kafka":
		return []string{kafka}
	case "KafkaConnect":
		return []string{d.connectName()}
	case "KafkaConnector":
		return d.connectors
	case "KafkaTopic":
		return []string{d.canaryTopicName()}
	case "KafkaUser":
		return []string{d.connectName(), d.readUserName()}
	case "Pod":
		return d.podNames()
	}
	return nil
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// objectPath is the API path of one named object in ns.
func objectPath(ns, kind, name string) string {
	k := kinds[kind]
	if k.strimzi {
		return apiGroupPath + "/namespaces/" + ns + "/" + k.plural + "/" + name
	}
	return "/api/v1/namespaces/" + ns + "/" + k.plural + "/" + name
}

// coreObjectPath is the path of a core object (Secret, ConfigMap) in ns.
func coreObjectPath(ns, plural, name string) string {
	return "/api/v1/namespaces/" + ns + "/" + plural + "/" + name
}

// scope resolves the namespace and the Kafka cluster from Env.Targets only
// (board U12a): a kubernetes target "<context>/<namespace>" and a kafka target
// "strimzi://<name>". A room without them reaches nothing.
func scope(env ops.Env) (ns, kafka string, err error) {
	for _, t := range env.Targets {
		switch t.Kind {
		case targetKubernetes:
			if i := strings.LastIndex(t.Address, "/"); i >= 0 {
				ns = t.Address[i+1:]
			}
		case targetKafka:
			if strings.HasPrefix(t.Address, strimziScheme) {
				kafka = strings.TrimPrefix(t.Address, strimziScheme)
			}
		}
	}
	if ns == "" || !nameRE.MatchString(ns) {
		return "", "", fmt.Errorf("%w: the room registered no kubernetes target \"<context>/<namespace>\"", ErrTarget)
	}
	if kafka == "" || !nameRE.MatchString(kafka) {
		return "", "", fmt.Errorf("%w: the room registered no kafka target \"strimzi://<name>\"", ErrTarget)
	}
	return ns, kafka, nil
}

// targetsOf is a helper for tests and the registrar: the two targets a
// Strimzi room registers.
func targetsOf(context, ns, kafka string) []events.SetupTarget {
	return []events.SetupTarget{
		{Kind: targetKubernetes, Address: context + "/" + ns, Class: protocol.ClassShared},
		{Kind: targetKafka, Address: strimziScheme + kafka, Class: protocol.ClassShared},
	}
}
