package strimzi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"

	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

// The rollback ops of design §8 (with the tiers of §7): k8s_delete removes a
// KafkaConnector or the canary KafkaTopic by its chart-fixed name, and
// k8s_unapply puts a filled object back the way the chart created it (a CR
// re-paused with the shell's spec, the db-credentials Secret and the logging
// ConfigMap patched back to empty). Neither creates anything, and neither
// reaches the Kafka CR, the metrics ConfigMap or any name outside the chart's.

// ErrProduction: the op is refused on a production target by the guard rows
// of design §7 (the canary topic is data; stopping a Connect cluster is a
// person's change). It wraps ErrParams, so the runner refuses before any
// request.
var ErrProduction = fmt.Errorf("%w: refused on a production target (design §7)", ErrParams)

// deleteKinds are the kinds k8s_delete removes; the Role grants delete on
// these two only (§4).
var deleteKinds = []string{"KafkaConnector", "KafkaTopic"}

// unapplyKinds are the kinds k8s_unapply puts back (§7's rollback row).
var unapplyKinds = []string{"ConfigMap", "KafkaConnect", "KafkaUser", "Secret"}

// rollbackParams are the params of k8s_delete and k8s_unapply. Name is
// required by k8s_delete and by k8s_unapply of a KafkaUser (the chart has two);
// for the other unapply kinds it is optional and must be the one chart name.
type rollbackParams struct {
	Kind string `json:"kind"`
	Name string `json:"name,omitempty"`
	// AdmissionPolicyConfirmed is the card confirmation of D11 on a
	// production target, as for k8s_apply.
	AdmissionPolicyConfirmed bool `json:"admission_policy_confirmed,omitempty"`
}

func rollbackSchema(kinds []string, nameRequired bool) map[string]any {
	required := []string{"kind"}
	if nameRequired {
		required = append(required, "name")
	}
	return map[string]any{"type": "object", "additionalProperties": false, "required": required,
		"properties": map[string]any{
			"kind":                       map[string]any{"type": "string", "enum": append([]string(nil), kinds...)},
			"name":                       map[string]any{"type": "string", "pattern": nameRE.String()},
			"admission_policy_confirmed": map[string]any{"type": "boolean"},
		}}
}

// rollbackRecord is how a preview names the op that undoes it, "<op> {json
// params}": the shape the room parses into the rollback proposal's step.
func rollbackRecord(op, kind, name string) string {
	b, _ := json.Marshal(rollbackParams{Kind: kind, Name: name})
	return op + " " + string(b)
}

// ---- k8s_delete (T2) ----

type k8sDeleteOp struct{ d *Driver }

func (k8sDeleteOp) Name() string   { return "k8s_delete" }
func (k8sDeleteOp) Target() string { return targetKubernetes }
func (k8sDeleteOp) Tier() string   { return protocol.TierT2 }
func (k8sDeleteOp) Layer() string  { return layer }
func (k8sDeleteOp) Describe() string {
	return "roll back a KafkaConnector or the canary KafkaTopic: delete it by its chart-fixed name with foreground propagation (the Topic Operator then deletes the topic); refused for any other kind or name, for an object the pipeline release does not own, and for both kinds on a production target"
}
func (k8sDeleteOp) ParamsSchema() map[string]any { return rollbackSchema(deleteKinds, true) }

func (o k8sDeleteOp) decode(params json.RawMessage) (rollbackParams, error) {
	var p rollbackParams
	if err := decode(params, &p); err != nil {
		return p, err
	}
	if !contains(deleteKinds, p.Kind) {
		return p, fmt.Errorf("%w: kind %q (k8s_delete deletes %s only)", ErrKind, clipText(p.Kind, 32), strings.Join(deleteKinds, ", "))
	}
	// Neither kind's names depend on the Kafka cluster, so the check needs
	// no target and runs before any request.
	if !contains(o.d.allowedNames(p.Kind, ""), p.Name) {
		return p, fmt.Errorf("%w: %s %q", ErrName, p.Kind, clipText(p.Name, 64))
	}
	if o.d.class == protocol.ClassProduction {
		if p.Kind == "KafkaTopic" {
			return p, fmt.Errorf("%w: the canary topic is data and is kept", ErrProduction)
		}
		// Parity with connect_delete_connector, which the guard refuses on
		// production unconditionally: a connector is deleted by a person.
		return p, fmt.Errorf("%w: deleting a connector on production is a person's change", ErrProduction)
	}
	return p, nil
}

func (o k8sDeleteOp) Validate(params json.RawMessage, _ ops.Env) error {
	_, err := o.decode(params)
	return err
}

func (o k8sDeleteOp) Execute(ctx context.Context, params json.RawMessage, env ops.Env) (ops.Result, error) {
	p, err := o.decode(params)
	if err != nil {
		return ops.Result{}, err
	}
	ns, _, err := scope(env)
	if err != nil {
		return ops.Result{}, err
	}
	if err := o.d.productionGate(ctx, p.AdmissionPolicyConfirmed); err != nil {
		return ops.Result{}, err
	}
	path := objectPath(ns, p.Kind, p.Name)
	rec := httpReceipt("DELETE", path)
	data := map[string]any{"kind": p.Kind, "name": p.Name}
	body, err := o.d.api.Get(ctx, path)
	switch {
	case errors.Is(err, ErrNotFound):
		data["deleted"] = false
		return ops.Result{Summary: fmt.Sprintf("%s %s/%s is already absent", p.Kind, ns, p.Name), Data: data, Receipt: httpReceipt("GET", path)}, nil
	case err != nil:
		return ops.Result{Receipt: httpReceipt("GET", path)}, err
	}
	var obj map[string]any
	if err := json.Unmarshal(body, &obj); err != nil {
		return ops.Result{Receipt: httpReceipt("GET", path)}, fmt.Errorf("strimzi: decoding %s %s: %w", p.Kind, p.Name, err)
	}
	if err := o.d.checkOwned(obj, p.Kind, p.Name); err != nil {
		return ops.Result{Receipt: httpReceipt("GET", path)}, err
	}
	if p.Kind == "KafkaTopic" {
		// Only the canary is deleted: the Topic Operator deletes the Kafka
		// topic the CR manages, so a CR that manages another topic is kept.
		st, _ := obj["status"].(map[string]any)
		if tn, _ := st["topicName"].(string); tn != "" && !strings.HasSuffix(tn, "."+CanaryTable) {
			return ops.Result{Receipt: httpReceipt("GET", path)}, fmt.Errorf("%w: KafkaTopic %s manages topic %s, not a canary", ErrField, p.Name, clipText(tn, 128))
		}
	}
	if err := o.d.api.Delete(ctx, path); err != nil {
		if errors.Is(err, ErrNotFound) {
			// A rollback that runs again after a partial one: the object is
			// already gone, which is the state the rollback wants.
			data["deleted"] = false
			return ops.Result{Summary: fmt.Sprintf("%s %s/%s is already absent", p.Kind, ns, p.Name), Data: data, Receipt: rec}, nil
		}
		return ops.Result{Receipt: rec}, err
	}
	data["deleted"] = true
	summary := fmt.Sprintf("%s %s/%s deleted", p.Kind, ns, p.Name)
	if p.Kind == "KafkaTopic" {
		summary += "; the Topic Operator deletes the Kafka topic"
	}
	return ops.Result{Summary: summary, Data: data, Receipt: rec}, nil
}

// ---- k8s_unapply (T2) ----

type k8sUnapplyOp struct{ d *Driver }

func (k8sUnapplyOp) Name() string   { return "k8s_unapply" }
func (k8sUnapplyOp) Target() string { return targetKubernetes }
func (k8sUnapplyOp) Tier() string   { return protocol.TierT2 }
func (k8sUnapplyOp) Layer() string  { return layer }
func (k8sUnapplyOp) Describe() string {
	return "roll back a k8s_apply or k8s_secret_apply: re-pause the KafkaConnect or a KafkaUser with the chart shell's spec, or patch the db-credentials Secret or the logging ConfigMap back to empty, under a resourceVersion test; the objects stay for helm uninstall"
}
func (k8sUnapplyOp) ParamsSchema() map[string]any { return rollbackSchema(unapplyKinds, false) }

// unapplyTarget is what one k8s_unapply writes.
type unapplyTarget struct {
	kind, name string
	cr         bool // a Strimzi CR: re-pause and replace /spec
}

func (o k8sUnapplyOp) decode(params json.RawMessage) (rollbackParams, unapplyTarget, error) {
	var p rollbackParams
	if err := decode(params, &p); err != nil {
		return p, unapplyTarget{}, err
	}
	var names []string
	cr := false
	switch p.Kind {
	case "KafkaConnect":
		names, cr = []string{o.d.connectName()}, true
	case "KafkaUser":
		names, cr = []string{o.d.connectName(), o.d.readUserName()}, true
	case "Secret":
		names = []string{o.d.dbSecretName()}
	case "ConfigMap":
		// The logging ConfigMap only: the metrics ConfigMap is fixed content
		// the runner never writes.
		names = []string{o.d.loggingName()}
	default:
		return p, unapplyTarget{}, fmt.Errorf("%w: kind %q (k8s_unapply puts back %s only)", ErrKind, clipText(p.Kind, 32), strings.Join(unapplyKinds, ", "))
	}
	name := p.Name
	if name == "" {
		if len(names) != 1 {
			return p, unapplyTarget{}, fmt.Errorf("%w: k8s_unapply %s needs the name (%s)", ErrParams, p.Kind, strings.Join(names, " or "))
		}
		name = names[0]
	}
	if !contains(names, name) {
		return p, unapplyTarget{}, fmt.Errorf("%w: %s %q", ErrName, p.Kind, clipText(name, 64))
	}
	if o.d.class == protocol.ClassProduction {
		switch p.Kind {
		case "KafkaConnect":
			return p, unapplyTarget{}, fmt.Errorf("%w: stopping a Connect cluster on production is a person's change", ErrProduction)
		case "Secret":
			// Emptying the credentials Secret under a running production
			// connector stops it at its next restart: a person's change.
			return p, unapplyTarget{}, fmt.Errorf("%w: emptying the db-credentials Secret on production is a person's change", ErrProduction)
		}
	}
	return p, unapplyTarget{kind: p.Kind, name: name, cr: cr}, nil
}

func (o k8sUnapplyOp) Validate(params json.RawMessage, _ ops.Env) error {
	_, _, err := o.decode(params)
	return err
}

// shellSpec is the spec the pipeline chart gives a CR shell (deploy/helm
// setup-pipeline.yaml; chart_names_test pins the two): the smallest spec the
// v1 schema accepts.
func shellSpec(kind, kafka string) map[string]any {
	if kind == "KafkaConnect" {
		return map[string]any{"replicas": 1, "bootstrapServers": kafka + "-kafka-bootstrap:9092"}
	}
	return map[string]any{}
}

func (o k8sUnapplyOp) Execute(ctx context.Context, params json.RawMessage, env ops.Env) (ops.Result, error) {
	p, t, err := o.decode(params)
	if err != nil {
		return ops.Result{}, err
	}
	ns, kafka, err := scope(env)
	if err != nil {
		return ops.Result{}, err
	}
	if err := o.d.productionGate(ctx, p.AdmissionPolicyConfirmed); err != nil {
		return ops.Result{}, err
	}
	var path string
	switch t.kind {
	case "Secret":
		path = coreObjectPath(ns, "secrets", t.name)
	case "ConfigMap":
		path = coreObjectPath(ns, "configmaps", t.name)
	default:
		path = objectPath(ns, t.kind, t.name)
	}
	rec := httpReceipt("PATCH", path)
	body, err := o.d.api.Get(ctx, path)
	if err != nil {
		return ops.Result{Receipt: rec}, err
	}
	var obj map[string]any
	if err := json.Unmarshal(body, &obj); err != nil {
		return ops.Result{Receipt: rec}, fmt.Errorf("strimzi: decoding %s %s: %w", t.kind, t.name, err)
	}
	st, err := parseState(body, "/spec")
	if err != nil {
		return ops.Result{Receipt: rec}, err
	}
	if err := o.d.checkOwned(obj, t.kind, t.name); err != nil {
		return ops.Result{Receipt: rec}, err
	}
	patch := []PatchOp{{Op: "test", Path: "/metadata/resourceVersion", Value: st.rv}}
	if t.cr {
		md, _ := obj["metadata"].(map[string]any)
		if _, ok := md["annotations"].(map[string]any); ok {
			patch = append(patch, PatchOp{Op: "add", Path: "/metadata/annotations/" + escapePointer(PauseAnnotation), Value: "true"})
		} else {
			// No annotations left: a JSON patch cannot add a key under a
			// missing parent, so the map is added whole.
			patch = append(patch, PatchOp{Op: "add", Path: "/metadata/annotations", Value: map[string]any{PauseAnnotation: "true"}})
		}
		patch = append(patch, PatchOp{Op: "replace", Path: "/spec", Value: shellSpec(t.kind, kafka)})
	} else {
		// "add" replaces /data when it is there and creates it when it is
		// not (an empty chart object has none).
		patch = append(patch, PatchOp{Op: "add", Path: "/data", Value: map[string]any{}})
	}
	out, err := o.d.api.Patch(ctx, path, patch, false)
	if err != nil {
		return ops.Result{Receipt: rec}, err
	}
	// The PATCH answers the stored object: the rollback is done only when it
	// says so (paused, or no data left). Values are never read into Data.
	got, err := parseState(out, "/data")
	if err != nil {
		return ops.Result{Receipt: rec}, err
	}
	data := map[string]any{"kind": t.kind, "name": t.name}
	var summary string
	if t.cr {
		if !got.paused {
			return ops.Result{Receipt: rec}, fmt.Errorf("%w: %s %s is not paused after the rollback patch", ErrReadBack, t.kind, t.name)
		}
		data["paused"] = true
		summary = fmt.Sprintf("%s %s/%s re-paused with the chart shell's spec", t.kind, ns, t.name)
		if t.kind == "KafkaConnect" {
			summary += "; the operator leaves its pods until helm uninstall of the pipeline release, and the built image stays in the registry"
		}
	} else {
		if left, _ := got.at.(map[string]any); len(left) > 0 {
			keys := make([]string, 0, len(left))
			for k := range left {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			return ops.Result{Receipt: rec}, fmt.Errorf("%w: %s %s still holds %s", ErrReadBack, t.kind, t.name, clipText(strings.Join(keys, ", "), 200))
		}
		summary = fmt.Sprintf("%s %s/%s patched back to empty", t.kind, ns, t.name)
	}
	return ops.Result{Summary: summary, Data: data, Receipt: rec}, nil
}

// ---- preview_k8s_secret_apply (T0) ----

// previewSecretApplyOp is k8s_secret_apply's preview (design §5: key names
// only). It reads the Secret's current key names and never a value, and
// records the rollback k8s_unapply {"kind":"Secret"}.
type previewSecretApplyOp struct{ d *Driver }

func (previewSecretApplyOp) Name() string   { return "preview_k8s_secret_apply" }
func (previewSecretApplyOp) Target() string { return targetKubernetes }
func (previewSecretApplyOp) Tier() string   { return protocol.TierT0 }
func (previewSecretApplyOp) Layer() string  { return layer }
func (previewSecretApplyOp) Describe() string {
	return "preview a k8s_secret_apply: the key names the db-credentials Secret holds now and the ones it would hold; never a value"
}
func (previewSecretApplyOp) ParamsSchema() map[string]any { return k8sSecretApplyOp{}.ParamsSchema() }
func (previewSecretApplyOp) Validate(params json.RawMessage, _ ops.Env) error {
	_, err := decodeSecretApply(params)
	return err
}

func (o previewSecretApplyOp) Execute(ctx context.Context, params json.RawMessage, env ops.Env) (ops.Result, error) {
	pr, err := k8sSecretApplyOp(o).Preview(ctx, params, env)
	if err != nil {
		return ops.Result{}, err
	}
	return ops.Result{Summary: "preview of " + pr.Target, Data: pr}, nil
}

// Preview is k8s_secret_apply's preview: key names before and after.
func (o k8sSecretApplyOp) Preview(ctx context.Context, params json.RawMessage, env ops.Env) (events.PreviewResult, error) {
	p, err := decodeSecretApply(params)
	if err != nil {
		return events.PreviewResult{}, err
	}
	ns, _, err := scope(env)
	if err != nil {
		return events.PreviewResult{}, err
	}
	path := coreObjectPath(ns, "secrets", o.d.dbSecretName())
	body, err := o.d.api.Get(ctx, path)
	if err != nil {
		return events.PreviewResult{}, err
	}
	st, err := parseState(body, "/data")
	if err != nil {
		return events.PreviewResult{}, err
	}
	stored, _ := st.at.(map[string]any)
	current := make([]string, 0, len(stored))
	for k := range stored {
		current = append(current, k)
	}
	sort.Strings(current)
	next := make([]string, 0, len(p.Keys))
	for k := range p.Keys {
		next = append(next, k)
	}
	sort.Strings(next)
	keyList := func(keys []string) string {
		if len(keys) == 0 {
			return "no keys"
		}
		return "keys " + strings.Join(keys, ", ")
	}
	pr := events.PreviewResult{
		Target:     "Secret " + ns + "/" + o.d.dbSecretName(),
		Current:    keyList(current),
		Expected:   keyList(next) + " (values materialised from ${secret:} references on the runner)",
		Reversible: true,
		Rollback:   rollbackRecord("k8s_unapply", "Secret", o.d.dbSecretName()),
	}
	if o.d.class == protocol.ClassProduction {
		// k8s_unapply refuses the Secret on production (§7).
		pr.Reversible = false
		pr.Rollback = "emptying the db-credentials Secret on a production target is a person's change: kubectl edit, or helm uninstall of the pipeline release"
	}
	return pr, nil
}
