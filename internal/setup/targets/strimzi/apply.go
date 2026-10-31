package strimzi

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"

	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

// applyParams are k8s_apply's typed params: the kind, the template and the
// template's own params. The object name comes from the template.
type applyParams struct {
	Kind     string          `json:"kind"`
	Template string          `json:"template"`
	Params   json.RawMessage `json:"params"`
	// AdmissionPolicyConfirmed is the card confirmation of owner decision
	// D11 (§4) on a production target when the Role cannot read the
	// cluster-scoped policy binding. Only the server's card sets it.
	AdmissionPolicyConfirmed bool `json:"admission_policy_confirmed,omitempty"`
}

// applyKinds are the kinds k8s_apply writes; Secret is k8s_secret_apply's.
var applyKinds = []string{"ConfigMap", "KafkaConnect", "KafkaConnector", "KafkaTopic", "KafkaUser"}

func applySchema() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": false, "required": []string{"kind", "template", "params"},
		"properties": map[string]any{
			"kind":                       map[string]any{"type": "string", "enum": applyKinds},
			"template":                   map[string]any{"type": "string", "enum": templateNames()},
			"params":                     map[string]any{"type": "object"},
			"admission_policy_confirmed": map[string]any{"type": "boolean"},
		}}
}

func decodeApply(raw json.RawMessage) (applyParams, error) {
	var p applyParams
	if err := decode(raw, &p); err != nil {
		return p, err
	}
	if !contains(applyKinds, p.Kind) {
		return p, fmt.Errorf("%w: kind %q (k8s_apply writes %s; anything else is refused)", ErrKind, clipText(p.Kind, 32), strings.Join(applyKinds, ", "))
	}
	if len(bytes.TrimSpace(p.Params)) == 0 {
		p.Params = json.RawMessage(`{}`)
	}
	return p, nil
}

// pathOf is the object's API path.
func (d *Driver) pathOf(ns string, r rendered) string {
	if r.kind == "ConfigMap" {
		return coreObjectPath(ns, "configmaps", r.name)
	}
	return objectPath(ns, r.kind, r.name)
}

// objectState is what a GET of the target object says.
type objectState struct {
	rv     string
	paused bool
	at     any // the value at the render's path ("/spec" or "/data")
	raw    map[string]any
}

func (d *Driver) state(ctx context.Context, path, at string) (objectState, error) {
	body, err := d.api.Get(ctx, path)
	if err != nil {
		return objectState{}, err
	}
	return parseState(body, at)
}

func parseState(body []byte, at string) (objectState, error) {
	var obj map[string]any
	if err := json.Unmarshal(body, &obj); err != nil {
		return objectState{}, fmt.Errorf("strimzi: decoding an object: %w", err)
	}
	md, _ := obj["metadata"].(map[string]any)
	rv, _ := md["resourceVersion"].(string)
	ann, _ := md["annotations"].(map[string]any)
	paused := fmt.Sprint(ann[PauseAnnotation]) == "true"
	return objectState{rv: rv, paused: paused, at: obj[strings.TrimPrefix(at, "/")], raw: obj}, nil
}

// patchOps is step 1 of §5: test the resourceVersion, then replace the whole
// subtree (add for /data, which an empty ConfigMap lacks).
func patchOps(rv string, r rendered) []PatchOp {
	op := "replace"
	if r.path == "/data" {
		op = "add"
	}
	return []PatchOp{{Op: "test", Path: "/metadata/resourceVersion", Value: rv}, {Op: op, Path: r.path, Value: r.value}}
}

// canonical marshals v with sorted keys (encoding/json sorts map keys), so
// two equal objects compare equal as bytes.
func canonical(v any) []byte {
	b, _ := json.Marshal(v)
	var generic any
	if json.Unmarshal(b, &generic) != nil {
		return b
	}
	out, _ := json.Marshal(generic)
	return out
}

// productionGate is owner decision D11 (§4): on a production target the
// room's ValidatingAdmissionPolicy must be bound, with Deny, to this
// pipeline's policy. A readable binding is checked (its policyName is
// piperoom-pipeline-<pipeline> and its validationActions include Deny), and so
// is the policy when readable (it exists and fails closed). A 403 on either
// (the namespaced Role cannot read cluster-scoped objects) falls back to the
// card confirmation; a 404, a binding to another policy, a binding without
// Deny or a policy that fails open refuses. Any other error refuses too.
func (d *Driver) productionGate(ctx context.Context, confirmed bool) error {
	if !d.production() {
		return nil
	}
	name := "piperoom-pipeline-" + d.pipeline
	const vap = "/apis/admissionregistration.k8s.io/v1/"
	forbidden := func() error {
		if confirmed {
			return nil
		}
		return fmt.Errorf("%w: production target: the Role cannot read the admission policy binding; the card must confirm the policy is bound (D11)", ErrParams)
	}
	body, err := d.api.Get(ctx, vap+"validatingadmissionpolicybindings/"+name)
	switch {
	case errors.Is(err, ErrForbidden):
		return forbidden()
	case errors.Is(err, ErrNotFound):
		return fmt.Errorf("%w: production target: the admission policy binding %s is not installed (D11: required for production)", ErrParams, name)
	case err != nil:
		return err
	}
	var binding struct {
		Spec struct {
			PolicyName        string   `json:"policyName"`
			ValidationActions []string `json:"validationActions"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(body, &binding); err != nil {
		return fmt.Errorf("%w: production target: the admission policy binding %s does not decode", ErrParams, name)
	}
	if binding.Spec.PolicyName != name {
		return fmt.Errorf("%w: production target: the binding %s binds policy %q, not %s (D11)", ErrParams, name, clipText(binding.Spec.PolicyName, 64), name)
	}
	if !contains(binding.Spec.ValidationActions, "Deny") {
		return fmt.Errorf("%w: production target: the binding %s does not Deny (validationActions %v; D11)", ErrParams, name, binding.Spec.ValidationActions)
	}
	body, err = d.api.Get(ctx, vap+"validatingadmissionpolicies/"+name)
	switch {
	case errors.Is(err, ErrForbidden):
		return forbidden()
	case errors.Is(err, ErrNotFound):
		return fmt.Errorf("%w: production target: the binding %s names a policy that is not installed (D11)", ErrParams, name)
	case err != nil:
		return err
	}
	var policy struct {
		Spec struct {
			FailurePolicy string `json:"failurePolicy"`
		} `json:"spec"`
	}
	if err := json.Unmarshal(body, &policy); err != nil {
		return fmt.Errorf("%w: production target: the admission policy %s does not decode", ErrParams, name)
	}
	// An absent failurePolicy defaults to Fail in admissionregistration/v1.
	if fp := policy.Spec.FailurePolicy; fp != "" && fp != "Fail" {
		return fmt.Errorf("%w: production target: the admission policy %s fails open (failurePolicy %s; D11)", ErrParams, name, clipText(fp, 16))
	}
	return nil
}

// releaseName is the pipeline release that creates every object the runner
// writes or deletes (deploy/helm _setup_validate.tpl pins the name).
func (d *Driver) releaseName() string { return "piperoom-pipeline-" + d.pipeline }

// ErrNotOwned: the object is not the pipeline release's (a same-named object
// someone else created, or one another release owns); the runner never
// patches or deletes it.
var ErrNotOwned = errors.New("strimzi: the object is not the pipeline release's")

// checkOwned is the ownership check before every patch or delete: the object
// carries the chart's app.kubernetes.io/instance label naming the pipeline
// release and the meta.helm.sh/release-name annotation Helm writes on every
// object it installs, naming the same release (and, when Helm recorded it,
// the object's own namespace as the release's). A label alone is a string
// anyone who can create the object can copy; the annotation is what Helm
// itself checks before it adopts an object. A same-named object a person or
// another release created is refused before any write.
func (d *Driver) checkOwned(obj map[string]any, kind, name string) error {
	md, _ := obj["metadata"].(map[string]any)
	labels, _ := md["labels"].(map[string]any)
	ann, _ := md["annotations"].(map[string]any)
	want := d.releaseName()
	if fmt.Sprint(labels["app.kubernetes.io/instance"]) != want {
		return fmt.Errorf("%w: %s %s has no app.kubernetes.io/instance=%s label", ErrNotOwned, kind, name, want)
	}
	rel, ok := ann[helmReleaseName].(string)
	if !ok {
		return fmt.Errorf("%w: %s %s has no %s annotation: Helm did not install it", ErrNotOwned, kind, name, helmReleaseName)
	}
	if rel != want {
		return fmt.Errorf("%w: %s %s belongs to Helm release %s, not %s", ErrNotOwned, kind, name, clipText(rel, 64), want)
	}
	if relNS, ok := ann[helmReleaseNamespace].(string); ok {
		if ns, _ := md["namespace"].(string); ns != "" && relNS != ns {
			return fmt.Errorf("%w: %s %s belongs to a release in namespace %s", ErrNotOwned, kind, name, clipText(relNS, 64))
		}
	}
	return nil
}

// The annotations Helm writes on every object of a release.
const (
	helmReleaseName      = "meta.helm.sh/release-name"
	helmReleaseNamespace = "meta.helm.sh/release-namespace"
)

// ---- k8s_apply (T2) ----

type k8sApplyOp struct{ d *Driver }

func (k8sApplyOp) Name() string   { return "k8s_apply" }
func (k8sApplyOp) Target() string { return targetKubernetes }
func (k8sApplyOp) Tier() string   { return protocol.TierT2 }
func (k8sApplyOp) Layer() string  { return layer }
func (k8sApplyOp) Describe() string {
	return "write one chart-created object from a template: a JSON patch that replaces its spec (or data) under a resourceVersion test, a read-back that requires the stored spec to equal the rendered one, then the pause annotation removed last"
}
func (k8sApplyOp) ParamsSchema() map[string]any { return applySchema() }

// Validate renders and validates with no I/O; the namespace and Kafka name
// come from Env.Targets.
func (o k8sApplyOp) Validate(params json.RawMessage, env ops.Env) error {
	p, err := decodeApply(params)
	if err != nil {
		return err
	}
	ns, kafka, err := scope(env)
	if err != nil {
		// No targets yet: the template and params still check.
		ns, kafka = "validate", "validate"
	}
	_, err = o.d.renderTemplate(p.Kind, p.Template, kafka, ns, p.Params)
	return err
}

func (o k8sApplyOp) Execute(ctx context.Context, params json.RawMessage, env ops.Env) (ops.Result, error) {
	p, err := decodeApply(params)
	if err != nil {
		return ops.Result{}, err
	}
	ns, kafka, err := scope(env)
	if err != nil {
		return ops.Result{}, err
	}
	r, err := o.d.renderTemplate(p.Kind, p.Template, kafka, ns, p.Params)
	if err != nil {
		return ops.Result{}, err
	}
	if r, err = o.d.resolveFacts(ctx, r, ns, kafka, env); err != nil {
		return ops.Result{}, err
	}
	if err := o.d.productionGate(ctx, p.AdmissionPolicyConfirmed); err != nil {
		return ops.Result{}, err
	}
	path := o.d.pathOf(ns, r)
	rec := httpReceipt("PATCH", path)
	note, err := o.d.apply(ctx, path, r)
	if err != nil {
		return ops.Result{Receipt: rec}, err
	}
	summary := fmt.Sprintf("%s %s/%s written from template %s", r.kind, ns, r.name, p.Template)
	if r.pause {
		summary += " and unpaused"
	}
	if note != "" {
		summary += "; " + note
	}
	return ops.Result{Summary: summary, Data: map[string]any{"kind": r.kind, "name": r.name, "template": p.Template, "unpaused": r.pause}, Receipt: rec}, nil
}

// apply is §5's three requests (plus a re-pause first when the object is a
// live CR, so a half-written spec is never reconciled).
func (d *Driver) apply(ctx context.Context, path string, r rendered) (string, error) {
	st, err := d.state(ctx, path, r.path)
	if err != nil {
		return "", err
	}
	if err := d.checkOwned(st.raw, r.kind, r.name); err != nil {
		return "", err
	}
	if r.kind == "KafkaTopic" {
		if err := checkTopicName(st.raw, r); err != nil {
			return "", err
		}
	}
	if r.pause && !st.paused {
		// A re-apply of a live CR: pause it again before the spec changes.
		if _, err := d.api.Patch(ctx, path, []PatchOp{
			{Op: "test", Path: "/metadata/resourceVersion", Value: st.rv},
			{Op: "add", Path: "/metadata/annotations/" + escapePointer(PauseAnnotation), Value: "true"},
		}, false); err != nil {
			return "", fmt.Errorf("strimzi: pausing %s before the write: %w", r.name, err)
		}
		if st, err = d.state(ctx, path, r.path); err != nil {
			return "", err
		}
	}
	// The dry run is the normalised form the read-back compares against: a
	// default the CRD schema adds is not a difference.
	dry, err := d.api.Patch(ctx, path, patchOps(st.rv, r), true)
	if err != nil {
		return "", err
	}
	want, err := parseState(dry, r.path)
	if err != nil {
		return "", err
	}
	if _, err := d.api.Patch(ctx, path, patchOps(st.rv, r), false); err != nil {
		return "", err
	}
	got, err := d.state(ctx, path, r.path)
	if err != nil {
		return "", err
	}
	note := ""
	if !bytes.Equal(canonical(got.at), canonical(want.at)) {
		// The server's defaults may have moved between the dry run and the
		// write: preview again against the stored object; only a stored
		// spec that equals that new normalised form is accepted.
		again, err := d.api.Patch(ctx, path, patchOps(got.rv, r), true)
		if err != nil {
			return "", fmt.Errorf("%w (%v)", ErrReadBack, err)
		}
		re, err := parseState(again, r.path)
		if err != nil || !bytes.Equal(canonical(got.at), canonical(re.at)) {
			return "", fmt.Errorf("%w: %s %s", ErrReadBack, r.kind, r.name)
		}
		note = "re-previewed after a server default moved"
	}
	if r.pause {
		if _, err := d.api.Patch(ctx, path, []PatchOp{
			{Op: "test", Path: "/metadata/resourceVersion", Value: got.rv},
			{Op: "remove", Path: "/metadata/annotations/" + escapePointer(PauseAnnotation)},
		}, false); err != nil {
			return "", fmt.Errorf("strimzi: unpausing %s: %w", r.name, err)
		}
	}
	return note, nil
}

// escapePointer escapes a JSON pointer token (RFC 6901): ~ then /.
func escapePointer(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "~", "~0"), "/", "~1")
}

// Preview is step 1 with dryRun=All: the object as the API server would
// store it. preview_k8s_apply exposes it as a T0 op.
func (o k8sApplyOp) Preview(ctx context.Context, params json.RawMessage, env ops.Env) (events.PreviewResult, error) {
	p, err := decodeApply(params)
	if err != nil {
		return events.PreviewResult{}, err
	}
	ns, kafka, err := scope(env)
	if err != nil {
		return events.PreviewResult{}, err
	}
	r, err := o.d.renderTemplate(p.Kind, p.Template, kafka, ns, p.Params)
	if err != nil {
		return events.PreviewResult{}, err
	}
	if r, err = o.d.resolveFacts(ctx, r, ns, kafka, env); err != nil {
		return events.PreviewResult{}, err
	}
	path := o.d.pathOf(ns, r)
	st, err := o.d.state(ctx, path, r.path)
	if err != nil {
		return events.PreviewResult{}, err
	}
	dry, err := o.d.api.Patch(ctx, path, patchOps(st.rv, r), true)
	dr := &events.DryRunResult{Source: "kubernetes dryRun=All", OK: err == nil}
	expected := ""
	if err != nil {
		dr.Text = clipText(err.Error(), 2000)
	} else if want, perr := parseState(dry, r.path); perr == nil {
		b, _ := json.MarshalIndent(want.at, "", "  ")
		expected = clipText(string(b), 16<<10)
	}
	current, _ := json.MarshalIndent(st.at, "", "  ")
	reversible, rollback := o.d.applyRollback(r)
	return events.PreviewResult{
		Target: r.kind + " " + ns + "/" + r.name, Current: clipText(string(current), 16<<10), Expected: expected,
		Reversible: reversible, Rollback: rollback, DryRun: dr,
	}, nil
}

// applyRollback is the rollback a k8s_apply preview records (design §8), as
// "<op> {json params}" for the room's rollback proposal: k8s_delete for a
// KafkaConnector or the canary KafkaTopic, k8s_unapply for the rest. On a
// production target the ones the guard refuses there (§7, plan's guard rows)
// are not reversible by the runner: the canary topic is kept, and deleting the
// KafkaConnector or stopping the KafkaConnect is a person's change.
func (d *Driver) applyRollback(r rendered) (bool, string) {
	prod := d.production()
	switch r.kind {
	case "KafkaTopic":
		if prod {
			return false, "the canary topic is kept on a production target (it is data); helm uninstall of the pipeline release removes the KafkaTopic"
		}
		return true, rollbackRecord("k8s_delete", r.kind, r.name)
	case "KafkaConnector":
		if prod {
			return false, "deleting a connector on a production target is a person's change (as connect_delete_connector is): kubectl delete kafkaconnector, or helm uninstall of the pipeline release"
		}
		return true, rollbackRecord("k8s_delete", r.kind, r.name)
	case "KafkaConnect":
		if prod {
			return false, "stopping a Connect cluster on a production target is a person's change: helm uninstall of the pipeline release"
		}
	}
	return true, rollbackRecord("k8s_unapply", r.kind, r.name)
}

// previewApplyOp is preview_k8s_apply (T0): the dry run, never the write.
type previewApplyOp struct{ d *Driver }

func (previewApplyOp) Name() string   { return "preview_k8s_apply" }
func (previewApplyOp) Target() string { return targetKubernetes }
func (previewApplyOp) Tier() string   { return protocol.TierT0 }
func (previewApplyOp) Layer() string  { return layer }
func (previewApplyOp) Describe() string {
	return "preview a k8s_apply: the JSON patch with dryRun=All, the object as it would be stored"
}
func (previewApplyOp) ParamsSchema() map[string]any { return applySchema() }
func (o previewApplyOp) Validate(params json.RawMessage, env ops.Env) error {
	return k8sApplyOp(o).Validate(params, env)
}
func (o previewApplyOp) Execute(ctx context.Context, params json.RawMessage, env ops.Env) (ops.Result, error) {
	pr, err := k8sApplyOp(o).Preview(ctx, params, env)
	if err != nil {
		return ops.Result{}, err
	}
	return ops.Result{Summary: "preview of " + pr.Target, Data: pr}, nil
}

// ---- k8s_secret_apply (T2) ----

type k8sSecretApplyOp struct{ d *Driver }

type secretApplyParams struct {
	// Keys maps a Secret key to a ${secret:name} reference the runner
	// materialises from its store. A value is never a literal.
	Keys map[string]string `json:"keys"`
	// AdmissionPolicyConfirmed is the card confirmation of D11 on a
	// production target, as for k8s_apply. Only the server's card sets it.
	AdmissionPolicyConfirmed bool `json:"admission_policy_confirmed,omitempty"`
}

var (
	secretKeyRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,63}$`)
	secretRefRE = regexp.MustCompile(`^\$\{secret:([A-Za-z_][A-Za-z0-9_.-]{0,127})\}$`)
)

func (k8sSecretApplyOp) Name() string   { return "k8s_secret_apply" }
func (k8sSecretApplyOp) Target() string { return targetKubernetes }
func (k8sSecretApplyOp) Tier() string   { return protocol.TierT2 }
func (k8sSecretApplyOp) Layer() string  { return layer }
func (k8sSecretApplyOp) Describe() string {
	return "fill the chart's <pipeline>-db-credentials Secret from ${secret:name} references the runner materialises; values never leave the runner, the read-back compares them on the runner"
}

func (k8sSecretApplyOp) ParamsSchema() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": false, "required": []string{"keys"},
		"properties": map[string]any{"keys": map[string]any{"type": "object", "minProperties": 1, "maxProperties": 16,
			"additionalProperties": map[string]any{"type": "string", "pattern": secretRefRE.String()}},
			"admission_policy_confirmed": map[string]any{"type": "boolean"}}}
}

func decodeSecretApply(raw json.RawMessage) (secretApplyParams, error) {
	var p secretApplyParams
	if err := decode(raw, &p); err != nil {
		return p, err
	}
	if len(p.Keys) == 0 || len(p.Keys) > 16 {
		return p, fmt.Errorf("%w: 1 to 16 keys", ErrParams)
	}
	for k, v := range p.Keys {
		if !secretKeyRE.MatchString(k) {
			return p, fmt.Errorf("%w: key %q is not a Secret key", ErrParams, clipText(k, 64))
		}
		if !secretRefRE.MatchString(v) {
			// The value is never echoed: it may be a credential typed by mistake.
			return p, fmt.Errorf("%w: key %s: the value must be a ${secret:name} reference, never a literal", ErrParams, k)
		}
	}
	return p, nil
}

func (k8sSecretApplyOp) Validate(params json.RawMessage, _ ops.Env) error {
	_, err := decodeSecretApply(params)
	return err
}

func (o k8sSecretApplyOp) Execute(ctx context.Context, params json.RawMessage, env ops.Env) (ops.Result, error) {
	p, err := decodeSecretApply(params)
	if err != nil {
		return ops.Result{}, err
	}
	ns, _, err := scope(env)
	if err != nil {
		return ops.Result{}, err
	}
	if o.d.secret == nil {
		return ops.Result{}, ErrNoSecrets
	}
	if err := o.d.productionGate(ctx, p.AdmissionPolicyConfirmed); err != nil {
		return ops.Result{}, err
	}
	values := make(map[string]string, len(p.Keys))
	keys := make([]string, 0, len(p.Keys))
	for k, ref := range p.Keys {
		name := secretRefRE.FindStringSubmatch(ref)[1]
		v, ok := o.d.secret(name)
		if !ok {
			return ops.Result{}, fmt.Errorf("%w: secret %s is not in the runner's store", ErrParams, name)
		}
		values[k] = v
		keys = append(keys, k)
	}
	sort.Strings(keys)
	path := coreObjectPath(ns, "secrets", o.d.dbSecretName())
	rec := httpReceipt("PATCH", path)
	body, err := o.d.api.Get(ctx, path)
	if err != nil {
		return ops.Result{Receipt: rec}, err
	}
	st, err := parseState(body, "/data")
	if err != nil {
		return ops.Result{}, err
	}
	if err := o.d.checkOwned(st.raw, "Secret", o.d.dbSecretName()); err != nil {
		return ops.Result{Receipt: rec}, err
	}
	// data is emptied and stringData set in one patch, so no key of an
	// earlier fill survives.
	if _, err := o.d.api.Patch(ctx, path, []PatchOp{
		{Op: "test", Path: "/metadata/resourceVersion", Value: st.rv},
		{Op: "add", Path: "/data", Value: map[string]any{}},
		{Op: "add", Path: "/stringData", Value: values},
	}, false); err != nil {
		return ops.Result{Receipt: rec}, err
	}
	// Read-back on the runner: the stored data, base64, decodes to exactly
	// the materialised values. Neither side is ever put in Data.
	body, err = o.d.api.Get(ctx, path)
	if err != nil {
		return ops.Result{Receipt: rec}, err
	}
	got, err := parseState(body, "/data")
	if err != nil {
		return ops.Result{}, err
	}
	stored, _ := got.at.(map[string]any)
	if len(stored) != len(values) {
		return ops.Result{Receipt: rec}, fmt.Errorf("%w: the Secret holds %d keys, %d were written", ErrReadBack, len(stored), len(values))
	}
	for k, want := range values {
		enc, _ := stored[k].(string)
		dec, err := base64.StdEncoding.DecodeString(enc)
		if err != nil || string(dec) != want {
			return ops.Result{Receipt: rec}, fmt.Errorf("%w: key %s does not read back as written", ErrReadBack, k)
		}
	}
	return ops.Result{
		Summary: fmt.Sprintf("Secret %s/%s filled with %d keys (%s); values read back on the runner", ns, o.d.dbSecretName(), len(keys), strings.Join(keys, ", ")),
		Data:    map[string]any{"secret": o.d.dbSecretName(), "keys": keys},
		Receipt: rec,
	}, nil
}

// checkTopicName refuses a k8s_apply of the canary KafkaTopic when the stored
// object already reconciles another Kafka topic (status.topicName): the Topic
// Operator would not rename it, and its rollback would delete that topic.
//
// Whether a Kafka topic of the rendered name already exists outside Strimzi's
// management is not checkable from here: the runner's Role reads the one
// KafkaTopic by name and no Kafka admin API. The rendered name is pinned to
// "<prefix>.<schema>.piperoom_canary" (ValidateTopic), the table the setup
// itself creates, so only a canary of an earlier run of the same pipeline can
// share it; the guard keeps it on production (k8s_delete refuses there).
func checkTopicName(obj map[string]any, r rendered) error {
	st, _ := obj["status"].(map[string]any)
	stored, _ := st["topicName"].(string)
	want, _ := r.value["topicName"].(string)
	if stored != "" && stored != want {
		return fmt.Errorf("%w: KafkaTopic %s already manages topic %s, not %s", ErrField, r.name, clipText(stored, 128), clipText(want, 128))
	}
	return nil
}
