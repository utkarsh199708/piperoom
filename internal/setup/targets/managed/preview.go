package managed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"piperoom/internal/clients/aiven"
	"piperoom/internal/clients/confluentcloud"
	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

// The managed writes' previews (design §2.5: every write has a preview the
// room shows before approval and records the rollback from). Each write op
// that has one gets a T0 op "preview_<write>" (the name the server's action
// spec sends, setupactions.PreviewOpName) whose Data is the
// events.PreviewResult. A preview runs the write's own Validate and the
// reads the write makes before its provider call, never the call: no
// managed provider has a dry run that needs no materialised credential, so
// the "dry run" is those checks.

// previewer is a write op that can preview itself.
type previewer interface {
	ops.Op
	Preview(ctx context.Context, raw json.RawMessage, env ops.Env) (events.PreviewResult, error)
	hasPreview() bool
}

// previewOp is preview_<write>: T0, the write's Validate, its Preview.
type previewOp struct{ w previewer }

// PreviewName is the preview op of a managed write.
func PreviewName(write string) string { return "preview_" + write }

func (o previewOp) Name() string                 { return PreviewName(o.w.Name()) }
func (o previewOp) Target() string               { return o.w.Target() }
func (o previewOp) Layer() string                { return o.w.Layer() }
func (o previewOp) Tier() string                 { return protocol.TierT0 }
func (o previewOp) ParamsSchema() map[string]any { return o.w.ParamsSchema() }
func (o previewOp) Describe() string {
	return "preview " + o.w.Name() + ": the reads and checks it makes before its provider call, never the call; the rollback it records"
}

// TierFor keeps a preview at T0 on every class: it writes nothing.
func (o previewOp) TierFor(json.RawMessage) (string, error) { return protocol.TierT0, nil }

func (o previewOp) Validate(raw json.RawMessage, env ops.Env) error { return o.w.Validate(raw, env) }

func (o previewOp) Execute(ctx context.Context, raw json.RawMessage, env ops.Env) (ops.Result, error) {
	pr, err := o.w.Preview(ctx, raw, env)
	if err != nil {
		return ops.Result{Summary: "preview of " + o.w.Name() + " failed: " + clipText(err.Error(), 300)}, err
	}
	return ops.Result{Summary: "preview of " + o.w.Name() + " on " + pr.Target, Data: pr}, nil
}

// withPreviews adds a preview op after every write that has a preview.
func withPreviews(base []ops.Op) []ops.Op {
	out := append([]ops.Op(nil), base...)
	for _, op := range base {
		if w, ok := op.(previewer); ok && w.hasPreview() {
			out = append(out, previewOp{w: w})
		}
	}
	return out
}

func (o mskOp) hasPreview() bool    { return o.write && o.preview != nil }
func (o simpleOp) hasPreview() bool { return o.write && o.preview != nil }

// Preview is a Confluent or Aiven write's preview: the write's Validate,
// then its reads.
func (o simpleOp) Preview(ctx context.Context, raw json.RawMessage, env ops.Env) (events.PreviewResult, error) {
	if o.preview == nil {
		return events.PreviewResult{}, fmt.Errorf("managed: %s has no preview", o.name)
	}
	if err := o.Validate(raw, env); err != nil {
		return events.PreviewResult{}, err
	}
	p, err := o.preview(ctx, raw)
	if err != nil {
		return events.PreviewResult{}, err
	}
	p.Target = o.kind + ":" + o.addr
	return p, nil
}

// notReversible is the Rollback text of a managed write the runner has no
// op to undo: prose, so the room records no rollback step for it.
func notReversible(what, how string) string {
	return "not undone automatically: the runner has no op that deletes " + what + "; " + how
}

// ---- Confluent Cloud ----

func (d *ConfluentDriver) previewCreate(ctx context.Context, raw json.RawMessage) (events.PreviewResult, error) {
	var p CCCreateParams
	if err := decode(raw, &p); err != nil {
		return events.PreviewResult{}, err
	}
	if err := secretsResolve(d.secrets, p.Config); err != nil {
		return events.PreviewResult{}, err
	}
	pr := events.PreviewResult{
		Expected: fmt.Sprintf("validate the config of %s (%d keys) against plugin %s, then create the connector only if validation finds no errors; ${secret:} values are materialised on the runner at that call, the preview sends none",
			p.Name, len(p.Config), p.Plugin),
		// A managed connector carries no tag that proves this room made it,
		// so the catalogue has no Confluent delete op (design §4).
		Rollback: notReversible("a Confluent Cloud connector", "delete "+p.Name+" in the Confluent Cloud console or with `confluent connect cluster delete`"),
	}
	plugins, err := d.api.Plugins(ctx)
	if err != nil {
		return events.PreviewResult{}, err
	}
	if !slices.ContainsFunc(plugins, func(pl confluentcloud.Plugin) bool { return pl.Class == p.Plugin }) {
		return events.PreviewResult{}, fmt.Errorf("%w: plugin %s is not a managed plugin of cluster %s", ErrCheckFailed, clipText(p.Plugin, 128), d.cluster)
	}
	st, err := d.api.Status(ctx, p.Name)
	switch {
	case errors.Is(err, confluentcloud.ErrNotFound):
		pr.Current = "no connector named " + p.Name + " on " + d.address() + "; plugin " + p.Plugin + " is available (connector-plugins, status)"
	case err != nil:
		return events.PreviewResult{}, err
	default:
		pr.Current = "connector " + p.Name + " exists (" + clipText(st.State, 32) + "): the create fails as a conflict (status)"
	}
	return pr, nil
}

func (d *ConfluentDriver) previewACLs(ctx context.Context, raw json.RawMessage) (events.PreviewResult, error) {
	var p ACLParams
	if err := decode(raw, &p); err != nil {
		return events.PreviewResult{}, err
	}
	rows, err := ACLRows(p)
	if err != nil {
		return events.PreviewResult{}, paramErr(err)
	}
	if d.acl == nil {
		return events.PreviewResult{}, ErrNoACLKey
	}
	// The write's checks, in its order, so a preview never passes for rows
	// the write refuses.
	if err := d.checkPrincipal(p); err != nil {
		return events.PreviewResult{}, err
	}
	checked := []string{"principal " + p.Principal + " is the " + p.Template + " account entered on the runner"}
	if p.Connector != "" {
		id, err := d.api.ConnectorID(ctx, p.Connector)
		if err != nil {
			return events.PreviewResult{}, err
		}
		if p.Group != "connect-"+id {
			return events.PreviewResult{}, fmt.Errorf("%w: connector %s is %s, so its group is connect-%s, not %s", ErrGroupNotTheConnectors, p.Connector, id, id, clipText(p.Group, 64))
		}
		checked = append(checked, "group "+p.Group+" is connector "+p.Connector+"'s")
	}
	if p.Template == ACLTemplateConnector {
		if _, err := d.checkBareTopic(ctx, p); err != nil {
			return events.PreviewResult{}, err
		}
		checked = append(checked, "topic "+p.TopicPrefix+" is absent or holds only this account's ACLs")
	}
	lines := make([]string, 0, len(rows))
	for _, r := range rows {
		lines = append(lines, fmt.Sprintf("%s %s on %s %s %s", r.Permission, r.Operation, r.ResourceType, r.PatternType, r.ResourceName))
	}
	return events.PreviewResult{
		Current:  "checked: " + strings.Join(checked, "; "),
		Expected: fmt.Sprintf("%d ACL rows for %s: %s", len(rows), p.Principal, clipText(strings.Join(lines, "; "), 1500)),
		Rollback: notReversible("ACLs", "remove the rows with `confluent kafka acl delete` for "+p.Principal),
	}, nil
}

// ---- Aiven ----

func (d *AivenDriver) previewCreate(ctx context.Context, raw json.RawMessage) (events.PreviewResult, error) {
	var p AivenCreateParams
	if err := decode(raw, &p); err != nil {
		return events.PreviewResult{}, err
	}
	if err := secretsResolve(d.secrets, p.Config); err != nil {
		return events.PreviewResult{}, err
	}
	class := p.Config["connector.class"]
	plugins, err := d.api.Plugins(ctx)
	if err != nil {
		return events.PreviewResult{}, err
	}
	if !slices.ContainsFunc(plugins, func(pl aiven.Plugin) bool { return pl.Class == class }) {
		return events.PreviewResult{}, fmt.Errorf("%w: %s is not an available plugin on %s", ErrCheckFailed, clipText(class, 128), d.service)
	}
	pr := events.PreviewResult{
		Expected: fmt.Sprintf("connector %s (%s) created on %s with %d config keys; ${secret:} values are materialised on the runner at that call, the preview sends none", p.Name, class, d.service, len(p.Config)),
		Rollback: notReversible("an Aiven connector", "delete "+p.Name+" in the Aiven console or with `avn service connector delete "+d.service+" "+p.Name+"`"),
	}
	st, err := d.api.Status(ctx, p.Name)
	switch {
	case errors.Is(err, aiven.ErrNotFound):
		pr.Current = "no connector named " + p.Name + " on " + d.service + "; " + class + " is an available plugin"
	case err != nil:
		return events.PreviewResult{}, err
	default:
		pr.Current = "connector " + p.Name + " exists (" + clipText(st.State, 32) + "): the create fails as a conflict"
	}
	return pr, nil
}

func (d *AivenDriver) previewIntegration(ctx context.Context, raw json.RawMessage) (events.PreviewResult, error) {
	var p AivenIntegrationParams
	if err := decode(raw, &p); err != nil {
		return events.PreviewResult{}, err
	}
	src, err := d.api.ServiceNamed(ctx, p.SourceService)
	if err != nil {
		return events.PreviewResult{}, err
	}
	if src.Type != "kafka" {
		return events.PreviewResult{}, fmt.Errorf("%w: %s is a %s service, not kafka", ErrCheckFailed, p.SourceService, clipText(src.Type, 32))
	}
	return events.PreviewResult{
		Current:  "service " + p.SourceService + " is a kafka service (" + clipText(src.State, 32) + ")",
		Expected: "integration " + p.SourceService + " -> " + d.service + " (kafka_connect) created",
		Rollback: notReversible("a service integration", "delete it in the Aiven console or with `avn service integration-delete <integration id>`"),
	}, nil
}
