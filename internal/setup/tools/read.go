package setuptools

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"piperoom/internal/events"
	"piperoom/internal/setup/catalog"
	"piperoom/internal/setup/forward"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/remoteclients"
	"piperoom/internal/tools"
)

// readTimeout bounds one forwarded read (AGENTS.md §4 rule 4: 10 s for
// tools) when the call's context has no earlier deadline.
const readTimeout = 10 * time.Second

// toolNames maps an op to its tool name where the op's own name is taken by
// another tool of the room's registry.
var toolNames = map[string]string{
	// Q6a's net_probe probes from the server; this one from the runner.
	"net_probe": "runner_net_probe",
}

// ToolName returns the tool name the read tool of op registers under.
func ToolName(op string) string {
	if n, ok := toolNames[op]; ok {
		return n
	}
	return op
}

// Config configures the forwarding read tools.
type Config struct {
	// Executor reaches the room's runner (the room service's
	// protocol.RunnerExecutor). Nil builds tools that answer ErrNoRunner.
	Executor forward.Executor
	// RoomID binds the tools to one room. "" takes the room from each call's
	// context (room.RoomID).
	RoomID string
	// Catalogue lists the ops; nil means catalog.Builtin().
	Catalogue *catalog.Catalogue
}

// ReadTools returns one KindRead tool per T0 setup op of the catalogue, in
// catalogue order.
func ReadTools(cfg Config) ([]tools.Tool, error) {
	cat, err := catalogueOr(cfg.Catalogue)
	if err != nil {
		return nil, err
	}
	sender := forward.New(cfg.Executor, cfg.RoomID)
	var out []tools.Tool
	for _, s := range cat.Reads() {
		out = append(out, &readTool{spec: s, name: ToolName(s.Name), sender: sender})
	}
	return out, nil
}

// RegisterReads adds the read tools to reg. reg should be a read-only
// registry (agent.Deps.Tools is one); Register refuses anything that is not
// KindRead there.
func RegisterReads(reg *tools.Registry, cfg Config) error {
	ts, err := ReadTools(cfg)
	if err != nil {
		return err
	}
	for _, t := range ts {
		if err := reg.Register(t); err != nil {
			return fmt.Errorf("setuptools: %w", err)
		}
	}
	return nil
}

// readTool forwards one T0 op to the runner.
type readTool struct {
	spec   catalog.Spec
	name   string
	sender *forward.Sender
}

var _ tools.Tool = (*readTool)(nil)

func (t *readTool) Name() string { return t.name }

func (t *readTool) Description() string {
	return t.spec.Describe + ". Runs read-only on the paired setup runner's machine (its project directory, its Docker, the targets the room registered) and changes nothing."
}

// InputSchema is the op's schema with a required list, as tools.Tool asks.
func (t *readTool) InputSchema() map[string]any {
	s := t.spec.SchemaMap()
	if _, ok := s["required"]; !ok {
		s["required"] = []string{}
	}
	return s
}

func (t *readTool) Kind() tools.Kind { return tools.KindRead }

// Run checks input against the op's schema, sends it as one T0 request and
// returns the runner's redacted result. A runner refusal or failure is a tool
// error; bad input wraps tools.ErrBadInput.
func (t *readTool) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	if err := t.spec.Check(input); err != nil {
		return tools.Result{}, fmt.Errorf("setuptools: %s: %w: %w", t.name, tools.ErrBadInput, err)
	}
	roomID, err := t.sender.Room(ctx)
	if err != nil {
		return tools.Result{}, fmt.Errorf("setuptools: %s: %w", t.name, err)
	}
	params := bytes.TrimSpace(input)
	if len(params) == 0 {
		params = []byte(`{}`)
	}
	class := remoteclients.ClassFrom(ctx)
	if class == "" {
		// A read changes nothing, so its class decides no tier; the lowest
		// one never lowers the runner's own floor for the target's kind.
		class = protocol.ClassLocal
	}
	res, err := t.sender.Send(ctx, roomID, protocol.OpRequest{
		Op: t.spec.Name, Params: append(json.RawMessage(nil), params...), Tier: protocol.TierT0, Class: class,
	}, readTimeout)
	if err != nil {
		var refused *protocol.RefusedError
		if errors.As(err, &refused) && refused.Refusal.Reason == protocol.RefuseSchema {
			err = fmt.Errorf("%w: %w", tools.ErrBadInput, err)
		}
		return tools.Result{}, fmt.Errorf("setuptools: %s: %w", t.name, err)
	}
	summary, cut := tools.Truncate(res.Summary)
	out := tools.Result{Summary: summary, Truncated: cut || res.Truncated}
	if len(res.Data) > 0 {
		out.Data = append(json.RawMessage(nil), res.Data...)
	}
	if call, ok := provenance(t.spec.Name, res.Receipt); ok {
		out.Provenance = []tools.Call{call}
	}
	return out, nil
}

// provenance renders the runner's receipt as the tool's read receipt. Only
// the kinds tools.Call defines map; an exec receipt (docker, java) has no
// Call kind and stays on the runner's own op log.
func provenance(op string, r events.SetupReceipt) (tools.Call, bool) {
	kind := ""
	switch r.Kind {
	case "http":
		kind = tools.CallHTTP
	case "sql":
		kind = tools.CallSQL
	case "file":
		kind = tools.CallFile
	default:
		return tools.Call{}, false
	}
	text := op
	if len(r.Argv) > 0 {
		text = strings.Join(r.Argv, " ")
	}
	return tools.Call{Kind: kind, Target: r.Target, Text: text, At: r.At, TookMS: r.TookMS}, true
}

// catalogueOr returns cat, or the builtin catalogue when cat is nil.
func catalogueOr(cat *catalog.Catalogue) (*catalog.Catalogue, error) {
	if cat != nil {
		return cat, nil
	}
	return catalog.Builtin()
}
