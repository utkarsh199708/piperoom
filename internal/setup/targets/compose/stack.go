package compose

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
)

// subcommandFlags is the one flag sequence each compose subcommand of this
// package may carry, on top of the pinned argv. The flags are literals this
// package chose, never request text: `-d` makes compose_up return instead of
// streaming logs, `--no-interpolate` keeps the ${KEY} references in the
// preview, and `ps --format json` / `ps -q` are the two read forms. There is
// deliberately no sequence for `down` that removes volumes (no `-v`).
var subcommandFlags = map[string][][]string{
	"up":      {{"-d"}},
	"down":    {{}},
	"restart": {{}},
	"ps":      {{"--format", "json"}, {"-q"}},
	"config":  {{"--no-interpolate"}},
}

// stackArgv builds the pinned compose argv with the op's fixed flags between
// the subcommand and the sorted service names:
//
//	docker compose --project-directory <dir> -f <file> --env-file <env> <sub> [flags] [services]
func stackArgv(projectDir, file, sub string, flags, services []string) ([]string, error) {
	if !allowedSubcommandFlags(sub, flags) {
		return nil, fmt.Errorf("%w: flags %s are not allowed for %s", ErrParams, q(strings.Join(flags, " ")), q(sub))
	}
	base, err := Argv(projectDir, file, EnvFileName, sub, nil)
	if err != nil {
		return nil, err
	}
	names := append([]string(nil), services...)
	sort.Strings(names)
	for _, name := range names {
		if !serviceNameRE.MatchString(name) {
			return nil, fmt.Errorf("%w: service %s is not a service name", ErrParams, q(name))
		}
	}
	out := append(base, flags...)
	return append(out, names...), nil
}

// allowedSubcommandFlags reports whether flags is one of the fixed sequences
// of sub.
func allowedSubcommandFlags(sub string, flags []string) bool {
	for _, allowed := range subcommandFlags[sub] {
		if strings.Join(allowed, "\x00") == strings.Join(flags, "\x00") {
			return true
		}
	}
	return false
}

// stackWriteOp is one T1 compose subcommand (up, restart, down).
type stackWriteOp struct {
	d        *Driver
	name     string
	sub      string
	describe string
	flags    []string
	// down marks compose_down: the result says the volumes were kept, and no
	// argv form this op can build removes them.
	down bool
}

func (o *stackWriteOp) Name() string     { return o.name }
func (o *stackWriteOp) Target() string   { return targetKind }
func (o *stackWriteOp) Tier() string     { return protocol.TierT1 }
func (o *stackWriteOp) Layer() string    { return layerName }
func (o *stackWriteOp) Describe() string { return o.describe }

func (o *stackWriteOp) ParamsSchema() map[string]any { return stackSchema() }

func (o *stackWriteOp) Validate(params json.RawMessage, env ops.Env) error {
	_, err := decodeStack(params)
	return err
}

func (o *stackWriteOp) Execute(ctx context.Context, params json.RawMessage, env ops.Env) (ops.Result, error) {
	p, err := decodeStack(params)
	if err != nil {
		return ops.Result{}, err
	}
	clean, err := stackFiles(env, p.File)
	if err != nil {
		return ops.Result{}, err
	}
	argv, err := stackArgv(env.Confine.Dir(), p.File, o.sub, o.flags, p.Services)
	if err != nil {
		return ops.Result{}, err
	}
	start := now(env)
	res, err := dockerCommand(ctx, env, argv)
	if err != nil {
		return ops.Result{}, err
	}
	receipt := events.SetupReceipt{Kind: "exec", Argv: argv, Target: clean, At: start, ExitCode: res.code}
	if res.code != 0 {
		return ops.Result{
			Summary:   failureSummary(o.name, res),
			Data:      map[string]any{"file": clean, "services": p.Services, "argv": argv, "exit_code": res.code, "output": tailText(res.out, maxFailureOutput)},
			Truncated: res.truncated,
			Receipt:   receipt,
		}, fmt.Errorf("%w: %s exited %d", ops.ErrExec, o.name, res.code)
	}
	data := map[string]any{"file": clean, "services": p.Services, "argv": argv}
	summary := o.successSummary(clean, p.Services)
	if o.down {
		// The promise the guard table relies on (plan.ReasonComposeDownVolumes):
		// the command removed containers and networks, never volumes.
		data["volumes_kept"] = true
	}
	return ops.Result{
		Summary:   summary,
		Data:      data,
		Truncated: res.truncated,
		Receipt:   receipt,
	}, nil
}

// successSummary is the one-line result of a successful compose write.
func (o *stackWriteOp) successSummary(file string, services []string) string {
	scope := file
	if len(services) > 0 {
		scope = strings.Join(services, ", ")
	}
	switch o.sub {
	case "up":
		return "started " + scope
	case "restart":
		return "restarted " + scope
	case "down":
		return "stopped the stack in " + file + "; volumes kept"
	}
	return o.name + " finished for " + scope
}
