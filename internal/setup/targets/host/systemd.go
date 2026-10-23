package host

import (
	"context"
	"errors"
	"fmt"
	"path"
	"regexp"
	"strings"
	"unicode/utf8"

	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
)

// serviceManager is the platform's service manager behind service_status,
// service_install and service_restart. systemdUser is the Linux one; row U8b
// adds the Windows one (svc/mgr, the service-host role) behind the same
// methods, so the ops, their params and their tiers do not change.
type serviceManager interface {
	// artifact renders what install registers (a unit file on Linux) so a
	// preview can diff it before anything runs.
	artifact(s serviceSpec) ([]byte, error)
	status(ctx context.Context, env ops.Env, unit string) (ops.Result, error)
	install(ctx context.Context, env ops.Env, s serviceSpec) (ops.Result, error)
	restart(ctx context.Context, env ops.Env, unit string) (ops.Result, error)
}

// systemctlBin is the one program this package runs. The AST test asserts
// every exec call names it.
const systemctlBin = "systemctl"

// maxOutputTail bounds the command output a failed op puts in Data.
const maxOutputTail = 4 << 10

// showProperties are the properties service_status reads, in this order.
var showProperties = []string{
	"LoadState", "ActiveState", "SubState", "UnitFileState", "Result",
	"MainPID", "ExecMainStatus", "NRestarts", "FragmentPath",
}

// fullUnitRE is a unit this driver may name: unitName's output only.
var fullUnitRE = regexp.MustCompile(`^piperoom-[a-z0-9](?:[a-z0-9-]{0,38}[a-z0-9])?\.service$`)

// systemctlVerbs is the fixed table of verbs: the flags each one carries
// (literals of this package) and what its one operand is.
var systemctlVerbs = map[string]struct {
	flags   []string
	operand string // "unit", "path" or "" (none)
}{
	"show":          {flags: []string{"--no-pager", "--property=" + strings.Join(showProperties, ",")}, operand: "unit"},
	"enable":        {operand: "path"},
	"daemon-reload": {},
	"restart":       {operand: "unit"},
}

// SystemctlArgv builds the one argv shape of this package:
//
//	systemctl --user <verb> [fixed flags] [operand]
//
// The verb comes from a fixed table; the operand is a piperoom unit name, or,
// for enable, the absolute path of a unit file in the project's unit
// directory. Nothing else reaches the argv and there is no shell.
func SystemctlArgv(verb, operand string) ([]string, error) {
	v, ok := systemctlVerbs[verb]
	if !ok {
		return nil, fmt.Errorf("%w: systemctl verb %s is not allowed", ErrParams, q(verb))
	}
	argv := append([]string{systemctlBin, "--user", verb}, v.flags...)
	switch v.operand {
	case "":
		if operand != "" {
			return nil, fmt.Errorf("%w: %s takes no operand", ErrParams, verb)
		}
		return argv, nil
	case "unit":
		if !fullUnitRE.MatchString(operand) {
			return nil, fmt.Errorf("%w: unit %s is not a piperoom unit name", ErrParams, q(operand))
		}
	case "path":
		if err := CheckUnitPath(operand); err != nil {
			return nil, err
		}
		if !fullUnitRE.MatchString(path.Base(operand)) || path.Base(path.Dir(operand)) != UnitDir {
			return nil, fmt.Errorf("%w: %s is not a unit file in the project's %s/ directory", ErrParams, q(operand), UnitDir)
		}
	}
	return append(argv, operand), nil
}

// cmdResult is one systemctl run.
type cmdResult struct {
	argv []string
	out  []byte
	code int
}

// runSystemctl runs one argv SystemctlArgv built, through the catalogue's
// injectable runner (argv slice, no shell, the request's deadline).
func runSystemctl(ctx context.Context, env ops.Env, argv []string) (cmdResult, error) {
	if len(argv) < 2 || argv[0] != systemctlBin {
		return cmdResult{}, errors.New("host: refusing a command that is not systemctl")
	}
	if env.Exec == nil {
		return cmdResult{}, errors.New("host: no command runner configured")
	}
	out, code, err := env.Exec(ctx, systemctlBin, argv[1:]...)
	res := cmdResult{argv: argv, out: out, code: code}
	if err != nil {
		return res, fmt.Errorf("%w: running systemctl: %w", ops.ErrExec, err)
	}
	return res, nil
}

// failed turns a non-zero exit into the op's error result: the first output
// line in the summary, the tail in Data, and a hint when the user's manager
// is not reachable at all.
func failed(what string, res cmdResult, unit string) (ops.Result, error) {
	summary := fmt.Sprintf("%s failed (exit %d)", what, res.code)
	if line := firstLine(res.out); line != "" {
		summary += ": " + line
	}
	if strings.Contains(string(res.out), "Failed to connect to bus") {
		summary += "; the user's systemd manager is not reachable: start the runner from a login session, or enable lingering for this user (loginctl enable-linger)"
	}
	return ops.Result{
			Summary: summary,
			Data:    map[string]any{"unit": unit, "argv": res.argv, "exit_code": res.code, "output": tail(res.out)},
			Receipt: events.SetupReceipt{Kind: "exec", Argv: res.argv, Target: unit, ExitCode: res.code},
		},
		fmt.Errorf("%w: %s exited %d", ops.ErrExec, what, res.code)
}

// systemdUser manages user-level units through `systemctl --user`.
type systemdUser struct{}

func (systemdUser) artifact(s serviceSpec) ([]byte, error) { return RenderUnit(s.Rendered) }

func (systemdUser) status(ctx context.Context, env ops.Env, unit string) (ops.Result, error) {
	argv, err := SystemctlArgv("show", unit)
	if err != nil {
		return ops.Result{}, err
	}
	res, err := runSystemctl(ctx, env, argv)
	if err != nil {
		return ops.Result{}, err
	}
	if res.code != 0 {
		return failed("systemctl show", res, unit)
	}
	st := parseShow(res.out)
	return ops.Result{
		Summary: statusSummary(unit, st),
		Data:    st.data(unit),
		Receipt: events.SetupReceipt{Kind: "exec", Argv: argv, Target: unit},
	}, nil
}

func (m systemdUser) install(ctx context.Context, env ops.Env, s serviceSpec) (ops.Result, error) {
	text, err := m.artifact(s)
	if err != nil {
		return ops.Result{}, err
	}
	clean, changed, err := writeConfined(env, s.UnitFile, text, 0o644, true)
	if err != nil {
		return ops.Result{}, err
	}
	enable, err := SystemctlArgv("enable", s.UnitPath)
	if err != nil {
		return ops.Result{}, err
	}
	reload, err := SystemctlArgv("daemon-reload", "")
	if err != nil {
		return ops.Result{}, err
	}
	// enable links the unit file into the user's search path and enables
	// it; daemon-reload makes a changed unit file the definition the next
	// (re)start uses even when the link already existed.
	for _, argv := range [][]string{enable, reload} {
		res, err := runSystemctl(ctx, env, argv)
		if err != nil {
			return ops.Result{}, err
		}
		if res.code != 0 {
			return failed(strings.Join(argv[:3], " "), res, s.Unit)
		}
	}
	state := "written"
	if !changed {
		state = "unchanged"
	}
	return ops.Result{
		Summary: fmt.Sprintf("installed %s (%s): unit file %s %s, enabled; service_restart starts it", s.Unit, s.Mode, clean, state),
		Data: map[string]any{
			"unit": s.Unit, "mode": s.Mode, "unit_file": clean, "changed": changed,
			"exec_start": s.Rendered.ExecStart, "working_directory": s.Rendered.WorkingDirectory,
			"commands": [][]string{enable, reload},
		},
		Receipt: events.SetupReceipt{Kind: "exec", Argv: enable, Target: s.Unit},
	}, nil
}

func (m systemdUser) restart(ctx context.Context, env ops.Env, unit string) (ops.Result, error) {
	// Only a unit this project installed: service_install wrote its file.
	if err := requireFile(env, UnitDir+"/"+unit, false); err != nil {
		return ops.Result{}, fmt.Errorf("%w; run service_install first", err)
	}
	argv, err := SystemctlArgv("restart", unit)
	if err != nil {
		return ops.Result{}, err
	}
	res, err := runSystemctl(ctx, env, argv)
	if err != nil {
		return ops.Result{}, err
	}
	receipt := events.SetupReceipt{Kind: "exec", Argv: argv, Target: unit, ExitCode: res.code}
	if res.code != 0 {
		out, ferr := failed("systemctl restart", res, unit)
		out.Summary += "; journal_tail shows why"
		return out, ferr
	}
	// The state right after the restart job; a JVM that dies a few seconds
	// later shows in a later service_status.
	// The restart itself succeeded, so a failed state read is reported, not
	// turned into a failed step.
	after, err := m.status(ctx, env, unit)
	data := map[string]any{"unit": unit, "argv": argv}
	summary := "restarted " + unit
	if err == nil {
		data["state"] = after.Data
		summary += "; now " + after.Summary
	} else {
		data["state_error"] = err.Error()
		summary += "; the state read after it failed: " + clipText(err.Error(), maxQuoted)
	}
	return ops.Result{Summary: summary, Data: data, Receipt: receipt}, nil
}

// unitState is the parsed `systemctl show` output.
type unitState map[string]string

// parseShow reads KEY=value lines; unknown keys are ignored and values are
// clipped, since the output is the host's text.
func parseShow(out []byte) unitState {
	want := make(map[string]bool, len(showProperties))
	for _, p := range showProperties {
		want[p] = true
	}
	st := unitState{}
	for _, line := range strings.Split(string(out), "\n") {
		k, v, ok := strings.Cut(strings.TrimRight(line, "\r"), "=")
		if ok && want[k] {
			st[k] = clipText(v, 256)
		}
	}
	return st
}

func (st unitState) data(unit string) map[string]any {
	return map[string]any{
		"unit": unit, "scope": "user",
		"installed":        st["LoadState"] == "loaded",
		"running":          st["ActiveState"] == "active" && st["SubState"] == "running",
		"load_state":       st["LoadState"],
		"active_state":     st["ActiveState"],
		"sub_state":        st["SubState"],
		"unit_file_state":  st["UnitFileState"],
		"result":           st["Result"],
		"main_pid":         st["MainPID"],
		"exec_main_status": st["ExecMainStatus"],
		"restarts":         st["NRestarts"],
		"fragment_path":    st["FragmentPath"],
	}
}

func statusSummary(unit string, st unitState) string {
	if st["LoadState"] == "not-found" || st["LoadState"] == "" {
		return unit + " is not installed"
	}
	s := fmt.Sprintf("%s: %s (%s)", unit, st["ActiveState"], st["SubState"])
	if st["UnitFileState"] != "" {
		s += ", " + st["UnitFileState"]
	}
	if r := st["Result"]; r != "" && r != "success" {
		s += ", last result " + r + ", exit status " + st["ExecMainStatus"]
	}
	return s
}

// firstLine is the first non-empty output line, clipped.
func firstLine(out []byte) string {
	for _, line := range strings.Split(string(out), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			return clipText(line, maxQuoted)
		}
	}
	return ""
}

// tail is the last maxOutputTail bytes of out, on a rune boundary.
func tail(out []byte) string {
	if len(out) <= maxOutputTail {
		return strings.ToValidUTF8(string(out), "?")
	}
	start := len(out) - maxOutputTail
	for start < len(out) && !utf8.RuneStart(out[start]) {
		start++
	}
	return "…" + strings.ToValidUTF8(string(out[start:]), "?")
}

// clipText is host text cut to max bytes with control characters replaced.
func clipText(s string, max int) string {
	s = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return '?'
		}
		return r
	}, strings.ToValidUTF8(s, "?"))
	if len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + "…"
}
