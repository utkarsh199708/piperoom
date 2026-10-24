package host

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
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
	// complete completes s with what only the platform's manager knows (on
	// Windows: the service home, the release and the paths in it); Linux
	// returns s unchanged.
	complete(env ops.Env, s serviceSpec) (serviceSpec, error)
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

// SystemctlArgv is SystemctlArgvScope for a user unit, the default scope.
func SystemctlArgv(verb, operand string) ([]string, error) {
	return SystemctlArgvScope(ScopeUser, verb, operand)
}

// SystemctlArgvScope builds the one argv shape of this package:
//
//	systemctl --user <verb> [fixed flags] [operand]   (ScopeUser)
//	systemctl <verb> [fixed flags] [operand]          (ScopeSystem)
//
// The scope is a typed value the driver selected (selectScope), never a
// string from params. The verb comes from a fixed table; the operand is a
// piperoom unit name, or, for a user unit's enable, the absolute path of a
// unit file in the project's unit directory (a system unit's file is already
// in SystemUnitDir, so its enable names the unit). Nothing else reaches the
// argv and there is no shell.
func SystemctlArgvScope(scope UnitScope, verb, operand string) ([]string, error) {
	v, ok := systemctlVerbs[verb]
	if !ok {
		return nil, fmt.Errorf("%w: systemctl verb %s is not allowed", ErrParams, q(verb))
	}
	var argv []string
	switch scope {
	case ScopeUser:
		argv = append([]string{systemctlBin, "--user", verb}, v.flags...)
	case ScopeSystem:
		argv = append([]string{systemctlBin, verb}, v.flags...)
		if v.operand == "path" {
			v.operand = "unit"
		}
	default:
		return nil, fmt.Errorf("%w: unknown unit scope %d", ErrParams, int(scope))
	}
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

// systemdManager manages units through systemctl in the one scope the driver
// selected at Register (selectScope): user units through `systemctl --user`
// by default, system units through `systemctl` when the runner is root or the
// target class is production. status, install and restart all use that scope,
// so service_status and service_restart reach the unit service_install
// installed, never the other scope's unit of the same name.
type systemdManager struct {
	// resolve resolves symlinks (Options.ResolvePath), to match a
	// FragmentPath with this project's unit file.
	resolve func(string) (string, error)
	scope   UnitScope
	// unitDir is SystemUnitDir; tests point it at a temporary directory.
	unitDir string
	// euid is the runner's effective uid at Register.
	euid int
	// owner reads a file's owning uid (fileOwner); tests fake it, since an
	// unprivileged test cannot create root-owned files.
	owner func(fs.FileInfo) (uint64, bool)
}

func (systemdManager) artifact(s serviceSpec) ([]byte, error) { return RenderUnit(s.Rendered) }

func (systemdManager) complete(_ ops.Env, s serviceSpec) (serviceSpec, error) { return s, nil }

// argv is SystemctlArgvScope in the manager's scope.
func (m systemdManager) argv(verb, operand string) ([]string, error) {
	return SystemctlArgvScope(m.scope, verb, operand)
}

func (m systemdManager) status(ctx context.Context, env ops.Env, unit string) (ops.Result, error) {
	st, res, err := m.show(ctx, env, unit)
	if err != nil {
		return ops.Result{}, err
	}
	if res.code != 0 {
		return failed("systemctl show", res, unit)
	}
	data := st.data(unit, m.scope)
	summary := statusSummary(unit, st)
	// Unit names are per OS user, not per project: another project's
	// service_install may own this name. Say so rather than report its
	// worker as this project's.
	if own, want := m.ownFragment(env, unit, st); !own && st["FragmentPath"] != "" {
		data["this_project"] = false
		summary += fmt.Sprintf("; loaded from %s, not this project's %s", q(st["FragmentPath"]), q(want))
	} else {
		data["this_project"] = own
	}
	return ops.Result{
		Summary: summary,
		Data:    data,
		Receipt: events.SetupReceipt{Kind: "exec", Argv: res.argv, Target: unit},
	}, nil
}

// show runs `systemctl [--user] show` for unit and parses it.
func (m systemdManager) show(ctx context.Context, env ops.Env, unit string) (unitState, cmdResult, error) {
	argv, err := m.argv("show", unit)
	if err != nil {
		return nil, cmdResult{}, err
	}
	res, err := runSystemctl(ctx, env, argv)
	if err != nil {
		return nil, res, err
	}
	return parseShow(res.out), res, nil
}

// ownFragment reports whether systemd loads unit from this project's unit
// file, and returns that file's absolute path. A FragmentPath that is a
// symlink to it (a linked unit seen through the user's search path) counts.
// A system unit lives in the shared SystemUnitDir, where the file name alone
// does not say which project wrote it, so its file must also carry this
// project's WorkingDirectory line, the one the template renders.
func (m systemdManager) ownFragment(env ops.Env, unit string, st unitState) (bool, string) {
	if env.Confine == nil {
		return false, ""
	}
	want := env.Confine.Dir() + "/" + UnitDir + "/" + unit
	if m.scope == ScopeSystem {
		want = m.unitDir + "/" + unit
	}
	got := st["FragmentPath"]
	if got == "" {
		return false, want
	}
	same := got == want
	if !same && m.resolve != nil {
		a, aerr := m.resolve(got)
		b, berr := m.resolve(want)
		same = aerr == nil && berr == nil && a == b
	}
	if !same || m.scope != ScopeSystem {
		return same, want
	}
	text, exists, err := readOutside(m.unitDir, unit, "")
	if err != nil || !exists {
		return false, want
	}
	return containsLine(text, "WorkingDirectory="+env.Confine.Dir()), want
}

// containsLine reports whether text has line as one whole line.
func containsLine(text, line string) bool {
	for _, l := range strings.Split(text, "\n") {
		if strings.TrimRight(l, "\r") == line {
			return true
		}
	}
	return false
}

func (m systemdManager) install(ctx context.Context, env ops.Env, s serviceSpec) (ops.Result, error) {
	if m.scope == ScopeSystem {
		return m.installSystem(ctx, env, s)
	}
	text, err := m.artifact(s)
	if err != nil {
		return ops.Result{}, err
	}
	// The path rule first, so an unsafe unit directory runs no command.
	if _, err := env.Confine.Check(s.UnitFile); err != nil {
		return ops.Result{}, err
	}
	// Unit names are per OS user: refuse, before writing anything, a name
	// systemd already loads from another file (another project's unit, a
	// hand-written one, a mask), or enable would fail after the write and a
	// later service_restart would reach that other worker.
	if err := m.refuseTaken(ctx, env, s); err != nil {
		return ops.Result{}, err
	}
	existed := requireFile(env, s.UnitFile, false) == nil
	clean, changed, err := writeDriverFile(env, s.UnitFile, text, 0o644, UnitDir)
	if err != nil {
		return ops.Result{}, err
	}
	enable, err := m.argv("enable", s.UnitPath)
	if err != nil {
		return ops.Result{}, err
	}
	reload, err := m.argv("daemon-reload", "")
	if err != nil {
		return ops.Result{}, err
	}
	// enable links the unit file into the user's search path and enables
	// it; daemon-reload makes a changed unit file the definition the next
	// (re)start uses even when the link already existed.
	for i, argv := range [][]string{enable, reload} {
		res, err := runSystemctl(ctx, env, argv)
		var out ops.Result
		if err == nil && res.code != 0 {
			out, err = failed(strings.Join(argv[:3], " "), res, s.Unit)
		}
		if err != nil {
			// A unit file this install created and could not enable is
			// removed: the project keeps no unit systemd never linked.
			if i == 0 && !existed {
				removeUnitFile(env, clean)
			}
			return out, err
		}
	}
	state := "written"
	if !changed {
		state = "unchanged"
	}
	return ops.Result{
		Summary: fmt.Sprintf("installed %s (%s, user unit): unit file %s %s, enabled; service_restart starts it", s.Unit, s.Mode, clean, state),
		Data: map[string]any{
			"unit": s.Unit, "mode": s.Mode, "scope": m.scope.String(), "unit_file": clean, "changed": changed,
			"exec_start": s.Rendered.ExecStart, "working_directory": s.Rendered.WorkingDirectory,
			"commands": [][]string{enable, reload},
		},
		Receipt: events.SetupReceipt{Kind: "exec", Argv: enable, Target: s.Unit},
	}, nil
}

// refuseTaken refuses, before anything is written, a unit name systemd
// already loads from another file (another project's unit, a hand-written
// one, a mask): enable would fail after the write, and a later
// service_restart would reach that other worker. Unit names are per OS user
// in the user scope and per machine in the system scope.
func (m systemdManager) refuseTaken(ctx context.Context, env ops.Env, s serviceSpec) error {
	st, res, err := m.show(ctx, env, s.Unit)
	if err != nil {
		return err
	}
	if res.code != 0 {
		_, ferr := failed("systemctl show", res, s.Unit)
		return ferr
	}
	if st["FragmentPath"] == "" {
		return nil
	}
	if own, _ := m.ownFragment(env, s.Unit, st); !own {
		return fmt.Errorf("%w: %s is already loaded from %s, not this project's %s; pick another service name",
			ErrDetect, s.Unit, q(st["FragmentPath"]), q(s.UnitPath))
	}
	return nil
}

// requireRoot is the system scope's precondition on the runner: only root
// can write SystemUnitDir and drive the system manager, and only a root-only
// tree may back a unit that runs as root (checkRootOnly).
func (m systemdManager) requireRoot(env ops.Env, s serviceSpec) error {
	if m.euid != 0 {
		return fmt.Errorf("%w: this target installs a system unit (a production target class, or a runner started as root), which needs the runner to run as root; run piperoom-setup as root, or register the target as local or shared for a user unit", ErrDetect)
	}
	owner := m.owner
	if owner == nil {
		owner = fileOwner
	}
	return checkRootOnly(env.Confine.Dir(), s.JavaHome, []string{DistDir, s.Worker}, []string{PluginDir}, owner)
}

// requireRootAt is requireRoot for service_restart, which has only the unit
// name: the JDK is the JAVA_HOME the installed unit file carries, and both
// worker files are checked where they exist.
func (m systemdManager) requireRootAt(env ops.Env, unit string) error {
	if m.euid != 0 {
		return fmt.Errorf("%w: %s is a system unit, which only a runner running as root may restart", ErrDetect, unit)
	}
	text, _, err := readOutside(m.unitDir, unit, env.Confine.Dir())
	if err != nil {
		return err
	}
	javaHome := ""
	for _, l := range strings.Split(text, "\n") {
		if v, ok := strings.CutPrefix(strings.TrimRight(l, "\r"), "Environment=JAVA_HOME="); ok {
			javaHome = v
		}
	}
	if javaHome == "" {
		return fmt.Errorf("%w: %s carries no JAVA_HOME; run service_install again", ErrDetect, unit)
	}
	owner := m.owner
	if owner == nil {
		owner = fileOwner
	}
	optional := []string{PluginDir}
	for _, files := range modeFiles {
		optional = append(optional, files[1])
	}
	return checkRootOnly(env.Confine.Dir(), javaHome, []string{DistDir}, optional, owner)
}

// installSystem writes the unit file into SystemUnitDir (not the project,
// which a system unit must never be linked from), reloads the system manager
// and enables the unit by name. The unit text is the same template with the
// system [Install] target; ExecStart, WorkingDirectory and Environment are the
// detected paths, as in the user scope.
func (m systemdManager) installSystem(ctx context.Context, env ops.Env, s serviceSpec) (ops.Result, error) {
	text, err := m.artifact(s)
	if err != nil {
		return ops.Result{}, err
	}
	if err := m.requireRoot(env, s); err != nil {
		return ops.Result{}, err
	}
	if err := m.refuseTaken(ctx, env, s); err != nil {
		return ops.Result{}, err
	}
	_, existed, err := readOutside(m.unitDir, s.Unit, env.Confine.Dir())
	if err != nil {
		return ops.Result{}, err
	}
	changed, err := writeOutside(m.unitDir, s.Unit, env.Confine.Dir(), text, 0o644)
	if err != nil {
		return ops.Result{}, err
	}
	reload, err := m.argv("daemon-reload", "")
	if err != nil {
		return ops.Result{}, err
	}
	enable, err := m.argv("enable", s.Unit)
	if err != nil {
		return ops.Result{}, err
	}
	// daemon-reload first: the system manager reads SystemUnitDir only on a
	// reload, so enable of a new unit file needs it to exist for systemd.
	for _, argv := range [][]string{reload, enable} {
		res, err := runSystemctl(ctx, env, argv)
		var out ops.Result
		if err == nil && res.code != 0 {
			out, err = failed(strings.Join(argv[:2], " "), res, s.Unit)
		}
		if err != nil {
			// A unit file this install created and could not enable is
			// removed: the machine keeps no piperoom unit nobody enabled.
			if !existed {
				removeOutside(m.unitDir, s.Unit)
			}
			return out, err
		}
	}
	state := "written"
	if !changed {
		state = "unchanged"
	}
	return ops.Result{
		Summary: fmt.Sprintf("installed %s (%s, system unit): unit file %s %s, enabled; service_restart starts it", s.Unit, s.Mode, s.UnitPath, state),
		Data: map[string]any{
			"unit": s.Unit, "mode": s.Mode, "scope": m.scope.String(), "unit_file": s.UnitPath, "changed": changed,
			"exec_start": s.Rendered.ExecStart, "working_directory": s.Rendered.WorkingDirectory,
			"commands": [][]string{reload, enable},
		},
		Receipt: events.SetupReceipt{Kind: "exec", Argv: enable, Target: s.Unit},
	}, nil
}

func (m systemdManager) restart(ctx context.Context, env ops.Env, unit string) (ops.Result, error) {
	// Only a unit this project installed: service_install wrote its file,
	// and systemd loads the unit from that file. Unit names are per OS
	// user (per machine for a system unit), so the file alone does not prove
	// the name is this project's; ownFragment checks what systemd loads.
	if m.scope == ScopeUser {
		if err := requireFile(env, UnitDir+"/"+unit, false); err != nil {
			return ops.Result{}, fmt.Errorf("%w; run service_install first", err)
		}
	}
	st, res, err := m.show(ctx, env, unit)
	if err != nil {
		return ops.Result{}, err
	}
	if res.code != 0 {
		return failed("systemctl show", res, unit)
	}
	if own, want := m.ownFragment(env, unit, st); !own {
		if st["FragmentPath"] == "" {
			return ops.Result{}, fmt.Errorf("%w: systemd has no unit file for %s; run service_install first", ErrDetect, unit)
		}
		return ops.Result{}, fmt.Errorf("%w: systemd loads %s from %s, not this project's %s; refusing to restart it",
			ErrDetect, unit, q(st["FragmentPath"]), q(want))
	}
	if m.scope == ScopeSystem {
		// What the root service runs may have changed since install: the
		// root-only precondition is checked again before every start.
		if err := m.requireRootAt(env, unit); err != nil {
			return ops.Result{}, err
		}
	}
	argv, err := m.argv("restart", unit)
	if err != nil {
		return ops.Result{}, err
	}
	res, err = runSystemctl(ctx, env, argv)
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

// removeUnitFile removes a unit file install wrote, best effort: the op
// already failed, and its error is what the caller reports.
func removeUnitFile(env ops.Env, clean string) {
	root, err := os.OpenRoot(env.Confine.Dir())
	if err != nil {
		return
	}
	defer root.Close()
	_ = root.Remove(clean)
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

func (st unitState) data(unit string, scope UnitScope) map[string]any {
	return map[string]any{
		"unit": unit, "scope": scope.String(),
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
