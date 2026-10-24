package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/setup/targets/host"
)

// unitSpec is the unit file server_unit_write writes, resolved from the
// project layout and detection only. No op param reaches the unit text.
type unitSpec struct {
	UnitFile string // project-relative: systemd/piperoom-dbzserver.service
	UnitPath string // absolute
	Rendered host.Unit
}

// buildUnit resolves the unit for env. It requires what the unit runs: the
// distribution's run.sh (executable, from DistDir) and the properties file the
// server reads (written by server_properties_write first), and pins the JDK
// the java on PATH resolves to.
func (d *Driver) buildUnit(env ops.Env) (unitSpec, error) {
	if env.Confine == nil {
		return unitSpec{}, errors.New("server: the project root is not open")
	}
	dir := env.Confine.Dir()
	if err := host.CheckUnitPath(dir); err != nil {
		return unitSpec{}, fmt.Errorf("%w: project directory: %w", ErrDetect, err)
	}
	if err := requireFile(env, DistDir+"/run.sh", true); err != nil {
		return unitSpec{}, fmt.Errorf("%w (a Debezium Server distribution extracted at %s/ under the project directory)", err, DistDir)
	}
	if err := requireFile(env, ConfigFile, false); err != nil {
		return unitSpec{}, fmt.Errorf("%w (server_properties_write writes it)", err)
	}
	javaHome, err := d.javaHome(env, dir)
	if err != nil {
		return unitSpec{}, err
	}
	return unitSpec{
		UnitFile: UnitDir + "/" + UnitName,
		UnitPath: dir + "/" + UnitDir + "/" + UnitName,
		Rendered: unitFor(dir, javaHome),
	}, nil
}

// unitFor is the one shape of this target's unit: run.sh from the distribution
// under the project directory, started in the distribution's own directory
// (run.sh lists its runner jar relative to the working directory and sources
// ./jmx/enable_jmx.sh there), with the detected JDK pinned in JAVA_HOME.
func unitFor(dir, javaHome string) host.Unit {
	return host.Unit{
		Description:      "Piperoom Debezium Server",
		WorkingDirectory: dir + "/" + DistDir,
		Environment:      []host.EnvVar{{Name: "JAVA_HOME", Value: javaHome}},
		ExecStart:        []string{dir + "/" + DistDir + "/run.sh"},
	}
}

// requireFile checks a project file through the §2.3 rule (no symlink or
// reparse point on the way) and requires a regular file, executable by its
// owner when exec is set.
func requireFile(env ops.Env, name string, exec bool) error {
	if env.Confine == nil {
		return errors.New("server: the project root is not open")
	}
	clean, err := env.Confine.Check(name)
	if err != nil {
		return err
	}
	root, err := os.OpenRoot(env.Confine.Dir())
	if err != nil {
		return fmt.Errorf("server: opening the project root: %w", err)
	}
	defer root.Close()
	fi, err := root.Lstat(clean)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		return fmt.Errorf("%w: %s does not exist", ErrDetect, q(name))
	case err != nil:
		return fmt.Errorf("server: stat %s: %w", q(name), err)
	case !fi.Mode().IsRegular():
		return fmt.Errorf("%w: %s is not a regular file", ErrDetect, q(name))
	case exec && fi.Mode().Perm()&0o100 == 0:
		return fmt.Errorf("%w: %s is not executable", ErrDetect, q(name))
	}
	return nil
}

// javaHome is the JDK the java on PATH resolves to: <home>/bin/java with the
// symlinks resolved, so the unit pins the java the runner detected rather than
// whatever PATH holds when systemd starts it. A JDK inside the project
// directory is refused: the unit runs it on every start, and a file a write op
// can change under it would change what the unit executes. The distribution
// directory is a mount directory, so nothing under it can be written either.
func (d *Driver) javaHome(env ops.Env, dir string) (string, error) {
	if env.LookPath == nil {
		return "", fmt.Errorf("%w: no PATH lookup configured", ErrDetect)
	}
	found, err := env.LookPath("java")
	if err != nil || found == "" {
		return "", fmt.Errorf("%w: java is not on PATH", ErrDetect)
	}
	resolved, err := d.resolve(found)
	if err != nil {
		return "", fmt.Errorf("%w: resolving %s: %v", ErrDetect, q(found), err)
	}
	if filepath.Base(resolved) != "java" || filepath.Base(filepath.Dir(resolved)) != "bin" {
		return "", fmt.Errorf("%w: java on PATH resolves to %s, which is not <JDK>/bin/java (a wrapper or shim?)", ErrDetect, q(resolved))
	}
	home := filepath.Dir(filepath.Dir(resolved))
	if err := host.CheckUnitPath(home); err != nil {
		return "", fmt.Errorf("%w: JAVA_HOME: %w", ErrDetect, err)
	}
	if underDir(filepath.ToSlash(home), filepath.ToSlash(dir)) {
		return "", fmt.Errorf("%w: JAVA_HOME %s is inside the project directory; install the JDK there or outside", ErrDetect, q(home))
	}
	return home, nil
}

// underDir reports whether p is dir or lies under it. A path on another
// volume (filepath.Rel fails) is not under it.
func underDir(p, dir string) bool {
	rel, err := filepath.Rel(dir, p)
	if err != nil {
		return false
	}
	return rel == "." || filepath.IsLocal(rel)
}

// ---- server_unit_write (T2) ----

type serverUnitWriteOp struct{ d *Driver }

func (serverUnitWriteOp) Name() string   { return "server_unit_write" }
func (serverUnitWriteOp) Target() string { return targetKind }
func (serverUnitWriteOp) Tier() string   { return protocol.TierT2 }
func (serverUnitWriteOp) Layer() string  { return layerName }
func (serverUnitWriteOp) Describe() string {
	return "render the Debezium Server systemd user unit from the template and write it under systemd/"
}

func (serverUnitWriteOp) ParamsSchema() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": false}
}

func (o serverUnitWriteOp) Validate(params json.RawMessage, _ ops.Env) error {
	if err := decodeStrict(params, &struct{}{}); err != nil {
		return err
	}
	return o.d.checkServiceMode()
}

func (o serverUnitWriteOp) Execute(ctx context.Context, params json.RawMessage, env ops.Env) (ops.Result, error) {
	if err := decodeStrict(params, &struct{}{}); err != nil {
		return ops.Result{}, err
	}
	if err := o.d.checkServiceMode(); err != nil {
		return ops.Result{}, err
	}
	spec, err := o.d.buildUnit(env)
	if err != nil {
		return ops.Result{}, err
	}
	text, err := host.RenderUnit(spec.Rendered)
	if err != nil {
		return ops.Result{}, fmt.Errorf("server: rendering the unit: %w", err)
	}
	clean, changed, err := writeFile(env, spec.UnitFile, text, 0o644)
	if err != nil {
		return ops.Result{}, err
	}
	state := "wrote"
	if !changed {
		state = "unchanged"
	}
	return ops.Result{
		Summary: fmt.Sprintf("%s %s; enabling it is the person's step (systemctl --user enable %s)", state, clean, spec.UnitPath),
		Data: map[string]any{
			"unit": spec.UnitPath, "unit_file": clean, "changed": changed,
			"exec_start": spec.Rendered.ExecStart, "working_directory": spec.Rendered.WorkingDirectory,
			"environment": []string{"JAVA_HOME"},
		},
		Receipt: events.SetupReceipt{Kind: "file", Target: clean, At: now(env)},
	}, nil
}

// checkServiceMode refuses the unit op outside a service-mode room: a compose
// room's server is a container, and a unit there would start a second one.
func (d *Driver) checkServiceMode() error {
	if d.mode != ModeService {
		return fmt.Errorf("%w: this room's Debezium Server target is %s mode; the unit template needs service mode", ErrParams, d.mode)
	}
	if d.goos != "linux" {
		return fmt.Errorf("%w (%s: the unit template is systemd on Linux)", ErrUnsupported, d.goos)
	}
	return nil
}
