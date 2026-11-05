package main

// Host-target setup breaks (row U8c, dev/scenarios.md "Setup breaks: host
// target"): the plain-host target of the setup runner
// (internal/setup/targets/host, row U8a) made to fail one named prerequisite
// check, so the setup agent's diagnosis and repair can be exercised on a host
// without a model. A host target renders a systemd unit that runs Kafka
// Connect from a distribution under the project directory, the worker
// properties file (properties_write) and pins the JDK the unit runs with. The
// dev stack has no host, so the `host` compose profile (dev/host) is a
// container that plays one on Linux: a JDK, a Kafka distribution's bin/
// scripts, the rendered unit and the worker files under
// /piperoom-host/project, copied from the image's baked base at every start.
//
//   - wrong-standalone-args: the unit's ExecStart passes the distributed
//     worker file to the standalone launcher, so the standalone-args check
//     (host-standalone-args) fails and names the argv;
//   - plugin-path-unset: connect-standalone.properties loses plugin.path, so
//     the plugin-path check (host-plugin-path) fails and names the key;
//   - java-too-old: the unit's JAVA_HOME points at the image's Java 11, below
//     the version the connector needs, so the java check (host-java-version)
//     fails and names the JAVA_HOME and the version.
//
// The container runs no systemd manager and no worker: the checks read the
// same host files a setup run reads (the unit's ExecStart argv and
// Environment, the worker properties, and `java -version` of the java the
// unit pins) through `docker exec`. The real `systemctl --user` path is U8a's
// own integration test and U8-R's real runs.
//
// Each break rewrites exactly one project file and its fix writes the one
// value back, so the three pairs are reversible in any order and idempotent
// like the dev-stack ones. A container restart copies the baked base again and
// therefore heals all three.

import (
	"context"
	"encoding/base64"
	"fmt"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	// hostContainer is the compose service `host` of the dev project
	// (`name: piperoom-dev` in dev/docker-compose.yml), like connectContainer.
	hostContainer = "piperoom-dev-host-1"
	hostService   = "host"
	hostProfile   = "host"

	// The host project as the host target lays it out under the project
	// directory (internal/setup/targets/host: systemd/<unit>,
	// connect-<mode>.properties at the root, kafka/ for the distribution,
	// connect-plugins/ for the plugin directory).
	hostProjectDir        = "/piperoom-host/project"
	hostUnitPath          = hostProjectDir + "/systemd/piperoom-connect.service"
	hostStandaloneProps   = hostProjectDir + "/connect-standalone.properties"
	hostDistributedProps  = hostProjectDir + "/connect-distributed.properties"
	hostPluginDir         = hostProjectDir + "/connect-plugins"
	hostStandaloneScript  = hostProjectDir + "/kafka/bin/connect-standalone.sh"
	hostDistributedScript = hostProjectDir + "/kafka/bin/connect-distributed.sh"

	// hostJavaHome is the JDK the unit pins when the host is healthy (the
	// image's Temurin 21); hostOldJavaHome is the Java 11 java-too-old pins.
	hostJavaHome    = "/opt/java/openjdk"
	hostOldJavaHome = "/opt/piperoom/jdk-11"

	// hostMinJava is the Java the Debezium 3.2 postgres connector needs; the
	// image's distribution is 3.2.0.Final (dev/docker-compose.yml).
	hostMinJava     = 17
	hostConnector   = "Debezium 3.2"
	hostStandalone  = "standalone"
	hostDistributed = "distributed"
)

// The host named checks; unique among every setup check (a test asserts it).
const (
	checkHostStandaloneArgs = "host-standalone-args"
	checkHostPluginPath     = "host-plugin-path"
	checkHostJavaVersion    = "host-java-version"
)

// hostSetupNames are the three host scenarios in the order dev/scenarios.md
// lists them; setupbreaks.go registers them in setupScenarios with the host
// profile, so the base-stack tests (profile "") skip them.
var hostSetupNames = []string{"wrong-standalone-args", "plugin-path-unset", "java-too-old"}

// hostStartHint is how to start the fake host by hand.
const hostStartHint = "start the fake host: docker compose -f dev/docker-compose.yml --profile " + hostProfile + " up -d --wait " + hostService

// scriptHostWrite replaces one project file with the base64 content of $2. The
// content travels base64-encoded as a positional argument, never as shell text:
// a check reads host files, and nothing a check read can reach the shell. The
// path ($1) is a constant of this file.
const scriptHostWrite = `set -e; mkdir -p "$(dirname "$1")"; printf '%s' "$2" | base64 -d > "$1"`

// ---------------------------------------------------------------- container

// prepareHost brings up the fake host before a break or a fix. The checks read
// the container and never start it, so a machine without the profile running
// gets an error from the check rather than a silent "check fails".
func prepareHost(ctx context.Context, s *stack) error {
	// --build picks up a changed dev/host/base or Dockerfile: the image is a
	// COPY and a chmod, so a cached build costs a second.
	if err := s.composeWith(ctx, "", "--profile", hostProfile, "up", "-d", "--build", "--wait", hostService); err != nil {
		return fmt.Errorf("starting the fake host (docker compose --profile %s up -d --wait %s): %w", hostProfile, hostService, err)
	}
	// --wait holds until the healthcheck sees the project; this is the same
	// read, so a container that came up without it is reported here rather
	// than by a check that then looks broken for the wrong reason.
	if err := s.waitFor(ctx, 60*time.Second, func() (bool, error) {
		_, code, err := s.execIn(ctx, hostContainer, "test", "-f", hostUnitPath)
		if err != nil {
			return false, err
		}
		return code == 0, nil
	}); err != nil {
		return fmt.Errorf("the fake host did not lay down %s: %w", hostUnitPath, err)
	}
	return nil
}

// hostRun runs one argv in the fake host container. A non-zero exit is a code,
// not an error; err means docker itself failed (no daemon, no such container).
func (s *stack) hostRun(ctx context.Context, argv ...string) (string, int, error) {
	out, code, err := s.execIn(ctx, hostContainer, argv...)
	if err != nil {
		return out, code, fmt.Errorf("devctl: %s in %s: %w", strings.Join(argv, " "), hostContainer, err)
	}
	return out, code, nil
}

// hostRead reads one file of the fake host's project. A missing or unreadable
// file is an error: a check guarded by it cannot pass, and its caller reports
// the host, not a check result it never made.
func (s *stack) hostRead(ctx context.Context, name string) (string, error) {
	out, code, err := s.hostRun(ctx, "cat", name)
	if err != nil {
		return "", err
	}
	if code != 0 {
		return "", fmt.Errorf("devctl: reading %s in %s: exit %d: %s (%s)", name, hostContainer, code, hostClip(out), hostStartHint)
	}
	return out, nil
}

// hostReadIfExists is hostRead for a file whose absence is a check failure (a
// worker file that was deleted rather than edited): it reports exists=false
// instead of an error.
func (s *stack) hostReadIfExists(ctx context.Context, name string) (text string, exists bool, err error) {
	out, code, err := s.hostRun(ctx, "cat", name)
	if err != nil {
		return "", false, err
	}
	if code != 0 {
		return "", false, nil
	}
	return out, true, nil
}

// hostWrite replaces one file of the fake host's project through
// scriptHostWrite. Only the constants of this file are ever passed as the path.
func (s *stack) hostWrite(ctx context.Context, name, text string) error {
	out, code, err := s.hostRun(ctx, "sh", "-c", scriptHostWrite, "_", name, base64.StdEncoding.EncodeToString([]byte(text)))
	if err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("devctl: writing %s in %s: exit %d: %s (%s)", name, hostContainer, code, hostClip(out), hostStartHint)
	}
	return nil
}

// hostAlive reports why the fake host cannot be read at all: docker failed, or
// the container is not running. A missing file and a stopped container both
// exit 1 through `docker exec`, so a check that tolerates a missing file must
// ask this first; otherwise a machine that never started the profile looks
// like a host with a broken worker.
func (s *stack) hostAlive(ctx context.Context) error {
	out, code, err := s.hostRun(ctx, "true")
	if err != nil {
		return err
	}
	if code != 0 {
		return fmt.Errorf("devctl: the fake host container %s is not running: %s (%s)", hostContainer, hostClip(out), hostStartHint)
	}
	return nil
}

// hostTest is `test <flag> <path>` in the fake host: the flag is "-f", "-d" or
// "-x". Exit 1 is "no"; any other non-zero exit is an error.
func (s *stack) hostTest(ctx context.Context, flag, name string) (bool, error) {
	out, code, err := s.hostRun(ctx, "test", flag, name)
	if err != nil {
		return false, err
	}
	switch code {
	case 0:
		return true, nil
	case 1:
		return false, nil
	}
	return false, fmt.Errorf("devctl: test %s %s in %s: exit %d: %s (%s)", flag, name, hostContainer, code, hostClip(out), hostStartHint)
}

// hostClip bounds host text quoted in an error: the first line, at most 200
// bytes, so a failed command cannot flood the log.
func hostClip(text string) string {
	line := firstLine(text)
	if len(line) <= 200 {
		return line
	}
	return line[:200] + "…"
}

// ------------------------------------------------------------------- unit

// hostUnitView is what the checks read from the rendered unit file.
type hostUnitView struct {
	raw         string
	description string
	execStart   []string // the fields of the ExecStart= line
	javaHome    string   // Environment=JAVA_HOME
}

// hostUnit reads and parses the unit the fake host runs.
func (s *stack) hostUnit(ctx context.Context) (hostUnitView, error) {
	text, err := s.hostRead(ctx, hostUnitPath)
	if err != nil {
		return hostUnitView{}, err
	}
	u := parseHostUnit(text)
	if !strings.Contains(text, "[Service]") {
		return u, fmt.Errorf("devctl: %s is not a unit file (no [Service] section)", hostUnitPath)
	}
	return u, nil
}

// parseHostUnit reads the lines the checks care about: Description, ExecStart
// and every Environment=NAME=value. An ExecStart= line has no quoting (the
// unit template refuses a value with a space), so fields is its argv.
func parseHostUnit(text string) hostUnitView {
	u := hostUnitView{raw: text}
	for _, line := range hostLines(text) {
		line = strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(line, "Description="):
			u.description = strings.TrimPrefix(line, "Description=")
		case strings.HasPrefix(line, "ExecStart="):
			u.execStart = strings.Fields(strings.TrimPrefix(line, "ExecStart="))
		case strings.HasPrefix(line, "Environment="):
			name, value, ok := strings.Cut(strings.TrimPrefix(line, "Environment="), "=")
			if ok && name == "JAVA_HOME" {
				u.javaHome = value
			}
		}
	}
	return u
}

// hostMode is the worker mode the unit's Description declares:
// "Piperoom Kafka Connect worker (standalone)" is what service_install renders.
func (u hostUnitView) hostMode() (string, bool) {
	switch {
	case strings.HasSuffix(u.description, "("+hostStandalone+")"):
		return hostStandalone, true
	case strings.HasSuffix(u.description, "("+hostDistributed+")"):
		return hostDistributed, true
	}
	return "", false
}

// unitSafePath quotes a path for an error or summary, so a value read from a
// host file cannot forge a line on someone's terminal.
func unitSafePath(p string) string { return strconv.Quote(hostClip(p)) }

// ---------------------------------------------- standalone args (the check)

// verifyHostStandaloneArgs is the host-standalone-args check: the unit runs
// the launcher of the mode its Description names, with that mode's worker
// properties file, and the arguments are the ones a worker of that mode
// reads. A standalone worker keeps its offsets in a local file; a distributed
// worker keeps them in Kafka topics (group.id), and the two files are not
// interchangeable — the classic wrong-standalone-args mistake is handing the
// standalone launcher the distributed file.
func verifyHostStandaloneArgs(ctx context.Context, s *stack) (bool, string, error) {
	if err := s.hostAlive(ctx); err != nil {
		return false, "", err
	}
	u, err := s.hostUnit(ctx)
	if err != nil {
		return false, "", err
	}
	mode, ok := u.hostMode()
	if !ok {
		return false, "the unit's Description " + unitSafePath(u.description) + " does not name the worker mode; service_install renders \"Piperoom Kafka Connect worker (<mode>)\"", nil
	}
	wantScript := hostScriptFor(mode)
	wantWorker := hostWorkerFile(mode)
	if len(u.execStart) == 0 {
		return false, hostUnitPath + " has no ExecStart= line", nil
	}
	if len(u.execStart) < 2 {
		return false, "ExecStart runs " + path.Base(u.execStart[0]) + " without a worker properties file; the " + mode + " worker starts with " + path.Base(wantWorker), nil
	}
	if len(u.execStart) > 2 {
		return false, "ExecStart has " + strconv.Itoa(len(u.execStart)) + " fields; the " + mode + " worker runs " + path.Base(wantScript) + " with one worker properties file, " + path.Base(wantWorker), nil
	}
	program, worker := u.execStart[0], u.execStart[1]
	switch {
	case program != wantScript:
		return false, "ExecStart runs " + path.Base(program) + " with " + path.Base(worker) + "; the unit's Description says the " + mode + " worker, whose launcher is " + path.Base(wantScript), nil
	case worker != wantWorker:
		other := hostDistributed
		if mode == hostDistributed {
			other = hostStandalone
		}
		return false, "ExecStart passes " + path.Base(worker) + " to " + path.Base(program) + "; the " + mode + " worker reads " + path.Base(wantWorker) +
			" (" + hostModeAnchorKey(mode) + "), not the " + other + " file (" + hostModeAnchorKey(other) + ")", nil
	}
	if ok, err := s.hostTest(ctx, "-x", program); err != nil {
		return false, "", err
	} else if !ok {
		return false, "ExecStart program " + program + " is missing or not executable", nil
	}
	text, exists, err := s.hostReadIfExists(ctx, worker)
	if err != nil {
		return false, "", err
	}
	if !exists {
		return false, "ExecStart's worker file " + worker + " does not exist", nil
	}
	entries := parseHostProperties(text)
	if _, ok := entries[hostModeAnchorKey(mode)]; !ok {
		return false, path.Base(worker) + " has no " + hostModeAnchorKey(mode) + ": " + hostModeAnchorWhy(mode), nil
	}
	return true, "ExecStart runs " + path.Base(program) + " with " + path.Base(worker), nil
}

// hostScriptFor is mode's launcher in the project's Kafka distribution.
func hostScriptFor(mode string) string {
	if mode == hostStandalone {
		return hostStandaloneScript
	}
	return hostDistributedScript
}

// hostWorkerFile is mode's properties file at the project root: the file
// properties_write writes and the unit's ExecStart passes.
func hostWorkerFile(mode string) string {
	if mode == hostStandalone {
		return hostStandaloneProps
	}
	return hostDistributedProps
}

// hostModeAnchorKey is the key that tells the two worker files apart: the
// distributed file's group.id, the standalone file's offset file.
func hostModeAnchorKey(mode string) string {
	if mode == hostStandalone {
		return "offset.storage.file.filename"
	}
	return "group.id"
}

// hostModeAnchorWhy is what the key means for a worker of mode.
func hostModeAnchorWhy(mode string) string {
	if mode == hostStandalone {
		return "a standalone worker keeps its offsets in a local file, not in Kafka topics"
	}
	return "a distributed worker keeps its config and offsets in Kafka topics"
}

func breakHostStandaloneArgs(ctx context.Context, s *stack) error {
	u, err := s.hostUnit(ctx)
	if err != nil {
		return err
	}
	if len(u.execStart) < 2 {
		return fmt.Errorf("%s has no worker properties argument to change", hostUnitPath)
	}
	if last := u.execStart[len(u.execStart)-1]; last != hostStandaloneProps {
		return fmt.Errorf("refusing: %s already passes %s; run `devctl setup-fix wrong-standalone-args` first", hostUnitPath, last)
	}
	if ok, err := s.hostTest(ctx, "-f", hostDistributedProps); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("%s is missing; the fake host image is stale (docker compose -f dev/docker-compose.yml --profile %s build %s)", hostDistributedProps, hostProfile, hostService)
	}
	text := setHostExecStart(u.raw, hostStandaloneScript, hostDistributedProps)
	if text == u.raw {
		return fmt.Errorf("%s has no ExecStart= line to change", hostUnitPath)
	}
	if err := s.hostWrite(ctx, hostUnitPath, text); err != nil {
		return fmt.Errorf("pointing ExecStart at the distributed worker file: %w", err)
	}
	s.log.Info("broken: the standalone unit passes the distributed worker file (the setup check " + checkHostStandaloneArgs + " fails)")
	return nil
}

func fixHostStandaloneArgs(ctx context.Context, s *stack) error {
	u, err := s.hostUnit(ctx)
	if err != nil {
		return err
	}
	text := setHostExecStart(u.raw, hostStandaloneScript, hostStandaloneProps)
	if text == u.raw {
		s.log.Info("skipped: ExecStart already runs " + path.Base(hostStandaloneScript) + " with " + path.Base(hostStandaloneProps))
		return nil
	}
	if err := s.hostWrite(ctx, hostUnitPath, text); err != nil {
		return fmt.Errorf("restoring ExecStart: %w", err)
	}
	s.log.Info("fixed: ExecStart runs " + path.Base(hostStandaloneScript) + " with " + path.Base(hostStandaloneProps))
	return nil
}

// ------------------------------------------------------ plugin.path (the check)

// verifyHostPluginPath is the host-plugin-path check: the standalone worker
// file names a plugin.path and that directory exists on the host. An unset
// plugin.path is the classic "Failed to find any class that implements
// Connector": the worker scans only the plugins it ships with.
func verifyHostPluginPath(ctx context.Context, s *stack) (bool, string, error) {
	if err := s.hostAlive(ctx); err != nil {
		return false, "", err
	}
	text, exists, err := s.hostReadIfExists(ctx, hostStandaloneProps)
	if err != nil {
		return false, "", err
	}
	if !exists {
		return false, "connect-standalone.properties is missing from the host; the worker has no properties file to read plugin.path from", nil
	}
	value, ok := parseHostProperties(text)["plugin.path"]
	if !ok || strings.TrimSpace(value) == "" {
		return false, "connect-standalone.properties has no plugin.path: the worker scans only the plugins it ships with and reports \"Failed to find any class that implements Connector\"", nil
	}
	for _, dir := range strings.Split(value, ",") {
		dir = strings.TrimSpace(dir)
		if ok, err := s.hostTest(ctx, "-d", dir); err != nil {
			return false, "", err
		} else if !ok {
			return false, "plugin.path=" + value + " names " + dir + ", which is not a directory on the host", nil
		}
	}
	return true, "plugin.path=" + value, nil
}

func breakHostPluginPathUnset(ctx context.Context, s *stack) error {
	text, exists, err := s.hostReadIfExists(ctx, hostStandaloneProps)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("%s is missing; recreate the fake host (docker compose -f dev/docker-compose.yml --profile %s up -d --force-recreate %s)", hostStandaloneProps, hostProfile, hostService)
	}
	out, removed := deleteHostProperty(text, "plugin.path")
	if !removed {
		return fmt.Errorf("refusing: %s has no plugin.path line to remove", hostStandaloneProps)
	}
	if err := s.hostWrite(ctx, hostStandaloneProps, out); err != nil {
		return fmt.Errorf("removing plugin.path: %w", err)
	}
	s.log.Info("broken: connect-standalone.properties has no plugin.path (the setup check " + checkHostPluginPath + " fails)")
	return nil
}

func fixHostPluginPathUnset(ctx context.Context, s *stack) error {
	text, exists, err := s.hostReadIfExists(ctx, hostStandaloneProps)
	if err != nil {
		return err
	}
	if !exists {
		return fmt.Errorf("%s is missing; recreate the fake host (docker compose -f dev/docker-compose.yml --profile %s up -d --force-recreate %s)", hostStandaloneProps, hostProfile, hostService)
	}
	out := setHostProperty(text, "plugin.path", hostPluginDir)
	if out == text {
		s.log.Info("skipped: connect-standalone.properties already has plugin.path=" + hostPluginDir)
		return nil
	}
	if err := s.hostWrite(ctx, hostStandaloneProps, out); err != nil {
		return fmt.Errorf("restoring plugin.path: %w", err)
	}
	s.log.Info("fixed: connect-standalone.properties has plugin.path=" + hostPluginDir)
	return nil
}

// ------------------------------------------------------- java version (check)

// hostJavaVersionRE matches the quoted version of `java -version`, the same
// shape the setup runner's java_version op reads.
var hostJavaVersionRE = regexp.MustCompile(`(?i)\bversion\s+"([^"]+)"`)

// hostJavaVersion is the version `java -version` printed, or "".
func hostJavaVersion(out string) string {
	m := hostJavaVersionRE.FindStringSubmatch(out)
	if m == nil {
		return ""
	}
	return m[1]
}

// hostJavaMajor is the feature version of a java version string: "21.0.8" and
// "21-ea" are 21, "1.8.0_392" is 8, "" and a non-numeric string are 0.
func hostJavaMajor(version string) int {
	v := strings.TrimPrefix(version, "1.")
	if i := strings.IndexFunc(v, func(r rune) bool { return r < '0' || r > '9' }); i >= 0 {
		v = v[:i]
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0
	}
	return n
}

// verifyHostJavaVersion is the host-java-version check: the java the unit pins
// (Environment=JAVA_HOME/bin/java, or java on PATH when the unit names none)
// is at least the version the connector needs.
func verifyHostJavaVersion(ctx context.Context, s *stack) (bool, string, error) {
	if err := s.hostAlive(ctx); err != nil {
		return false, "", err
	}
	u, err := s.hostUnit(ctx)
	if err != nil {
		return false, "", err
	}
	java, where := "java", "java on PATH (the unit sets no JAVA_HOME)"
	if u.javaHome != "" {
		// The value comes from a host file; it names the executable this check
		// runs (one docker exec argv element, no shell), so it must be the
		// absolute path the unit template renders.
		if !strings.HasPrefix(u.javaHome, "/") {
			return false, "JAVA_HOME " + unitSafePath(u.javaHome) + " is not an absolute path", nil
		}
		java, where = u.javaHome+"/bin/java", "JAVA_HOME="+u.javaHome
	}
	out, code, err := s.hostRun(ctx, java, "-version")
	if err != nil {
		return false, "", err
	}
	if code != 0 {
		return false, "running " + java + " -version exited " + strconv.Itoa(code) + ": " + hostClip(out), nil
	}
	version := hostJavaVersion(out)
	if version == "" {
		return false, java + " -version printed no version: " + hostClip(out), nil
	}
	if major := hostJavaMajor(version); major < hostMinJava {
		return false, where + " reports java " + version + "; the " + hostConnector + " postgres connector needs Java " + strconv.Itoa(hostMinJava) + " or newer", nil
	}
	return true, where + " is java " + version + " (the connector needs " + strconv.Itoa(hostMinJava) + "+)", nil
}

func breakHostJavaTooOld(ctx context.Context, s *stack) error {
	if ok, err := s.hostTest(ctx, "-x", hostOldJavaHome+"/bin/java"); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("%s/bin/java is missing; the fake host image is stale (docker compose -f dev/docker-compose.yml --profile %s build %s)", hostOldJavaHome, hostProfile, hostService)
	}
	u, err := s.hostUnit(ctx)
	if err != nil {
		return err
	}
	if u.javaHome == hostOldJavaHome {
		return fmt.Errorf("refusing: the unit already pins JAVA_HOME=%s", hostOldJavaHome)
	}
	text := setHostEnvironment(u.raw, "JAVA_HOME", hostOldJavaHome)
	if text == u.raw {
		return fmt.Errorf("%s has no Environment=JAVA_HOME= line to point at %s", hostUnitPath, hostOldJavaHome)
	}
	if err := s.hostWrite(ctx, hostUnitPath, text); err != nil {
		return fmt.Errorf("pointing JAVA_HOME at the old JDK: %w", err)
	}
	s.log.Info("broken: the unit pins JAVA_HOME=" + hostOldJavaHome + " (the setup check " + checkHostJavaVersion + " fails)")
	return nil
}

func fixHostJavaTooOld(ctx context.Context, s *stack) error {
	u, err := s.hostUnit(ctx)
	if err != nil {
		return err
	}
	if u.javaHome == hostJavaHome {
		s.log.Info("skipped: the unit already pins JAVA_HOME=" + hostJavaHome)
		return nil
	}
	text := setHostEnvironment(u.raw, "JAVA_HOME", hostJavaHome)
	if text == u.raw {
		return fmt.Errorf("%s has no Environment=JAVA_HOME= line; service_install renders one (run service_install, or recreate the fake host)", hostUnitPath)
	}
	if err := s.hostWrite(ctx, hostUnitPath, text); err != nil {
		return fmt.Errorf("restoring JAVA_HOME: %w", err)
	}
	s.log.Info("fixed: the unit pins JAVA_HOME=" + hostJavaHome)
	return nil
}

// ------------------------------------------------------------ file helpers

// hostLines splits text into lines without a trailing empty one.
func hostLines(text string) []string {
	return strings.Split(strings.TrimSuffix(strings.ReplaceAll(text, "\r\n", "\n"), "\n"), "\n")
}

// joinHostLines is hostLines' inverse; trailing keeps the file's final newline.
func joinHostLines(lines []string, trailing bool) string {
	out := strings.Join(lines, "\n")
	if trailing && out != "" {
		out += "\n"
	}
	return out
}

// hostReplaceLine replaces the first line whose trimmed form matches, keeping
// the file's trailing newline. It returns text unchanged when nothing matches.
func hostReplaceLine(text string, match func(string) bool, replacement string) string {
	trailing := strings.HasSuffix(text, "\n")
	lines := hostLines(text)
	for i, line := range lines {
		if match(strings.TrimSpace(line)) {
			lines[i] = replacement
			return joinHostLines(lines, trailing)
		}
	}
	return text
}

// setHostExecStart writes the unit's one ExecStart= line as program with its
// single worker argument: the argv service_install renders.
func setHostExecStart(text, program, worker string) string {
	return hostReplaceLine(text, func(line string) bool { return strings.HasPrefix(line, "ExecStart=") },
		"ExecStart="+program+" "+worker)
}

// setHostEnvironment writes the unit's Environment=NAME=value line.
func setHostEnvironment(text, name, value string) string {
	return hostReplaceLine(text, func(line string) bool { return strings.HasPrefix(line, "Environment="+name+"=") },
		"Environment="+name+"="+value)
}

// hostPropertyKey is the key of a properties line, or ok=false for a comment,
// a blank line and a line with no key.
func hostPropertyKey(line string) (key string, ok bool) {
	t := strings.TrimLeft(line, " \t\f")
	if t == "" || t[0] == '#' || t[0] == '!' {
		return "", false
	}
	end := strings.IndexAny(t, "=: \t")
	if end <= 0 {
		return "", false
	}
	return t[:end], true
}

// setHostProperty sets key=value: in place when the key is there, otherwise
// inserted in lexical order among the key lines, which is the order
// properties_write renders, so a fix restores the base file's bytes.
func setHostProperty(text, key, value string) string {
	trailing := strings.HasSuffix(text, "\n")
	lines := hostLines(text)
	line := key + "=" + value
	insertAt := -1
	for i, l := range lines {
		k, ok := hostPropertyKey(l)
		if !ok {
			continue
		}
		if k == key {
			lines[i] = line
			return joinHostLines(lines, trailing)
		}
		if insertAt < 0 && k > key {
			insertAt = i
		}
	}
	if insertAt < 0 {
		insertAt = len(lines)
	}
	lines = append(lines, "")
	copy(lines[insertAt+1:], lines[insertAt:])
	lines[insertAt] = line
	return joinHostLines(lines, trailing)
}

// deleteHostProperty removes the key's line, reporting whether it was there.
func deleteHostProperty(text, key string) (string, bool) {
	trailing := strings.HasSuffix(text, "\n")
	lines := hostLines(text)
	for i, l := range lines {
		if k, ok := hostPropertyKey(l); ok && k == key {
			lines = append(lines[:i], lines[i+1:]...)
			return joinHostLines(lines, trailing), true
		}
	}
	return text, false
}

// parseHostProperties reads the Java properties forms a worker file uses: #
// and ! comments and key=value, key:value and key value. The values stay as
// written; the checks treat them as host text.
func parseHostProperties(text string) map[string]string {
	entries := map[string]string{}
	const maxLines = 512
	lines := hostLines(text)
	for i := 0; i < len(lines) && i < maxLines; i++ {
		key, ok := hostPropertyKey(lines[i])
		if !ok {
			continue
		}
		t := strings.TrimLeft(lines[i], " \t\f")
		value := strings.TrimLeft(t[len(key):], " \t\f")
		if value != "" && (value[0] == '=' || value[0] == ':') {
			value = strings.TrimLeft(value[1:], " \t\f")
		}
		entries[key] = value
	}
	return entries
}
