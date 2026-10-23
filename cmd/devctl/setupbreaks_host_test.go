package main

// Unit tests for the host-target setup breaks (setupbreaks_host.go): a fake
// `docker exec` plays the fake host container — its project files, the paths
// that are directories or executable, and the java each JDK answers
// `java -version` with — so every break, fix and named check runs without
// Docker. The files the fake host starts from are the image's own
// (dev/host/base), so these tests also keep the image and the checks in step.
// The integration test (setupbreaks_host_integration_test.go) runs the same
// pairs on the real container.

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// hostWorld is the fake host container: the project files under
// /piperoom-host/project, which of them are directories or executable, what
// each JDK prints for `java -version`, and whether the container runs.
type hostWorld struct {
	t            *testing.T
	files        map[string]string
	dirs         map[string]bool
	executable   map[string]bool
	java         map[string]string
	running      bool
	dockerErr    error
	composeCalls [][]string
	writes       []string
}

// run is the execInFn of the fake host.
func (w *hostWorld) run(_ context.Context, container string, argv ...string) (string, int, error) {
	if container != hostContainer {
		w.t.Errorf("docker exec in %q, want %q", container, hostContainer)
		return "", -1, fmt.Errorf("wrong container %q", container)
	}
	if w.dockerErr != nil {
		return "", -1, w.dockerErr
	}
	if !w.running {
		return "Error response from daemon: No such container: " + hostContainer, 1, nil
	}
	switch argv[0] {
	case "true":
		return "", 0, nil
	case "cat":
		content, ok := w.files[argv[1]]
		if !ok {
			return "cat: " + argv[1] + ": No such file or directory", 1, nil
		}
		return content, 0, nil
	case "sh":
		// sh -c scriptHostWrite _ <path> <base64>
		if len(argv) != 6 || argv[1] != "-c" || argv[2] != scriptHostWrite || argv[3] != "_" {
			w.t.Fatalf("argv = %q, want sh -c scriptHostWrite _ <path> <content>", argv)
		}
		data, err := base64.StdEncoding.DecodeString(argv[5])
		if err != nil {
			w.t.Fatalf("the write payload is not base64: %v", err)
		}
		w.files[argv[4]] = string(data)
		w.writes = append(w.writes, argv[4])
		return "", 0, nil
	case "test":
		if len(argv) != 3 {
			w.t.Fatalf("argv = %q, want test <flag> <path>", argv)
		}
		var ok bool
		switch argv[1] {
		case "-f", "-e":
			_, ok = w.files[argv[2]]
		case "-d":
			ok = w.dirs[argv[2]]
		case "-x":
			_, isJava := w.java[argv[2]]
			ok = w.executable[argv[2]] || isJava
		default:
			w.t.Fatalf("test flag %q, want -f, -d or -x", argv[1])
		}
		if !ok {
			return "", 1, nil
		}
		return "", 0, nil
	}
	if out, ok := w.java[argv[0]]; ok && len(argv) == 2 && argv[1] == "-version" {
		return out, 0, nil
	}
	return argv[0] + ": not found", 127, nil
}

// compose records the profile start prepareHost runs.
func (w *hostWorld) compose(_ context.Context, override string, args ...string) error {
	w.composeCalls = append(w.composeCalls, append([]string{override}, args...))
	if slices.Contains(args, "up") {
		w.running = true
	}
	return nil
}

// hostWorldFromBase is the fake host as the image starts it: its project is
// dev/host/base (so a base file the checks cannot read fails here), its
// distribution's launchers are executable, and the two JDKs answer
// java -version.
func hostWorldFromBase(t *testing.T) *hostWorld {
	t.Helper()
	root, err := repoRoot()
	if err != nil {
		t.Fatalf("%v", err)
	}
	w := &hostWorld{
		t:          t,
		files:      map[string]string{},
		dirs:       map[string]bool{},
		executable: map[string]bool{},
		java: map[string]string{
			hostJavaHome + "/bin/java":    "openjdk version \"21.0.8\" 2025-07-15\nOpenJDK Runtime Environment (build 21.0.8+9-LTS)\n",
			hostOldJavaHome + "/bin/java": "openjdk version \"11.0.22\" 2024-01-16\nOpenJDK Runtime Environment (build 11.0.22+7-post-Ubuntu-0ubuntu2.22.04)\n",
		},
		running: true,
	}
	base := filepath.Join(root, "dev", "host", "base")
	err = filepath.WalkDir(base, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(base, p)
		if err != nil {
			return err
		}
		if rel == "." {
			return nil
		}
		slashed := filepath.ToSlash(rel)
		container := hostProjectDir + "/" + slashed
		if d.IsDir() {
			w.dirs[container] = true
			return nil
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		w.files[container] = string(data)
		if strings.HasPrefix(slashed, "kafka/bin/") {
			w.executable[container] = true
		}
		return nil
	})
	if err != nil {
		t.Fatalf("reading dev/host/base: %v", err)
	}
	return w
}

// testHostStack is a stack whose docker exec and compose are the fake host.
func testHostStack(t *testing.T, w *hostWorld) *stack {
	t.Helper()
	return &stack{
		root:              t.TempDir(),
		log:               slog.New(slog.NewTextHandler(io.Discard, nil)),
		poll:              time.Millisecond,
		execInFn:          w.run,
		composeOverrideFn: w.compose,
	}
}

// hostCheckDetail is what each break's named check must report: the argv, the
// properties key or the version, which is also what the U8d scenario labels
// name.
var hostCheckDetail = map[string]string{
	"wrong-standalone-args": "connect-distributed.properties",
	"plugin-path-unset":     "no plugin.path",
	"java-too-old":          "reports java 11.0.22",
}

func TestHostSetupScenariosAreRegistered(t *testing.T) {
	if got := strings.Join(hostSetupNames, ","); got != "wrong-standalone-args,plugin-path-unset,java-too-old" {
		t.Fatalf("hostSetupNames = %s", got)
	}
	seenCheck := map[string]string{}
	for _, set := range setupScenarioSets {
		for name, sc := range set {
			if other, dup := seenCheck[sc.check]; dup {
				t.Errorf("%s and %s share check %q", other, name, sc.check)
			}
			seenCheck[sc.check] = name
		}
	}
	for _, name := range hostSetupNames {
		sc, ok := setupScenarios[name]
		if !ok {
			t.Fatalf("%s is not registered in setupScenarios (the CLI looks names up there)", name)
		}
		if sc.name != name || sc.breakIt == nil || sc.fix == nil || sc.verify == nil || sc.prepare == nil {
			t.Errorf("%s: incomplete scenario: %+v", name, sc)
		}
		if sc.profile != hostProfile {
			t.Errorf("%s: profile = %q, want %q", name, sc.profile, hostProfile)
		}
		if !slices.Contains(allSetupScenarioNames(), name) {
			t.Errorf("allSetupScenarioNames lacks %q", name)
		}
		// The base-stack tests and the dev-stack integration test must not pick
		// a host scenario up: its stack is the host profile's container.
		if slices.Contains(setupScenarioNamesFor(""), name) {
			t.Errorf("%s is in the base dev stack's scenarios", name)
		}
		if !slices.Contains(setupScenarioNamesFor(hostProfile), name) {
			t.Errorf("%s is not in the %s profile's scenarios", name, hostProfile)
		}
		if !strings.Contains(usage, name) {
			t.Errorf("usage text does not name %s", name)
		}
	}
	if got := setupScenarios["wrong-standalone-args"].check; got != checkHostStandaloneArgs {
		t.Errorf("wrong-standalone-args check = %q", got)
	}
}

// TestHostSetupBreakFixPair: each named check passes on the host as it comes
// up, fails after its break with the detail naming the argv, key or version,
// and passes after the fix. A second break and a second fix are no-ops, and
// each half writes exactly one project file.
func TestHostSetupBreakFixPair(t *testing.T) {
	for _, name := range hostSetupNames {
		t.Run(name, func(t *testing.T) {
			w := hostWorldFromBase(t)
			s := testHostStack(t, w)
			ctx := context.Background()
			sc := setupScenarios[name]

			if ok, detail, err := sc.verify(ctx, s); err != nil || !ok {
				t.Fatalf("check %s on the fake host as it comes up: ok=%v %s err=%v", sc.check, ok, detail, err)
			}

			w.writes = nil
			if err := setupBreakCmd(ctx, s, name); err != nil {
				t.Fatalf("setup-break %s: %v", name, err)
			}
			ok, detail, err := sc.verify(ctx, s)
			if err != nil {
				t.Fatalf("check %s after the break: %v", sc.check, err)
			}
			if ok {
				t.Fatalf("check %s still passes after the break (%s)", sc.check, detail)
			}
			if !strings.Contains(detail, hostCheckDetail[name]) {
				t.Errorf("check %s after the break reports %q, want it to name %q", sc.check, detail, hostCheckDetail[name])
			}
			if len(w.writes) != 1 {
				t.Errorf("the break wrote %v, want exactly one project file", w.writes)
			}

			w.writes = nil
			if err := setupFixCmd(ctx, s, name); err != nil {
				t.Fatalf("setup-fix %s: %v", name, err)
			}
			if ok, detail, err := sc.verify(ctx, s); err != nil || !ok {
				t.Fatalf("check %s after the fix: ok=%v %s err=%v", sc.check, ok, detail, err)
			}
			if len(w.writes) != 1 {
				t.Errorf("the fix wrote %v, want exactly one project file", w.writes)
			}

			// Idempotent, like the dev-stack pairs: a break on a stack that
			// already fails the check changes nothing, and so does a fix after
			// the check already passes.
			w.writes = nil
			if err := setupBreakCmd(ctx, s, name); err != nil {
				t.Fatalf("second break: %v", err)
			}
			if ok, detail, err := sc.verify(ctx, s); err != nil || ok {
				t.Fatalf("check %s after the second break: ok=%v %s err=%v", sc.check, ok, detail, err)
			}
			if len(w.writes) != 1 {
				t.Errorf("the second break wrote %v, want one project file", w.writes)
			}
			w.writes = nil
			if err := setupBreakCmd(ctx, s, name); err != nil {
				t.Fatalf("third break (already broken): %v", err)
			}
			if len(w.writes) != 0 {
				t.Errorf("a break on an already broken host wrote %v, want nothing", w.writes)
			}
			if err := setupFixCmd(ctx, s, name); err != nil {
				t.Fatalf("second fix: %v", err)
			}
			w.writes = nil
			if err := setupFixCmd(ctx, s, name); err != nil {
				t.Fatalf("third fix (already fixed): %v", err)
			}
			if len(w.writes) != 0 {
				t.Errorf("a fix on an already healthy host wrote %v, want nothing", w.writes)
			}
			if ok, detail, err := sc.verify(ctx, s); err != nil || !ok {
				t.Fatalf("check after the second fix: ok=%v detail=%s err=%v", ok, detail, err)
			}
		})
	}
}

// TestHostSetupPrepareStartsTheProfile: the check reads a container that is
// not part of the dev stack, so setup-break starts the profile first; the
// command works on a machine that never ran the host profile.
func TestHostSetupPrepareStartsTheProfile(t *testing.T) {
	w := hostWorldFromBase(t)
	w.running = false
	s := testHostStack(t, w)
	ctx := context.Background()

	if ok, detail, err := setupScenarios["plugin-path-unset"].verify(ctx, s); err == nil || ok {
		t.Fatalf("the check on a stopped container: ok=%v detail=%q err=%v, want an error", ok, detail, err)
	}
	if err := setupBreakCmd(ctx, s, "plugin-path-unset"); err != nil {
		t.Fatalf("setup-break with the profile stopped: %v", err)
	}
	if !w.running {
		t.Fatal("the break did not start the fake host")
	}
	want := "--profile host up -d --build --wait host"
	found := false
	for _, call := range w.composeCalls {
		if got := strings.Join(call[1:], " "); got == want {
			found = true
		}
	}
	if !found {
		t.Fatalf("compose calls %v lack %q", w.composeCalls, want)
	}
}

// TestHostSetupCheckErrorPaths: a container that is down or a daemon that is
// not reachable is an error naming how to start the host, never a silent
// "check fails" (a check that cannot run has no result).
func TestHostSetupCheckErrorPaths(t *testing.T) {
	ctx := context.Background()
	t.Run("container down", func(t *testing.T) {
		w := hostWorldFromBase(t)
		w.running = false
		s := testHostStack(t, w)
		for _, name := range hostSetupNames {
			ok, _, err := setupScenarios[name].verify(ctx, s)
			if err == nil || ok {
				t.Errorf("%s on a stopped container: ok=%v err=%v, want an error", name, ok, err)
				continue
			}
			if !strings.Contains(err.Error(), "start the fake host") {
				t.Errorf("%s: error %q does not say how to start the host", name, err)
			}
		}
	})
	t.Run("docker daemon down", func(t *testing.T) {
		w := hostWorldFromBase(t)
		w.dockerErr = errors.New("Cannot connect to the Docker daemon")
		s := testHostStack(t, w)
		for _, name := range hostSetupNames {
			ok, _, err := setupScenarios[name].verify(ctx, s)
			if err == nil || ok || !strings.Contains(err.Error(), "Cannot connect to the Docker daemon") {
				t.Errorf("%s with the daemon down: ok=%v err=%v", name, ok, err)
			}
		}
	})
	t.Run("unit file missing", func(t *testing.T) {
		w := hostWorldFromBase(t)
		delete(w.files, hostUnitPath)
		s := testHostStack(t, w)
		for _, name := range []string{"wrong-standalone-args", "java-too-old"} {
			if ok, _, err := setupScenarios[name].verify(ctx, s); err == nil || ok {
				t.Errorf("%s without the unit file: ok=%v err=%v", name, ok, err)
			}
		}
	})
	t.Run("breaks refuse a missing worker file", func(t *testing.T) {
		w := hostWorldFromBase(t)
		delete(w.files, hostStandaloneProps)
		s := testHostStack(t, w)
		if err := breakHostPluginPathUnset(ctx, s); err == nil || !strings.Contains(err.Error(), "recreate the fake host") {
			t.Errorf("break plugin-path-unset without the file: %v", err)
		}
	})
	t.Run("breaks refuse when already broken", func(t *testing.T) {
		w := hostWorldFromBase(t)
		s := testHostStack(t, w)
		if err := breakHostStandaloneArgs(ctx, s); err != nil {
			t.Fatalf("first break: %v", err)
		}
		if err := breakHostStandaloneArgs(ctx, s); err == nil || !strings.Contains(err.Error(), "refusing") {
			t.Errorf("second break: %v", err)
		}
		u := parseHostUnit(w.files[hostUnitPath])
		w.files[hostUnitPath] = setHostEnvironment(u.raw, "JAVA_HOME", hostOldJavaHome)
		if err := breakHostJavaTooOld(ctx, s); err == nil || !strings.Contains(err.Error(), "refusing") {
			t.Errorf("java break on the old JDK already: %v", err)
		}
		text, _ := deleteHostProperty(w.files[hostStandaloneProps], "plugin.path")
		w.files[hostStandaloneProps] = text
		if err := breakHostPluginPathUnset(ctx, s); err == nil || !strings.Contains(err.Error(), "refusing") {
			t.Errorf("plugin break without plugin.path: %v", err)
		}
	})
	t.Run("stale image", func(t *testing.T) {
		w := hostWorldFromBase(t)
		delete(w.java, hostOldJavaHome+"/bin/java")
		delete(w.files, hostDistributedProps)
		s := testHostStack(t, w)
		if err := breakHostJavaTooOld(ctx, s); err == nil || !strings.Contains(err.Error(), "image is stale") {
			t.Errorf("java break without the old JDK: %v", err)
		}
		if err := breakHostStandaloneArgs(ctx, s); err == nil || !strings.Contains(err.Error(), "image is stale") {
			t.Errorf("args break without the distributed file: %v", err)
		}
	})
}

// TestHostStandaloneArgsBranches covers the argv shapes the check must name:
// no ExecStart, no worker argument, the wrong launcher for the mode, a
// launcher that is not executable, a missing worker file and a worker file
// without its mode's key.
func TestHostStandaloneArgsBranches(t *testing.T) {
	ctx := context.Background()
	setExec := func(w *hostWorld, argv ...string) {
		u := parseHostUnit(w.files[hostUnitPath])
		w.files[hostUnitPath] = setHostExecStart(u.raw, argv[0], argv[1])
	}
	cases := []struct {
		name   string
		mutate func(*hostWorld)
		want   string
	}{
		{"no ExecStart", func(w *hostWorld) {
			lines := hostLines(w.files[hostUnitPath])
			var kept []string
			for _, l := range lines {
				if !strings.HasPrefix(l, "ExecStart=") {
					kept = append(kept, l)
				}
			}
			w.files[hostUnitPath] = strings.Join(kept, "\n") + "\n"
		}, "no ExecStart= line"},
		{"no worker argument", func(w *hostWorld) {
			u := parseHostUnit(w.files[hostUnitPath])
			w.files[hostUnitPath] = strings.Replace(u.raw, " "+hostStandaloneProps, "", 1)
		}, "without a worker properties file"},
		{"extra arguments", func(w *hostWorld) {
			u := parseHostUnit(w.files[hostUnitPath])
			w.files[hostUnitPath] = strings.Replace(u.raw, hostStandaloneProps+"\n", hostStandaloneProps+" --verbose\n", 1)
		}, "with one worker properties file"},
		{"wrong launcher", func(w *hostWorld) {
			setExec(w, hostDistributedScript, hostStandaloneProps)
		}, "the standalone worker, whose launcher is connect-standalone.sh"},
		{"not executable", func(w *hostWorld) {
			delete(w.executable, hostStandaloneScript)
		}, "missing or not executable"},
		{"worker file missing", func(w *hostWorld) {
			delete(w.files, hostStandaloneProps)
		}, "does not exist"},
		{"no mode in the description", func(w *hostWorld) {
			u := parseHostUnit(w.files[hostUnitPath])
			w.files[hostUnitPath] = strings.Replace(u.raw, "Description=Piperoom Kafka Connect worker (standalone)", "Description=Piperoom Kafka Connect worker", 1)
		}, "does not name the worker mode"},
		{"no offset file key", func(w *hostWorld) {
			text, _ := deleteHostProperty(w.files[hostStandaloneProps], "offset.storage.file.filename")
			w.files[hostStandaloneProps] = text
		}, "has no offset.storage.file.filename"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := hostWorldFromBase(t)
			tc.mutate(w)
			ok, detail, err := verifyHostStandaloneArgs(ctx, testHostStack(t, w))
			if err != nil {
				t.Fatalf("check: %v", err)
			}
			if ok || !strings.Contains(detail, tc.want) {
				t.Fatalf("ok=%v detail=%q, want a failure containing %q", ok, detail, tc.want)
			}
		})
	}

	t.Run("a distributed unit passes", func(t *testing.T) {
		w := hostWorldFromBase(t)
		u := parseHostUnit(w.files[hostUnitPath])
		raw := strings.Replace(u.raw, "(standalone)", "(distributed)", 1)
		w.files[hostUnitPath] = setHostExecStart(raw, hostDistributedScript, hostDistributedProps)
		if ok, detail, err := verifyHostStandaloneArgs(ctx, testHostStack(t, w)); err != nil || !ok {
			t.Fatalf("ok=%v detail=%q err=%v", ok, detail, err)
		}
	})
}

// TestHostPluginPathBranches: a missing worker file, an unset key, a path that
// is not a directory and a list with one bad entry each fail the check.
func TestHostPluginPathBranches(t *testing.T) {
	ctx := context.Background()
	cases := []struct {
		name   string
		mutate func(*hostWorld)
		want   string
	}{
		{"worker file missing", func(w *hostWorld) { delete(w.files, hostStandaloneProps) }, "is missing from the host"},
		{"key unset", func(w *hostWorld) {
			text, _ := deleteHostProperty(w.files[hostStandaloneProps], "plugin.path")
			w.files[hostStandaloneProps] = text
		}, "has no plugin.path"},
		{"empty value", func(w *hostWorld) {
			w.files[hostStandaloneProps] = setHostProperty(w.files[hostStandaloneProps], "plugin.path", "")
		}, "has no plugin.path"},
		{"not a directory", func(w *hostWorld) {
			delete(w.dirs, hostPluginDir)
		}, "which is not a directory on the host"},
		{"one bad entry in a list", func(w *hostWorld) {
			w.files[hostStandaloneProps] = setHostProperty(w.files[hostStandaloneProps], "plugin.path", hostPluginDir+","+hostProjectDir+"/missing-plugins")
		}, hostProjectDir + "/missing-plugins, which is not a directory"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			w := hostWorldFromBase(t)
			tc.mutate(w)
			ok, detail, err := verifyHostPluginPath(ctx, testHostStack(t, w))
			if err != nil {
				t.Fatalf("check: %v", err)
			}
			if ok || !strings.Contains(detail, tc.want) {
				t.Fatalf("ok=%v detail=%q, want a failure containing %q", ok, detail, tc.want)
			}
		})
	}
}

// TestHostJavaCheckBranches: an old JDK, a java that cannot run, output with
// no version, a unit without JAVA_HOME (java on PATH) and a healthy JDK.
func TestHostJavaCheckBranches(t *testing.T) {
	ctx := context.Background()
	t.Run("old JDK", func(t *testing.T) {
		w := hostWorldFromBase(t)
		u := parseHostUnit(w.files[hostUnitPath])
		w.files[hostUnitPath] = setHostEnvironment(u.raw, "JAVA_HOME", hostOldJavaHome)
		ok, detail, err := verifyHostJavaVersion(ctx, testHostStack(t, w))
		if err != nil || ok || !strings.Contains(detail, "JAVA_HOME=/opt/piperoom/jdk-11") || !strings.Contains(detail, "needs Java 17") {
			t.Fatalf("ok=%v detail=%q err=%v", ok, detail, err)
		}
	})
	t.Run("no version in the output", func(t *testing.T) {
		w := hostWorldFromBase(t)
		w.java[hostJavaHome+"/bin/java"] = "java: error while loading shared libraries\n"
		ok, detail, err := verifyHostJavaVersion(ctx, testHostStack(t, w))
		if err != nil || ok || !strings.Contains(detail, "printed no version") {
			t.Fatalf("ok=%v detail=%q err=%v", ok, detail, err)
		}
	})
	t.Run("java on PATH", func(t *testing.T) {
		w := hostWorldFromBase(t)
		u := parseHostUnit(w.files[hostUnitPath])
		w.files[hostUnitPath] = strings.Replace(u.raw, "Environment=JAVA_HOME="+hostJavaHome+"\n", "", 1)
		w.java["java"] = "openjdk version \"17.0.12\" 2024-07-16\n"
		ok, detail, err := verifyHostJavaVersion(ctx, testHostStack(t, w))
		if err != nil || !ok || !strings.Contains(detail, "java on PATH") {
			t.Fatalf("ok=%v detail=%q err=%v", ok, detail, err)
		}
	})
	t.Run("PATH java too old", func(t *testing.T) {
		w := hostWorldFromBase(t)
		u := parseHostUnit(w.files[hostUnitPath])
		w.files[hostUnitPath] = strings.Replace(u.raw, "Environment=JAVA_HOME="+hostJavaHome+"\n", "", 1)
		w.java["java"] = "java version \"1.8.0_392\"\n"
		ok, detail, err := verifyHostJavaVersion(ctx, testHostStack(t, w))
		if err != nil || ok || !strings.Contains(detail, "reports java 1.8.0_392") {
			t.Fatalf("ok=%v detail=%q err=%v", ok, detail, err)
		}
	})
	t.Run("no java at all", func(t *testing.T) {
		w := hostWorldFromBase(t)
		delete(w.java, hostJavaHome+"/bin/java")
		ok, detail, err := verifyHostJavaVersion(ctx, testHostStack(t, w))
		if err != nil || ok || !strings.Contains(detail, "exited 127") {
			t.Fatalf("ok=%v detail=%q err=%v", ok, detail, err)
		}
	})
	t.Run("relative JAVA_HOME", func(t *testing.T) {
		w := hostWorldFromBase(t)
		u := parseHostUnit(w.files[hostUnitPath])
		w.files[hostUnitPath] = setHostEnvironment(u.raw, "JAVA_HOME", "jdk")
		ok, detail, err := verifyHostJavaVersion(ctx, testHostStack(t, w))
		if err != nil || ok || !strings.Contains(detail, "is not an absolute path") {
			t.Fatalf("ok=%v detail=%q err=%v", ok, detail, err)
		}
	})
}

// TestHostJavaVersionParsing pins the version shapes a host reports.
func TestHostJavaVersionParsing(t *testing.T) {
	cases := []struct {
		line, version string
		major         int
	}{
		{`openjdk version "21.0.8" 2025-07-15`, "21.0.8", 21},
		{`openjdk version "17" 2024-03-19`, "17", 17},
		{`java version "1.8.0_392"`, "1.8.0_392", 8},
		{`openjdk version "21-ea" 2025-09-16`, "21-ea", 21},
		{"no version here", "", 0},
	}
	for _, tc := range cases {
		t.Run(tc.line, func(t *testing.T) {
			if got := hostJavaVersion(tc.line); got != tc.version {
				t.Errorf("hostJavaVersion = %q, want %q", got, tc.version)
			}
			if got := hostJavaMajor(tc.version); got != tc.major {
				t.Errorf("hostJavaMajor(%q) = %d, want %d", tc.version, got, tc.major)
			}
		})
	}
}

// TestHostFileEdits: the helpers the fixes use write the unit's two lines and
// the properties key, and a fix restores the base bytes exactly.
func TestHostFileEdits(t *testing.T) {
	base := "ExecStart=/a.sh /a.properties\nEnvironment=JAVA_HOME=/jdk\n"
	if got := setHostExecStart(base, "/b.sh", "/b.properties"); got != "ExecStart=/b.sh /b.properties\nEnvironment=JAVA_HOME=/jdk\n" {
		t.Errorf("setHostExecStart = %q", got)
	}
	if got := setHostExecStart("no exec\n", "/b.sh", "/b.properties"); got != "no exec\n" {
		t.Errorf("setHostExecStart without a line = %q", got)
	}
	if got := setHostEnvironment(base, "JAVA_HOME", "/old"); got != "ExecStart=/a.sh /a.properties\nEnvironment=JAVA_HOME=/old\n" {
		t.Errorf("setHostEnvironment = %q", got)
	}
	if got := setHostEnvironment(base, "LOG_DIR", "/logs"); got != base {
		t.Errorf("setHostEnvironment of a missing name = %q", got)
	}

	props := "# header\nbootstrap.servers=h:1\nplugin.path=/p\nvalue.converter=C\n"
	without, removed := deleteHostProperty(props, "plugin.path")
	if !removed || without != "# header\nbootstrap.servers=h:1\nvalue.converter=C\n" {
		t.Errorf("deleteHostProperty = %q, %v", without, removed)
	}
	if got := setHostProperty(without, "plugin.path", "/p"); got != props {
		t.Errorf("setHostProperty did not restore the base bytes: %q", got)
	}
	if got := setHostProperty(props, "plugin.path", "/q"); !strings.Contains(got, "plugin.path=/q\n") {
		t.Errorf("setHostProperty did not replace in place: %q", got)
	}
	// A file without a trailing newline keeps that shape.
	if got := setHostProperty("a=1", "b", "2"); got != "a=1\nb=2" {
		t.Errorf("setHostProperty without a trailing newline = %q", got)
	}
	if _, removed := deleteHostProperty(props, "absent"); removed {
		t.Error("deleteHostProperty removed a key that is not there")
	}
	entries := parseHostProperties("  a=1\r\nb: 2\n! comment\nc 3\n")
	for key, want := range map[string]string{"a": "1", "b": "2", "c": "3"} {
		if got := entries[key]; got != want {
			t.Errorf("parseHostProperties[%q] = %q, want %q", key, got, want)
		}
	}
}

// TestHostFakeHostImage checks what Docker would do with dev/host: the base
// project is a valid host project for all three checks, the old JDK reports a
// version below the minimum, and the Dockerfile keeps the launchers and the
// shim executable (a Windows checkout has no executable bit).
func TestHostFakeHostImage(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatalf("%v", err)
	}
	dir := filepath.Join(root, "dev", "host")

	dockerfile, err := os.ReadFile(filepath.Join(dir, "Dockerfile"))
	if err != nil {
		t.Fatalf("read dev/host/Dockerfile: %v", err)
	}
	text := string(dockerfile)
	for _, want := range []string{
		"FROM eclipse-temurin:21-jre", "COPY base /opt/piperoom/host-base", "COPY jdk-11 /opt/piperoom/jdk-11",
		"COPY start.sh /opt/piperoom/start.sh", "chmod 0755 /opt/piperoom/start.sh /opt/piperoom/jdk-11/bin/java",
		"/opt/piperoom/host-base/kafka/bin/connect-standalone.sh", "/opt/piperoom/host-base/kafka/bin/connect-distributed.sh",
		`CMD ["bash", "/opt/piperoom/start.sh"]`,
	} {
		if !strings.Contains(text, want) {
			t.Errorf("dev/host/Dockerfile lacks %q", want)
		}
	}

	// The old JDK prints a version below the connector's minimum, which is
	// what java-too-old makes the check report.
	shim, err := os.ReadFile(filepath.Join(dir, "jdk-11", "bin", "java"))
	if err != nil {
		t.Fatalf("read dev/host/jdk-11/bin/java: %v", err)
	}
	version := hostJavaVersion(string(shim))
	if version == "" {
		t.Fatal("dev/host/jdk-11/bin/java prints no version line")
	}
	if major := hostJavaMajor(version); major >= hostMinJava {
		t.Errorf("the old JDK reports java %s (major %d); java-too-old needs a version below %d", version, major, hostMinJava)
	}
	// start.sh lays the project down at every start, which is what makes a
	// container restart heal every break.
	start, err := os.ReadFile(filepath.Join(dir, "start.sh"))
	if err != nil {
		t.Fatalf("read dev/host/start.sh: %v", err)
	}
	for _, want := range []string{"mkdir -p /piperoom-host", "rm -rf /piperoom-host/project", "cp -a /opt/piperoom/host-base /piperoom-host/project"} {
		if !strings.Contains(string(start), want) {
			t.Errorf("dev/host/start.sh lacks %q", want)
		}
	}

	// The unit the base project carries is the one service_install renders,
	// with paths the devctl constants name.
	w := hostWorldFromBase(t)
	u, err := testHostStack(t, w).hostUnit(context.Background())
	if err != nil {
		t.Fatalf("parse the base unit: %v", err)
	}
	if u.description != "Piperoom Kafka Connect worker (standalone)" {
		t.Errorf("base unit description = %q", u.description)
	}
	if len(u.execStart) != 2 || u.execStart[0] != hostStandaloneScript || u.execStart[1] != hostStandaloneProps {
		t.Errorf("base unit ExecStart = %v, want [%s %s]", u.execStart, hostStandaloneScript, hostStandaloneProps)
	}
	if u.javaHome != hostJavaHome {
		t.Errorf("base unit JAVA_HOME = %q, want %q", u.javaHome, hostJavaHome)
	}
	if got := parseHostProperties(w.files[hostStandaloneProps])["plugin.path"]; got != hostPluginDir {
		t.Errorf("base connect-standalone.properties plugin.path = %q, want %q", got, hostPluginDir)
	}
	if got := parseHostProperties(w.files[hostDistributedProps])["group.id"]; got == "" {
		t.Error("base connect-distributed.properties has no group.id; the wrong-standalone-args break needs a distributed file")
	}

	// All three checks pass on the base project, which is the state the
	// container comes up in.
	s := testHostStack(t, w)
	for _, name := range hostSetupNames {
		sc := setupScenarios[name]
		if ok, detail, err := sc.verify(context.Background(), s); err != nil || !ok {
			t.Errorf("check %s on the baked base: ok=%v %s err=%v", sc.check, ok, detail, err)
		}
	}
}

// TestHostComposeProfile: `devctl setup-break` starts the host profile
// itself, so the service must be opt-in, buildable, portless and healthy once
// the project is laid down.
func TestHostComposeProfile(t *testing.T) {
	root, err := repoRoot()
	if err != nil {
		t.Fatalf("%v", err)
	}
	raw, err := os.ReadFile(filepath.Join(root, "dev", "docker-compose.yml"))
	if err != nil {
		t.Fatalf("read dev/docker-compose.yml: %v", err)
	}
	var doc struct {
		Services map[string]struct {
			Profiles    []string                `yaml:"profiles"`
			Image       string                  `yaml:"image"`
			Build       any                     `yaml:"build"`
			Ports       []string                `yaml:"ports"`
			DependsOn   any                     `yaml:"depends_on"`
			Healthcheck struct{ Test []string } `yaml:"healthcheck"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("parse dev/docker-compose.yml: %v", err)
	}
	svc, ok := doc.Services[hostService]
	if !ok {
		t.Fatalf("no %s service in dev/docker-compose.yml", hostService)
	}
	if len(svc.Profiles) != 1 || svc.Profiles[0] != hostProfile {
		t.Errorf("profiles = %v, want [%s] (the default stack must not start the fake host)", svc.Profiles, hostProfile)
	}
	if svc.Build.(map[string]any)["context"] != "./host" {
		t.Errorf("build = %v, want a context of ./host", svc.Build)
	}
	if !strings.HasPrefix(svc.Image, "piperoom-dev/host") {
		t.Errorf("image = %q, want a piperoom-dev/host tag", svc.Image)
	}
	if len(svc.Ports) != 0 {
		t.Errorf("ports = %v: nothing listens on the fake host", svc.Ports)
	}
	if svc.DependsOn != nil {
		t.Errorf("depends_on = %v: the profile starts alone", svc.DependsOn)
	}
	hc := strings.Join(svc.Healthcheck.Test, " ")
	if !strings.Contains(hc, "test -f "+hostUnitPath) {
		t.Errorf("healthcheck %q does not read %s", hc, hostUnitPath)
	}
	for name, other := range doc.Services {
		if name != hostService && slices.Contains(other.Profiles, hostProfile) {
			t.Errorf("service %s is also in the %s profile", name, hostProfile)
		}
	}
}
