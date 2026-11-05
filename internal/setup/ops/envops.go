package ops

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os/exec"
	"path"
	"regexp"
	"runtime"
	"sort"
	"strings"

	"piperoom/internal/events"
	"piperoom/internal/redact"
	"piperoom/internal/setup/protocol"
)

func goos() string   { return runtime.GOOS }
func goarch() string { return runtime.GOARCH }

func defaultLookPath(name string) (string, error) { return exec.LookPath(name) }

// envDetectOp reports the runner's OS, arch, project files and the setup
// tools on PATH. It runs nothing: java_version and docker_info have their own
// ops, and a T0 op that spawned every tool would be slow and noisy.
type envDetectOp struct{}

// detectTools is the fixed probe list of design §2.1. A name here is a
// literal; no request adds one.
var detectTools = []string{"docker", "kubectl", "helm", "java", "aws", "confluent", "avn", "systemctl"}

// detectFiles are the project files the classifier may read a fact from.
var detectFiles = []string{
	"compose.yaml", "compose.yml", "docker-compose.yml", "docker-compose.yaml",
	"Dockerfile", ".env", "plugins", "kafka", "connect", "config",
}

type fileFact struct {
	Name string `json:"name"`
	Kind string `json:"kind"` // "file" | "dir"
}

type envInfo struct {
	OS           string     `json:"os"`
	Arch         string     `json:"arch"`
	ProjectDir   string     `json:"project_dir"`
	ProjectFiles []fileFact `json:"project_files"`
	Tools        []string   `json:"tools"`
}

func (envDetectOp) Name() string     { return "env_detect" }
func (envDetectOp) Target() string   { return "" }
func (envDetectOp) Tier() string     { return protocol.TierT0 }
func (envDetectOp) Layer() string    { return "environment" }
func (envDetectOp) Describe() string { return "report OS, architecture, project files and PATH tools" }

func (envDetectOp) ParamsSchema() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": false}
}

func (envDetectOp) Validate(params json.RawMessage, env Env) error {
	return decodeParams(params, &struct{}{})
}

func (envDetectOp) Execute(ctx context.Context, params json.RawMessage, env Env) (Result, error) {
	info := envInfo{OS: goos(), Arch: goarch(), ProjectDir: env.ProjectDir, ProjectFiles: []fileFact{}, Tools: []string{}}
	for _, name := range detectFiles {
		fi, err := env.Confine.root.Lstat(name)
		if err != nil || fi.Mode()&fs.ModeSymlink != 0 {
			continue
		}
		kind := "file"
		if fi.IsDir() {
			kind = "dir"
		} else if !fi.Mode().IsRegular() {
			continue
		}
		info.ProjectFiles = append(info.ProjectFiles, fileFact{Name: name, Kind: kind})
	}
	look := env.LookPath
	if look == nil {
		look = defaultLookPath
	}
	for _, name := range detectTools {
		if p, err := look(name); err == nil && p != "" {
			info.Tools = append(info.Tools, name)
		}
	}
	sort.Strings(info.Tools)
	summary := info.OS + "/" + info.Arch
	if len(info.ProjectFiles) > 0 {
		names := make([]string, 0, len(info.ProjectFiles))
		for _, f := range info.ProjectFiles {
			names = append(names, f.Name)
		}
		summary += "; project has " + strings.Join(names, ", ")
	}
	if len(info.Tools) > 0 {
		summary += "; " + strings.Join(info.Tools, ", ") + " on PATH"
	}
	return Result{Summary: summary, Data: info, Receipt: events.SetupReceipt{Kind: "", Target: env.ProjectDir, At: env.Now()}}, nil
}

// javaVersionOp runs the fixed literal "java -version" with an argv slice.
type javaVersionOp struct{}

type javaVersionInfo struct {
	Version string `json:"version"`
	Vendor  string `json:"vendor,omitempty"`
	Line    string `json:"line"`
}

// javaVersionLine matches the quoted version of `java -version` and of the
// OpenJDK builds that print "openjdk version "21.0.2" 2024-01-16".
var javaVersionLine = regexp.MustCompile(`(?i)\bversion\s+"([^"]+)"`)

func (javaVersionOp) Name() string     { return "java_version" }
func (javaVersionOp) Target() string   { return "" }
func (javaVersionOp) Tier() string     { return protocol.TierT0 }
func (javaVersionOp) Layer() string    { return "environment" }
func (javaVersionOp) Describe() string { return "report the java on PATH and its version" }

func (javaVersionOp) ParamsSchema() map[string]any {
	return map[string]any{"type": "object", "additionalProperties": false}
}

func (javaVersionOp) Validate(params json.RawMessage, env Env) error {
	return decodeParams(params, &struct{}{})
}

func (javaVersionOp) Execute(ctx context.Context, params json.RawMessage, env Env) (Result, error) {
	run := env.Exec
	if run == nil {
		run = runCommand
	}
	out, code, err := run(ctx, "java", "-version")
	if err != nil {
		return Result{Summary: "java not found on PATH"}, fmt.Errorf("%w: java: %v", ErrExec, err)
	}
	// java's stderr can echo JAVA_TOOL_OPTIONS and _JAVA_OPTIONS, which carry
	// whatever the operator put there (keystore passwords included). Redact
	// before clipping, so a mask is never cut in half.
	text, _ := redact.Text(strings.TrimSpace(string(out)))
	if code != 0 {
		return Result{Summary: fmt.Sprintf("java -version exited %d", code), Data: map[string]any{"output": clip(text)}},
			fmt.Errorf("%w: java -version exited %d", ErrExec, code)
	}
	line := firstLine(text)
	m := javaVersionLine.FindStringSubmatch(line)
	if m == nil {
		return Result{Summary: "could not read the java version", Data: map[string]any{"line": clip(line)}},
			fmt.Errorf("%w: unexpected java -version output", ErrExec)
	}
	vendor := ""
	if fields := strings.Fields(line); len(fields) > 0 {
		vendor = strings.ToLower(fields[0])
	}
	return Result{
		Summary: "java " + m[1],
		Data:    javaVersionInfo{Version: m[1], Vendor: vendor, Line: clip(line)},
		Receipt: events.SetupReceipt{Kind: "exec", Argv: []string{"java", "-version"}, Target: "java", At: env.Now()},
	}, nil
}

// jarListOp inspects Debezium plugin directories: it flags jars directly in
// the plugins root, two Debezium versions side by side, and a connector
// directory missing the debezium-core/debezium-api jars it needs.
type jarListOp struct{}

type jarListParams struct {
	Path string `json:"path,omitempty"`
}

type missingJars struct {
	Dir     string   `json:"dir"`
	Version string   `json:"version"`
	Missing []string `json:"missing"`
}

type jarListing struct {
	Path        string        `json:"path"`
	Jars        []string      `json:"jars"`
	Flat        []string      `json:"flat,omitempty"`
	Versions    []string      `json:"debezium_versions,omitempty"`
	Missing     []missingJars `json:"missing_converters,omitempty"`
	Issues      []string      `json:"issues,omitempty"`
	Truncated   bool          `json:"truncated,omitempty"`
	ScannedDirs int           `json:"scanned_dirs"`
}

const maxListedJars = 200

func (jarListOp) Name() string     { return "jar_list" }
func (jarListOp) Target() string   { return "" }
func (jarListOp) Tier() string     { return protocol.TierT0 }
func (jarListOp) Layer() string    { return "environment" }
func (jarListOp) Describe() string { return "inspect Debezium plugin directories for layout problems" }

func (jarListOp) ParamsSchema() map[string]any {
	return map[string]any{
		"type": "object", "additionalProperties": false,
		"properties": map[string]any{"path": map[string]any{"type": "string"}},
	}
}

func (jarListOp) Validate(params json.RawMessage, env Env) error {
	var p jarListParams
	if err := decodeParams(params, &p); err != nil {
		return err
	}
	if p.Path != "" {
		if why := lexicalRefusal(p.Path); why != "" {
			return fmt.Errorf("%w: %s: %s", ErrPath, why, clip(p.Path))
		}
	}
	return nil
}

func (jarListOp) Execute(ctx context.Context, params json.RawMessage, env Env) (Result, error) {
	var p jarListParams
	if err := decodeParams(params, &p); err != nil {
		return Result{}, err
	}
	if p.Path == "" {
		p.Path = "."
	}
	clean, _, err := env.Confine.prepare(p.Path)
	if err != nil {
		return Result{}, err
	}
	rootJars, subdirs, err := listJars(env, clean)
	if err != nil {
		return Result{}, err
	}
	listing := jarListing{Path: clean, Jars: []string{}, ScannedDirs: 1 + len(subdirs)}

	// A plugins root holds one connector per subdirectory; jars at the root
	// are "flat" unless the scanned path is itself a connector directory.
	if !isConnectorDir(path.Base(clean)) {
		for _, j := range rootJars {
			listing.Flat = append(listing.Flat, j)
		}
	}

	type dirJars struct {
		dir  string
		jars []string
	}
	dirs := []dirJars{{dir: clean, jars: rootJars}}
	for _, sub := range subdirs {
		jars, _, serr := listJars(env, clean+"/"+sub)
		if serr != nil {
			continue
		}
		dirs = append(dirs, dirJars{dir: clean + "/" + sub, jars: jars})
	}

	coreVersions := map[string]bool{}
	for _, d := range dirs {
		sort.Strings(d.jars)
		// listJars returns bare file names; prefix each with its directory
		// relative to the scanned root, so two same-named jars in different
		// connector directories stay apart in the listing.
		rel := ""
		if d.dir != clean {
			rel = strings.TrimPrefix(d.dir, clean+"/") + "/"
		}
		for _, j := range d.jars {
			if len(listing.Jars) < maxListedJars {
				listing.Jars = append(listing.Jars, rel+j)
			} else {
				listing.Truncated = true
			}
			if kind, _, version, ok := parseDebeziumJar(j); ok && (kind == "core" || kind == "api") {
				coreVersions[version] = true
			}
		}
	}
	for v := range coreVersions {
		listing.Versions = append(listing.Versions, v)
	}
	sort.Strings(listing.Versions)
	if len(listing.Versions) > 1 {
		listing.Issues = append(listing.Issues,
			"two Debezium versions side by side: "+strings.Join(listing.Versions, ", "))
	}
	if len(listing.Flat) > 0 {
		listing.Issues = append(listing.Issues,
			fmt.Sprintf("%d jar(s) directly in %s; each connector needs its own directory", len(listing.Flat), clean))
	}

	for _, d := range dirs {
		connectors, cores, apis := map[string]string{}, map[string]bool{}, map[string]bool{}
		for _, j := range d.jars {
			kind, name, version, ok := parseDebeziumJar(j)
			if !ok {
				continue
			}
			switch kind {
			case "core":
				cores[version] = true
			case "api":
				apis[version] = true
			case "connector":
				connectors[name] = version
			}
		}
		for name, version := range connectors {
			var missing []string
			if !cores[version] {
				missing = append(missing, "debezium-core-"+version+".jar")
			}
			if !apis[version] {
				missing = append(missing, "debezium-api-"+version+".jar")
			}
			if len(missing) > 0 {
				sort.Strings(missing)
				dirName := strings.TrimPrefix(d.dir, clean+"/")
				if dirName == "" {
					dirName = "."
				}
				listing.Missing = append(listing.Missing, missingJars{
					Dir: dirName, Version: version, Missing: missing,
				})
				listing.Issues = append(listing.Issues,
					"connector "+name+" "+version+" is missing "+strings.Join(missing, " and "))
			}
		}
	}
	sort.Slice(listing.Missing, func(i, j int) bool {
		if listing.Missing[i].Dir == listing.Missing[j].Dir {
			return listing.Missing[i].Version < listing.Missing[j].Version
		}
		return listing.Missing[i].Dir < listing.Missing[j].Dir
	})

	summary := fmt.Sprintf("jar_list %s: %d jars", clean, len(listing.Jars))
	if len(listing.Versions) > 0 {
		summary += ", debezium " + strings.Join(listing.Versions, " and ")
	}
	for _, issue := range listing.Issues {
		summary += "; " + issue
	}
	if listing.Truncated {
		summary += "; truncated"
	}
	return Result{
		Summary:   summary,
		Data:      listing,
		Truncated: listing.Truncated,
		Receipt:   events.SetupReceipt{Kind: "file", Target: clean, At: env.Now()},
	}, nil
}

// listJars lists the jar file names in one directory (through the root) and
// its immediate subdirectories (names only; the caller opens each).
func listJars(env Env, dir string) (jars []string, subdirs []string, err error) {
	clean, _, err := env.Confine.prepare(dir)
	if err != nil {
		return nil, nil, err
	}
	f, err := env.Confine.root.Open(clean)
	if err != nil {
		return nil, nil, fmt.Errorf("ops: opening %s: %w", clip(dir), err)
	}
	defer f.Close()
	entries, err := f.ReadDir(-1)
	if err != nil {
		return nil, nil, fmt.Errorf("ops: listing %s: %w", clip(dir), err)
	}
	for _, e := range entries {
		info, ierr := e.Info()
		if ierr != nil {
			continue
		}
		name := e.Name()
		if info.Mode()&(fs.ModeSymlink|fs.ModeIrregular) != 0 || deniedDir(name) {
			continue
		}
		if info.IsDir() {
			subdirs = append(subdirs, name)
			continue
		}
		if info.Mode().IsRegular() && strings.EqualFold(path.Ext(name), ".jar") {
			jars = append(jars, name)
		}
	}
	sort.Strings(subdirs)
	return jars, subdirs, nil
}

// parseDebeziumJar parses debezium-core-<v>.jar, debezium-api-<v>.jar and
// debezium-connector-<name>-<v>.jar. The connector name may contain dashes,
// so the version starts at the first dash followed by a digit.
func parseDebeziumJar(name string) (kind, connector, version string, ok bool) {
	if !strings.EqualFold(path.Ext(name), ".jar") {
		return "", "", "", false
	}
	n := strings.TrimSuffix(name, path.Ext(name))
	rest, found := strings.CutPrefix(n, "debezium-")
	if !found {
		return "", "", "", false
	}
	if v, found := strings.CutPrefix(rest, "core-"); found {
		return "core", "", v, v != ""
	}
	if v, found := strings.CutPrefix(rest, "api-"); found {
		return "api", "", v, v != ""
	}
	c, found := strings.CutPrefix(rest, "connector-")
	if !found {
		return "", "", "", false
	}
	for i := 0; i < len(c); i++ {
		if c[i] == '-' && i+1 < len(c) && c[i+1] >= '0' && c[i+1] <= '9' {
			return "connector", c[:i], c[i+1:], true
		}
	}
	return "", "", "", false
}

// isConnectorDir reports whether base names a Debezium connector plugin dir.
func isConnectorDir(base string) bool {
	return strings.HasPrefix(strings.ToLower(base), "debezium-connector-")
}

func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return strings.TrimSpace(s[:i])
	}
	return s
}
