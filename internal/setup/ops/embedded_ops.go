package ops

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"piperoom/internal/events"
	"piperoom/internal/redact"
	"piperoom/internal/setup/knowledge"
	"piperoom/internal/setup/protocol"
)

// The embedded target's ops (row U6b, docs/design/setup-agent.md §2.5 "the
// embedded reference project"): a person embeds the Debezium Engine in their
// own application, so the runner has no stack to render or start.
//
//   - project_inspect (T0) reads the project's build files — and only build
//     files, never source — for the Debezium, Kafka, Quarkus and Java versions
//     they declare, and runs U6a's version rules over them
//     (knowledge.CheckEmbedded).
//   - reference_write (T1) renders U6a's reference project from typed params
//     (knowledge.RenderEmbedded) under <project dir>/piperoom-reference/.
//   - embedded_log_read (T0) reads a log file the person names under the
//     project directory and keeps only the reference app's own
//     "piperoom-reference ..." lines: the verification ladder's embedded rungs
//     (plan.EmbeddedLadder) judge them.
//
// There is no op that builds or runs anything: no reference_run, no mvn, no
// gradle, no java -jar. The person builds and runs the reference project; the
// runner reads what it logs (TestEmbeddedCatalogueRunsNothing).
//
// RegisterEmbedded also puts the registry in embedded mode: every write op
// other than reference_write is refused before it executes, so in an
// embedded room nothing the agent proposes can write outside
// piperoom-reference/ (and piperoom-reference/ is a mount directory, so
// file_write and file_delete never reach it in any mode).

// Names of the embedded ops.
const (
	OpProjectInspect  = "project_inspect"
	OpReferenceWrite  = "reference_write"
	OpEmbeddedLogRead = "embedded_log_read"
)

// Bounds of the embedded ops.
const (
	// maxBuildFiles caps the build files project_inspect reads.
	maxBuildFiles = 32
	// maxInspectDepth is how deep project_inspect looks for build files
	// (the project root is depth 0): a multi-module build's modules.
	maxInspectDepth = 3
	// maxBuildFileBytes caps one build file.
	maxBuildFileBytes = 256 << 10
	// maxLogTailBytes is how much of the end of a log file embedded_log_read
	// reads.
	maxLogTailBytes = 1 << 20
	// maxLogLines caps the reference app lines embedded_log_read returns.
	maxLogLines = 50
	// maxDestinations caps the event destinations embedded_log_read lists.
	maxDestinations = 32
)

// ErrEmbeddedMode: a write op other than reference_write in an embedded room.
var ErrEmbeddedMode = errors.New("ops: embedded mode writes only the reference project")

// RegisterEmbedded adds the embedded target's ops to r, registers
// piperoom-reference/ as a mount directory and puts r in embedded mode.
func RegisterEmbedded(r *Registry) error {
	if r == nil {
		return errors.New("ops: a registry is required")
	}
	for _, op := range []Op{projectInspectOp{}, referenceWriteOp{}, embeddedLogReadOp{}} {
		if err := r.Register(op); err != nil {
			return fmt.Errorf("ops: registering the embedded target: %w", err)
		}
	}
	if err := r.RegisterMountDir(knowledge.EmbeddedRootDir); err != nil {
		return err
	}
	r.mu.Lock()
	r.embedded = true
	r.mu.Unlock()
	return nil
}

// embeddedRefusal is the embedded mode's rule: in embedded mode an op that
// is not a read and not reference_write is refused.
func (r *Registry) embeddedRefusal(op Op) error {
	r.mu.RLock()
	embedded := r.embedded
	r.mu.RUnlock()
	if !embedded || op.Tier() == protocol.TierT0 || op.Name() == OpReferenceWrite {
		return nil
	}
	return fmt.Errorf("%w: %s is refused in an embedded room (the runner writes only %s/ there, through %s)", ErrEmbeddedMode, op.Name(), knowledge.EmbeddedRootDir, OpReferenceWrite)
}

// ---- confined file helpers ----

// readConfined reads at most limit bytes of one regular file under the
// project directory, through the Confiner (no symlink, junction or reparse
// point on the way). tail reads the last limit bytes instead of the first.
func readConfined(env Env, name string, limit int64, tail bool) ([]byte, bool, error) {
	if why := lexicalRefusal(name); why != "" {
		return nil, false, fmt.Errorf("%w: %s: %s", ErrPath, why, clip(name))
	}
	if err := secretReadPath(name); err != nil {
		return nil, false, err
	}
	if err := materialisedRefusal(name, env); err != nil {
		return nil, false, err
	}
	clean, denied, err := env.Confine.prepare(name)
	if err != nil {
		return nil, false, err
	}
	f, err := env.Confine.root.Open(clean)
	if err != nil {
		return nil, false, fmt.Errorf("ops: opening %s: %w", clip(name), err)
	}
	defer f.Close()
	if err := env.Confine.verifyHandle(f, denied); err != nil {
		return nil, false, fmt.Errorf("ops: reading %s: %w", clip(name), err)
	}
	fi, err := f.Stat()
	if err != nil {
		return nil, false, fmt.Errorf("ops: stat %s: %w", clip(name), err)
	}
	if !fi.Mode().IsRegular() {
		return nil, false, fmt.Errorf("%w: %s", ErrNotRegular, clip(name))
	}
	cut := fi.Size() > limit
	if tail && cut {
		if _, err := f.Seek(fi.Size()-limit, io.SeekStart); err != nil {
			return nil, false, fmt.Errorf("ops: seeking %s: %w", clip(name), err)
		}
	}
	data, err := io.ReadAll(io.LimitReader(f, limit))
	if err != nil {
		return nil, false, fmt.Errorf("ops: reading %s: %w", clip(name), err)
	}
	return data, cut, nil
}

// writeConfined writes content to one file under the project directory the
// way file_write does: through the Confiner, a regular-file destination, an
// O_EXCL temporary file renamed over it, and the path walked again after the
// rename. It does not apply the mount-directory rule: its caller is the op
// that owns the directory.
func writeConfined(env Env, name string, content []byte) (string, error) {
	if why := lexicalRefusal(name); why != "" {
		return "", fmt.Errorf("%w: %s: %s", ErrPath, why, clip(name))
	}
	if err := materialisedRefusal(name, env); err != nil {
		return "", err
	}
	clean, denied, err := env.Confine.prepare(name)
	if err != nil {
		return "", err
	}
	if fi, lerr := env.Confine.root.Lstat(clean); lerr == nil {
		if !fi.Mode().IsRegular() {
			return "", fmt.Errorf("%w: %s exists and is not a regular file", ErrNotRegular, clip(name))
		}
	} else if !errors.Is(lerr, fs.ErrNotExist) {
		return "", fmt.Errorf("ops: stat %s: %w", clip(name), lerr)
	}
	if dir := path.Dir(clean); dir != "." {
		if err := env.Confine.root.MkdirAll(dir, 0o755); err != nil {
			return "", fmt.Errorf("ops: creating %s: %w", clip(name), err)
		}
	}
	tmp, err := tempName(clean)
	if err != nil {
		return "", err
	}
	f, err := env.Confine.root.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return "", fmt.Errorf("ops: creating a temp file for %s: %w", clip(name), err)
	}
	_, werr := f.Write(content)
	if werr == nil {
		werr = f.Sync()
	}
	if cerr := f.Close(); werr == nil {
		werr = cerr
	}
	if werr == nil {
		werr = env.Confine.root.Rename(tmp, clean)
	}
	if werr != nil {
		_ = env.Confine.root.Remove(tmp)
		return "", fmt.Errorf("ops: writing %s: %w", clip(name), werr)
	}
	if why := walkRefusal(clean, env.Confine.root.Lstat, denied, os.SameFile); why != "" {
		_ = env.Confine.root.Remove(clean)
		return "", fmt.Errorf("%w: %s: %s (path changed during the write; the file was removed)", ErrPath, why, clip(name))
	}
	return clean, nil
}

// ---- project_inspect (T0) ----

// buildFileNames are the only files project_inspect opens: Maven and Gradle
// build scripts, Gradle's properties and its version catalog. Source files are
// never read.
var buildFileNames = map[string]bool{
	"pom.xml": true, "build.gradle": true, "build.gradle.kts": true,
	"settings.gradle": true, "settings.gradle.kts": true, "gradle.properties": true,
	"libs.versions.toml": true,
}

// skipDirs are never walked: the reference project itself (U6b writes it, and
// its versions are not the person's), version control, IDE state, build
// output and dependency caches.
var skipDirs = map[string]bool{
	knowledge.EmbeddedRootDir: true, ".git": true, ".idea": true, ".vscode": true, ".gradle": true,
	".mvn": true, "target": true, "build": true, "out": true, "node_modules": true, ".piperoom": true,
}

// IsBuildFile reports whether project_inspect may read a file of this name.
func IsBuildFile(name string) bool {
	return buildFileNames[path.Base(strings.ReplaceAll(name, "\\", "/"))]
}

type projectInspectOp struct{}

type projectInspectParams struct {
	// Dir is the project-relative folder to inspect ("" is the project root).
	Dir string `json:"dir,omitempty"`
}

// inspectedVersions is knowledge.EmbeddedVersions on the wire.
type inspectedVersions struct {
	Engine  string `json:"engine,omitempty"`
	Outbox  string `json:"outbox,omitempty"`
	Kafka   string `json:"kafka,omitempty"`
	Quarkus string `json:"quarkus,omitempty"`
	Java    int    `json:"java,omitempty"`
}

func (projectInspectOp) Name() string   { return OpProjectInspect }
func (projectInspectOp) Target() string { return "" }
func (projectInspectOp) Tier() string   { return protocol.TierT0 }
func (projectInspectOp) Layer() string  { return "environment" }
func (projectInspectOp) Describe() string {
	return "read the project's build files (pom.xml, build.gradle, gradle.properties, libs.versions.toml; never source) for the Debezium, Kafka, Quarkus and Java versions they declare, and check them against the embedded version rules"
}

func (projectInspectOp) ParamsSchema() map[string]any {
	return map[string]any{
		"type": "object", "additionalProperties": false,
		"properties": map[string]any{"dir": map[string]any{"type": "string", "maxLength": 512}},
	}
}

func (projectInspectOp) decode(params json.RawMessage) (projectInspectParams, error) {
	var p projectInspectParams
	if err := decodeParams(params, &p); err != nil {
		return p, err
	}
	if p.Dir != "" && p.Dir != "." {
		if why := lexicalRefusal(p.Dir); why != "" {
			return p, fmt.Errorf("%w: %s: %s", ErrPath, why, clip(p.Dir))
		}
	}
	return p, nil
}

func (op projectInspectOp) Validate(params json.RawMessage, _ Env) error {
	_, err := op.decode(params)
	return err
}

func (op projectInspectOp) Execute(ctx context.Context, params json.RawMessage, env Env) (Result, error) {
	p, err := op.decode(params)
	if err != nil {
		return Result{}, err
	}
	root := "."
	if p.Dir != "" && p.Dir != "." {
		clean, _, err := env.Confine.prepare(p.Dir)
		if err != nil {
			return Result{}, err
		}
		root = clean
	}
	files, err := findBuildFiles(ctx, env, root)
	if err != nil {
		return Result{}, err
	}
	var parsed buildInfo
	for _, f := range files {
		data, _, err := readConfined(env, f, maxBuildFileBytes, false)
		if err != nil {
			return Result{}, err
		}
		parsed.add(f, data)
	}
	v := parsed.versions()
	findings, err := knowledge.CheckEmbedded(v)
	if err != nil {
		return Result{}, fmt.Errorf("ops: %s: %w", op.Name(), err)
	}
	if findings == nil {
		findings = []knowledge.Finding{}
	}
	if files == nil {
		files = []string{}
	}
	summary := fmt.Sprintf("%d build files (%s); debezium-embedded %s, kafka %s, quarkus %s, java %s; %d findings",
		len(files), orNone(parsed.tool()), orNone(v.Engine), orNone(v.Kafka), orNone(v.Quarkus), orNone(javaText(v.Java)), len(findings))
	return Result{
		Summary: summary,
		Data: map[string]any{
			"build_files": files,
			"build_tool":  parsed.tool(),
			"versions":    inspectedVersions{Engine: v.Engine, Outbox: v.Outbox, Kafka: v.Kafka, Quarkus: v.Quarkus, Java: v.Java},
			"findings":    findings,
		},
	}, nil
}

func orNone(s string) string {
	if s == "" {
		return "none"
	}
	return s
}

func javaText(n int) string {
	if n == 0 {
		return ""
	}
	return strconv.Itoa(n)
}

// findBuildFiles walks root (project-relative) to maxInspectDepth and returns
// the build files in it, sorted, through the Confiner: no symlinked entry is
// followed (Lstat), and skipDirs are never entered.
func findBuildFiles(ctx context.Context, env Env, root string) ([]string, error) {
	var out []string
	var walk func(dir string, depth int) error
	walk = func(dir string, depth int) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		f, err := env.Confine.root.Open(dir)
		if err != nil {
			return fmt.Errorf("ops: opening %s: %w", clip(dir), err)
		}
		entries, err := f.ReadDir(-1)
		_ = f.Close()
		if err != nil {
			return fmt.Errorf("ops: listing %s: %w", clip(dir), err)
		}
		sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
		for _, e := range entries {
			name := path.Join(dir, e.Name())
			if dir == "." {
				name = e.Name()
			}
			if e.Type()&fs.ModeSymlink != 0 {
				continue
			}
			if e.IsDir() {
				if depth < maxInspectDepth && !skipDirs[e.Name()] && !strings.HasPrefix(e.Name(), ".") {
					if err := walk(name, depth+1); err != nil {
						return err
					}
				}
				continue
			}
			if e.Type().IsRegular() && buildFileNames[e.Name()] && len(out) < maxBuildFiles {
				out = append(out, name)
			}
		}
		return nil
	}
	if err := walk(root, 0); err != nil {
		return nil, err
	}
	return out, nil
}

// buildInfo accumulates what the build files declare.
type buildInfo struct {
	maven, gradle bool
	props         map[string]string // Maven <properties> and gradle.properties, by name
	coords        []coord           // group:artifact:version as written (version may be a reference)
	catalog       map[string]string // libs.versions.toml [versions]
	java          []string          // Java release values as written (may be references)
}

type coord struct{ group, artifact, version string }

func (b *buildInfo) tool() string {
	switch {
	case b.maven && b.gradle:
		return "maven+gradle"
	case b.maven:
		return "maven"
	case b.gradle:
		return "gradle"
	}
	return ""
}

func (b *buildInfo) add(name string, data []byte) {
	if b.props == nil {
		b.props = map[string]string{}
		b.catalog = map[string]string{}
	}
	if !utf8.Valid(data) {
		data = bytes.ToValidUTF8(data, []byte("?"))
	}
	switch path.Base(name) {
	case "pom.xml":
		b.maven = true
		b.addPOM(data)
	case "gradle.properties":
		b.gradle = true
		for _, line := range strings.Split(string(data), "\n") {
			k, v, ok := strings.Cut(strings.TrimSpace(line), "=")
			if ok && !strings.HasPrefix(k, "#") {
				b.props[strings.TrimSpace(k)] = strings.TrimSpace(v)
			}
		}
	case "libs.versions.toml":
		b.gradle = true
		b.addCatalog(string(data))
	default:
		b.gradle = true
		b.addGradle(string(data))
	}
}

// pom is the part of a Maven POM project_inspect reads.
type pom struct {
	Properties struct {
		Entries []struct {
			XMLName xml.Name
			Value   string `xml:",chardata"`
		} `xml:",any"`
	} `xml:"properties"`
	Dependencies []pomDep `xml:"dependencies>dependency"`
	Managed      []pomDep `xml:"dependencyManagement>dependencies>dependency"`
	Plugins      []struct {
		ArtifactID string `xml:"artifactId"`
		Config     struct {
			Release string `xml:"release"`
			Source  string `xml:"source"`
			Target  string `xml:"target"`
		} `xml:"configuration"`
	} `xml:"build>plugins>plugin"`
}

type pomDep struct {
	GroupID    string `xml:"groupId"`
	ArtifactID string `xml:"artifactId"`
	Version    string `xml:"version"`
}

func (b *buildInfo) addPOM(data []byte) {
	var p pom
	dec := xml.NewDecoder(bytes.NewReader(data))
	dec.Strict = false
	// An entity or DTD never reaches a resolver: encoding/xml expands only
	// the five predefined entities, and Strict=false keeps a stray & from
	// failing the whole file.
	if err := dec.Decode(&p); err != nil {
		return
	}
	for _, e := range p.Properties.Entries {
		b.props[e.XMLName.Local] = strings.TrimSpace(e.Value)
	}
	for _, d := range append(p.Dependencies, p.Managed...) {
		b.coords = append(b.coords, coord{strings.TrimSpace(d.GroupID), strings.TrimSpace(d.ArtifactID), strings.TrimSpace(d.Version)})
	}
	for _, k := range []string{"maven.compiler.release", "maven.compiler.target", "maven.compiler.source", "java.version"} {
		if v := b.props[k]; v != "" {
			b.java = append(b.java, v)
			break
		}
	}
	for _, pl := range p.Plugins {
		if strings.TrimSpace(pl.ArtifactID) != "maven-compiler-plugin" {
			continue
		}
		for _, v := range []string{pl.Config.Release, pl.Config.Target, pl.Config.Source} {
			if v = strings.TrimSpace(v); v != "" {
				b.java = append(b.java, v)
				break
			}
		}
	}
}

var (
	// gradleCoordRe finds "group:artifact:version" string literals.
	gradleCoordRe = regexp.MustCompile(`["']([A-Za-z0-9_.-]+):([A-Za-z0-9_.-]+):([A-Za-z0-9_.${}+-]+)["']`)
	// gradleJavaRes find the Java release a Gradle build sets.
	gradleJavaRes = []*regexp.Regexp{
		regexp.MustCompile(`JavaLanguageVersion\.of\(\s*["']?([0-9]{1,2})["']?\s*\)`),
		regexp.MustCompile(`(?:source|target)Compatibility\s*=\s*JavaVersion\.VERSION_(?:1_)?([0-9]{1,2})`),
		regexp.MustCompile(`(?:source|target)Compatibility\s*=\s*["']?(?:1\.)?([0-9]{1,2})["']?`),
		regexp.MustCompile(`options\.release(?:\.set\()?\s*=?\s*\(?([0-9]{1,2})`),
	}
	// gradleVarRes find simple version variables a build script defines.
	gradleVarRes = []*regexp.Regexp{
		regexp.MustCompile(`(?m)^\s*(?:def|val|var)\s+([A-Za-z_][A-Za-z0-9_]*)\s*=\s*["']([A-Za-z0-9_.+-]+)["']`),
		regexp.MustCompile(`(?m)^\s*set\(\s*["']([A-Za-z_][A-Za-z0-9_]*)["']\s*,\s*["']([A-Za-z0-9_.+-]+)["']\s*\)`),
		regexp.MustCompile(`(?m)^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*["']([0-9][A-Za-z0-9_.+-]*)["']\s*$`),
	}
	// catalogLibRe finds a [libraries] entry with a module and a version or a
	// version.ref.
	catalogLibRe = regexp.MustCompile(`module\s*=\s*"([A-Za-z0-9_.-]+):([A-Za-z0-9_.-]+)"\s*,\s*version(?:\.ref)?\s*=\s*"([A-Za-z0-9_.+-]+)"`)
	catalogRefRe = regexp.MustCompile(`version\.ref\s*=`)
)

func (b *buildInfo) addGradle(s string) {
	for _, m := range gradleCoordRe.FindAllStringSubmatch(s, -1) {
		b.coords = append(b.coords, coord{m[1], m[2], m[3]})
	}
	for _, re := range gradleJavaRes {
		if m := re.FindStringSubmatch(s); m != nil {
			b.java = append(b.java, m[1])
			break
		}
	}
	for _, re := range gradleVarRes {
		for _, m := range re.FindAllStringSubmatch(s, -1) {
			if _, seen := b.props[m[1]]; !seen {
				b.props[m[1]] = m[2]
			}
		}
	}
}

func (b *buildInfo) addCatalog(s string) {
	section := ""
	for _, raw := range strings.Split(s, "\n") {
		line := strings.TrimSpace(raw)
		if strings.HasPrefix(line, "[") {
			section = strings.Trim(line, "[] ")
			continue
		}
		switch section {
		case "versions":
			k, v, ok := strings.Cut(line, "=")
			if ok {
				b.catalog[strings.TrimSpace(k)] = strings.Trim(strings.TrimSpace(v), `"'`)
			}
		case "libraries":
			if m := catalogLibRe.FindStringSubmatch(line); m != nil {
				v := m[3]
				if catalogRefRe.MatchString(line) {
					v = "catalog:" + v
				}
				b.coords = append(b.coords, coord{m[1], m[2], v})
			}
		}
	}
}

// resolve expands one version as written: ${name} or $name from the
// properties, catalog:<ref> from the version catalog. An unresolved reference
// is "" (not declared), never the reference text.
func (b *buildInfo) resolve(v string) string {
	for i := 0; i < 4 && v != ""; i++ {
		switch {
		case strings.HasPrefix(v, "catalog:"):
			v = b.catalog[strings.TrimPrefix(v, "catalog:")]
		case strings.HasPrefix(v, "${") && strings.HasSuffix(v, "}"):
			v = b.props[v[2:len(v)-1]]
		case strings.HasPrefix(v, "$"):
			v = b.props[v[1:]]
		default:
			if strings.ContainsAny(v, "${}") {
				return ""
			}
			return v
		}
	}
	if strings.ContainsAny(v, "${}") {
		return ""
	}
	return v
}

// versions maps the declared coordinates to knowledge.EmbeddedVersions: the
// first declared version of each component wins.
func (b *buildInfo) versions() knowledge.EmbeddedVersions {
	var v knowledge.EmbeddedVersions
	set := func(dst *string, val string) {
		if *dst == "" {
			*dst = b.resolve(val)
		}
	}
	for _, c := range b.coords {
		switch {
		case c.group == "io.debezium" && c.artifact == "debezium-embedded":
			set(&v.Engine, c.version)
		case c.group == "io.debezium" && (c.artifact == "debezium-quarkus-outbox" || c.artifact == "debezium-quarkus-outbox-reactive"):
			set(&v.Outbox, c.version)
		case c.group == "org.apache.kafka" && (c.artifact == "connect-runtime" || c.artifact == "connect-api" || c.artifact == "kafka-clients"):
			set(&v.Kafka, c.version)
		case c.group == "io.quarkus.platform" && c.artifact == "quarkus-bom":
			set(&v.Quarkus, c.version)
		}
	}
	for _, j := range b.java {
		s := strings.TrimPrefix(b.resolve(j), "1.")
		if n, err := strconv.Atoi(s); err == nil && n > 0 && n < 100 {
			v.Java = n
			break
		}
	}
	return v
}

// ---- reference_write (T1) ----

type referenceWriteOp struct{}

// ReferenceParams are reference_write's typed params: knowledge.EmbeddedParams
// on the wire. Every field is checked by EmbeddedParams.Validate before
// anything renders.
type ReferenceParams struct {
	Build           string   `json:"build"`
	Source          string   `json:"source"`
	GroupID         string   `json:"group_id"`
	ArtifactID      string   `json:"artifact_id"`
	Release         string   `json:"release"`
	Host            string   `json:"host"`
	Port            int      `json:"port"`
	Database        string   `json:"database"`
	User            string   `json:"user"`
	PasswordSecret  string   `json:"password_secret"`
	TopicPrefix     string   `json:"topic_prefix"`
	Tables          []string `json:"tables,omitempty"`
	SlotName        string   `json:"slot_name,omitempty"`
	PublicationName string   `json:"publication_name,omitempty"`
	ServerID        uint32   `json:"server_id,omitempty"`
	TLS             string   `json:"tls,omitempty"`
}

// Embedded maps the wire params to the renderer's.
func (p ReferenceParams) Embedded() knowledge.EmbeddedParams {
	return knowledge.EmbeddedParams{
		Build: knowledge.BuildTool(p.Build), Source: knowledge.SourceKind(p.Source),
		GroupID: p.GroupID, ArtifactID: p.ArtifactID, Release: p.Release,
		Host: p.Host, Port: p.Port, Database: p.Database, User: p.User,
		PasswordSecret: p.PasswordSecret, TopicPrefix: p.TopicPrefix, Tables: p.Tables,
		SlotName: p.SlotName, PublicationName: p.PublicationName, ServerID: p.ServerID,
		TLS: knowledge.TLSMode(p.TLS),
	}
}

func (referenceWriteOp) Name() string   { return OpReferenceWrite }
func (referenceWriteOp) Target() string { return "" }
func (referenceWriteOp) Tier() string   { return protocol.TierT1 }
func (referenceWriteOp) Layer() string  { return "environment" }
func (referenceWriteOp) Describe() string {
	return "render the Debezium Engine reference project (Maven or Gradle, Postgres or MySQL) from typed params into " + knowledge.EmbeddedRootDir + "/ under the project directory; the runner never builds or runs it"
}

func (referenceWriteOp) ParamsSchema() map[string]any { return ReferenceWriteSchema() }

// ReferenceWriteSchema is reference_write's params schema, shared with the
// server's action spec (internal/setup/actions) so the two cannot drift.
func ReferenceWriteSchema() map[string]any {
	str := func(max int) map[string]any { return map[string]any{"type": "string", "maxLength": max} }
	return map[string]any{
		"type": "object", "additionalProperties": false,
		"required": []string{"build", "source", "group_id", "artifact_id", "release", "host", "port", "database", "user", "password_secret", "topic_prefix"},
		"properties": map[string]any{
			"build":            map[string]any{"type": "string", "enum": []string{string(knowledge.BuildMaven), string(knowledge.BuildGradle)}},
			"source":           map[string]any{"type": "string", "enum": []string{string(knowledge.SourcePostgres), string(knowledge.SourceMySQL)}},
			"group_id":         str(320),
			"artifact_id":      str(63),
			"release":          str(32),
			"host":             str(253),
			"port":             map[string]any{"type": "integer", "minimum": 1, "maximum": 65535},
			"database":         str(63),
			"user":             str(63),
			"password_secret":  str(64),
			"topic_prefix":     str(63),
			"tables":           map[string]any{"type": "array", "maxItems": 64, "items": str(129)},
			"slot_name":        str(63),
			"publication_name": str(63),
			"server_id":        map[string]any{"type": "integer", "minimum": 0, "maximum": 4294967295},
			"tls":              map[string]any{"type": "string", "enum": []string{"", string(knowledge.TLSVerify), string(knowledge.TLSRequire), string(knowledge.TLSDisable)}},
		},
	}
}

func (referenceWriteOp) decode(params json.RawMessage) (ReferenceParams, error) {
	var p ReferenceParams
	if err := decodeParams(params, &p); err != nil {
		return p, err
	}
	if err := p.Embedded().Validate(); err != nil {
		return p, fmt.Errorf("%w: %v", ErrParams, err)
	}
	return p, nil
}

func (op referenceWriteOp) Validate(params json.RawMessage, _ Env) error {
	_, err := op.decode(params)
	return err
}

func (op referenceWriteOp) Execute(ctx context.Context, params json.RawMessage, env Env) (Result, error) {
	p, err := op.decode(params)
	if err != nil {
		return Result{}, err
	}
	files, err := knowledge.RenderEmbedded(p.Embedded())
	if err != nil {
		return Result{}, fmt.Errorf("ops: %s: %w", op.Name(), err)
	}
	written := make([]string, 0, len(files))
	for _, f := range files {
		if err := ctx.Err(); err != nil {
			return Result{}, err
		}
		rel := path.Clean(f.Path)
		if rel == "." || strings.HasPrefix(rel, "../") || path.IsAbs(rel) {
			return Result{}, fmt.Errorf("%w: the renderer produced %q, outside %s/", ErrPath, clip(f.Path), knowledge.EmbeddedRootDir)
		}
		clean, err := writeConfined(env, knowledge.EmbeddedRootDir+"/"+rel, f.Content)
		if err != nil {
			return Result{Data: map[string]any{"written": written}}, err
		}
		written = append(written, clean)
	}
	cmds := p.Embedded().Commands()
	return Result{
		Summary: fmt.Sprintf("wrote the %s %s reference project (%d files) under %s/; build it with %q and run it with %q yourself", p.Build, p.Source, len(written), knowledge.EmbeddedRootDir, strings.Join(cmds.Build, " "), strings.Join(cmds.Run, " ")),
		Data:    map[string]any{"dir": knowledge.EmbeddedRootDir, "files": written, "build": cmds.Build, "run": cmds.Run},
		Receipt: events.SetupReceipt{Kind: "file", Target: knowledge.EmbeddedRootDir, At: env.Now()},
	}, nil
}

// ---- embedded_log_read (T0) ----

// The reference app's own log lines (internal/setup/knowledge/reference/
// ReferenceApp.java.tmpl). Only lines with refLinePrefix are ever returned:
// the person's own application may log row data, the reference app never does.
const (
	refLinePrefix = "piperoom-reference "
	refStarted    = "piperoom-reference started: "
	refEvent      = "piperoom-reference event destination="
	refCanary     = "piperoom-reference canary "
	refStopped    = "piperoom-reference engine stopped with an error: "
)

// logNonceRe is the reference app's CANARY pattern: what it may print as a
// nonce.
var logNonceRe = regexp.MustCompile(`^[A-Za-z0-9-]{8,64}$`)

type embeddedLogReadOp struct{}

type embeddedLogParams struct {
	// Path is the project-relative log file the person named.
	Path string `json:"path"`
	// Nonce, when set, is the ladder's canary nonce: Data says whether the
	// reference app saw it and how many events followed it.
	Nonce string `json:"nonce,omitempty"`
}

// EmbeddedLog is embedded_log_read's Data.
type EmbeddedLog struct {
	Path string `json:"path"`
	// Started: a "started" line is in the read part of the log; Connector is
	// the connector class it names.
	Started   bool   `json:"started"`
	Connector string `json:"connector,omitempty"`
	// Stopped: an "engine stopped with an error" line follows the last
	// "started" line; Error is its message, redacted and clipped.
	Stopped bool   `json:"stopped_with_error"`
	Error   string `json:"error,omitempty"`
	// Events counts the event lines after the last "started" line, and
	// Destinations lists their destinations (topics).
	Events       int      `json:"events"`
	Destinations []string `json:"destinations"`
	// NonceSeen: the reference app printed the requested nonce after the last
	// start; EventsAfterNonce counts the event lines after that.
	NonceSeen        bool `json:"nonce_seen"`
	EventsAfterNonce int  `json:"events_after_nonce"`
	// Lines are the newest reference app lines, redacted, oldest first.
	Lines []string `json:"lines"`
	// Cut: the file was longer than the part read (its end is read).
	Cut bool `json:"cut"`
}

func (embeddedLogReadOp) Name() string   { return OpEmbeddedLogRead }
func (embeddedLogReadOp) Target() string { return "" }
func (embeddedLogReadOp) Tier() string   { return protocol.TierT0 }
func (embeddedLogReadOp) Layer() string  { return "environment" }
func (embeddedLogReadOp) Describe() string {
	return "read the end of a log file the person named under the project directory and report only the reference app's own piperoom-reference lines: started, events, the canary nonce, a stop with an error"
}

func (embeddedLogReadOp) ParamsSchema() map[string]any {
	return map[string]any{
		"type": "object", "additionalProperties": false, "required": []string{"path"},
		"properties": map[string]any{
			"path":  map[string]any{"type": "string", "minLength": 1, "maxLength": 512},
			"nonce": map[string]any{"type": "string", "pattern": logNonceRe.String()},
		},
	}
}

func (embeddedLogReadOp) decode(params json.RawMessage) (embeddedLogParams, error) {
	var p embeddedLogParams
	if err := decodeParams(params, &p); err != nil {
		return p, err
	}
	if p.Path == "" {
		return p, fmt.Errorf("%w: path is required", ErrParams)
	}
	if why := lexicalRefusal(p.Path); why != "" {
		return p, fmt.Errorf("%w: %s: %s", ErrPath, why, clip(p.Path))
	}
	if IsBuildFile(p.Path) {
		return p, fmt.Errorf("%w: %s is a build file, not a log (project_inspect reads build files)", ErrParams, clip(p.Path))
	}
	if p.Nonce != "" && !logNonceRe.MatchString(p.Nonce) {
		return p, fmt.Errorf("%w: nonce is not a canary nonce", ErrParams)
	}
	return p, nil
}

func (op embeddedLogReadOp) Validate(params json.RawMessage, _ Env) error {
	_, err := op.decode(params)
	return err
}

func (op embeddedLogReadOp) Execute(_ context.Context, params json.RawMessage, env Env) (Result, error) {
	p, err := op.decode(params)
	if err != nil {
		return Result{}, err
	}
	data, cut, err := readConfined(env, p.Path, maxLogTailBytes, true)
	if err != nil {
		return Result{}, err
	}
	log := ParseEmbeddedLog(string(bytes.ToValidUTF8(data, []byte("?"))), p.Nonce)
	log.Path = p.Path
	log.Cut = cut
	state := "not started"
	switch {
	case log.Stopped:
		state = "stopped with an error"
	case log.Started:
		state = "started"
	}
	summary := fmt.Sprintf("%s: reference app %s, %d events after the last start", p.Path, state, log.Events)
	if p.Nonce != "" {
		summary += fmt.Sprintf(", canary seen %t", log.NonceSeen)
	}
	return Result{Summary: summary, Data: log}, nil
}

// ParseEmbeddedLog reads the reference app's lines out of log text. Every
// line that does not start with refLinePrefix is dropped unread; the kept
// lines are redacted.
func ParseEmbeddedLog(text, nonce string) EmbeddedLog {
	out := EmbeddedLog{Destinations: []string{}, Lines: []string{}}
	seenDest := map[string]bool{}
	for _, raw := range strings.Split(text, "\n") {
		line := strings.TrimRight(raw, "\r")
		if i := strings.Index(line, refLinePrefix); i > 0 {
			// A logger may prefix its own timestamp; the app's text starts at
			// the prefix.
			line = line[i:]
		}
		if !strings.HasPrefix(line, refLinePrefix) {
			continue
		}
		clean, _ := redact.Text(line)
		clean = clipTo(clean, 300)
		out.Lines = append(out.Lines, clean)
		if len(out.Lines) > maxLogLines {
			out.Lines = out.Lines[1:]
		}
		switch {
		case strings.HasPrefix(line, refStarted):
			// A restart begins a new run: what the earlier run logged no
			// longer says anything about this one.
			out.Started, out.Stopped, out.Error = true, false, ""
			out.Connector = clipTo(strings.TrimSpace(strings.TrimPrefix(line, refStarted)), 128)
			out.Events, out.NonceSeen, out.EventsAfterNonce = 0, false, 0
			out.Destinations, seenDest = []string{}, map[string]bool{}
		case strings.HasPrefix(line, refStopped):
			out.Stopped = true
			msg, _ := redact.Text(strings.TrimPrefix(line, refStopped))
			out.Error = clipTo(msg, 200)
		case strings.HasPrefix(line, refEvent):
			out.Events++
			if out.NonceSeen {
				out.EventsAfterNonce++
			}
			dest := clipTo(strings.TrimSpace(strings.TrimPrefix(line, refEvent)), 249)
			if dest != "" && !seenDest[dest] && len(out.Destinations) < maxDestinations {
				seenDest[dest] = true
				out.Destinations = append(out.Destinations, dest)
			}
		case strings.HasPrefix(line, refCanary):
			rest := strings.TrimPrefix(line, refCanary)
			got, _, _ := strings.Cut(rest, " ")
			if nonce != "" && got == nonce {
				out.NonceSeen = true
				out.EventsAfterNonce = 0
			}
		}
	}
	return out
}
