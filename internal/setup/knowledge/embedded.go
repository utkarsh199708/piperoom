package knowledge

import (
	"bytes"
	"embed"
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"text/template"
)

// The embedded reference project (U6a): a minimal Debezium Engine
// application, Maven or Gradle, for a Postgres or MySQL source. The setup
// agent writes it under <project dir>/piperoom-reference/ (U6b) so the
// person can compare it with their own build and run it; the runner never
// builds or runs it. Every file is rendered with text/template from typed,
// regex-checked params, never from model text, and no password is ever
// rendered: the database password is a ${secret:NAME} reference that the
// program reads from the environment variable PIPEROOM_SECRET_NAME.

// EmbeddedRootDir is the folder under the project directory that holds the
// reference project. Rendered paths are relative to it.
const EmbeddedRootDir = "piperoom-reference"

// BuildTool is the build system of a reference project.
type BuildTool string

// The build tools a reference project is rendered for.
const (
	BuildMaven  BuildTool = "maven"
	BuildGradle BuildTool = "gradle"
)

// SourceKind is the source database of a reference project.
type SourceKind string

// The sources a reference project is rendered for.
const (
	SourcePostgres SourceKind = "postgres"
	SourceMySQL    SourceKind = "mysql"
)

// TLSMode is how a reference project connects to its source database.
type TLSMode string

// The TLS modes a reference project is rendered for. The empty mode is
// TLSVerify, so a caller that sets nothing gets the strict default.
const (
	// TLSVerify requires TLS and checks the server certificate and host name:
	// Postgres database.sslmode=verify-full, MySQL database.ssl.mode=
	// verify_identity.
	TLSVerify TLSMode = "verify"
	// TLSRequire requires TLS but checks no certificate (Postgres require,
	// MySQL required): encrypted, open to a man in the middle.
	TLSRequire TLSMode = "require"
	// TLSDisable connects in plain text (Postgres disable, MySQL disabled),
	// for a database on the same host only.
	TLSDisable TLSMode = "disable"
)

// ErrEmbeddedParams is returned when reference project params fail
// validation. Compare with errors.Is.
var ErrEmbeddedParams = errors.New("setup/knowledge: bad reference project params")

// EmbeddedParams are the typed inputs of a reference project. Every string is
// checked against a strict pattern before rendering, so nothing can break out
// of the XML, Gradle, Java or properties syntax it lands in.
type EmbeddedParams struct {
	Build  BuildTool  // maven or gradle
	Source SourceKind // postgres or mysql

	GroupID    string // Maven group id, also the Java package: "com.example.cdc"
	ArtifactID string // Maven artifact id and Gradle project name: "orders-cdc"
	Release    string // Debezium release; must be in the plugin index (Releases)

	Host           string // source database host name or IPv4 address
	Port           int    // source database port
	Database       string // Postgres: the database; MySQL: the captured database
	User           string // database user
	PasswordSecret string // secret name; rendered as ${secret:NAME}, never a value
	TopicPrefix    string // Debezium topic.prefix
	// Tables is the optional table.include.list, as "schema.table" (Postgres)
	// or "database.table" (MySQL). Empty captures every table.
	Tables []string

	SlotName        string // Postgres only: the replication slot
	PublicationName string // Postgres only; empty means SlotName
	ServerID        uint32 // MySQL only: database.server.id, unique among replicas

	TLS TLSMode // verify (the default when empty), require or disable
}

// EmbeddedFile is one rendered file of a reference project. Path is relative
// to EmbeddedRootDir and always uses forward slashes.
type EmbeddedFile struct {
	Path    string
	Content []byte
}

// EmbeddedCommands are the argv a person runs from the reference project's
// folder: Build compiles it, Run starts it. The runner never executes them;
// they are rendered into the README and used by the CI integration test.
type EmbeddedCommands struct {
	Build      []string
	Run        []string
	RunWindows []string // nil when Run works unchanged on Windows
}

const (
	// referenceProjectVersion is the reference project's own version.
	referenceProjectVersion = "1.0.0"
	// slf4jSimpleVersion matches the slf4j-api 1.7.36 that Debezium's build
	// parent pins (version.org.slf4j in debezium-build-parent 3.2.0.Final).
	slf4jSimpleVersion = "1.7.36"
	maxTables          = 64
)

var (
	// A Java package of 1 to 10 lowercase segments; also a valid Maven group id.
	groupIDRe = regexp.MustCompile(`^[a-z][a-z0-9_]{0,30}(\.[a-z][a-z0-9_]{0,30}){0,9}$`)
	// A Maven artifact id that is also a safe folder and script name.
	artifactIDRe = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)
	// A DNS host name or an IPv4 address (no scheme, no port, no user info).
	hostRe = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?(\.[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*$`)
	// An unquoted SQL identifier.
	identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)
	// The secret name of a ${secret:NAME} reference (the runner's pattern).
	secretNameRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,63}$`)
	// Debezium topic.prefix: letters, digits, '.', '_' and '-'.
	topicPrefixRe = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9._-]{0,62}$`)
	// A Postgres replication slot or publication name.
	slotRe = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)
)

// javaKeywords are reserved words that cannot be a Java package segment.
var javaKeywords = map[string]bool{
	"abstract": true, "assert": true, "boolean": true, "break": true, "byte": true, "case": true,
	"catch": true, "char": true, "class": true, "const": true, "continue": true, "default": true,
	"do": true, "double": true, "else": true, "enum": true, "extends": true, "false": true,
	"final": true, "finally": true, "float": true, "for": true, "goto": true, "if": true,
	"implements": true, "import": true, "instanceof": true, "int": true, "interface": true,
	"long": true, "native": true, "new": true, "null": true, "package": true, "private": true,
	"protected": true, "public": true, "return": true, "short": true, "static": true,
	"strictfp": true, "super": true, "switch": true, "synchronized": true, "this": true,
	"throw": true, "throws": true, "transient": true, "true": true, "try": true, "void": true,
	"volatile": true, "while": true, "_": true,
}

// Validate checks every param against its pattern and the source-specific
// rules. The error wraps ErrEmbeddedParams and names the field, never echoes
// a value that failed (it may be anything the caller passed).
func (p EmbeddedParams) Validate() error {
	bad := func(field, why string) error {
		return fmt.Errorf("%w: %s %s", ErrEmbeddedParams, field, why)
	}
	switch p.Build {
	case BuildMaven, BuildGradle:
	default:
		return bad("Build", "must be maven or gradle")
	}
	switch p.Source {
	case SourcePostgres, SourceMySQL:
	default:
		return bad("Source", "must be postgres or mysql")
	}
	switch p.TLS {
	case "", TLSVerify, TLSRequire, TLSDisable:
	default:
		return bad("TLS", "must be verify, require or disable")
	}
	if !groupIDRe.MatchString(p.GroupID) {
		return bad("GroupID", "must be a lowercase Java package such as com.example.cdc")
	}
	for _, seg := range strings.Split(p.GroupID, ".") {
		if javaKeywords[seg] {
			return bad("GroupID", "has a Java keyword as a segment")
		}
	}
	if !artifactIDRe.MatchString(p.ArtifactID) {
		return bad("ArtifactID", "must match "+artifactIDRe.String())
	}
	if !hasRelease(p.Release) {
		return bad("Release", "is not a Debezium release in the plugin index")
	}
	if _, err := parseDbzVersion(p.Release); err != nil {
		return bad("Release", "does not parse as a Debezium version")
	}
	if len(p.Host) > 253 || !hostRe.MatchString(p.Host) {
		return bad("Host", "must be a host name or an IPv4 address")
	}
	if p.Port < 1 || p.Port > 65535 {
		return bad("Port", "must be 1 to 65535")
	}
	if !identRe.MatchString(p.Database) {
		return bad("Database", "must be an unquoted identifier")
	}
	if !identRe.MatchString(p.User) {
		return bad("User", "must be an unquoted identifier")
	}
	if !secretNameRe.MatchString(p.PasswordSecret) {
		return bad("PasswordSecret", "must be a secret name (the NAME of ${secret:NAME}), never a password")
	}
	if !topicPrefixRe.MatchString(p.TopicPrefix) {
		return bad("TopicPrefix", "must match "+topicPrefixRe.String())
	}
	if len(p.Tables) > maxTables {
		return bad("Tables", fmt.Sprintf("has more than %d entries", maxTables))
	}
	seen := make(map[string]bool, len(p.Tables))
	for i, t := range p.Tables {
		schema, table, ok := strings.Cut(t, ".")
		if !ok || !identRe.MatchString(schema) || !identRe.MatchString(table) {
			return bad(fmt.Sprintf("Tables[%d]", i), "must be schema.table with unquoted identifiers")
		}
		if p.Source == SourceMySQL && schema != p.Database {
			return bad(fmt.Sprintf("Tables[%d]", i), "must be in the captured database")
		}
		if seen[t] {
			return bad(fmt.Sprintf("Tables[%d]", i), "is listed twice")
		}
		seen[t] = true
	}
	switch p.Source {
	case SourcePostgres:
		if !slotRe.MatchString(p.SlotName) {
			return bad("SlotName", "must be a lowercase replication slot name")
		}
		if p.PublicationName != "" && !slotRe.MatchString(p.PublicationName) {
			return bad("PublicationName", "must be a lowercase publication name")
		}
		if p.ServerID != 0 {
			return bad("ServerID", "is MySQL only")
		}
	case SourceMySQL:
		if p.ServerID == 0 {
			return bad("ServerID", "must be a non-zero server id unique among the replicas")
		}
		if p.SlotName != "" || p.PublicationName != "" {
			return bad("SlotName", "and PublicationName are Postgres only")
		}
	}
	return nil
}

// JavaRelease is the Java release a Debezium release needs at run time:
// 17 from 3.0.0.Alpha1 (DBZ-7224), 11 from 2.0.0.Alpha1 (DBZ-4949), else 8.
// The rules dbz3-java17 and dbz2-java11 in embedded_rules.go cite the release
// notes.
func JavaRelease(release string) (int, error) {
	v, err := parseDbzVersion(release)
	if err != nil {
		return 0, err
	}
	switch {
	case v.compare(mustDbzVersion("3.0.0.Alpha1")) >= 0:
		return 17, nil
	case v.compare(mustDbzVersion("2.0.0.Alpha1")) >= 0:
		return 11, nil
	default:
		return 8, nil
	}
}

// Commands returns the build and run argv for the reference project.
func (p EmbeddedParams) Commands() EmbeddedCommands {
	if p.Build == BuildGradle {
		script := "build/install/" + p.ArtifactID + "/bin/" + p.ArtifactID
		return EmbeddedCommands{
			Build:      []string{"gradle", "--no-daemon", "-q", "installDist"},
			Run:        []string{script},
			RunWindows: []string{strings.ReplaceAll(script, "/", `\`) + ".bat"},
		}
	}
	return EmbeddedCommands{
		Build: []string{"mvn", "-B", "-q", "package", "-DskipTests"},
		Run:   []string{"java", "-jar", "target/" + p.ArtifactID + "-" + referenceProjectVersion + ".jar"},
	}
}

//go:embed reference/*.tmpl
var referenceFS embed.FS

// referenceTemplates parses the embedded templates once per call; rendering
// is rare (one write per setup), so no package-level cache is kept.
func referenceTemplates() (*template.Template, error) {
	t, err := template.New("reference").Option("missingkey=error").ParseFS(referenceFS, "reference/*.tmpl")
	if err != nil {
		return nil, fmt.Errorf("knowledge: parsing the reference templates: %w", err)
	}
	return t, nil
}

// renderData is what the templates see: the validated params plus values
// derived from them, never anything else.
type renderData struct {
	EmbeddedParams
	Package           string
	SourceTitle       string
	BuildTitle        string
	ConnectorClass    string
	ConnectorArtifact string
	TableList         string
	JavaRelease       int
	ProjectVersion    string
	SLF4JVersion      string
	BuildCommand      string
	RunCommand        string
	RunCommandWindows string
	TLSKey            string // database.sslmode (Postgres) or database.ssl.mode (MySQL)
	TLSValue          string // the source's value for the TLS mode
}

// RenderEmbedded validates p and renders the reference project's files:
// the build file(s), the Java program, its properties and a README. It
// returns ErrEmbeddedParams (wrapped) when p is invalid and renders nothing.
func RenderEmbedded(p EmbeddedParams) ([]EmbeddedFile, error) {
	if err := p.Validate(); err != nil {
		return nil, err
	}
	if p.Source == SourcePostgres && p.PublicationName == "" {
		p.PublicationName = p.SlotName
	}
	if p.TLS == "" {
		p.TLS = TLSVerify
	}
	java, err := JavaRelease(p.Release)
	if err != nil {
		return nil, fmt.Errorf("%w: Release does not parse", ErrEmbeddedParams)
	}
	cmds := p.Commands()
	d := renderData{
		EmbeddedParams: p,
		Package:        p.GroupID,
		JavaRelease:    java,
		ProjectVersion: referenceProjectVersion,
		SLF4JVersion:   slf4jSimpleVersion,
		BuildCommand:   strings.Join(cmds.Build, " "),
		RunCommand:     strings.Join(cmds.Run, " "),
	}
	if cmds.RunWindows != nil {
		d.RunCommandWindows = strings.Join(cmds.RunWindows, " ")
	}
	switch p.Source {
	case SourcePostgres:
		d.SourceTitle = "Postgres"
		d.ConnectorClass = "io.debezium.connector.postgresql.PostgresConnector"
		d.ConnectorArtifact = "debezium-connector-postgres"
		d.TLSKey = "database.sslmode"
		d.TLSValue = map[TLSMode]string{TLSVerify: "verify-full", TLSRequire: "require", TLSDisable: "disable"}[p.TLS]
	case SourceMySQL:
		d.SourceTitle = "MySQL"
		d.ConnectorClass = "io.debezium.connector.mysql.MySqlConnector"
		d.ConnectorArtifact = "debezium-connector-mysql"
		d.TLSKey = "database.ssl.mode"
		d.TLSValue = map[TLSMode]string{TLSVerify: "verify_identity", TLSRequire: "required", TLSDisable: "disabled"}[p.TLS]
	}
	d.BuildTitle = "Maven"
	if p.Build == BuildGradle {
		d.BuildTitle = "Gradle"
	}
	// table.include.list is a list of regular expressions: escape the dot so
	// "public.orders" matches only that table. "\\." in a properties file
	// loads as "\.".
	quoted := make([]string, len(p.Tables))
	for i, t := range p.Tables {
		quoted[i] = strings.Replace(t, ".", `\\.`, 1)
	}
	d.TableList = strings.Join(quoted, ",")

	t, err := referenceTemplates()
	if err != nil {
		return nil, err
	}
	javaPath := "src/main/java/" + strings.ReplaceAll(p.GroupID, ".", "/") + "/ReferenceApp.java"
	type out struct{ path, tmpl string }
	files := []out{{"README.md", "README.md.tmpl"}, {"piperoom-reference.properties", "piperoom-reference.properties.tmpl"}, {javaPath, "ReferenceApp.java.tmpl"}}
	if p.Build == BuildMaven {
		files = append(files, out{"pom.xml", "pom.xml.tmpl"})
	} else {
		files = append(files, out{"build.gradle", "build.gradle.tmpl"}, out{"settings.gradle", "settings.gradle.tmpl"})
	}
	rendered := make([]EmbeddedFile, 0, len(files))
	for _, f := range files {
		var buf bytes.Buffer
		if err := t.ExecuteTemplate(&buf, f.tmpl, d); err != nil {
			return nil, fmt.Errorf("knowledge: rendering %s: %w", f.path, err)
		}
		rendered = append(rendered, EmbeddedFile{Path: f.path, Content: buf.Bytes()})
	}
	return rendered, nil
}

// dbzVersion is a Debezium release number: "3.2.0.Final", "3.0.0.Alpha1",
// "2.7.0.CR1". Kafka and Quarkus versions ("3.9.0", "3.22.2") parse too and
// rank as releases.
type dbzVersion struct {
	nums [3]int
	rank int // 0 Alpha, 1 Beta, 2 CR, 3 release (Final or none)
	pre  int // the number after Alpha/Beta/CR
}

var dbzVersionRe = regexp.MustCompile(`^(\d{1,4})\.(\d{1,4})(?:\.(\d{1,4}))?(?:[.-]([A-Za-z]+)(\d{0,3}))?$`)

func parseDbzVersion(s string) (dbzVersion, error) {
	m := dbzVersionRe.FindStringSubmatch(strings.TrimSpace(s))
	if m == nil {
		return dbzVersion{}, fmt.Errorf("knowledge: version %q does not parse", s)
	}
	var v dbzVersion
	for i := 0; i < 3; i++ {
		if m[i+1] != "" {
			v.nums[i], _ = strconv.Atoi(m[i+1])
		}
	}
	v.rank = 3
	switch strings.ToLower(m[4]) {
	case "alpha":
		v.rank = 0
	case "beta":
		v.rank = 1
	case "cr", "rc":
		v.rank = 2
	case "", "final", "ga":
	default:
		return dbzVersion{}, fmt.Errorf("knowledge: version %q has an unknown qualifier", s)
	}
	if m[5] != "" {
		v.pre, _ = strconv.Atoi(m[5])
	}
	return v, nil
}

func mustDbzVersion(s string) dbzVersion {
	v, err := parseDbzVersion(s)
	if err != nil {
		// Only called with literals from this package; a test covers them.
		return dbzVersion{}
	}
	return v
}

func (v dbzVersion) compare(o dbzVersion) int {
	for i := 0; i < 3; i++ {
		if v.nums[i] != o.nums[i] {
			return cmpInt(v.nums[i], o.nums[i])
		}
	}
	if v.rank != o.rank {
		return cmpInt(v.rank, o.rank)
	}
	return cmpInt(v.pre, o.pre)
}

// line is "major.minor", the granularity of the Kafka and Quarkus rules.
func (v dbzVersion) line() string {
	return strconv.Itoa(v.nums[0]) + "." + strconv.Itoa(v.nums[1])
}
