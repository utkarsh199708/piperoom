package server

import (
	"errors"
	"fmt"
	"net/netip"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"piperoom/internal/actions"
	"piperoom/internal/connprobe"
	"piperoom/internal/redact"
	"piperoom/internal/setup/knowledge"
)

// Limits of one properties file.
const (
	maxPropertiesEntries = 64
	maxPropertiesBytes   = 32 << 10
	maxValueBytes        = 1024
)

// The Debezium Server keys this driver writes itself, beside the sink keys of
// the knowledge index and the source connector's own keys. Every one is
// documented at
// https://debezium.io/documentation/reference/3.2/operations/debezium-server.html
// ("Debezium Server configuration"): debezium.sink.type selects the sink,
// debezium.source.connector.class the source connector, debezium.source.<key>
// passes a connector property through, debezium.format.key/value the output
// format, and debezium.source.offset.storage(.file.filename) the file offset
// store its default class and file.
const (
	keySinkType       = "debezium.sink.type"
	keyConnectorClass = "debezium.source.connector.class"
	keyFormatKey      = "debezium.format.key"
	keyFormatValue    = "debezium.format.value"
	keyOffsetStorage  = "debezium.source.offset.storage"
	keyOffsetFile     = "debezium.source.offset.storage.file.filename"

	// fileOffsetStore is the class the server docs name as the default offset
	// store for a non-Kafka deployment.
	fileOffsetStore = "org.apache.kafka.connect.storage.FileOffsetBackingStore"
	// DefaultOffsetsFile is the offset file under the server's working
	// directory (the distribution root for a unit, /debezium for the image).
	DefaultOffsetsFile = "data/offsets.dat"
)

// valueFormats and keyFormats are the debezium.format.value and
// debezium.format.key values the server documents (the same page); "json" is
// both defaults.
var (
	valueFormats = []string{"json", "jsonbytearray", "avro", "protobuf", "cloudevents", "simplestring", "binary"}
	keyFormats   = []string{"json", "jsonbytearray", "avro", "protobuf", "simplestring", "binary"}
)

// sourceConnectors maps the connector short name a source driver uses to the
// class debezium.source.connector.class names. The five connectors the U wave
// has a source driver for; a connector outside this table is refused rather
// than guessed. Each class is the one the connector page documents
// (https://debezium.io/documentation/reference/3.2/connectors/<name>.html).
var sourceConnectors = map[string]string{
	"postgres":  "io.debezium.connector.postgresql.PostgresConnector",
	"mysql":     "io.debezium.connector.mysql.MySqlConnector",
	"sqlserver": "io.debezium.connector.sqlserver.SqlServerConnector",
	"mongodb":   "io.debezium.connector.mongodb.MongoDbConnector",
	"oracle":    "io.debezium.connector.oracle.OracleConnector",
}

// reservedSourceKeys are connector properties this builder writes itself; a
// caller may not pass a second value for one.
var reservedSourceKeys = map[string]bool{
	"connector.class":              true,
	"offset.storage":               true,
	"offset.storage.file.filename": true,
	"offset.flush.interval.ms":     true,
}

// Patterns of a key and of a source connector key; the offset file stays
// under data/ (the directory the Compose render mounts as a named volume and
// the unit's working directory carries).
var (
	secretRefOnlyRE = regexp.MustCompile(`^\$\{secret:[A-Za-z_][A-Za-z0-9_]*\}$`)
	secretRefRE     = regexp.MustCompile(`\$\{secret:[A-Za-z_][A-Za-z0-9_]*\}`)
	sourceKeyRE     = regexp.MustCompile(`^[a-z][a-z0-9_.-]{0,127}$`)
	offsetsFileRE   = regexp.MustCompile(`^data/[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
)

// PropertiesParams are server_properties_write's typed params: one sink the
// index knows, its keys, the source connector and its keys.
type PropertiesParams struct {
	// Sink is the debezium.sink.type value of an index sink entry
	// ("http", "kinesis", "pubsub", "redis", "kafka", ...).
	Sink string `json:"sink"`
	// SinkKeys are the sink's configuration keys; every name must be one the
	// index carries for Sink.
	SinkKeys map[string]string `json:"sink_keys,omitempty"`
	// Source is the source connector short name ("postgres", "mysql",
	// "sqlserver", "mongodb", "oracle").
	Source string `json:"source"`
	// SourceKeys are the connector's own properties, without the
	// debezium.source. prefix, which the builder adds.
	SourceKeys map[string]string `json:"source_keys,omitempty"`
	// Format is debezium.format.value; "" means json.
	Format string `json:"format,omitempty"`
	// KeyFormat is debezium.format.key; "" means json.
	KeyFormat string `json:"key_format,omitempty"`
	// OffsetsFile is the file offset store's file; "" means
	// DefaultOffsetsFile.
	OffsetsFile string `json:"offsets_file,omitempty"`
}

// Properties is a validated, rendered Debezium Server configuration.
type Properties struct {
	// Sink is the sink type, SinkName the index entry it came from.
	Sink     string
	SinkName string
	// Source is the connector short name, ConnectorClass its Java class.
	Source         string
	ConnectorClass string
	// Entries are every written key and value; Keys is its sorted key list.
	Entries map[string]string
	Keys    []string
	// Aliases maps a requested former key name to the canonical one written.
	Aliases map[string]string
	// Checks are the sink entry's checks, in the index's words, for the plan.
	Checks []string
	// Doc and IndexSource are the sink entry's citations.
	Doc         string
	IndexSource string
}

// BuildProperties validates p against the knowledge index and the server's
// own key table without I/O. A key the sink's index entry does not carry, a
// missing required key, a value outside the key's allowed values, a plain-text
// credential and a ${secret:name} reference on a key the index does not mark
// secret are all refused.
func (d *Driver) BuildProperties(p PropertiesParams) (Properties, error) {
	if d == nil || d.ix == nil {
		return Properties{}, errors.New("server: the knowledge index is not loaded")
	}
	sink := strings.TrimSpace(p.Sink)
	if sink == "" {
		return Properties{}, fmt.Errorf("%w: sink is required (one of %s)", ErrProperties, strings.Join(d.SinkTypes(), ", "))
	}
	entry, err := d.ix.Lookup(knowledge.KindSink, sink)
	if err != nil {
		return Properties{}, fmt.Errorf("%w: sink %s: %w", ErrProperties, q(sink), err)
	}
	if entry.Runtime != knowledge.RuntimeServer {
		return Properties{}, fmt.Errorf("%w: sink %s is a %s sink; this target configures Debezium Server sinks only", ErrProperties, q(sink), entry.Runtime)
	}
	// The entry is verified for a release range of its component; the value
	// this driver writes has to be one the pinned image's release supports.
	if sup, err := entry.Supports(d.image.Tag); err != nil {
		return Properties{}, fmt.Errorf("%w: sink %s: %v", ErrProperties, q(sink), err)
	} else if sup != knowledge.Supported {
		return Properties{}, fmt.Errorf("%w: the index does not verify sink %s for %s (the pinned server image is %s)",
			ErrProperties, q(sink), entry.Versions, q(d.image.Tag))
	}
	format := p.Format
	if format == "" {
		format = "json"
	}
	if !contains(valueFormats, format) {
		return Properties{}, fmt.Errorf("%w: format %s is not one of %s", ErrProperties, q(format), strings.Join(valueFormats, ", "))
	}
	keyFormat := p.KeyFormat
	if keyFormat == "" {
		keyFormat = "json"
	}
	if !contains(keyFormats, keyFormat) {
		return Properties{}, fmt.Errorf("%w: key_format %s is not one of %s", ErrProperties, q(keyFormat), strings.Join(keyFormats, ", "))
	}
	offsets := p.OffsetsFile
	if offsets == "" {
		offsets = DefaultOffsetsFile
	}
	if !offsetsFileRE.MatchString(offsets) {
		return Properties{}, fmt.Errorf("%w: offsets_file %s must be data/<name> (the directory the stack keeps the offsets in)", ErrProperties, q(offsets))
	}
	class, ok := sourceConnectors[strings.TrimSpace(p.Source)]
	if !ok {
		return Properties{}, fmt.Errorf("%w: source %s is not one of %s", ErrProperties, q(p.Source), strings.Join(d.Sources(), ", "))
	}
	props := Properties{
		Sink: entry.SinkType, SinkName: entry.Name, Source: strings.ToLower(strings.TrimSpace(p.Source)),
		ConnectorClass: class, Aliases: map[string]string{}, Checks: entry.Checks,
		Doc: entry.Doc, IndexSource: entry.Source, Entries: map[string]string{},
	}
	props.Entries[keySinkType] = entry.SinkType
	props.Entries[keyConnectorClass] = class
	props.Entries[keyFormatKey] = keyFormat
	props.Entries[keyFormatValue] = format
	props.Entries[keyOffsetStorage] = fileOffsetStore
	props.Entries[keyOffsetFile] = offsets
	if err := addSinkKeys(&props, entry, d.image.Tag, p.SinkKeys); err != nil {
		return Properties{}, err
	}
	if err := addSourceKeys(&props, p.SourceKeys); err != nil {
		return Properties{}, err
	}
	if len(props.Entries) > maxPropertiesEntries {
		return Properties{}, fmt.Errorf("%w: %d keys; the cap is %d", ErrProperties, len(props.Entries), maxPropertiesEntries)
	}
	total := 0
	for key, value := range props.Entries {
		total += len(key) + len(value)
		props.Keys = append(props.Keys, key)
	}
	if total > maxPropertiesBytes {
		return Properties{}, fmt.Errorf("%w: the file would be %d bytes; the cap is %d", ErrProperties, total, maxPropertiesBytes)
	}
	sort.Strings(props.Keys)
	return props, nil
}

// addSinkKeys validates the sink half: index keys only, required keys present,
// canonical names written.
func addSinkKeys(props *Properties, entry knowledge.Entry, tag string, given map[string]string) error {
	names := make([]string, 0, len(given))
	for name := range given {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		key, ok := entry.Key(name)
		if !ok {
			return fmt.Errorf("%w: key %s is not in the %s allow-list (%s)", ErrProperties, q(name), q(entry.SinkType), strings.Join(entryKeyNames(entry), ", "))
		}
		if err := keyAtImageTag(key, tag); err != nil {
			return fmt.Errorf("%w: key %s: %v", ErrProperties, q(name), err)
		}
		if key.Name != name {
			props.Aliases[name] = key.Name
		}
		value, err := checkSinkValue(key, given[name])
		if err != nil {
			return err
		}
		props.Entries[key.Name] = value
	}
	for _, key := range entry.Keys {
		if !key.Required {
			continue
		}
		if _, ok := props.Entries[key.Name]; !ok {
			return fmt.Errorf("%w: sink %s needs %s (the index marks it required)", ErrProperties, q(entry.SinkType), q(key.Name))
		}
	}
	return nil
}

// keyAtImageTag refuses an index key the pinned release does not have (its
// since is newer) or no longer has (its until has arrived).
func keyAtImageTag(key knowledge.Key, tag string) error {
	if key.Since != "" && releaseCompare(tag, key.Since) < 0 {
		return fmt.Errorf("the key was added in %s and the pinned release is %s", key.Since, tag)
	}
	if key.Until != "" && releaseCompare(tag, key.Until) >= 0 {
		return fmt.Errorf("the key was removed in %s and the pinned release is %s", key.Until, tag)
	}
	return nil
}

// checkSinkValue validates one sink value against its index key.
func checkSinkValue(key knowledge.Key, value string) (string, error) {
	if err := checkCommon(value); err != nil {
		return "", fmt.Errorf("%w: key %s", err, q(key.Name))
	}
	if key.Secret {
		if !secretRefOnlyRE.MatchString(value) {
			return "", fmt.Errorf("%w: key %s is a credential; its value must be a ${secret:name} reference", ErrSecretValue, q(key.Name))
		}
		return value, nil
	}
	if secretRefRE.MatchString(value) {
		return "", fmt.Errorf("%w: key %s carries a ${secret:name} reference the index does not mark secret", ErrProperties, q(key.Name))
	}
	if len(key.Values) > 0 && !contains(key.Values, value) {
		return "", fmt.Errorf("%w: key %s: value %s is not one of %s", ErrProperties, q(key.Name), q(value), strings.Join(key.Values, ", "))
	}
	if plaintextCredential(value) {
		return "", fmt.Errorf("%w: key %s: the value looks like a credential", ErrSecretValue, q(key.Name))
	}
	if err := checkURLValue(value); err != nil {
		return "", fmt.Errorf("key %s: %w", q(key.Name), err)
	}
	return value, nil
}

// addSourceKeys validates the source half. The keys are the connector's own
// (design §6: the source driver's ConnectorConfig), so their list lives with
// that driver; this package checks the shape, refuses the keys it writes
// itself, and holds a credential-named key to a ${secret:name} reference.
func addSourceKeys(props *Properties, given map[string]string) error {
	names := make([]string, 0, len(given))
	for name := range given {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		value := given[name]
		if !sourceKeyRE.MatchString(name) {
			return fmt.Errorf("%w: source key %s is not a connector property name", ErrProperties, q(name))
		}
		if reservedSourceKeys[name] {
			return fmt.Errorf("%w: source key %s is written by this driver and cannot be set twice", ErrProperties, q(name))
		}
		if err := checkCommon(value); err != nil {
			return fmt.Errorf("%w: source key %s", err, q(name))
		}
		rest := secretRefRE.ReplaceAllString(value, "")
		if strings.Contains(rest, "$") {
			return fmt.Errorf("%w: source key %s uses a $ form other than ${secret:name}", ErrProperties, q(name))
		}
		if actions.LooksSecret(name) && !secretRefOnlyRE.MatchString(value) {
			return fmt.Errorf("%w: source key %s is a credential; its value must be a ${secret:name} reference", ErrSecretValue, q(name))
		}
		if !secretRefOnlyRE.MatchString(value) && plaintextCredential(value) {
			return fmt.Errorf("%w: source key %s: the value looks like a credential", ErrSecretValue, q(name))
		}
		if err := checkURLValue(value); err != nil {
			return fmt.Errorf("source key %s: %w", q(name), err)
		}
		props.Entries["debezium.source."+name] = value
	}
	return nil
}

// checkCommon is the value rule every key shares: non-empty, no control
// character, no surrounding space, and not a comment a properties parser
// would swallow.
func checkCommon(value string) error {
	if len(value) > maxValueBytes {
		return fmt.Errorf("%w: the value is longer than %d bytes", ErrProperties, maxValueBytes)
	}
	switch {
	case value == "":
		return fmt.Errorf("%w: the value is empty", ErrProperties)
	case strings.ContainsFunc(value, unicode.IsControl):
		return fmt.Errorf("%w: the value carries a control character", ErrProperties)
	case strings.TrimSpace(value) != value:
		return fmt.Errorf("%w: the value has a leading or trailing space", ErrProperties)
	case strings.HasPrefix(value, "#"):
		return fmt.Errorf("%w: the value starts with #, which a properties parser reads as a comment", ErrProperties)
	}
	return nil
}

// urlGuard classifies an IP literal in an http(s) value. Loopback and private
// ranges are normal sinks (a dev stack, a VPC endpoint); link-local, the cloud
// metadata addresses, multicast and the unspecified address never are, because
// a sink pointed at one would post every change event to it.
var urlGuard = connprobe.Guard{AllowLoopback: true, AllowPrivate: true}

// checkURLValue validates an http(s) URL value: a host is required, a user or
// password in it is refused (credentials belong in the connector's own
// password key, as a ${secret:name} reference), and an IP literal is checked
// against the denylist. A name is not resolved here - Validate does no I/O -
// and the server's own dialer is what sees the resolved address at run time.
// Values with any other scheme are connector-specific (a JDBC DSN, a Redis
// address) and are left to the key's own checks.
func checkURLValue(value string) error {
	if !strings.HasPrefix(value, "http://") && !strings.HasPrefix(value, "https://") {
		return nil
	}
	u, err := url.Parse(value)
	if err != nil || u.Host == "" {
		return fmt.Errorf("%w: the value is not a well-formed http(s) URL", ErrProperties)
	}
	if u.User != nil {
		return fmt.Errorf("%w: the URL carries a username or password; use the connector's user and password keys", ErrSecretValue)
	}
	if ip, err := netip.ParseAddr(u.Hostname()); err == nil {
		if gerr := urlGuard.CheckAddr(ip); gerr != nil {
			return fmt.Errorf("%w: %v", ErrProperties, gerr)
		}
	}
	return nil
}

// plaintextCredential reports whether a value carries a credential value.
// redact.Text finds passwords in URLs, bearer tokens, API keys and private key
// material; a masked or counted hit is refused, and the value is in no error.
func plaintextCredential(value string) bool {
	masked, st := redact.Text(value)
	return masked != value || st.Total > 0
}

// render is the file text: a fixed header, then the keys in lexical order.
// Values passed checkCommon, so no entry can add or continue a line.
func (p Properties) render() []byte {
	var b strings.Builder
	b.WriteString("# Generated by piperoom setup (server_properties_write) for " + p.Source + " -> " + p.Sink +
		"; the op rewrites this file.\n")
	for _, key := range p.Keys {
		b.WriteString(key + "=" + p.Entries[key] + "\n")
	}
	return []byte(b.String())
}

// SinkTypes lists the Debezium Server sinks the index carries, sorted: the
// allow-list of the sink param.
func (d *Driver) SinkTypes() []string {
	if d == nil || d.ix == nil {
		return nil
	}
	seen := map[string]bool{}
	var out []string
	for _, e := range d.ix.Entries() {
		if e.Kind == knowledge.KindSink && e.Runtime == knowledge.RuntimeServer && e.SinkType != "" && !seen[e.SinkType] {
			seen[e.SinkType] = true
			out = append(out, e.SinkType)
		}
	}
	sort.Strings(out)
	return out
}

// Sinks returns the index entries of the server sinks, in index order.
func (d *Driver) Sinks() []knowledge.Entry {
	if d == nil || d.ix == nil {
		return nil
	}
	var out []knowledge.Entry
	for _, e := range d.ix.Entries() {
		if e.Kind == knowledge.KindSink && e.Runtime == knowledge.RuntimeServer {
			out = append(out, e)
		}
	}
	return out
}

// Sources lists the source connectors this target can configure, sorted.
func (d *Driver) Sources() []string {
	out := make([]string, 0, len(sourceConnectors))
	for name := range sourceConnectors {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// entryKeyNames is the entry's canonical key names, for a refusal.
func entryKeyNames(e knowledge.Entry) []string {
	out := make([]string, 0, len(e.Keys))
	for _, k := range e.Keys {
		out = append(out, k.Name)
	}
	sort.Strings(out)
	return out
}

// releaseCompare compares two release strings ("3.2.0.Final") segment by
// segment: numeric segments numerically, other segments as text, a missing
// segment before a present one. It mirrors the numeric reading of the
// knowledge index's own release parser for the forms the index uses; it is
// only applied to the image tag and to key since/until values.
func releaseCompare(a, b string) int {
	as, bs := strings.Split(a, "."), strings.Split(b, ".")
	for i := 0; i < len(as) || i < len(bs); i++ {
		if i >= len(as) {
			return -1
		}
		if i >= len(bs) {
			return 1
		}
		an, aerr := strconv.Atoi(as[i])
		bn, berr := strconv.Atoi(bs[i])
		switch {
		case aerr == nil && berr == nil:
			if an != bn {
				if an < bn {
					return -1
				}
				return 1
			}
		case aerr == nil:
			return -1 // a numeric segment sorts before a label (3.2.0 < 3.2.0.Final)
		case berr == nil:
			return 1
		default:
			if c := strings.Compare(as[i], bs[i]); c != 0 {
				return c
			}
		}
	}
	return 0
}

// contains reports whether list holds s.
func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}
