package server

import (
	"errors"
	"fmt"
	"net"
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

// sourceRule is the shape a source key's value must have.
type sourceRule int

const (
	// sourceText is a literal with no further shape (checkCommon and the
	// credential and expression rules still apply).
	sourceText sourceRule = iota
	// sourceName is a connector identifier (a slot, a publication, a PDB).
	sourceName
	// sourceTopicPrefix is topic.prefix, the connector's namespace.
	sourceTopicPrefix
	// sourceBool is "true" or "false".
	sourceBool
	// sourceInt is a bounded integer (a count, an interval in ms).
	sourceInt
	// sourcePort is a TCP port, 1-65535.
	sourcePort
	// sourceHost is a host name or an IP literal: a metadata address and a
	// metadata service name are refused, loopback and private addresses are
	// normal for a source DB.
	sourceHost
	// sourceList is a comma-separated list with no empty element.
	sourceList
	// sourceDataPath is data/<name>, the directory the stack keeps state in.
	sourceDataPath
	// sourceClass is a Java class name (the exact class is in values).
	sourceClass
	// sourceMongoURI is a mongodb:// or mongodb+srv:// string without user
	// info (credentials belong in mongodb.user and mongodb.password).
	sourceMongoURI
)

// sourceKeySpec is the rule of one allow-listed source key plus the exact
// values the connector accepts, when the key has an enumerated set.
type sourceKeySpec struct {
	rule   sourceRule
	values []string
}

// Patterns of a key, a source connector key, a transform alias and the host
// and path values; the offset file stays under data/ (the directory the
// Compose render mounts as a named volume and the unit's working directory
// carries).
var (
	secretRefOnlyRE = regexp.MustCompile(`^\$\{secret:[A-Za-z_][A-Za-z0-9_]*\}$`)
	secretRefRE     = regexp.MustCompile(`\$\{secret:[A-Za-z_][A-Za-z0-9_]*\}`)
	offsetsFileRE   = regexp.MustCompile(`^data/[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	transformRE     = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_-]{0,63}$`)
	sourceNameRE    = regexp.MustCompile(`^[A-Za-z_$][A-Za-z0-9_.$-]{0,127}$`)
	hostnameRE      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$`)
	intRE           = regexp.MustCompile(`^-?[0-9]{1,9}$`)
	classValueRE    = regexp.MustCompile(`^[A-Za-z_$][A-Za-z0-9_$]*(\.[A-Za-z_$][A-Za-z0-9_$]*)+$`)
)

// metadataHostNames are the names connprobe.Guard refuses by name (its list is
// unexported, so this is a copy). A source key's host must not name them: a
// Debezium connector pointed at a metadata service would read its credentials.
var metadataHostNames = map[string]bool{
	"metadata.google.internal": true,
	"metadata":                 true,
	"instance-data":            true,
}

// commonSourceKeys are the source connector keys that are not tied to one
// connector database. They are the keys design §2.5 lets a config set; a key
// outside this table (and the connector tables below, and the transforms and
// predicates families) is refused.
var commonSourceKeys = map[string]sourceKeySpec{
	"topic.prefix":                           {rule: sourceTopicPrefix},
	"snapshot.mode":                          {rule: sourceText, values: []string{"initial", "initial_only", "schema_only", "schema_only_recovery", "never", "when_needed", "configuration_based", "custom"}},
	"snapshot.locking.mode":                  {rule: sourceText, values: []string{"minimal", "minimal_per_table", "extended", "none", "custom"}},
	"snapshot.isolation.mode":                {rule: sourceText, values: []string{"read_uncommitted", "read_committed", "repeatable_read", "snapshot", "serializable"}},
	"snapshot.include.collection.list":       {rule: sourceList},
	"snapshot.fetch.size":                    {rule: sourceInt},
	"snapshot.delay.ms":                      {rule: sourceInt},
	"snapshot.max.threads":                   {rule: sourceInt},
	"event.processing.failure.handling.mode": {rule: sourceText, values: []string{"fail", "warn", "skip"}},
	"tombstones.on.delete":                   {rule: sourceBool},
	"include.schema.changes":                 {rule: sourceBool},
	"provide.transaction.metadata":           {rule: sourceBool},
	"decimal.handling.mode":                  {rule: sourceText, values: []string{"precise", "double", "string"}},
	"time.precision.mode":                    {rule: sourceText, values: []string{"adaptive", "adaptive_time_microseconds", "connect"}},
	"truncate.handling.mode":                 {rule: sourceText, values: []string{"skip", "include", "fail"}},
	"binary.handling.mode":                   {rule: sourceText, values: []string{"bytes", "base64", "hex"}},
	"schema.name.adjustment.mode":            {rule: sourceText, values: []string{"none", "avro", "avro_unicode"}},
	"field.name.adjustment.mode":             {rule: sourceText, values: []string{"none", "avro", "avro_unicode"}},
	"message.key.columns":                    {rule: sourceList},
	"heartbeat.interval.ms":                  {rule: sourceInt},
	"topic.naming.strategy":                  {rule: sourceClass, values: []string{"io.debezium.schema.DefaultTopicNamingStrategy"}},
}

// relationalSourceKeys are the keys the relational connectors share: the
// database coordinates, the include/exclude lists, and a file schema history
// under data/ (the only file path this target lets a source key name).
var relationalSourceKeys = mergeSourceKeys(commonSourceKeys, map[string]sourceKeySpec{
	"database.hostname": {rule: sourceHost},
	"database.port":     {rule: sourcePort},
	// A connector identifier: the driver interpolates these into a JDBC URL or
	// a DSN, so ? & ; = / would inject driver properties (sslmode, socketFactory,
	// options) and undo the allow-listed database.* keys.
	"database.user":     {rule: sourceName},
	"database.password": {rule: sourceText}, // a credential by name: secret ref only
	"database.dbname":   {rule: sourceName},

	"table.include.list":  {rule: sourceList},
	"table.exclude.list":  {rule: sourceList},
	"schema.include.list": {rule: sourceList},
	"schema.exclude.list": {rule: sourceList},
	"column.include.list": {rule: sourceList},
	"column.exclude.list": {rule: sourceList},

	"schema.history.internal":                                   {rule: sourceClass, values: []string{"io.debezium.storage.file.history.FileSchemaHistory"}},
	"schema.history.internal.file.filename":                     {rule: sourceDataPath},
	"schema.history.internal.skip.unparseable.ddl":              {rule: sourceBool},
	"schema.history.internal.store.only.captured.tables.ddl":    {rule: sourceBool},
	"schema.history.internal.store.only.captured.databases.ddl": {rule: sourceBool},
})

// sourceKeyTables is the per-connector allow-list. The connector keys are the
// documented Debezium 3.2 keys (each connector page under
// https://debezium.io/documentation/reference/3.2/connectors/, and the server's
// key pass-through documented at operations/debezium-server.html). The U3a
// knowledge index does not carry connector keys yet (a source driver is U9+),
// so this is the fixed table the review of U11a allows until one lands; the
// transforms and predicates families are routed through the index.
var sourceKeyTables = map[string]map[string]sourceKeySpec{
	"postgres": mergeSourceKeys(relationalSourceKeys, map[string]sourceKeySpec{
		"plugin.name":                              {rule: sourceText, values: []string{"pgoutput", "decoderbufs", "wal2json", "wal2json_rds", "wal2json_streaming", "wal2json_rds_streaming"}},
		"slot.name":                                {rule: sourceName},
		"slot.drop.on.stop":                        {rule: sourceBool},
		"publication.name":                         {rule: sourceName},
		"publication.autocreate.mode":              {rule: sourceText, values: []string{"all_tables", "disabled", "filtered"}},
		"publication.autocreate.filter.expression": {rule: sourceText},
		"replica.identity.autoset.values":          {rule: sourceList},
		"hstore.handling.mode":                     {rule: sourceText, values: []string{"json", "map"}},
		"interval.handling.mode":                   {rule: sourceText, values: []string{"numeric", "string"}},
		"money.fraction.digits":                    {rule: sourceInt},
		"database.sslmode":                         {rule: sourceText, values: []string{"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}},
	}),
	"mysql": mergeSourceKeys(relationalSourceKeys, map[string]sourceKeySpec{
		"database.include.list":                       {rule: sourceList},
		"database.exclude.list":                       {rule: sourceList},
		"database.server.id":                          {rule: sourceInt},
		"database.connectionTimeZone":                 {rule: sourceText},
		"database.ssl.mode":                           {rule: sourceText, values: []string{"disabled", "preferred", "required", "verify_ca", "verify_identity"}},
		"table.ignore.builtin":                        {rule: sourceBool},
		"include.query":                               {rule: sourceBool},
		"binlog.buffer.size":                          {rule: sourceInt},
		"event.deserialization.failure.handling.mode": {rule: sourceText, values: []string{"fail", "warn", "skip"}},
		"inconsistent.schema.handling.mode":           {rule: sourceText, values: []string{"fail", "warn", "skip"}},
	}),
	"sqlserver": mergeSourceKeys(relationalSourceKeys, map[string]sourceKeySpec{
		"database.names":                  {rule: sourceList},
		"database.instance":               {rule: sourceName},
		"database.encrypt":                {rule: sourceBool},
		"database.trustServerCertificate": {rule: sourceBool},
		"database.applicationName":        {rule: sourceName},
		// SQL Server adds the recovery mode to the shared snapshot modes.
		"snapshot.mode": {rule: sourceText, values: []string{"initial", "initial_only", "schema_only", "schema_only_recovery", "recovery", "never"}},
	}),
	"oracle": mergeSourceKeys(relationalSourceKeys, map[string]sourceKeySpec{
		"database.pdb.name":           {rule: sourceName},
		"database.connection.adapter": {rule: sourceText, values: []string{"logminer", "xstream"}},
		"database.out.server.name":    {rule: sourceName},
		"log.mining.strategy":         {rule: sourceText, values: []string{"redo_log_catalog", "online_catalog"}},
		"lob.enabled":                 {rule: sourceBool},
	}),
	"mongodb": mergeSourceKeys(commonSourceKeys, map[string]sourceKeySpec{
		"mongodb.connection.string":            {rule: sourceMongoURI},
		"mongodb.user":                         {rule: sourceText},
		"mongodb.password":                     {rule: sourceText}, // a credential by name: secret ref only
		"mongodb.authsource":                   {rule: sourceText},
		"database.include.list":                {rule: sourceList},
		"database.exclude.list":                {rule: sourceList},
		"collection.include.list":              {rule: sourceList},
		"collection.exclude.list":              {rule: sourceList},
		"field.include.list":                   {rule: sourceList},
		"field.exclude.list":                   {rule: sourceList},
		"capture.mode":                         {rule: sourceText, values: []string{"change_streams", "change_streams_with_pre_image", "change_streams_update_full", "change_streams_update_full_with_pre_image"}},
		"mongodb.ssl.enabled":                  {rule: sourceBool},
		"mongodb.ssl.invalid.hostname.allowed": {rule: sourceBool},
		"mongodb.poll.interval.sec":            {rule: sourceInt},
		"mongodb.members.auto.discover":        {rule: sourceBool},
	}),
}

// mergeSourceKeys returns the union of the tables, later ones winning.
func mergeSourceKeys(tables ...map[string]sourceKeySpec) map[string]sourceKeySpec {
	out := map[string]sourceKeySpec{}
	for _, table := range tables {
		for name, spec := range table {
			out[name] = spec
		}
	}
	return out
}

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
	if err := addSourceKeys(&props, props.Source, d.ix, d.image.Tag, p.SourceKeys); err != nil {
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
	if err := checkNoExpression(value); err != nil {
		return "", fmt.Errorf("%w: key %s", err, q(key.Name))
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

// addSourceKeys validates the source half against the connector's allow-list.
// A key not in the table, a value outside its rule, a plain-text credential, a
// ${...} form other than ${secret:name} and a host or path the rule refuses are
// rejected. The transforms and predicates families are checked against the
// U3a index (addSourceFamilies), never treated as free-form engine keys.
func addSourceKeys(props *Properties, source string, ix *knowledge.Index, tag string, given map[string]string) error {
	table, ok := sourceKeyTables[source]
	if !ok {
		return fmt.Errorf("%w: source %s has no key allow-list", ErrProperties, q(source))
	}
	names := make([]string, 0, len(given))
	for name := range given {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if reservedSourceKeys[name] {
			return fmt.Errorf("%w: source key %s is written by this driver and cannot be set twice", ErrProperties, q(name))
		}
		if isSourceFamilyKey(name) {
			continue // addSourceFamilies owns these
		}
		spec, ok := table[name]
		if !ok {
			return fmt.Errorf("%w: source key %s is not in the %s connector allow-list (%s)", ErrProperties, q(name), q(source), strings.Join(sourceKeyNames(source), ", "))
		}
		value, err := checkSourceValue(name, given[name], spec)
		if err != nil {
			return err
		}
		props.Entries["debezium.source."+name] = value
	}
	return addSourceFamilies(props, ix, tag, given)
}

// isSourceFamilyKey reports whether name belongs to a family the index owns:
// the list key itself or one of its aliases' keys.
func isSourceFamilyKey(name string) bool {
	return name == "transforms" || name == "predicates" ||
		strings.HasPrefix(name, "transforms.") || strings.HasPrefix(name, "predicates.")
}

// checkSourceValue validates one allow-listed source value.
func checkSourceValue(name, value string, spec sourceKeySpec) (string, error) {
	if err := checkCommon(value); err != nil {
		return "", fmt.Errorf("%w: source key %s", err, q(name))
	}
	if actions.LooksSecret(name) {
		if !secretRefOnlyRE.MatchString(value) {
			return "", fmt.Errorf("%w: source key %s is a credential; its value must be a ${secret:name} reference", ErrSecretValue, q(name))
		}
		return value, nil
	}
	if secretRefRE.MatchString(value) {
		return "", fmt.Errorf("%w: source key %s carries a ${secret:name} reference but is not a credential", ErrProperties, q(name))
	}
	if err := checkNoExpression(value); err != nil {
		return "", fmt.Errorf("%w: source key %s", err, q(name))
	}
	if len(spec.values) > 0 && !contains(spec.values, value) {
		return "", fmt.Errorf("%w: source key %s: value %s is not one of %s", ErrProperties, q(name), q(value), strings.Join(spec.values, ", "))
	}
	if plaintextCredential(value) {
		return "", fmt.Errorf("%w: source key %s: the value looks like a credential", ErrSecretValue, q(name))
	}
	if err := checkURLValue(value); err != nil {
		return "", fmt.Errorf("source key %s: %w", q(name), err)
	}
	if err := checkSourceRule(name, value, spec.rule); err != nil {
		return "", err
	}
	return value, nil
}

// checkSourceRule applies the key's shape rule to an otherwise valid value.
func checkSourceRule(name, value string, rule sourceRule) error {
	bad := func(what string) error {
		return fmt.Errorf("%w: source key %s: %s", ErrProperties, q(name), what)
	}
	switch rule {
	case sourceText:
	case sourceName, sourceTopicPrefix:
		if !sourceNameRE.MatchString(value) {
			return bad("the value must be an identifier of letters, digits, '_', '.', '$' and '-'")
		}
	case sourceBool:
		if value != "true" && value != "false" {
			return bad("the value must be true or false")
		}
	case sourceInt:
		if !intRE.MatchString(value) {
			return bad("the value must be an integer")
		}
	case sourcePort:
		n, err := strconv.Atoi(value)
		if err != nil || n < 1 || n > 65535 {
			return bad("the value must be a TCP port (1-65535)")
		}
	case sourceHost:
		if err := checkHost(value); err != nil {
			return fmt.Errorf("source key %s: %w", q(name), err)
		}
	case sourceList:
		for _, part := range strings.Split(value, ",") {
			if strings.TrimSpace(part) == "" {
				return bad("the comma-separated list has an empty element")
			}
		}
	case sourceDataPath:
		if !offsetsFileRE.MatchString(value) {
			return bad("the value must be data/<name> (the directory the stack keeps state in)")
		}
	case sourceClass:
		if !classValueRE.MatchString(value) {
			return bad("the value must be a Java class name")
		}
	case sourceMongoURI:
		if err := checkMongoURI(value); err != nil {
			return fmt.Errorf("source key %s: %w", q(name), err)
		}
	}
	return nil
}

// checkHost validates a source host. A name is checked by shape only (Validate
// does no I/O); an IP literal is checked against connprobe's guard, so a
// link-local or metadata address is refused while loopback and private
// addresses stay normal for a local source.
func checkHost(value string) error {
	host := strings.ToLower(strings.TrimSuffix(value, "."))
	if host == "" {
		return fmt.Errorf("%w: the host is empty", ErrProperties)
	}
	if metadataHostNames[host] {
		return fmt.Errorf("%w: %s is a metadata service name", ErrProperties, host)
	}
	if ip, err := netip.ParseAddr(strings.Trim(host, "[]")); err == nil {
		if err := urlGuard.CheckAddr(ip); err != nil {
			return fmt.Errorf("%w: %v", ErrProperties, err)
		}
		return nil
	}
	if !hostnameRE.MatchString(host) || strings.HasSuffix(host, "-") {
		return fmt.Errorf("%w: %s is not a host name or IP literal", ErrProperties, q(value))
	}
	// Java's InetAddress reads an all-numeric (or 0x) last label as an IPv4
	// address, so 2852039166 and 169.254.43518 reach 169.254.169.254 even
	// though netip refuses them; a numeric top-level label is not a host name.
	if decimalAddress(host) {
		return fmt.Errorf("%w: %s is a decimal or hex IPv4 spelling; write the dotted quad", ErrProperties, q(value))
	}
	return nil
}

// decimalAddress reports whether the host's last label is all digits or a
// 0x-prefixed hex number (see checkHost).
func decimalAddress(host string) bool {
	last := host
	if i := strings.LastIndexByte(host, '.'); i >= 0 {
		last = host[i+1:]
	}
	if last == "" {
		return false
	}
	if strings.HasPrefix(last, "0x") || strings.HasPrefix(last, "0X") {
		return true
	}
	for _, r := range last {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// checkMongoURI validates a mongodb:// or mongodb+srv:// connection string: no
// user info (the connector has mongodb.user/mongodb.password) and every host
// checked like any other source host.
func checkMongoURI(value string) error {
	u, err := url.Parse(value)
	if err != nil || (u.Scheme != "mongodb" && u.Scheme != "mongodb+srv") || u.Host == "" {
		return fmt.Errorf("%w: the value is not a mongodb:// or mongodb+srv:// connection string", ErrProperties)
	}
	if u.User != nil {
		return fmt.Errorf("%w: the connection string carries a username or password; use mongodb.user and mongodb.password", ErrSecretValue)
	}
	for _, hostPort := range strings.Split(u.Host, ",") {
		host := hostPort
		if h, _, err := net.SplitHostPort(hostPort); err == nil {
			host = h
		}
		if err := checkHost(host); err != nil {
			return err
		}
	}
	return nil
}

// addSourceFamilies validates transforms and predicates: an alias must be
// declared in its list key, its type must be an entry of the U3a index, and
// every other key must be a key of that entry. This is how an SMT bypasses the
// index only if the index permits it.
func addSourceFamilies(props *Properties, ix *knowledge.Index, tag string, given map[string]string) error {
	transforms, err := addSourceFamily(props, ix, tag, given, "transforms", knowledge.KindSMT)
	if err != nil {
		return err
	}
	predicates, err := addSourceFamily(props, ix, tag, given, "predicates", knowledge.KindPredicate)
	if err != nil {
		return err
	}
	for alias := range transforms {
		ref := "transforms." + alias + ".predicate"
		value, ok := given[ref]
		if !ok {
			continue
		}
		if _, declared := predicates[value]; !declared {
			return fmt.Errorf("%w: source key %s names predicate %s, which predicates does not declare", ErrProperties, q(ref), q(value))
		}
	}
	return nil
}

// refuseScriptingSMT refuses an SMT whose values are code the engine runs with
// the server's privileges: Debezium's Filter carries a script in `condition`
// (with a `language` selecting the engine) and ContentBasedRouter in
// `topic.expression`. The index lists both because they are valid Connect
// configs, but a setup proposal must never supply that code: design §2.5 says
// an executed artifact comes from a template with typed params.
func refuseScriptingSMT(entry knowledge.Entry) error {
	for _, k := range entry.Keys {
		switch {
		case k.Name == "language", k.Name == "condition", k.Name == "expression",
			strings.HasSuffix(k.Name, ".condition"), strings.HasSuffix(k.Name, ".expression"):
			return fmt.Errorf("%s runs script code through %s; the setup never supplies scripts", q(entry.Name), q(k.Name))
		}
	}
	return nil
}

// addSourceFamily validates one family (transforms or predicates) and writes
// its entries. It returns the declared aliases and the index entry each names.
func addSourceFamily(props *Properties, ix *knowledge.Index, tag string, given map[string]string, listKey string, kind knowledge.Kind) (map[string]knowledge.Entry, error) {
	var aliases []string
	seen := map[string]bool{}
	if list, ok := given[listKey]; ok {
		if err := checkCommon(list); err != nil {
			return nil, fmt.Errorf("%w: source key %s", err, q(listKey))
		}
		if err := checkNoExpression(list); err != nil {
			return nil, fmt.Errorf("%w: source key %s", err, q(listKey))
		}
		for _, part := range strings.Split(list, ",") {
			alias := strings.TrimSpace(part)
			if !transformRE.MatchString(alias) {
				return nil, fmt.Errorf("%w: source key %s: alias %s is not an identifier", ErrProperties, q(listKey), q(alias))
			}
			if seen[alias] {
				return nil, fmt.Errorf("%w: source key %s lists %s twice", ErrProperties, q(listKey), q(alias))
			}
			seen[alias] = true
			aliases = append(aliases, alias)
		}
	}
	entries := make(map[string]knowledge.Entry, len(aliases))
	for _, alias := range aliases {
		typeKey := listKey + "." + alias + ".type"
		value, ok := given[typeKey]
		if !ok {
			return nil, fmt.Errorf("%w: source key %s declares %s but %s is missing (the index needs the class)", ErrProperties, q(listKey), q(alias), q(typeKey))
		}
		entry, err := ix.Lookup(kind, value)
		if err != nil {
			return nil, fmt.Errorf("%w: source key %s: %w", ErrProperties, q(typeKey), err)
		}
		if kind == knowledge.KindSMT {
			if err := refuseScriptingSMT(entry); err != nil {
				return nil, fmt.Errorf("%w: source key %s: %v", ErrProperties, q(typeKey), err)
			}
		}
		entries[alias] = entry
	}
	names := make([]string, 0, len(given))
	for name := range given {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if !strings.HasPrefix(name, listKey+".") {
			continue
		}
		alias, sub, ok := strings.Cut(strings.TrimPrefix(name, listKey+"."), ".")
		if !ok || alias == "" || sub == "" {
			return nil, fmt.Errorf("%w: source key %s is not %s.<alias>.<key>", ErrProperties, q(name), q(listKey))
		}
		entry, declared := entries[alias]
		if !declared {
			return nil, fmt.Errorf("%w: source key %s names %s %s, which %s does not declare", ErrProperties, q(name), listKey, q(alias), q(listKey))
		}
		value := given[name]
		if sub == "type" {
			if err := checkCommon(value); err != nil {
				return nil, fmt.Errorf("%w: source key %s", err, q(name))
			}
			if err := checkNoExpression(value); err != nil {
				return nil, fmt.Errorf("%w: source key %s", err, q(name))
			}
			written := entry.Class
			if written == "" {
				written = value
			} else if strings.HasSuffix(value, "$Key") {
				written += "$Key"
			} else if strings.HasSuffix(value, "$Value") {
				written += "$Value"
			}
			props.Entries["debezium.source."+name] = written
			continue
		}
		key, ok := entry.Key(sub)
		if !ok {
			return nil, fmt.Errorf("%w: source key %s is not a key of %s (%s)", ErrProperties, q(name), q(entry.Name), strings.Join(entryKeyNames(entry), ", "))
		}
		// A Debezium entry's keys are versioned against the server release; a
		// Kafka entry's against Kafka's, which is not the image tag, so it is
		// not compared.
		if entry.Component == "debezium" {
			if err := keyAtImageTag(key, tag); err != nil {
				return nil, fmt.Errorf("%w: source key %s: %v", ErrProperties, q(name), err)
			}
		}
		checked, err := checkSinkValue(key, value)
		if err != nil {
			return nil, fmt.Errorf("%w: source key %s: %w", ErrProperties, q(name), err)
		}
		canonical := listKey + "." + alias + "." + key.Name
		if key.Name != sub {
			props.Aliases[name] = canonical
		}
		props.Entries["debezium.source."+canonical] = checked
	}
	if len(aliases) > 0 {
		props.Entries["debezium.source."+listKey] = strings.Join(aliases, ",")
	}
	return entries, nil
}

// sourceKeyNames is one connector's allow-listed key names, sorted, for a
// refusal; the transforms and predicates families are appended.
func sourceKeyNames(source string) []string {
	table := sourceKeyTables[source]
	out := make([]string, 0, len(table)+2)
	for name := range table {
		out = append(out, name)
	}
	out = append(out, "transforms", "predicates")
	sort.Strings(out)
	return out
}

// checkNoExpression refuses every config expression other than a ${secret:name}
// reference. SmallRye Config expands ${name} and ${name:default} from every
// source, including the process environment, so a value with one could read a
// credential the builder never meant to expose. A bare $ is ordinary text.
func checkNoExpression(value string) error {
	if strings.Contains(value, "${") {
		return fmt.Errorf("%w: the value carries a ${...} expression; only a literal value or a ${secret:name} reference on a credential key is allowed", ErrProperties)
	}
	return nil
}

// checkCommon is the value rule every key shares: non-empty, no control
// character, no backslash, no surrounding space, and not a comment a
// properties parser would swallow. A backslash is refused because in a Java
// properties file it escapes the next character, and a value ending in one
// continues onto the next physical line: that would swallow the key rendered
// after it (sink type, offset store, ...) and silently change other values
// (C:\certs\ca.pem would read as C:certsca.pem). The host target refuses it
// for the same reason.
func checkCommon(value string) error {
	if len(value) > maxValueBytes {
		return fmt.Errorf("%w: the value is longer than %d bytes", ErrProperties, maxValueBytes)
	}
	switch {
	case value == "":
		return fmt.Errorf("%w: the value is empty", ErrProperties)
	case strings.ContainsFunc(value, unicode.IsControl):
		return fmt.Errorf("%w: the value carries a control character", ErrProperties)
	case strings.ContainsRune(value, '\\'):
		return fmt.Errorf("%w: the value carries a backslash, which escapes or continues the line in a properties file", ErrProperties)
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
	// A sink URL's host is checked like a source host: the same metadata names
	// and decimal-IPv4 spellings, so http://metadata.google.internal/ is
	// refused here as well.
	if err := checkHost(u.Hostname()); err != nil {
		return err
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
// Values passed checkCommon (no control character, no backslash), so no entry
// can add or continue a line. The only ${...} form left is a credential key's
// ${secret:name} reference (see properties_ops.go: materialising it is a
// runner gap until U1-4).
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
