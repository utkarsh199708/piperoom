package oracle

import (
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
)

// ConnectorOptions are the typed inputs of the Debezium Oracle connector
// config a setup records. Every field is a value the person or the provisional
// pipeline supplied, never model text; PasswordSecret is a name in the
// runner's secret store, so the config carries ${secret:<name>} and no
// credential.
//
// Database is the database name (V$DATABASE.NAME: the CDB name in a CDB,
// such as FREE or XE). PDB is the pluggable database that holds the tables,
// empty on a non-CDB. SchemaHistoryServers is the Kafka bootstrap list the
// connector keeps its schema history in (required by the Oracle connector);
// SchemaHistoryTopic defaults to "schema-history.<topic prefix>".
type ConnectorOptions struct {
	Host                 string
	Port                 int
	Database             string
	PDB                  string
	User                 string
	PasswordSecret       string
	TopicPrefix          string
	Tables               []string
	SchemaHistoryServers []string
	SchemaHistoryTopic   string
}

// ConnectorConfig builds the Debezium Oracle connector config over LogMiner.
// It contains no plaintext credential: the password is a ${secret:...}
// reference the runner materialises, and every other value is a literal the
// Q4a validate host and key rules accept.
//
// table.include.list is a list of regular expressions in Debezium; '$' is
// legal in an Oracle name and a regex anchor, so each name part is escaped
// (regexp.QuoteMeta) and a table such as ORD$ITEMS matches itself only.
// schema.history.internal.store.only.captured.tables.ddl keeps the history
// topic to the captured tables' DDL, not every table of the database.
func ConnectorConfig(o ConnectorOptions) (map[string]string, error) {
	host, err := checkHost(o.Host)
	if err != nil {
		return nil, fmt.Errorf("database.hostname: %w", err)
	}
	port := o.Port
	if port == 0 {
		port = DefaultPort
	}
	if port < 1 || port > 65535 {
		return nil, fmt.Errorf("%w: database.port %d is not a port", ErrParams, o.Port)
	}
	if !dbNameRe.MatchString(o.Database) {
		return nil, fmt.Errorf("%w: database.dbname %q (the upper-case database name of V$DATABASE.NAME, such as FREE)", ErrParams, clipName(o.Database))
	}
	if o.PDB != "" {
		if err := upperIdent(o.PDB); err != nil {
			return nil, fmt.Errorf("database.pdb.name: %w", err)
		}
	}
	if err := checkUser(o.User); err != nil {
		return nil, err
	}
	if o.PDB != "" && !isCommonUser(o.User) {
		return nil, fmt.Errorf("%w: database.user %s: a CDB's LogMiner user is a common user (C##...)", ErrParams, o.User)
	}
	if !secretNameRe.MatchString(o.PasswordSecret) {
		return nil, fmt.Errorf("%w: the password's secret name must be letters, digits, '.', '_' or '-' (at most 128 bytes)", ErrParams)
	}
	if !topicPrefixRe.MatchString(o.TopicPrefix) {
		return nil, fmt.Errorf("%w: topic.prefix %q (letters, digits, '.', '_', '-', at most 64 bytes)", ErrParams, clipName(o.TopicPrefix))
	}
	if err := checkTables(o.Tables); err != nil {
		return nil, err
	}
	include := make([]string, len(o.Tables))
	for i, t := range o.Tables {
		owner, name, _ := tableParts(t)
		include[i] = regexp.QuoteMeta(owner) + "." + regexp.QuoteMeta(name)
	}
	servers, err := checkServers(o.SchemaHistoryServers)
	if err != nil {
		return nil, err
	}
	historyTopic := o.SchemaHistoryTopic
	if historyTopic == "" {
		historyTopic = "schema-history." + o.TopicPrefix
	}
	if !topicRe.MatchString(historyTopic) || historyTopic == "." || historyTopic == ".." {
		return nil, fmt.Errorf("%w: schema.history.internal.kafka.topic %q is not a Kafka topic name", ErrParams, clipName(historyTopic))
	}
	cfg := map[string]string{
		"connector.class":             ConnectorClass,
		"database.hostname":           host,
		"database.port":               strconv.Itoa(port),
		"database.dbname":             o.Database,
		"database.user":               o.User,
		"database.password":           "${secret:" + o.PasswordSecret + "}",
		"database.connection.adapter": ConnectionAdapter,
		"log.mining.strategy":         MiningStrategy,
		"topic.prefix":                o.TopicPrefix,
		"table.include.list":          strings.Join(include, ","),
		"schema.history.internal.kafka.bootstrap.servers":        strings.Join(servers, ","),
		"schema.history.internal.kafka.topic":                    historyTopic,
		"schema.history.internal.store.only.captured.tables.ddl": "true",
	}
	if o.PDB != "" {
		cfg["database.pdb.name"] = o.PDB
	}
	return cfg, nil
}

// checkHost accepts a host name or an IP literal, nothing that could be a
// URL, a reference or a list.
func checkHost(h string) (string, error) {
	host := strings.TrimSpace(h)
	if host == "" || strings.ContainsAny(host, "${}\r\n\t /,") || (net.ParseIP(host) == nil && !hostnameRe.MatchString(host)) {
		return "", fmt.Errorf("%w: %q must be a host name or IP address", ErrParams, clipName(host))
	}
	return host, nil
}

// checkServers checks a non-empty host:port bootstrap list.
func checkServers(servers []string) ([]string, error) {
	if len(servers) == 0 {
		return nil, fmt.Errorf("%w: schema.history.internal.kafka.bootstrap.servers is required (the Oracle connector keeps its schema history in Kafka)", ErrParams)
	}
	out := make([]string, len(servers))
	for i, s := range servers {
		h, p, err := net.SplitHostPort(strings.TrimSpace(s))
		if err != nil {
			return nil, fmt.Errorf("%w: schema history server %q is not host:port", ErrParams, clipName(s))
		}
		host, err := checkHost(h)
		if err != nil {
			return nil, fmt.Errorf("schema history server: %w", err)
		}
		n, err := strconv.Atoi(p)
		if err != nil || n < 1 || n > 65535 {
			return nil, fmt.Errorf("%w: schema history server %q has no valid port", ErrParams, clipName(s))
		}
		out[i] = net.JoinHostPort(host, p)
	}
	return out, nil
}
