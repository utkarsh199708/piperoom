package mysql

import (
	"fmt"
	"net"
	"regexp"
	"strconv"
	"strings"
)

// hostnameRe is a DNS name or a host label. An IP literal is accepted by
// net.ParseIP instead, so an IPv6 address needs no brackets here.
var hostnameRe = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9.-]{0,251}[A-Za-z0-9])?$`)

// hostPortRe is one host:port of a Kafka bootstrap list.
var hostPortRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9.-]{0,251}:[0-9]{1,5}$`)

// validBootstrap reports whether s is 1 to 10 comma-separated host:port pairs.
func validBootstrap(s string) bool {
	parts := strings.Split(s, ",")
	if len(parts) > 10 {
		return false
	}
	for _, p := range parts {
		if !hostPortRe.MatchString(p) {
			return false
		}
		if n, err := strconv.Atoi(p[strings.LastIndex(p, ":")+1:]); err != nil || n < 1 || n > 65535 {
			return false
		}
	}
	return true
}

// historyTopicRe is a Kafka topic name (the schema history topic).
var historyTopicRe = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_.-]{0,248}$`)

// ConnectorOptions are the typed inputs of the Debezium MySQL connector config a
// setup records. Every field is a value the person or the provisional pipeline
// supplied, never model text; PasswordSecret is a name in the runner's secret
// store, so the config carries ${secret:<name>} and no credential.
type ConnectorOptions struct {
	Host           string
	Port           int
	User           string
	PasswordSecret string
	// ServerID is database.server.id: the id the connector joins the source
	// with as a replica. 1..4294967295, and different from the server's own
	// server_id (the server_id check).
	ServerID    int64
	TopicPrefix string
	// Tables are "db.table" names; the databases of database.include.list are
	// derived from them, so the two lists cannot disagree.
	Tables []string
	// HistoryBootstrap is the Kafka bootstrap list the schema history topic is
	// on (schema.history.internal.kafka.bootstrap.servers).
	HistoryBootstrap string
	// HistoryTopic is the schema history topic; "" means schema-history.<prefix>.
	HistoryTopic string
}

// ConnectorConfig builds the Debezium MySQL connector config. It contains no
// plaintext credential: the password is a ${secret:...} reference the runner
// materialises, and every other value is a literal the Q4a validate host and key
// rules accept. tasks.max is 1: the MySQL connector reads one binlog.
func ConnectorConfig(o ConnectorOptions) (map[string]string, error) {
	host := strings.TrimSpace(o.Host)
	if host == "" || strings.ContainsAny(host, "${}\r\n\t /") || (net.ParseIP(host) == nil && !hostnameRe.MatchString(host)) {
		return nil, fmt.Errorf("%w: database.hostname %q must be a host name or IP address", ErrParams, clipName(host))
	}
	if o.Port < 1 || o.Port > 65535 {
		return nil, fmt.Errorf("%w: database.port %d is not a port", ErrParams, o.Port)
	}
	if err := checkUser(o.User); err != nil {
		return nil, fmt.Errorf("%w: database.user: %w", ErrParams, err)
	}
	if !secretNameRe.MatchString(o.PasswordSecret) {
		return nil, fmt.Errorf("%w: the password's secret name must be letters, digits, '.', '_' or '-' (at most 128 bytes)", ErrParams)
	}
	if o.ServerID < 1 || o.ServerID > maxServerID {
		return nil, fmt.Errorf("%w: database.server.id %d is outside 1..%d", ErrParams, o.ServerID, uint64(maxServerID))
	}
	if !topicPrefixRe.MatchString(o.TopicPrefix) {
		return nil, fmt.Errorf("%w: topic.prefix %q (letters, digits, '.', '_', '-', at most 64 bytes)", ErrParams, clipName(o.TopicPrefix))
	}
	if err := checkTables(o.Tables); err != nil {
		return nil, err
	}
	if !validBootstrap(o.HistoryBootstrap) {
		return nil, fmt.Errorf("%w: schema.history.internal.kafka.bootstrap.servers %q must be host:port[,host:port...]", ErrParams, clipName(o.HistoryBootstrap))
	}
	topic := o.HistoryTopic
	if topic == "" {
		topic = "schema-history." + o.TopicPrefix
	}
	if !historyTopicRe.MatchString(topic) {
		return nil, fmt.Errorf("%w: schema.history.internal.kafka.topic %q is not a Kafka topic name", ErrParams, clipName(topic))
	}
	return map[string]string{
		"connector.class":       ConnectorClass,
		"database.hostname":     host,
		"database.port":         strconv.Itoa(o.Port),
		"database.user":         o.User,
		"database.password":     "${secret:" + o.PasswordSecret + "}",
		"database.server.id":    strconv.FormatInt(o.ServerID, 10),
		"topic.prefix":          o.TopicPrefix,
		"database.include.list": strings.Join(databasesOf(o.Tables), ","),
		"table.include.list":    strings.Join(o.Tables, ","),
		"schema.history.internal.kafka.bootstrap.servers": o.HistoryBootstrap,
		"schema.history.internal.kafka.topic":             topic,
		"tasks.max":                                       "1",
	}, nil
}

// databasesOf returns the distinct databases of "db.table" names, in first-seen
// order. The names already passed checkTables.
func databasesOf(tables []string) []string {
	var out []string
	seen := map[string]bool{}
	for _, t := range tables {
		db, _, _ := strings.Cut(t, ".")
		if !seen[db] {
			seen[db] = true
			out = append(out, db)
		}
	}
	return out
}
