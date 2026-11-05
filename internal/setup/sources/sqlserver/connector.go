package sqlserver

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// ConnectorOptions are the typed inputs of the Debezium SQL Server connector
// config a setup records. Every field is a value the person or the provisional
// pipeline supplied, never model text; PasswordSecret is a name in the
// runner's secret store, so the config carries ${secret:<name>} and no
// credential.
type ConnectorOptions struct {
	Host           string
	Port           int
	Database       string
	User           string
	PasswordSecret string
	TopicPrefix    string
	Tables         []string // include the canary table (DefaultCanaryTable)
	// HistoryBootstrap is the Kafka bootstrap list ("host:port,...") of the
	// schema history topic, which the SQL Server connector requires.
	HistoryBootstrap string
	// HistoryTopic defaults to schema-changes.<TopicPrefix>.
	HistoryTopic string
	// Encrypt turns on TLS to the server; TrustServerCertificate skips the
	// certificate check (a dev server's self-signed certificate) and is only
	// written when Encrypt is on.
	Encrypt                bool
	TrustServerCertificate bool
}

// checkHost accepts a host name or an IP literal, never a URL or reference.
func checkHost(field, host string) error {
	if host == "" || strings.ContainsAny(host, "${}\r\n\t /") || (net.ParseIP(host) == nil && !hostnameRe.MatchString(host)) {
		return fmt.Errorf("%w: %s %q must be a host name or IP address", ErrParams, field, clipName(host))
	}
	return nil
}

// checkPort accepts 1..65535.
func checkPort(field string, port int) error {
	if port < 1 || port > 65535 {
		return fmt.Errorf("%w: %s %d is not a port", ErrParams, field, port)
	}
	return nil
}

// ConnectorConfig builds the Debezium SQL Server connector config. It contains
// no plaintext credential, and every other value is a literal the Q4a validate
// host and key rules accept.
func ConnectorConfig(o ConnectorOptions) (map[string]string, error) {
	host := strings.TrimSpace(o.Host)
	if err := checkHost("database.hostname", host); err != nil {
		return nil, err
	}
	if err := checkPort("database.port", o.Port); err != nil {
		return nil, err
	}
	if err := checkDatabase(o.Database); err != nil {
		return nil, fmt.Errorf("database.names: %w", err)
	}
	if err := checkLogin(o.User); err != nil {
		return nil, fmt.Errorf("database.user: %w", err)
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
	brokers := strings.Split(o.HistoryBootstrap, ",")
	for i, b := range brokers {
		h, p, err := net.SplitHostPort(strings.TrimSpace(b))
		if err != nil {
			return nil, fmt.Errorf("%w: schema.history.internal.kafka.bootstrap.servers entry %q is not host:port", ErrParams, clipName(b))
		}
		if err := checkHost("schema history broker", h); err != nil {
			return nil, err
		}
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("%w: schema history broker port %q", ErrParams, clipName(p))
		}
		if err := checkPort("schema history broker port", n); err != nil {
			return nil, err
		}
		brokers[i] = net.JoinHostPort(h, p)
	}
	topic := o.HistoryTopic
	if topic == "" {
		topic = "schema-changes." + o.TopicPrefix
	}
	if !topicRe.MatchString(topic) || topic == "." || topic == ".." {
		return nil, fmt.Errorf("%w: schema.history.internal.kafka.topic %q is not a topic name", ErrParams, clipName(topic))
	}
	cfg := map[string]string{
		"connector.class":    ConnectorClass,
		"database.hostname":  host,
		"database.port":      strconv.Itoa(o.Port),
		"database.names":     o.Database,
		"database.user":      o.User,
		"database.password":  "${secret:" + o.PasswordSecret + "}",
		"database.encrypt":   strconv.FormatBool(o.Encrypt),
		"topic.prefix":       o.TopicPrefix,
		"table.include.list": strings.Join(o.Tables, ","),
		"schema.history.internal.kafka.bootstrap.servers": strings.Join(brokers, ","),
		"schema.history.internal.kafka.topic":             topic,
	}
	if o.Encrypt {
		cfg["database.trustServerCertificate"] = strconv.FormatBool(o.TrustServerCertificate)
	}
	return cfg, nil
}
