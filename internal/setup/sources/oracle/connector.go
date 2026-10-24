package oracle

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// ConnectorOptions are the typed inputs for Debezium Oracle. PasswordSecret
// is a secret-store name; plaintext credentials never enter the config.
type ConnectorOptions struct {
	Host           string
	Port           int
	Database       string
	PDB            string
	User           string
	PasswordSecret string
	TopicPrefix    string
	Tables         []string
	CanaryTable    string
}

// ConnectorConfig builds the Debezium Oracle connector configuration using
// LogMiner and only a secret reference for the password.
func ConnectorConfig(o ConnectorOptions) (map[string]string, error) {
	host := strings.TrimSpace(o.Host)
	if host == "" || strings.ContainsAny(host, "$"+"{}\r\n\t /") || (net.ParseIP(host) == nil && !hostnameRe.MatchString(host)) {
		return nil, fmt.Errorf("%w: database.hostname must be a host name or IP address", ErrParams)
	}
	if o.Port < 1 || o.Port > 65535 {
		return nil, fmt.Errorf("%w: database.port %d is not a port", ErrParams, o.Port)
	}
	if err := checkIdent(o.Database); err != nil {
		return nil, fmt.Errorf("%w: database.dbname: %v", ErrParams, err)
	}
	if o.PDB != "" {
		if err := checkIdent(o.PDB); err != nil {
			return nil, fmt.Errorf("%w: database.pdb.name: %v", ErrParams, err)
		}
	}
	if err := checkIdent(o.User); err != nil {
		return nil, fmt.Errorf("%w: database.user: %v", ErrParams, err)
	}
	if !secretNameRe.MatchString(o.PasswordSecret) {
		return nil, fmt.Errorf("%w: password secret name must be letters, digits, '.', '_' or '-' (at most 128 bytes)", ErrParams)
	}
	if !topicPrefixRe.MatchString(o.TopicPrefix) {
		return nil, fmt.Errorf("%w: topic.prefix %q is invalid", ErrParams, clipName(o.TopicPrefix))
	}
	if len(o.Tables) == 0 {
		return nil, ErrNoTables
	}
	canary := strings.TrimSpace(o.CanaryTable)
	if canary == "" {
		canary = DefaultCanaryTable
	}
	tables := withCanary(o.Tables, canary)
	if err := checkTables(tables); err != nil {
		return nil, err
	}
	cfg := map[string]string{
		"connector.class":             ConnectorClass,
		"database.connection.adapter": "logminer",
		"database.hostname":           host,
		"database.port":               strconv.Itoa(o.Port),
		"database.dbname":             o.Database,
		"database.user":               o.User,
		"database.password":           "$" + "{secret:" + o.PasswordSecret + "}",
		"topic.prefix":                o.TopicPrefix,
		"table.include.list":          strings.Join(tables, ","),
	}
	if o.PDB != "" {
		cfg["database.pdb.name"] = o.PDB
	}
	return cfg, nil
}
