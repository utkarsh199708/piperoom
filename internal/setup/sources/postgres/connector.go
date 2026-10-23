package postgres

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

// ConnectorOptions are the typed inputs of the Debezium Postgres connector
// config a setup records. Every field is a value the person or the provisional
// pipeline supplied, never model text; PasswordSecret is a name in the
// runner's secret store, so the config carries ${secret:<name>} and no
// credential.
type ConnectorOptions struct {
	Host            string
	Port            int
	Database        string
	User            string
	PasswordSecret  string
	TopicPrefix     string
	Publication     string
	Slot            string
	Tables          []string
	PublicationMode PublicationMode
}

// ConnectorConfig builds the Debezium Postgres connector config. It contains
// no plaintext credential: the password is a ${secret:...} reference the
// runner materialises, and every other value is a literal the Q4a validate
// host and key rules accept.
func ConnectorConfig(o ConnectorOptions) (map[string]string, error) {
	host := strings.TrimSpace(o.Host)
	if host == "" || strings.ContainsAny(host, "${}\r\n\t /") || (net.ParseIP(host) == nil && !hostnameRe.MatchString(host)) {
		return nil, fmt.Errorf("%w: database.hostname %q must be a host name or IP address", ErrParams, clipName(host))
	}
	if o.Port < 1 || o.Port > 65535 {
		return nil, fmt.Errorf("%w: database.port %d is not a port", ErrParams, o.Port)
	}
	if !databaseRe.MatchString(o.Database) {
		return nil, fmt.Errorf("%w: database.dbname %q (letters, digits, '.', '_', '$', '-', at most 63 bytes)", ErrParams, clipName(o.Database))
	}
	if _, err := quoteRole(o.User); err != nil {
		return nil, fmt.Errorf("%w: database.user: %w", ErrParams, err)
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
	publication := o.Publication
	if publication == "" {
		publication = DefaultPublication
	}
	if _, err := quoteIdent(publication); err != nil {
		return nil, fmt.Errorf("publication.name: %w", err)
	}
	slot := o.Slot
	if slot == "" {
		slot = DefaultSlot
	}
	if _, err := quoteSlot(slot); err != nil {
		return nil, fmt.Errorf("slot.name: %w", err)
	}
	mode := o.PublicationMode
	if mode == "" {
		mode = PublicationFiltered
	}
	if mode != PublicationFiltered && mode != PublicationAllTables {
		return nil, fmt.Errorf("%w: publication.autocreate.mode %q (filtered or all_tables)", ErrParams, clipName(string(mode)))
	}
	return map[string]string{
		"connector.class":             ConnectorClass,
		"database.hostname":           host,
		"database.port":               strconv.Itoa(o.Port),
		"database.dbname":             o.Database,
		"database.user":               o.User,
		"database.password":           "${secret:" + o.PasswordSecret + "}",
		"plugin.name":                 OutputPlugin,
		"slot.name":                   slot,
		"publication.name":            publication,
		"publication.autocreate.mode": string(mode),
		"topic.prefix":                o.TopicPrefix,
		"table.include.list":          strings.Join(o.Tables, ","),
	}, nil
}
