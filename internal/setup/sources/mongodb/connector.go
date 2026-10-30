package mongodb

import (
	"fmt"
	"strings"
)

// ConnectorOptions are the typed inputs of the Debezium MongoDB connector
// config a setup records. Every field is a value the person or the provisional
// pipeline supplied, never model text. ConnectionStringSecret is a name in the
// runner's secret store holding the whole mongodb:// or mongodb+srv://
// connection string (its user and password included), so the config carries
// ${secret:<name>} and no credential, and no host or option the model could
// steer.
type ConnectorOptions struct {
	ConnectionStringSecret string
	TopicPrefix            string
	Collections            []string // include the canary collection (Driver.CanaryNamespace)
}

// ConnectorConfig builds the Debezium MongoDB connector config. It contains no
// plaintext credential: mongodb.connection.string is a ${secret:} reference
// the runner materialises, and every other value is a literal the Q4a
// validate host and key rules accept.
func ConnectorConfig(o ConnectorOptions) (map[string]string, error) {
	if !secretNameRe.MatchString(o.ConnectionStringSecret) {
		return nil, fmt.Errorf("%w: the connection string's secret name must be letters, digits, '.', '_' or '-' (at most 128 bytes)", ErrParams)
	}
	if !topicPrefixRe.MatchString(o.TopicPrefix) {
		return nil, fmt.Errorf("%w: topic.prefix %q (letters, digits, '.', '_', '-', at most 64 bytes)", ErrParams, clipName(o.TopicPrefix))
	}
	if err := checkCollections(o.Collections); err != nil {
		return nil, err
	}
	return map[string]string{
		"connector.class":           ConnectorClass,
		"mongodb.connection.string": "${secret:" + o.ConnectionStringSecret + "}",
		"topic.prefix":              o.TopicPrefix,
		"collection.include.list":   strings.Join(o.Collections, ","),
	}, nil
}
