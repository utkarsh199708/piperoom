package owners

import (
	"errors"
	"strings"
)

// DefaultSlot is the slot a Debezium Postgres connector uses when its config
// has no slot.name.
const DefaultSlot = "debezium"

// ErrInvalidSlotName is returned by ConnectorSlot for a slot.name that is set
// but is not a valid Postgres replication slot name (blank included).
var ErrInvalidSlotName = errors.New("invalid slot.name")

// ValidSlotName reports whether name is a valid Postgres replication slot
// name, the same rule as watch.slot.
func ValidSlotName(name string) bool { return identRE.MatchString(name) }

// ConnectorSlot returns the replication slot one Kafka Connect connector
// config streams from, by the watcher's rule. It is the one rule shared by the
// read tools' scope (tools.Scope.Slots) and the pipeline map, so both name the
// same slot:
//   - a slot.name key that is present must be a valid slot name, else
//     ErrInvalidSlotName: a broken or unresolved name (a config provider
//     reference, "") never turns into the default slot, which can belong to
//     another pipeline;
//   - with no slot.name key, a PostgresConnector uses DefaultSlot;
//   - any other connector (a sink, a JDBC source) has no slot: "" and nil.
//
// The error never carries the slot.name value; callers that echo it must cap
// it themselves.
func ConnectorSlot(cfg map[string]string) (string, error) {
	if n, present := cfg["slot.name"]; present {
		if !ValidSlotName(n) {
			return "", ErrInvalidSlotName
		}
		return n, nil
	}
	if strings.Contains(cfg["connector.class"], "PostgresConnector") {
		return DefaultSlot, nil
	}
	return "", nil
}
