package actions

import (
	"encoding/json"
	"fmt"

	"piperoom/internal/events"
)

// Compile-time check that the connector-config action says what it changes
// beyond the kafka-connect layer (Q1, QA G5).
var (
	_ Impacting = (*updateConnectorConfig)(nil)
	_ Impacting = (*registerSchema)(nil)
	_ Impacting = (*setCompatibility)(nil)
)

// Impact: a new schema version under a topic's subject changes the shape every
// reader of the topic decodes, so the consumer and the warehouse must approve
// too (Q2 F10). It decides from params alone.
func (a *registerSchema) Impact(raw json.RawMessage) Impact {
	var p registerSchemaParams
	if json.Unmarshal(raw, &p) != nil || p.Subject == "" {
		return Impact{}
	}
	return Impact{
		Layers:    []string{"consumer", "warehouse"},
		RiskLevel: events.RiskMedium,
		Reason:    fmt.Sprintf("registering a new schema under %s changes the record shape that consumers decode and the warehouse loads.", p.Subject),
	}
}

// Impact: compatibility NONE lets any later schema through, including one that
// breaks every reader and the warehouse load, so it is high risk. Stricter
// levels only restrict what can be registered later and have no opinion.
func (a *setCompatibility) Impact(raw json.RawMessage) Impact {
	var p setCompatibilityParams
	if json.Unmarshal(raw, &p) != nil || p.Subject == "" || p.Level != "NONE" {
		return Impact{}
	}
	return Impact{
		Layers:    []string{"consumer", "warehouse"},
		RiskLevel: events.RiskHigh,
		Reason:    fmt.Sprintf("compatibility NONE on %s lets any schema be registered, including one that breaks consumers and the warehouse load.", p.Subject),
	}
}

// Impact says which other layers a connector-config change reaches, so their
// owners must approve too, and how risky it is. It decides from the params
// alone and does no I/O. Params that do not parse give no opinion: Validate
// already refuses them.
func (a *updateConnectorConfig) Impact(raw json.RawMessage) Impact {
	p, err := a.parseParams(raw)
	if err != nil {
		return Impact{}
	}
	return configImpact(p.Key, p.Value)
}

// valueSchemaKeys are Debezium settings that decide the type a column's values
// have in the change event. Changing one changes what every reader of the
// topic receives. Only time.precision.mode is on the allow-list today
// (decimal.handling.mode has its own, finer rules); the others are listed so
// a key added to the allow-list later is covered the day it is added.
var valueSchemaKeys = map[string]string{
	"time.precision.mode":    "date and time columns",
	"binary.handling.mode":   "binary columns",
	"interval.handling.mode": "interval columns",
	"hstore.handling.mode":   "hstore columns",
}

// configImpact is the Impact of setting key to value. A setting that changes
// the value type or the events themselves reaches the consumer and, through it,
// the warehouse; a visibility or slot setting touches no other layer.
func configImpact(key, value string) Impact {
	downstream := []string{"consumer", "warehouse"}
	switch key {
	case "decimal.handling.mode":
		switch value {
		case "double":
			return Impact{
				Layers:    downstream,
				RiskLevel: events.RiskHigh,
				Reason:    "decimal.handling.mode=double turns NUMERIC/DECIMAL values (money) into floating-point doubles, which can lose precision; consumers and the warehouse store the rounded values.",
			}
		case "string":
			return Impact{
				Layers:    downstream,
				RiskLevel: events.RiskMedium,
				Reason:    "decimal.handling.mode=string changes the value type of NUMERIC/DECIMAL columns from a decimal to a string; consumers and warehouse loads see a new type.",
			}
		case "precise":
			return Impact{
				Layers:    downstream,
				RiskLevel: events.RiskMedium,
				Reason:    "decimal.handling.mode=precise changes NUMERIC/DECIMAL values back to exact decimals; consumers and warehouse loads see another type than the double or string they read now.",
			}
		}
	case "tombstones.on.delete":
		switch value {
		case "false":
			return Impact{
				Layers:    downstream,
				RiskLevel: events.RiskMedium,
				Reason:    "tombstones.on.delete=false stops the tombstone record after each delete; compacted topics keep deleted keys and consumers or sinks that act on a tombstone miss deletes.",
			}
		case "true":
			return Impact{
				Layers:    []string{"consumer"},
				RiskLevel: events.RiskLow,
				Reason:    "tombstones.on.delete=true adds a null-valued tombstone record after each delete; a consumer that does not expect a null value can fail on it.",
			}
		}
	case "errors.log.enable", "errors.deadletterqueue.context.headers.enable":
		return Impact{
			RiskLevel: events.RiskLow,
			Reason:    key + "=true only adds visibility (worker log lines or Connect error headers); no record is changed or dropped.",
		}
	case "heartbeat.interval.ms":
		if value == "0" {
			return Impact{
				RiskLevel: events.RiskLow,
				Reason:    "heartbeat.interval.ms=0 turns heartbeats off; a replication slot on a table with no changes can again hold WAL back.",
			}
		}
		return Impact{
			RiskLevel: events.RiskLow,
			Reason:    fmt.Sprintf("heartbeat.interval.ms=%s makes Debezium emit a heartbeat record every %s ms so the replication slot advances on a table with no changes; it writes nothing to the source database.", value, value),
		}
	}
	if what, ok := valueSchemaKeys[key]; ok {
		return Impact{
			Layers:    downstream,
			RiskLevel: events.RiskMedium,
			Reason:    fmt.Sprintf("%s=%s changes how %s are encoded; consumers and warehouse loads see a different value type or precision.", key, value, what),
		}
	}
	return Impact{}
}
