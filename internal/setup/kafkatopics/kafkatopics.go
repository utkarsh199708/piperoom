// Package kafkatopics holds the topic rules a Kafka Connect connector's
// config must keep that the server's scope check and the runner's allow-list
// both apply: one implementation, so the two sides cannot disagree. It has
// no dependency, so the server (actions) and the runner (targets/strimzi)
// can both import it.
package kafkatopics

import (
	"errors"
	"regexp"
	"strings"
)

// NamePattern is a Kafka topic name (the admission policy reads it too).
const NamePattern = `^[A-Za-z0-9._-]{1,249}$`

// nameRE is a Kafka topic name: at most 249 of letters, digits, '.', '_'
// and '-'.
var nameRE = regexp.MustCompile(NamePattern)

// Errors name the rule a topic broke.
var (
	ErrNoPrefix     = errors.New("the pipeline's topic prefix is required for this topic")
	ErrNotUnder     = errors.New("not a topic under the pipeline's topic prefix (<prefix>.<name>)")
	ErrReadTopic    = errors.New("the dead letter queue is never a topic the sink reads")
	ErrHistoryTopic = errors.New("the schema history topic is <prefix>.<name> or schemahistory.<prefix>[.<name>]")
)

// Under reports whether topic is <prefix>.<name> with a non-empty name.
func Under(prefix, topic string) bool {
	return prefix != "" && nameRE.MatchString(topic) && strings.HasPrefix(topic, prefix+".") && len(topic) > len(prefix)+1
}

// DeadLetter checks a sink's errors.deadletterqueue.topic.name: a topic
// under the pipeline's prefix that is not one of the topics the sink reads
// (a comma list; topics.regex sinks pass "").
func DeadLetter(prefix, dlq, topics string) error {
	if prefix == "" {
		return ErrNoPrefix
	}
	if !Under(prefix, dlq) {
		return ErrNotUnder
	}
	for _, t := range strings.Split(topics, ",") {
		if strings.TrimSpace(t) == dlq {
			return ErrReadTopic
		}
	}
	return nil
}

// SchemaHistory checks a source's schema.history.internal.kafka.topic: a
// topic under the pipeline's prefix, or Debezium's documented
// schemahistory.<prefix> (optionally with a further .<name>), which is the
// pipeline's by its prefix too.
func SchemaHistory(prefix, topic string) error {
	if prefix == "" {
		return ErrNoPrefix
	}
	if Under(prefix, topic) || (nameRE.MatchString(topic) && (topic == "schemahistory."+prefix || strings.HasPrefix(topic, "schemahistory."+prefix+"."))) {
		return nil
	}
	return ErrHistoryTopic
}
