package owners

import (
	"fmt"

	"piperoom/internal/dlq"
)

// validateReprocessTopic checks the pipeline's own reprocess_topic (X2b): a
// valid Kafka topic name that is none of the pipeline's CDC topics. The
// checks that need other pipelines are in Owners.validateReprocessTopics.
func (p *Pipeline) validateReprocessTopic() error {
	t := p.ReprocessTopic
	if t == "" {
		return nil
	}
	if !dlq.ValidTopic(t) {
		return fmt.Errorf("reprocess_topic %q: not a valid Kafka topic name (letters, digits, '.', '_', '-', at most 249 characters)", t)
	}
	for _, st := range p.SourceTables {
		if t == p.TopicPrefix+"."+st {
			// kafka_replay_range would copy the topic's own records back
			// into it: every consumer would get them twice.
			return fmt.Errorf("reprocess_topic %q is a CDC topic of this pipeline; declare a separate topic", t)
		}
	}
	return nil
}

// validateReprocessTopics runs the reprocess_topic checks that span
// pipelines, per Kafka cluster as validateDLQ does: a reprocess topic must not
// be any pipeline's CDC topic or declared DLQ, and belongs to one pipeline,
// so a room's replay never writes into a topic another pipeline's consumers
// read.
func (o *Owners) validateReprocessTopics() error {
	used := map[string]string{} // cluster|topic -> what already uses it
	for _, p := range o.Pipelines {
		cluster := p.kafkaClusterKey()
		for _, t := range p.SourceTables {
			used[cluster+"|"+p.TopicPrefix+"."+t] = fmt.Sprintf("a CDC topic of pipeline %q", p.ID)
		}
		if p.watch == nil {
			continue
		}
		for _, d := range p.watch.DLQTopics {
			used[cluster+"|"+d.Topic] = fmt.Sprintf("a DLQ topic of pipeline %q", p.ID)
		}
	}
	for _, p := range o.Pipelines {
		if p.ReprocessTopic == "" {
			continue
		}
		key := p.kafkaClusterKey() + "|" + p.ReprocessTopic
		if what, ok := used[key]; ok {
			return fmt.Errorf("pipeline %q: reprocess_topic %q is %s; give the pipeline a topic of its own", p.ID, p.ReprocessTopic, what)
		}
		used[key] = fmt.Sprintf("the reprocess_topic of pipeline %q", p.ID)
	}
	return nil
}
