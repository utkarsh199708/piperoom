package owners

import (
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"

	"piperoom/internal/clients/workerlogs/logql"
)

// ConsumerLogsSpec is the optional cdc `consumer_logs:` block (H2): where the
// pipeline's consumer writes its own log, so consumer_logs can show the agent
// why a consumer group is Empty or not moving (a crash loop on one record, a
// decode error) when the group state cannot.
//
//	consumer_logs:
//	  selector: '{service="devconsumer"}'
//
// Selector is a LogQL stream selector on the server's one Loki (the same URL
// and credentials as the worker logs), checked by the rule WORKER_LOGS_SELECTOR
// follows (logql.CheckSelector): matchers only, no pipeline stage. The
// operator picks the streams here; the model never does.
type ConsumerLogsSpec struct {
	Selector string `yaml:"selector"`
}

// UnmarshalYAML rejects unknown keys in the block, like ShadowSpec: a typo
// such as `selecter:` must stop the start, not leave the tool unconfigured.
func (s *ConsumerLogsSpec) UnmarshalYAML(node *yaml.Node) error {
	if err := checkKeys(node, "consumer_logs", "selector"); err != nil {
		return err
	}
	type plain ConsumerLogsSpec // no methods, so Decode does not recurse into this one
	return node.Decode((*plain)(s))
}

// ConsumerLogSelector returns the pipeline's consumer log stream selector,
// trimmed; "" when the pipeline has no consumer_logs block.
func (p Pipeline) ConsumerLogSelector() string {
	if p.ConsumerLogs == nil {
		return ""
	}
	return strings.TrimSpace(p.ConsumerLogs.Selector)
}

// validateConsumerLogs checks the block: when present, its selector is set
// and is a pure stream selector.
func (p *Pipeline) validateConsumerLogs() error {
	if p.ConsumerLogs == nil {
		return nil
	}
	sel := strings.TrimSpace(p.ConsumerLogs.Selector)
	if sel == "" {
		return fmt.Errorf("consumer_logs: selector is required (a LogQL stream selector such as {service=\"order-sync\"}); remove the block to leave consumer logs unconfigured")
	}
	if err := logql.CheckSelector(sel); err != nil {
		return fmt.Errorf("consumer_logs: selector: %w", err)
	}
	return nil
}
