// Package dlq reads what a Kafka Connect connector config says about bad
// records (errors.tolerance, dead-letter topic, error logging) and turns it
// into dead-letter topics and config-risk findings. It also holds
// RedactMessage, the redaction every reader of Connect error headers must use.
//
// The package is pure: no I/O, no logging, no dependencies beyond the
// standard library. The watcher, the topology builder, the Kafka client and
// the dlq_peek tool all use it, so they never disagree (docs/design/dlq.md
// section 3.1.1).
package dlq

import "strings"

// Config keys the package reads. Nothing else in a connector config is ever
// looked at, copied or printed.
const (
	KeyConnectorClass  = "connector.class"
	KeyTolerance       = "errors.tolerance"
	KeyTopic           = "errors.deadletterqueue.topic.name"
	KeyLogEnable       = "errors.log.enable"
	KeyLogMessages     = "errors.log.include.messages"
	KeyContextHeaders  = "errors.deadletterqueue.context.headers.enable"
	KeyDebeziumFailure = "event.processing.failure.handling.mode"
)

// Tolerance values after Parse. Anything that is not "all" reads as "none",
// which is Connect's default and what Connect itself does with a bad value
// (it refuses to start the connector).
const (
	ToleranceNone = "none"
	ToleranceAll  = "all"
)

// ErrorsConfig is what a connector config says about bad records.
type ErrorsConfig struct {
	// IsSink is true when connector.class ends in "SinkConnector" (best
	// effort: a custom sink with another class name reads as a source).
	IsSink bool
	// Tolerance is ToleranceNone (default) or ToleranceAll.
	Tolerance string
	// Topic is errors.deadletterqueue.topic.name; "" = no DLQ topic. It is
	// set only when it is a valid Kafka topic name, so callers can use it in
	// requests, logs and node ids without more checks.
	Topic string
	// TopicInvalid is true when the key is set to something that is not a
	// valid Kafka topic name. Topic is then "" but the connector does have a
	// DLQ setting, so it is not reported as "no DLQ".
	TopicInvalid bool
	// LogEnabled is errors.log.enable (default false).
	LogEnabled bool
	// LogMessages is errors.log.include.messages (default false).
	LogMessages bool
	// ContextHeaders is errors.deadletterqueue.context.headers.enable
	// (default false).
	ContextHeaders bool
	// DebeziumSkip is true when event.processing.failure.handling.mode is
	// "skip" (Debezium source connectors only; default "fail").
	DebeziumSkip bool
}

// ValidTopic reports whether s is a legal Kafka topic name:
// [A-Za-z0-9._-], 1 to 249 bytes, and not "." or "..".
func ValidTopic(s string) bool {
	if s == "" || len(s) > 249 || s == "." || s == ".." {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '.' || c == '_' || c == '-') {
			return false
		}
	}
	return true
}

// Parse reads the error-handling keys of a connector config. A nil or empty
// config gives Connect's defaults.
func Parse(cfg map[string]string) ErrorsConfig {
	c := ErrorsConfig{
		IsSink:         strings.HasSuffix(strings.ToLower(strings.TrimSpace(cfg[KeyConnectorClass])), "sinkconnector"),
		Tolerance:      ToleranceNone,
		LogEnabled:     isTrue(cfg[KeyLogEnable]),
		LogMessages:    isTrue(cfg[KeyLogMessages]),
		ContextHeaders: isTrue(cfg[KeyContextHeaders]),
		DebeziumSkip:   strings.EqualFold(strings.TrimSpace(cfg[KeyDebeziumFailure]), "skip"),
	}
	if strings.EqualFold(strings.TrimSpace(cfg[KeyTolerance]), ToleranceAll) {
		c.Tolerance = ToleranceAll
	}
	if t := strings.TrimSpace(cfg[KeyTopic]); t != "" {
		if ValidTopic(t) {
			c.Topic = t
		} else {
			c.TopicInvalid = true
		}
	}
	return c
}

// isTrue follows Connect's boolean parsing: "true" in any case, spaces ignored.
func isTrue(v string) bool { return strings.EqualFold(strings.TrimSpace(v), "true") }

// Risk severities.
const (
	SevSev2 = "sev2"
	SevSev3 = "sev3"
	SevInfo = "info"
)

// Risk codes (docs/design/dlq.md section 3.1.1).
const (
	RiskSilentDropSink   = "silent_drop_sink"
	RiskSilentSkipSource = "silent_skip_source"
	RiskDebeziumSkip     = "debezium_skip"
	RiskDLQTopicMissing  = "dlq_topic_missing"
	RiskDLQNoLog         = "dlq_no_log"
	RiskDLQLogMessages   = "dlq_log_messages"
	RiskDLQNoHeaders     = "dlq_no_headers"
)

// Risk is one config-risk finding.
type Risk struct {
	Code string // one of the Risk* constants
	Sev  string // SevSev2, SevSev3 or SevInfo
	// Text is one fixed line per Code. It names config keys only, never a
	// config value, so it is safe to show to people and to the model.
	Text string
}

// riskTexts is a switch in disguise: one constant string per code, never
// built from config values.
func riskText(code string) string {
	switch code {
	case RiskSilentDropSink:
		return "errors.tolerance=all with no DLQ topic and errors.log.enable=false: bad records are dropped with no trace"
	case RiskSilentSkipSource:
		return "errors.tolerance=all on a source connector: CDC events that fail conversion are skipped for good (sources have no DLQ)"
	case RiskDebeziumSkip:
		return "event.processing.failure.handling.mode=skip: Debezium drops events it cannot process"
	case RiskDLQTopicMissing:
		return "errors.deadletterqueue.topic.name is set but the topic does not exist: Connect creates it when the task starts, so the worker could not create it (Create ACL, or the task never started); until it exists bad records fail the task instead"
	case RiskDLQNoLog:
		return "DLQ is on but errors.log.enable=false: the worker log will not say why records were dead-lettered. Enable it WITHOUT errors.log.include.messages=true: that option writes record values into the worker log, which connect_worker_logs then shows"
	case RiskDLQLogMessages:
		return "errors.log.include.messages=true: failed record values are written into the worker log"
	case RiskDLQNoHeaders:
		return "DLQ records carry no error headers (errors.deadletterqueue.context.headers.enable=false): dlq_peek can show counts and positions only"
	}
	return ""
}

func newRisk(code, sev string) Risk { return Risk{Code: code, Sev: sev, Text: riskText(code)} }

// Risks returns the findings for one connector, in the order of the design
// table. topicExists is the caller's last reading of the DLQ topic (nil =
// unknown, which never raises dlq_topic_missing).
//
// errors.tolerance=none (the default) is not a risk: one bad record then
// fails the task and the connector_down signal catches it.
func Risks(c ErrorsConfig, topicExists *bool) []Risk {
	var out []Risk
	hasDLQ := c.Topic != ""
	if c.IsSink && c.Tolerance == ToleranceAll && !hasDLQ && !c.TopicInvalid && !c.LogEnabled {
		out = append(out, newRisk(RiskSilentDropSink, SevSev2))
	}
	if !c.IsSink && c.Tolerance == ToleranceAll {
		out = append(out, newRisk(RiskSilentSkipSource, SevSev2))
	}
	if c.DebeziumSkip {
		out = append(out, newRisk(RiskDebeziumSkip, SevSev2))
	}
	if c.IsSink && hasDLQ && topicExists != nil && !*topicExists {
		out = append(out, newRisk(RiskDLQTopicMissing, SevSev3))
	}
	if c.IsSink && hasDLQ && !c.LogEnabled {
		out = append(out, newRisk(RiskDLQNoLog, SevSev3))
	}
	if c.LogEnabled && c.LogMessages {
		out = append(out, newRisk(RiskDLQLogMessages, SevSev3))
	}
	if c.IsSink && hasDLQ && !c.ContextHeaders {
		out = append(out, newRisk(RiskDLQNoHeaders, SevInfo))
	}
	return out
}

// WorstSev returns the most severe Sev among rs ("sev2" over "sev3" over
// "info"), or "" when rs is empty.
func WorstSev(rs []Risk) string {
	worst := ""
	for _, r := range rs {
		if sevRank(r.Sev) > sevRank(worst) {
			worst = r.Sev
		}
	}
	return worst
}

func sevRank(sev string) int {
	switch sev {
	case SevSev2:
		return 3
	case SevSev3:
		return 2
	case SevInfo:
		return 1
	}
	return 0
}

// Codes returns the Code of each risk, for evidence and facts that must carry
// codes only.
func Codes(rs []Risk) []string {
	out := make([]string, 0, len(rs))
	for _, r := range rs {
		out = append(out, r.Code)
	}
	return out
}
