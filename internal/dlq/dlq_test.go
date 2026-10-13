package dlq

import (
	"reflect"
	"strings"
	"testing"
)

func boolp(b bool) *bool { return &b }

const (
	sinkClass   = "io.confluent.connect.jdbc.JdbcSinkConnector"
	sourceClass = "io.debezium.connector.postgresql.PostgresConnector"
)

func codes(rs []Risk) []string {
	out := []string{}
	for _, r := range rs {
		out = append(out, r.Code)
	}
	return out
}

func TestParse(t *testing.T) {
	tests := []struct {
		name string
		cfg  map[string]string
		want ErrorsConfig
	}{
		{"nil config is Connect's defaults", nil, ErrorsConfig{Tolerance: "none"}},
		{"empty config", map[string]string{}, ErrorsConfig{Tolerance: "none"}},
		{"sink class", map[string]string{"connector.class": sinkClass}, ErrorsConfig{IsSink: true, Tolerance: "none"}},
		{"source class", map[string]string{"connector.class": sourceClass}, ErrorsConfig{Tolerance: "none"}},
		{"sink class, other case and spaces", map[string]string{"connector.class": "  com.x.S3SINKCONNECTOR "}, ErrorsConfig{IsSink: true, Tolerance: "none"}},
		{"class that only contains Sink", map[string]string{"connector.class": "com.x.SinkConnectorFactory"}, ErrorsConfig{Tolerance: "none"}},
		{"tolerance ALL", map[string]string{"errors.tolerance": "ALL"}, ErrorsConfig{Tolerance: "all"}},
		{"tolerance with spaces", map[string]string{"errors.tolerance": " all "}, ErrorsConfig{Tolerance: "all"}},
		{"tolerance garbage reads as none", map[string]string{"errors.tolerance": "SENTINEL"}, ErrorsConfig{Tolerance: "none"}},
		{"topic", map[string]string{"errors.deadletterqueue.topic.name": " orders-dlq "}, ErrorsConfig{Tolerance: "none", Topic: "orders-dlq"}},
		{"topic not a Kafka name", map[string]string{"errors.deadletterqueue.topic.name": "a b\nc"}, ErrorsConfig{Tolerance: "none", TopicInvalid: true}},
		{"topic dot-dot", map[string]string{"errors.deadletterqueue.topic.name": ".."}, ErrorsConfig{Tolerance: "none", TopicInvalid: true}},
		{"topic too long", map[string]string{"errors.deadletterqueue.topic.name": strings.Repeat("a", 250)}, ErrorsConfig{Tolerance: "none", TopicInvalid: true}},
		{"topic of 249 characters", map[string]string{"errors.deadletterqueue.topic.name": strings.Repeat("a", 249)}, ErrorsConfig{Tolerance: "none", Topic: strings.Repeat("a", 249)}},
		{"log flags, mixed case", map[string]string{
			"errors.log.enable": "True", "errors.log.include.messages": "TRUE",
			"errors.deadletterqueue.context.headers.enable": " true",
		}, ErrorsConfig{Tolerance: "none", LogEnabled: true, LogMessages: true, ContextHeaders: true}},
		{"flags false or garbage", map[string]string{
			"errors.log.enable": "false", "errors.log.include.messages": "yes",
			"errors.deadletterqueue.context.headers.enable": "1",
		}, ErrorsConfig{Tolerance: "none"}},
		{"debezium skip", map[string]string{"event.processing.failure.handling.mode": "SKIP"}, ErrorsConfig{Tolerance: "none", DebeziumSkip: true}},
		{"debezium fail", map[string]string{"event.processing.failure.handling.mode": "fail"}, ErrorsConfig{Tolerance: "none"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := Parse(tc.cfg); got != tc.want {
				t.Fatalf("Parse = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestRisks(t *testing.T) {
	tests := []struct {
		name   string
		cfg    map[string]string
		exists *bool
		want   []string
	}{
		{"default sink: tolerance none is no risk", map[string]string{"connector.class": sinkClass}, nil, []string{}},
		{"default source", map[string]string{"connector.class": sourceClass}, nil, []string{}},
		{"no config at all", nil, nil, []string{}},
		{"sink all, no DLQ, no log", map[string]string{"connector.class": sinkClass, "errors.tolerance": "all"}, nil, []string{"silent_drop_sink"}},
		{"sink ALL (mixed case)", map[string]string{"connector.class": sinkClass, "errors.tolerance": "ALL"}, nil, []string{"silent_drop_sink"}},
		{"sink all, no DLQ, log on: nothing is silent", map[string]string{"connector.class": sinkClass, "errors.tolerance": "all", "errors.log.enable": "true"}, nil, []string{}},
		{"sink all, DLQ, no log, no headers", map[string]string{"connector.class": sinkClass, "errors.tolerance": "all", "errors.deadletterqueue.topic.name": "orders-dlq"}, nil, []string{"dlq_no_log", "dlq_no_headers"}},
		{"sink all, DLQ with log and headers", map[string]string{
			"connector.class": sinkClass, "errors.tolerance": "all", "errors.deadletterqueue.topic.name": "orders-dlq",
			"errors.log.enable": "true", "errors.deadletterqueue.context.headers.enable": "true",
		}, boolp(true), []string{}},
		{"sink DLQ topic does not exist", map[string]string{
			"connector.class": sinkClass, "errors.deadletterqueue.topic.name": "orders-dlq",
			"errors.log.enable": "true", "errors.deadletterqueue.context.headers.enable": "true",
		}, boolp(false), []string{"dlq_topic_missing"}},
		{"existence unknown never raises dlq_topic_missing", map[string]string{
			"connector.class": sinkClass, "errors.deadletterqueue.topic.name": "orders-dlq",
			"errors.log.enable": "true", "errors.deadletterqueue.context.headers.enable": "true",
		}, nil, []string{}},
		{"topic missing but no DLQ configured", map[string]string{"connector.class": sinkClass}, boolp(false), []string{}},
		{"invalid DLQ topic is not reported as no DLQ", map[string]string{"connector.class": sinkClass, "errors.tolerance": "all", "errors.deadletterqueue.topic.name": "a b"}, nil, []string{}},
		{"log with include.messages", map[string]string{"connector.class": sinkClass, "errors.log.enable": "true", "errors.log.include.messages": "true"}, nil, []string{"dlq_log_messages"}},
		{"include.messages without log is inert", map[string]string{"connector.class": sinkClass, "errors.log.include.messages": "true"}, nil, []string{}},
		{"source with tolerance all", map[string]string{"connector.class": sourceClass, "errors.tolerance": "all"}, nil, []string{"silent_skip_source"}},
		{"debezium skip", map[string]string{"connector.class": sourceClass, "event.processing.failure.handling.mode": "skip"}, nil, []string{"debezium_skip"}},
		{"source all and skip", map[string]string{"connector.class": sourceClass, "errors.tolerance": "all", "event.processing.failure.handling.mode": "skip"}, nil, []string{"silent_skip_source", "debezium_skip"}},
		{"DLQ on a source is no sink risk", map[string]string{"connector.class": sourceClass, "errors.deadletterqueue.topic.name": "x-dlq"}, boolp(false), []string{}},
		{"everything at once", map[string]string{
			"connector.class": sinkClass, "errors.tolerance": "all", "errors.deadletterqueue.topic.name": "orders-dlq",
			"errors.log.enable": "true", "errors.log.include.messages": "true",
		}, boolp(false), []string{"dlq_topic_missing", "dlq_log_messages", "dlq_no_headers"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := codes(Risks(Parse(tc.cfg), tc.exists))
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("risk codes = %v, want %v", got, tc.want)
			}
		})
	}
}

// Every code has a fixed severity and a non-empty text that names keys, not values.
func TestRiskSeverityAndText(t *testing.T) {
	wantSev := map[string]string{
		RiskSilentDropSink: SevSev2, RiskSilentSkipSource: SevSev2, RiskDebeziumSkip: SevSev2,
		RiskDLQTopicMissing: SevSev3, RiskDLQNoLog: SevSev3, RiskDLQLogMessages: SevSev3,
		RiskDLQNoHeaders: SevInfo,
	}
	seen := map[string]bool{}
	for _, c := range riskCases() {
		for _, r := range Risks(Parse(c.cfg), c.exists) {
			seen[r.Code] = true
			if r.Sev != wantSev[r.Code] {
				t.Errorf("%s: sev = %q, want %q", r.Code, r.Sev, wantSev[r.Code])
			}
			if r.Text == "" || strings.Contains(r.Text, "\n") {
				t.Errorf("%s: text %q must be one non-empty line", r.Code, r.Text)
			}
			if r.Text != riskText(r.Code) {
				t.Errorf("%s: text is not the fixed text of the code", r.Code)
			}
		}
	}
	for code := range wantSev {
		if !seen[code] {
			t.Errorf("no test config raises %s", code)
		}
	}
}

type riskCase struct {
	cfg    map[string]string
	exists *bool
}

// riskCases returns configs that together raise every risk code.
func riskCases() []riskCase {
	return []riskCase{
		{map[string]string{"connector.class": sinkClass, "errors.tolerance": "all"}, nil},
		{map[string]string{"connector.class": sourceClass, "errors.tolerance": "all", "event.processing.failure.handling.mode": "skip"}, nil},
		{map[string]string{"connector.class": sinkClass, "errors.deadletterqueue.topic.name": "orders-dlq", "errors.log.enable": "true", "errors.log.include.messages": "true"}, boolp(false)},
		{map[string]string{"connector.class": sinkClass, "errors.deadletterqueue.topic.name": "orders-dlq"}, boolp(true)},
	}
}

// The sentinel test: Risk.Text may name config keys, but no config VALUE may
// reach it. Every value in the config is a sentinel that Parse still accepts
// as meaningful where it has to be, and Text must hold none of them.
func TestRiskTextNeverHoldsConfigValues(t *testing.T) {
	const (
		topicSentinel = "SENTINEL-TOPIC-7f3a"
		classSentinel = "SENTINEL.class.JdbcSinkConnector"
		extra         = "SENTINEL-EXTRA-VALUE"
	)
	cfg := map[string]string{
		"connector.class":                   classSentinel,
		"errors.tolerance":                  "all",
		"errors.deadletterqueue.topic.name": topicSentinel,
		"errors.log.enable":                 "true",
		"errors.log.include.messages":       "true",
		"connection.password":               extra + "-password",
		"connection.url":                    "jdbc:postgresql://" + extra + ":5432/orders",
		"topics":                            extra + "-topics",
		"name":                              extra + "-name",
	}
	sentinels := []string{"SENTINEL", extra, topicSentinel, classSentinel, "jdbc:postgresql"}

	// Sink variants that raise every sink code, and a source variant.
	variants := []struct {
		name string
		edit func(map[string]string)
		ex   *bool
	}{
		{"sink, DLQ missing, log messages", func(map[string]string) {}, boolp(false)},
		{"sink, no DLQ, no log", func(m map[string]string) {
			delete(m, "errors.deadletterqueue.topic.name")
			delete(m, "errors.log.enable")
		}, nil},
		{"sink, DLQ, no log, no headers", func(m map[string]string) { delete(m, "errors.log.enable") }, boolp(true)},
		{"source all", func(m map[string]string) {
			m["connector.class"] = "SENTINEL.class.PostgresConnector"
			m["event.processing.failure.handling.mode"] = "skip"
		}, nil},
	}
	raised := map[string]bool{}
	for _, v := range variants {
		m := map[string]string{}
		for k, val := range cfg {
			m[k] = val
		}
		v.edit(m)
		// "SENTINEL.class.JdbcSinkConnector" ends in SinkConnector: a sink.
		for _, r := range Risks(Parse(m), v.ex) {
			raised[r.Code] = true
			for _, s := range sentinels {
				if strings.Contains(r.Text, s) {
					t.Errorf("%s / %s: Text %q contains config value %q", v.name, r.Code, r.Text, s)
				}
			}
		}
	}
	for _, code := range []string{RiskSilentDropSink, RiskSilentSkipSource, RiskDebeziumSkip, RiskDLQTopicMissing, RiskDLQNoLog, RiskDLQLogMessages, RiskDLQNoHeaders} {
		if !raised[code] {
			t.Errorf("sentinel test never raised %s", code)
		}
	}
	// The same holds for every code, straight from the function.
	for _, code := range []string{RiskSilentDropSink, RiskSilentSkipSource, RiskDebeziumSkip, RiskDLQTopicMissing, RiskDLQNoLog, RiskDLQLogMessages, RiskDLQNoHeaders} {
		for _, s := range sentinels {
			if strings.Contains(riskText(code), s) {
				t.Errorf("riskText(%s) contains %q", code, s)
			}
		}
	}
	if riskText("unknown") != "" {
		t.Error("an unknown code has no text")
	}
}

func TestWorstSevAndCodes(t *testing.T) {
	if got := WorstSev(nil); got != "" {
		t.Errorf("WorstSev(nil) = %q", got)
	}
	rs := []Risk{{Code: "a", Sev: SevInfo}, {Code: "b", Sev: SevSev3}}
	if got := WorstSev(rs); got != SevSev3 {
		t.Errorf("WorstSev = %q, want sev3", got)
	}
	rs = append(rs, Risk{Code: "c", Sev: SevSev2})
	if got := WorstSev(rs); got != SevSev2 {
		t.Errorf("WorstSev = %q, want sev2", got)
	}
	if got := Codes(rs); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		t.Errorf("Codes = %v", got)
	}
	if got := Codes(nil); got == nil || len(got) != 0 {
		t.Errorf("Codes(nil) = %#v, want empty non-nil", got)
	}
}
