package strimzi

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// The templates of k8s_apply (design §6). Each renders the spec (or data) of
// one chart-created object from typed params and names the object itself: a
// name is never a param. renderTemplate validates the render with the kind's
// allow-list before it returns it.

// template is one renderable object.
type template struct {
	kind   string
	render func(d *Driver, kafka string, raw json.RawMessage) (rendered, error)
}

// rendered is what k8s_apply writes: the object name and the JSON patch path
// it replaces (/spec for a CR, /data for the ConfigMap) with its value.
type rendered struct {
	kind, name, path string
	value            map[string]any
	pause            bool // a Strimzi CR shell: unpause after the read-back
	// template and topicPrefix are what a KafkaUser render was made for: its
	// ACL table is checked against them.
	template, topicPrefix string
}

var templates = map[string]template{
	"canary-topic":    {kind: "KafkaTopic", render: renderCanaryTopic},
	"connect-user":    {kind: "KafkaUser", render: renderUser("connect-user")},
	"read-user":       {kind: "KafkaUser", render: renderUser("read-user")},
	"connect-logging": {kind: "ConfigMap", render: renderLogging},
}

// templateNames returns the registered template names, sorted.
func templateNames() []string {
	out := make([]string, 0, len(templates))
	for n := range templates {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

var (
	topicPrefixRE = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9._-]{0,63}$`)
	identRE       = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,62}$`)
)

func (d *Driver) renderTemplate(kind, name string, kafka, ns string, raw json.RawMessage) (rendered, error) {
	t, ok := templates[name]
	if !ok {
		return rendered{}, fmt.Errorf("%w: template %q (one of %s)", ErrKind, clipText(name, 32), strings.Join(templateNames(), ", "))
	}
	if t.kind != kind {
		return rendered{}, fmt.Errorf("%w: template %s renders a %s, not a %s", ErrKind, name, t.kind, clipText(kind, 32))
	}
	r, err := t.render(d, kafka, raw)
	if err != nil {
		return rendered{}, err
	}
	if err := d.validate(r, kafka, ns); err != nil {
		return rendered{}, err
	}
	return r, nil
}

// validate runs the kind's allow-list over a render (also over a hand-made
// object in the tests).
func (d *Driver) validate(r rendered, kafka, ns string) error {
	switch r.kind {
	case "KafkaConnect":
		return d.ValidateConnect(r.value, kafka)
	case "KafkaConnector":
		return d.ValidateConnector(r.value, ns)
	case "KafkaUser":
		return d.ValidateUser(r.value, r.template, r.topicPrefix)
	case "KafkaTopic":
		name, _ := r.value["topicName"].(string)
		prefix, _, _ := strings.Cut(name, ".")
		return d.ValidateTopic(r.value, prefix)
	case "ConfigMap":
		return keysOnly("data", r.value, "log4j.properties")
	}
	return fmt.Errorf("%w: kind %q", ErrKind, r.kind)
}

// ---- canary-topic ----

type topicParams struct {
	TopicPrefix string `json:"topic_prefix"`
	Schema      string `json:"schema"`
	Table       string `json:"table"`
	Replicas    int    `json:"replicas,omitempty"`
}

func renderCanaryTopic(d *Driver, _ string, raw json.RawMessage) (rendered, error) {
	var p topicParams
	if err := decode(raw, &p); err != nil {
		return rendered{}, err
	}
	if !topicPrefixRE.MatchString(p.TopicPrefix) || !identRE.MatchString(p.Schema) || !identRE.MatchString(p.Table) {
		return rendered{}, fmt.Errorf("%w: topic_prefix, schema and table must be identifiers", ErrParams)
	}
	if p.Replicas == 0 {
		p.Replicas = 1
	}
	if p.Replicas < 1 || p.Replicas > 3 {
		return rendered{}, fmt.Errorf("%w: replicas must be 1 to 3", ErrParams)
	}
	return rendered{kind: "KafkaTopic", name: d.canaryTopicName(), path: "/spec", pause: true, value: map[string]any{
		"topicName":  p.TopicPrefix + "." + p.Schema + "." + p.Table,
		"partitions": 1,
		"replicas":   p.Replicas,
		"config":     map[string]any{"cleanup.policy": "delete", "retention.ms": "604800000"},
	}}, nil
}

// ---- connect-user, read-user ----

type userParams struct {
	TopicPrefix string `json:"topic_prefix"`
	Auth        string `json:"auth"`
}

func renderUser(tmpl string) func(*Driver, string, json.RawMessage) (rendered, error) {
	return func(d *Driver, _ string, raw json.RawMessage) (rendered, error) {
		var p userParams
		if err := decode(raw, &p); err != nil {
			return rendered{}, err
		}
		if !topicPrefixRE.MatchString(p.TopicPrefix) {
			return rendered{}, fmt.Errorf("%w: topic_prefix is not a topic prefix", ErrParams)
		}
		if p.Auth != "tls" && p.Auth != "scram-sha-512" {
			return rendered{}, fmt.Errorf("%w: auth must be tls or scram-sha-512 (whichever the Kafka listener uses)", ErrParams)
		}
		acls := d.userACLs(tmpl, p.TopicPrefix)
		list := make([]any, len(acls))
		for i, a := range acls {
			list[i] = a
		}
		name := d.connectName()
		if tmpl == "read-user" {
			name = d.readUserName()
		}
		return rendered{kind: "KafkaUser", name: name, path: "/spec", pause: true, template: tmpl, topicPrefix: p.TopicPrefix, value: map[string]any{
			"authentication": map[string]any{"type": p.Auth},
			"authorization":  map[string]any{"type": "simple", "acls": list},
		}}, nil
	}
}

// ---- connect-logging ----

type loggingParams struct {
	Levels map[string]string `json:"levels"`
}

var logLevels = map[string]bool{"ERROR": true, "WARN": true, "INFO": true, "DEBUG": true}
var loggerNames = map[string]string{"root": "log4j.rootLogger", "io.debezium": "log4j.logger.io.debezium", "org.apache.kafka.connect": "log4j.logger.org.apache.kafka.connect"}

func renderLogging(d *Driver, _ string, raw json.RawMessage) (rendered, error) {
	var p loggingParams
	if err := decode(raw, &p); err != nil {
		return rendered{}, err
	}
	keys := make([]string, 0, len(p.Levels))
	for k, v := range p.Levels {
		if _, ok := loggerNames[k]; !ok {
			return rendered{}, fmt.Errorf("%w: logger %q (root, io.debezium or org.apache.kafka.connect)", ErrParams, clipText(k, 64))
		}
		if !logLevels[v] {
			return rendered{}, fmt.Errorf("%w: level %q", ErrParams, clipText(v, 16))
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString("log4j.appender.CONSOLE=org.apache.log4j.ConsoleAppender\nlog4j.appender.CONSOLE.layout=org.apache.log4j.PatternLayout\nlog4j.appender.CONSOLE.layout.ConversionPattern=%d{ISO8601} %p %m (%c) [%t]%n\n")
	if _, ok := p.Levels["root"]; !ok {
		b.WriteString("log4j.rootLogger=INFO, CONSOLE\n")
	}
	for _, k := range keys {
		v := p.Levels[k]
		if k == "root" {
			v += ", CONSOLE"
		}
		b.WriteString(loggerNames[k] + "=" + v + "\n")
	}
	return rendered{kind: "ConfigMap", name: d.loggingName(), path: "/data", value: map[string]any{"log4j.properties": b.String()}}, nil
}
