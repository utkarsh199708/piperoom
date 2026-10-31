package strimzi

import (
	"encoding/json"
	"os"
	"testing"
)

// topicCase is one row of the shared kafkatopics/testdata/cases.json, which
// the server's scope test runs too: the runner and the server keep one rule
// for a connector's dead letter queue and schema history topics.
type topicCase struct {
	Name   string `json:"name"`
	Prefix string `json:"prefix"`
	Key    string `json:"key"`
	Topic  string `json:"topic"`
	Topics string `json:"topics"`
	OK     bool   `json:"ok"`
}

func TestTopicRulesMatchTheServer(t *testing.T) {
	b, err := os.ReadFile("../../kafkatopics/testdata/cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []topicCase
	if err := json.Unmarshal(b, &cases); err != nil {
		t.Fatal(err)
	}
	h := newHarness(t, "", nil)
	for _, c := range cases {
		plugin := "mysql"
		cfg := map[string]any{"topic.prefix": c.Prefix, c.Key: c.Topic, "database.ssl.mode": "verify_identity",
			"schema.history.internal.producer.security.protocol": "SSL", "schema.history.internal.consumer.security.protocol": "SSL"}
		if c.Key == "errors.deadletterqueue.topic.name" {
			plugin = "jdbc"
			cfg = map[string]any{c.Key: c.Topic, "connection.url": "jdbc:postgresql://pg:5432/w?sslmode=verify-full"}
			if c.Topics != "" {
				cfg["topics"] = c.Topics
			}
		}
		err := h.d.ValidateConnector(connectorSpec(plugin, cfg), testNS, testKafka, c.Prefix)
		if (err == nil) != c.OK {
			t.Errorf("%s: err = %v, want ok %v", c.Name, err, c.OK)
		}
	}
}
