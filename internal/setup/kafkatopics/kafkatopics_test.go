package kafkatopics

import (
	"encoding/json"
	"os"
	"testing"
)

// Case is one row of testdata/cases.json, which the server's scope test
// (internal/setup/actions) and the runner's allow-list test
// (internal/setup/targets/strimzi) run too.
type Case struct {
	Name   string `json:"name"`
	Prefix string `json:"prefix"`
	Key    string `json:"key"`
	Topic  string `json:"topic"`
	Topics string `json:"topics"`
	OK     bool   `json:"ok"`
}

func TestCases(t *testing.T) {
	b, err := os.ReadFile("testdata/cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []Case
	if err := json.Unmarshal(b, &cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		var err error
		if c.Key == "errors.deadletterqueue.topic.name" {
			err = DeadLetter(c.Prefix, c.Topic, c.Topics)
		} else {
			err = SchemaHistory(c.Prefix, c.Topic)
		}
		if (err == nil) != c.OK {
			t.Errorf("%s: err = %v, want ok %v", c.Name, err, c.OK)
		}
	}
	if DeadLetter("", "pg.dlq", "") == nil || SchemaHistory("", "pg.h") == nil {
		t.Fatal("no prefix accepted")
	}
}
