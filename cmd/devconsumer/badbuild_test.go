package main

import (
	"errors"
	"maps"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestParseSkipOps(t *testing.T) {
	tests := []struct {
		raw     string
		want    map[string]bool
		joined  string
		wantErr bool
	}{
		{"", map[string]bool{}, "", false},
		{"c", map[string]bool{"c": true}, "c", false},
		{" u , c ,", map[string]bool{"c": true, "u": true}, "c,u", false},
		{"d,r", map[string]bool{"d": true, "r": true}, "d,r", false},
		{"x", nil, "", true},
		{"c,create", nil, "", true},
	}
	for _, tt := range tests {
		got, err := parseSkipOps(tt.raw)
		if (err != nil) != tt.wantErr {
			t.Fatalf("parseSkipOps(%q) error = %v, wantErr %v", tt.raw, err, tt.wantErr)
		}
		if tt.wantErr {
			continue
		}
		if !maps.Equal(got, tt.want) {
			t.Errorf("parseSkipOps(%q) = %v, want %v", tt.raw, got, tt.want)
		}
		if j := joinOps(got); j != tt.joined {
			t.Errorf("joinOps(%v) = %q, want %q", got, j, tt.joined)
		}
	}
}

func TestCheckRejectStatus(t *testing.T) {
	gift := change{Op: "c", ID: 7, After: &orderRow{ID: 7, Status: "gift"}}
	placed := change{Op: "c", ID: 8, After: &orderRow{ID: 8, Status: "placed"}}
	del := change{Op: "d", ID: 9}
	strict := options{rejectStatus: "gift", dlqTopic: "dlq"}
	tests := []struct {
		name   string
		c      change
		o      options
		reject bool
	}{
		{"rejected status", gift, strict, true},
		{"other status", placed, strict, false},
		{"delete has no row", del, strict, false},
		{"good build", gift, options{}, false},
	}
	for _, tt := range tests {
		err := check(tt.c, tt.o)
		var rj *rejection
		if got := errors.As(err, &rj); got != tt.reject {
			t.Errorf("%s: check = %v, reject %v", tt.name, err, tt.reject)
		}
		if tt.reject && (rj.id != tt.c.ID || rj.reason != `unknown status "gift"`) {
			t.Errorf("%s: rejection = %+v", tt.name, rj)
		}
	}
}

func TestDLQRecordKeepsDataAndAddsContext(t *testing.T) {
	in := &kgo.Record{Topic: "pg.public.orders", Partition: 2, Offset: 41, Key: []byte("k"), Value: []byte("v"),
		Headers: []kgo.RecordHeader{{Key: "trace-id", Value: []byte("t")}}}
	out := dlqRecord(in, "devctl.order-sync.dlq", &rejection{id: 7, reason: `unknown status "gift"`})
	if out.Topic != "devctl.order-sync.dlq" || string(out.Key) != "k" || string(out.Value) != "v" {
		t.Fatalf("record = %s %q %q", out.Topic, out.Key, out.Value)
	}
	got := map[string]string{}
	for _, h := range out.Headers {
		got[h.Key] = string(h.Value)
	}
	want := map[string]string{
		"trace-id":                              "t",
		"__connect.errors.topic":                "pg.public.orders",
		"__connect.errors.partition":            "2",
		"__connect.errors.offset":               "41",
		"__connect.errors.exception.class.name": "devconsumer.ValidationError",
		"__connect.errors.exception.message":    `unknown status "gift"`,
	}
	if !maps.Equal(got, want) {
		t.Errorf("headers = %v, want %v", got, want)
	}
	if len(in.Headers) != 1 {
		t.Errorf("dlqRecord changed the input record's headers: %v", in.Headers)
	}
}
