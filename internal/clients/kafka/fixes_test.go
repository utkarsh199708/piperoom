package kafka

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"piperoom/internal/clients"
)

func TestKeyMatcher(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		decoded string
		want    bool
	}{
		{"raw equal", "k2", "k2", true},
		{"raw differs", "k2", "k3", false},
		{"multi-field same order", `{"a":1,"b":"x"}`, `{"a":1,"b":"x"}`, true},
		{"multi-field other order", `{"b":"x","a":1}`, `{"a":1,"b":"x"}`, true},
		{"multi-field spacing", `{ "b" : "x", "a" : 1 }`, `{"a":1,"b":"x"}`, true},
		{"multi-field value differs", `{"b":"y","a":1}`, `{"a":1,"b":"x"}`, false},
		{"big number keeps digits", `{"b":1,"a":9007199254740993}`, `{"a":9007199254740993,"b":1}`, true},
		{"big number off by one", `{"b":1,"a":9007199254740993}`, `{"a":9007199254740992,"b":1}`, false},
		{"one-field JSON vs bare decoded", `{"id":42}`, "42", true},
		{"one-field JSON string vs bare", `{"id":"abc"}`, "abc", true},
		{"bare vs bare", "42", "42", true},
		{"not JSON decoded", `{"a":1}`, "{a:1", false},
		{"trailing garbage is not JSON", `{"a":1} x`, `{"a":1}`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := newKeyMatcher(tt.key).matches(tt.decoded); got != tt.want {
				t.Fatalf("matches(%q, %q) = %v, want %v", tt.key, tt.decoded, got, tt.want)
			}
		})
	}
}

func TestSortRefsNewestLast(t *testing.T) {
	t0 := time.Unix(1000, 0)
	refs := []clients.RecordRef{
		{Partition: 1, Offset: 9, Timestamp: t0.Add(2 * time.Second)},
		{Partition: 0, Offset: 5, Timestamp: t0},
		{Partition: 1, Offset: 3, Timestamp: t0},
		{Partition: 0, Offset: 7, Timestamp: t0.Add(time.Second)},
	}
	sortRefs(refs)
	want := []int64{5, 3, 7, 9}
	for i, w := range want {
		if refs[i].Offset != w {
			t.Fatalf("order = %+v, want offsets %v", refs, want)
		}
	}
}

func lagEntry(topic string, p int32, commit, start, end int64, err error) kadm.GroupMemberLag {
	l := kadm.GroupMemberLag{
		Topic: topic, Partition: p,
		Commit: kadm.Offset{Topic: topic, Partition: p, At: commit},
		Start:  kadm.ListedOffset{Topic: topic, Partition: p, Offset: start},
		End:    kadm.ListedOffset{Topic: topic, Partition: p, Offset: end},
		Err:    err,
	}
	if err != nil {
		l.Lag = -1
		l.End.Err = err
		return l
	}
	l.Lag = end - commit
	if commit < 0 {
		l.Lag = end - start
	}
	if l.Lag < 0 {
		l.Lag = 0 // kadm rounds a commit past the end to 0
	}
	return l
}

func TestLagReportAnomalies(t *testing.T) {
	dg := kadm.DescribedGroupLag{
		Group: "g", State: "Empty",
		Lag: kadm.GroupLag{
			"live": {
				0: lagEntry("live", 0, 10, 0, 15, nil),   // fine
				1: lagEntry("live", 1, 50, 0, 20, nil),   // past the end
				2: lagEntry("live", 2, 3, 100, 120, nil), // below log start
				3: lagEntry("live", 3, -1, 0, 4, nil),    // never committed: fine
			},
			"gone": {
				0: lagEntry("gone", 0, 7, 0, 0, kerr.UnknownTopicOrPartition),
			},
			"denied": {
				0: lagEntry("denied", 0, 7, 0, 0, kerr.TopicAuthorizationFailed),
			},
		},
	}
	rep, err := lagReportFrom(dg)
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Partitions) != 4 {
		t.Fatalf("partitions = %+v, want the 4 readable ones", rep.Partitions)
	}
	kinds := map[string]string{}
	for _, a := range rep.Anomalies {
		kinds[a.Topic+"/"+string(rune('0'+a.Partition))] = a.Kind
	}
	want := map[string]string{
		"live/1":   AnomalyCommitBeyondEnd,
		"live/2":   AnomalyCommitBelowLogStart,
		"gone/0":   AnomalyTopicMissing,
		"denied/0": AnomalyOffsetsUnavailable,
	}
	if len(kinds) != len(want) {
		t.Fatalf("anomalies = %+v, want %v", rep.Anomalies, want)
	}
	for k, v := range want {
		if kinds[k] != v {
			t.Fatalf("anomaly %s = %q, want %q (all: %+v)", k, kinds[k], v, rep.Anomalies)
		}
	}
	for _, a := range rep.Anomalies {
		if a.Kind == AnomalyCommitBelowLogStart && (a.Start != 100 || a.Committed != 3) {
			t.Fatalf("below-start anomaly lacks offsets: %+v", a)
		}
		if a.Kind == AnomalyTopicMissing && a.Error == "" {
			t.Fatalf("topic-missing anomaly lacks the broker error: %+v", a)
		}
	}
}

func TestLagReportGroupNotFound(t *testing.T) {
	tests := []struct {
		name string
		dg   kadm.DescribedGroupLag
	}{
		{"describe GROUP_ID_NOT_FOUND", kadm.DescribedGroupLag{Group: "g", DescribeErr: kerr.GroupIDNotFound}},
		{"fetch GROUP_ID_NOT_FOUND", kadm.DescribedGroupLag{Group: "g", FetchErr: kerr.GroupIDNotFound}},
		{"dead and empty", kadm.DescribedGroupLag{Group: "g", State: "Dead"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := lagReportFrom(tt.dg)
			if !errors.Is(err, clients.ErrNotFound) {
				t.Fatalf("err = %v, want ErrNotFound", err)
			}
		})
	}
	_, err := lagReportFrom(kadm.DescribedGroupLag{Group: "g", DescribeErr: kerr.GroupAuthorizationFailed})
	if err == nil || errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("authorization failure must not read as not found: %v", err)
	}
}

// hungBroker accepts TCP connections and never answers, like a broker stuck
// in GC or behind a black-holing proxy.
func hungBroker(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, c)
			mu.Unlock()
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		mu.Lock()
		defer mu.Unlock()
		for _, c := range conns {
			c.Close()
		}
	})
	return ln.Addr().String()
}

// TestCallerDeadlineBoundsHungBroker: kgo bounds the connection handshake by
// its own 10 s, not the request context; every admin read must still return
// at the caller's deadline.
func TestCallerDeadlineBoundsHungBroker(t *testing.T) {
	c, err := New([]string{hungBroker(t)}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)

	calls := map[string]func(ctx context.Context) error{
		"DLQOffsets": func(ctx context.Context) error { _, err := c.DLQOffsets(ctx, "t"); return err },
		"PeekDLQ":    func(ctx context.Context) error { _, err := c.PeekDLQ(ctx, "t", 5); return err },
		"TopicOffsets": func(ctx context.Context) error {
			_, err := c.TopicOffsets(ctx, "t")
			return err
		},
		"FindByKey": func(ctx context.Context) error {
			_, err := c.FindByKey(ctx, "t", "k", time.Now().Add(-time.Hour), 10)
			return err
		},
		"GroupLag": func(ctx context.Context) error { _, err := c.GroupLag(ctx, "g"); return err },
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			start := time.Now()
			err := call(ctx)
			if d := time.Since(start); d > 2*time.Second {
				t.Fatalf("%s took %s with a 300ms deadline", name, d)
			}
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("err = %v, want DeadlineExceeded", err)
			}
		})
	}
}
