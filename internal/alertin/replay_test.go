package alertin

import (
	"errors"
	"net/http"
	"strconv"
	"testing"
	"time"
)

func TestPreVerify(t *testing.T) {
	ts := strconv.FormatInt(testNow.Unix(), 10)
	oldTS := strconv.FormatInt(testNow.Add(-time.Hour).Unix(), 10)
	goodMAC := hexMAC("x", "y") // right shape; PreVerify cannot check the value
	gfSig, gfTS := DefaultGrafanaSignatureHeader, DefaultGrafanaTimestampHeader

	tests := []struct {
		name string
		src  Source
		h    http.Header
		ok   bool
	}{
		{"am good token", Alertmanager, hdr("Authorization", "Bearer am-token"), true},
		{"am wrong token", Alertmanager, hdr("Authorization", "Bearer nope"), false},
		{"am missing", Alertmanager, hdr(), false},
		{"dd good token", Datadog, hdr(DefaultDatadogTokenHeader, "dd-token"), true},
		{"dd wrong token", Datadog, hdr(DefaultDatadogTokenHeader, "nope"), false},
		{"grafana shaped", Grafana, hdr(gfSig, goodMAC, gfTS, ts), true},
		{"grafana missing sig", Grafana, hdr(gfTS, ts), false},
		{"grafana garbage sig", Grafana, hdr(gfSig, "not-hex", gfTS, ts), false},
		{"grafana short sig", Grafana, hdr(gfSig, "abcd", gfTS, ts), false},
		{"grafana missing ts", Grafana, hdr(gfSig, goodMAC), false},
		{"grafana old ts", Grafana, hdr(gfSig, goodMAC, gfTS, oldTS), false},
		{"pd shaped", PagerDuty, hdr(PagerDutySignatureHeader, "v1="+goodMAC), true},
		{"pd missing", PagerDuty, hdr(), false},
		{"pd garbage", PagerDuty, hdr(PagerDutySignatureHeader, "v1=zz,v0="+goodMAC), false},
		{"generic shaped", Generic, hdr(GenericSignatureHeader, "t="+ts+",v1="+goodMAC), true},
		{"generic missing", Generic, hdr(), false},
		{"generic no v1", Generic, hdr(GenericSignatureHeader, "t="+ts), false},
		{"generic garbage v1", Generic, hdr(GenericSignatureHeader, "t="+ts+",v1=garbage"), false},
		{"generic old ts", Generic, hdr(GenericSignatureHeader, "t="+oldTS+",v1="+goodMAC), false},
		{"unknown source", Source("opsgenie"), hdr(), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := testConfig().PreVerify(tt.src, tt.h)
			if (err == nil) != tt.ok {
				t.Fatalf("PreVerify = %v, want ok=%v", err, tt.ok)
			}
			if err != nil && !errors.Is(err, ErrUnauthenticated) {
				t.Fatalf("error %v does not wrap ErrUnauthenticated", err)
			}
		})
	}
	disabled := testConfig()
	disabled.GenericSecret = ""
	if err := disabled.PreVerify(Generic, hdr(GenericSignatureHeader, "t="+ts+",v1="+goodMAC)); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("disabled source: %v", err)
	}
}

func TestDeliveryKey(t *testing.T) {
	c := testConfig()
	body := []byte(`{"a":1}`)
	ts := strconv.FormatInt(testNow.Unix(), 10)
	sig := SignGeneric("generic-secret", testNow, body)
	k := c.DeliveryKey(Generic, hdr(GenericSignatureHeader, sig), body)

	// A padded or reordered header that still verifies is the same delivery.
	padded := sig + ",v1=" + hexMAC("old-secret", "x") + ",x=1"
	if err := c.Verify(Generic, hdr(GenericSignatureHeader, padded), body); err != nil {
		t.Fatalf("padded header should verify: %v", err)
	}
	if got := c.DeliveryKey(Generic, hdr(GenericSignatureHeader, padded), body); got != k {
		t.Fatal("padded signature header changed the delivery key")
	}
	// A new signed time or a different body is a different delivery.
	later := SignGeneric("generic-secret", testNow.Add(time.Second), body)
	if c.DeliveryKey(Generic, hdr(GenericSignatureHeader, later), body) == k {
		t.Fatal("new timestamp kept the delivery key")
	}
	if c.DeliveryKey(Generic, hdr(GenericSignatureHeader, sig), []byte(`{"a":2}`)) == k {
		t.Fatal("new body kept the delivery key")
	}
	// The same bytes from another source are another delivery.
	if c.DeliveryKey(Alertmanager, nil, body) == c.DeliveryKey(Datadog, nil, body) {
		t.Fatal("sources share keys")
	}
	gf := c.DeliveryKey(Grafana, hdr(DefaultGrafanaTimestampHeader, ts), body)
	if c.DeliveryKey(Grafana, hdr(DefaultGrafanaTimestampHeader, " "+ts+" "), body) != gf {
		t.Fatal("whitespace around the grafana timestamp changed the key")
	}
}

func TestReplayWindow(t *testing.T) {
	c := testConfig()
	if got := c.ReplayWindow(Generic); got != 2*DefaultMaxSkew {
		t.Fatalf("generic window %s", got)
	}
	if got := c.ReplayWindow(PagerDuty); got != DefaultMaxEventAge+DefaultMaxSkew {
		t.Fatalf("pagerduty window %s", got)
	}
	if got := c.ReplayWindow(Alertmanager); got != DefaultBearerDedupWindow {
		t.Fatalf("alertmanager window %s", got)
	}
}

func TestReplayCache(t *testing.T) {
	rc := NewReplayCache(3)
	now := testNow
	if rc.Seen("a", time.Minute, now) {
		t.Fatal("first a seen")
	}
	if !rc.Seen("a", time.Minute, now.Add(59*time.Second)) {
		t.Fatal("a within ttl not seen")
	}
	if rc.Seen("a", time.Minute, now.Add(time.Minute)) {
		t.Fatal("a after ttl seen")
	}
	rc.Forget("a")
	if rc.Seen("a", time.Minute, now) {
		t.Fatal("forgotten a seen")
	}
	// The cap holds: the oldest key goes first.
	rc.Seen("b", time.Hour, now)
	rc.Seen("c", time.Hour, now)
	rc.Seen("d", time.Hour, now)
	if n := rc.Len(); n != 3 {
		t.Fatalf("len %d, want 3", n)
	}
	if !rc.Seen("d", time.Hour, now) || !rc.Seen("c", time.Hour, now) {
		t.Fatal("recent keys evicted")
	}
	// Expired entries at the front are dropped on insert.
	rc2 := NewReplayCache(10)
	for i := 0; i < 5; i++ {
		rc2.Seen(strconv.Itoa(i), time.Second, now)
	}
	rc2.Seen("late", time.Second, now.Add(time.Hour))
	if n := rc2.Len(); n != 1 {
		t.Fatalf("len %d after expiry, want 1", n)
	}
}
