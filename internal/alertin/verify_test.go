package alertin

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

var testNow = time.Date(2026, 10, 11, 11, 0, 0, 0, time.UTC)

func testConfig() Config {
	return Config{
		AlertmanagerToken: "am-token",
		GrafanaSecret:     "grafana-secret",
		PagerDutySecret:   "pd-secret",
		DatadogToken:      "dd-token",
		GenericSecret:     "generic-secret",
		PagerDutyServices: map[string]string{"PF9KMXH": "orders"},
		Now:               func() time.Time { return testNow },
	}
}

func hexMAC(secret string, parts ...string) string {
	m := hmac.New(sha256.New, []byte(secret))
	for _, p := range parts {
		m.Write([]byte(p))
	}
	return hex.EncodeToString(m.Sum(nil))
}

func hdr(kv ...string) http.Header {
	h := http.Header{}
	for i := 0; i+1 < len(kv); i += 2 {
		h.Set(kv[i], kv[i+1])
	}
	return h
}

func TestVerify(t *testing.T) {
	body := `{"x":1}`
	unix := func(d time.Duration) string { return strconv.FormatInt(testNow.Add(d).Unix(), 10) }
	ts, oldTS, futureTS := unix(0), unix(-10*time.Minute), unix(10*time.Minute)
	msTS := strconv.FormatInt(testNow.UnixMilli(), 10)
	goodPD := "v1=" + hexMAC("pd-secret", body)
	goodGF := hexMAC("grafana-secret", ts, ":", body)
	gfSig, gfTS := DefaultGrafanaSignatureHeader, DefaultGrafanaTimestampHeader
	goodGen := SignGeneric("generic-secret", testNow, []byte(body))

	tests := []struct {
		name   string
		src    Source
		header http.Header
		body   string
		ok     bool
	}{
		{"am ok", Alertmanager, hdr("Authorization", "Bearer am-token"), body, true},
		{"am scheme case", Alertmanager, hdr("Authorization", "bearer am-token"), body, true},
		{"am wrong token", Alertmanager, hdr("Authorization", "Bearer nope"), body, false},
		{"am token prefix", Alertmanager, hdr("Authorization", "Bearer am-toke"), body, false},
		{"am basic scheme", Alertmanager, hdr("Authorization", "Basic am-token"), body, false},
		{"am missing", Alertmanager, hdr(), body, false},

		{"grafana ok", Grafana, hdr(gfSig, goodGF, gfTS, ts), body, true},
		{"grafana ms timestamp", Grafana, hdr(gfSig, hexMAC("grafana-secret", msTS, ":", body), gfTS, msTS), body, true},
		{"grafana body changed", Grafana, hdr(gfSig, goodGF, gfTS, ts), `{"x":2}`, false},
		{"grafana timestamp changed", Grafana, hdr(gfSig, goodGF, gfTS, unix(-time.Second)), body, false},
		{"grafana stale", Grafana, hdr(gfSig, hexMAC("grafana-secret", oldTS, ":", body), gfTS, oldTS), body, false},
		{"grafana future", Grafana, hdr(gfSig, hexMAC("grafana-secret", futureTS, ":", body), gfTS, futureTS), body, false},
		{"grafana body-only signature", Grafana, hdr(gfSig, hexMAC("grafana-secret", body)), body, false},
		{"grafana bad timestamp", Grafana, hdr(gfSig, goodGF, gfTS, "soon"), body, false},
		{"grafana not hex", Grafana, hdr(gfSig, "zz", gfTS, ts), body, false},
		{"grafana wrong secret", Grafana, hdr(gfSig, hexMAC("other", ts, ":", body), gfTS, ts), body, false},
		{"grafana missing sig", Grafana, hdr(gfTS, ts), body, false},

		{"pd ok", PagerDuty, hdr(PagerDutySignatureHeader, goodPD), body, true},
		{"pd rotation second matches", PagerDuty, hdr(PagerDutySignatureHeader, "v1="+hexMAC("old", body)+", "+goodPD), body, true},
		{"pd wrong version", PagerDuty, hdr(PagerDutySignatureHeader, "v0="+hexMAC("pd-secret", body)), body, false},
		{"pd body changed", PagerDuty, hdr(PagerDutySignatureHeader, goodPD), `{"x":2}`, false},
		{"pd garbage", PagerDuty, hdr(PagerDutySignatureHeader, "v1=xyz,junk"), body, false},
		{"pd missing", PagerDuty, hdr(), body, false},

		{"dd ok", Datadog, hdr(DefaultDatadogTokenHeader, "dd-token"), body, true},
		{"dd wrong", Datadog, hdr(DefaultDatadogTokenHeader, "dd-token2"), body, false},
		{"dd missing", Datadog, hdr("Authorization", "Bearer dd-token"), body, false},

		{"generic ok", Generic, hdr(GenericSignatureHeader, goodGen), body, true},
		{"generic rotation", Generic, hdr(GenericSignatureHeader, "t="+ts+",v1="+hexMAC("old", ts, ".", body)+",v1="+hexMAC("generic-secret", ts, ".", body)), body, true},
		{"generic body changed", Generic, hdr(GenericSignatureHeader, goodGen), `{"x":2}`, false},
		{"generic stale", Generic, hdr(GenericSignatureHeader, SignGeneric("generic-secret", testNow.Add(-6*time.Minute), []byte(body))), body, false},
		{"generic no t", Generic, hdr(GenericSignatureHeader, "v1="+hexMAC("generic-secret", ts, ".", body)), body, false},
		{"generic no v1", Generic, hdr(GenericSignatureHeader, "t="+ts), body, false},
		{"generic t swapped", Generic, hdr(GenericSignatureHeader, "t="+unix(time.Second)+",v1="+hexMAC("generic-secret", ts, ".", body)), body, false},
		{"generic missing", Generic, hdr(), body, false},

		{"unknown source", Source("opsgenie"), hdr(), body, false},
	}
	cfg := testConfig()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := cfg.Verify(tt.src, tt.header, []byte(tt.body))
			if tt.ok && err != nil {
				t.Fatalf("verify: %v", err)
			}
			if !tt.ok && !errors.Is(err, ErrUnauthenticated) {
				t.Fatalf("err = %v, want ErrUnauthenticated", err)
			}
		})
	}
}

func TestVerifyDisabledSourceFailsClosed(t *testing.T) {
	cfg := Config{}
	body := []byte("{}")
	// Headers that would match an empty secret must still not pass.
	h := hdr(
		"Authorization", "Bearer ",
		PagerDutySignatureHeader, "v1="+hexMAC("", "{}"),
		GenericSignatureHeader, SignGeneric("", testNow, body),
	)
	for _, src := range Sources {
		if cfg.Enabled(src) {
			t.Fatalf("%s enabled with no secret", src)
		}
		if err := cfg.Verify(src, h, body); !errors.Is(err, ErrUnauthenticated) {
			t.Fatalf("%s: err = %v, want ErrUnauthenticated", src, err)
		}
	}
}

func TestVerifyCustomHeaders(t *testing.T) {
	cfg := testConfig()
	cfg.GrafanaSignatureHeader = "X-Sig"
	cfg.GrafanaTimestampHeader = "X-Ts"
	cfg.DatadogTokenHeader = "X-DD"
	ts := strconv.FormatInt(testNow.Unix(), 10)
	if err := cfg.Verify(Grafana, hdr("X-Sig", hexMAC("grafana-secret", ts, ":", "{}"), "X-Ts", ts), []byte("{}")); err != nil {
		t.Fatalf("grafana custom headers: %v", err)
	}
	if err := cfg.Verify(Datadog, hdr("X-DD", "dd-token"), []byte("{}")); err != nil {
		t.Fatalf("datadog custom header: %v", err)
	}
}

func TestVerifyErrorDoesNotLeakSecret(t *testing.T) {
	cfg := testConfig()
	for _, src := range Sources {
		err := cfg.Verify(src, hdr(), []byte("{}"))
		if err == nil {
			t.Fatalf("%s: verify passed with no headers", src)
		}
		for _, s := range []string{"am-token", "grafana-secret", "pd-secret", "dd-token", "generic-secret"} {
			if strings.Contains(err.Error(), s) {
				t.Fatalf("%s error leaks secret: %v", src, err)
			}
		}
	}
}
