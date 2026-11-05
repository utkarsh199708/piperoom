package config

import (
	"strings"
	"testing"
)

func TestParseWorkerLogs(t *testing.T) {
	good := Config{WorkerLogsLokiURL: "http://loki:3100", WorkerLogsSelector: `{service="connect"}`}
	for _, tc := range []struct {
		name    string
		c       Config
		want    WorkerLogs
		errPart string
	}{
		{"unset is not configured", Config{}, WorkerLogs{}, ""},
		{"url and selector", good, WorkerLogs{URL: "http://loki:3100", Selector: `{service="connect"}`}, ""},
		{"https with token and org", Config{WorkerLogsLokiURL: "https://loki.example:443", WorkerLogsSelector: `{app="connect",env=~"prod|stage"}`, WorkerLogsToken: "t", WorkerLogsOrgID: "acme"},
			WorkerLogs{URL: "https://loki.example:443", Selector: `{app="connect",env=~"prod|stage"}`, Token: "t", OrgID: "acme"}, ""},
		{"basic auth user with token", Config{WorkerLogsLokiURL: "https://l:3100", WorkerLogsSelector: `{a="b"}`, WorkerLogsUser: "u", WorkerLogsToken: "p"},
			WorkerLogs{URL: "https://l:3100", Selector: `{a="b"}`, User: "u", Token: "p"}, ""},
		{"selector without url", Config{WorkerLogsSelector: `{a="b"}`}, WorkerLogs{}, "WORKER_LOGS_LOKI_URL"},
		{"token without url", Config{WorkerLogsToken: "t"}, WorkerLogs{}, "WORKER_LOGS_LOKI_URL"},
		{"ftp scheme", Config{WorkerLogsLokiURL: "ftp://loki", WorkerLogsSelector: `{a="b"}`}, WorkerLogs{}, "WORKER_LOGS_LOKI_URL"},
		{"no host", Config{WorkerLogsLokiURL: "http:///x", WorkerLogsSelector: `{a="b"}`}, WorkerLogs{}, "WORKER_LOGS_LOKI_URL"},
		{"user info in url", Config{WorkerLogsLokiURL: "http://u:p@loki:3100", WorkerLogsSelector: `{a="b"}`}, WorkerLogs{}, "user info"},
		{"url without selector", Config{WorkerLogsLokiURL: "http://loki:3100"}, WorkerLogs{}, "WORKER_LOGS_SELECTOR"},
		{"pipeline stage refused", Config{WorkerLogsLokiURL: "http://loki:3100", WorkerLogsSelector: `{a="b"} |= "x"`}, WorkerLogs{}, "WORKER_LOGS_SELECTOR"},
		{"metric query refused", Config{WorkerLogsLokiURL: "http://loki:3100", WorkerLogsSelector: `rate({a="b"}[5m])`}, WorkerLogs{}, "WORKER_LOGS_SELECTOR"},
		{"empty matcher refused", Config{WorkerLogsLokiURL: "http://loki:3100", WorkerLogsSelector: `{}`}, WorkerLogs{}, "WORKER_LOGS_SELECTOR"},
		{"user without token", Config{WorkerLogsLokiURL: "http://loki:3100", WorkerLogsSelector: `{a="b"}`, WorkerLogsUser: "u"}, WorkerLogs{}, "WORKER_LOGS_TOKEN"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := ParseWorkerLogs(tc.c)
			if tc.errPart != "" {
				if err == nil || !strings.Contains(err.Error(), tc.errPart) {
					t.Fatalf("want an error naming %q, got %v", tc.errPart, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
			if got.Configured() != (tc.want.URL != "") {
				t.Fatalf("Configured() = %v for %+v", got.Configured(), got)
			}
		})
	}
}

func TestWorkerLogsFromEnvAndNeverLogged(t *testing.T) {
	t.Setenv("WORKER_LOGS_LOKI_URL", "http://127.0.0.1:3100")
	t.Setenv("WORKER_LOGS_SELECTOR", `{service="connect"}`)
	t.Setenv("WORKER_LOGS_TOKEN", "s3cret-token")
	c := Load()
	w, err := ParseWorkerLogs(c)
	if err != nil || !w.Configured() || w.Token != "s3cret-token" || w.Selector != `{service="connect"}` {
		t.Fatalf("Load+Parse = %+v, %v", w, err)
	}
	// The whole Config is redacted when logged: the token must not appear.
	if got := c.LogValue().String(); strings.Contains(got, "s3cret-token") {
		t.Fatalf("Config.LogValue leaks the token: %s", got)
	}
}
