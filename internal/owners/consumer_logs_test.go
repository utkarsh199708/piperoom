package owners

import (
	"strings"
	"testing"

	"piperoom/internal/clients/workerlogs/logql"
)

func TestConsumerLogsBlock(t *testing.T) {
	cases := []struct {
		name    string
		block   string
		want    string
		wantErr string
	}{
		{name: "absent", block: "", want: ""},
		{name: "valid", block: "    consumer_logs:\n      selector: '{service=\"devconsumer\"}'\n", want: `{service="devconsumer"}`},
		{name: "flow form, trimmed", block: "    consumer_logs: {selector: '  {service=\"devconsumer\", container=~\"order-sync.*\"} '}\n",
			want: `{service="devconsumer", container=~"order-sync.*"}`},
		{name: "empty block", block: "    consumer_logs: {}\n", wantErr: "consumer_logs: selector is required"},
		{name: "blank selector", block: "    consumer_logs: {selector: '  '}\n", wantErr: "consumer_logs: selector is required"},
		{name: "null block", block: "    consumer_logs:\n", wantErr: "consumer_logs: is empty (null)"},
		{name: "pipeline stage refused", block: "    consumer_logs: {selector: '{service=\"devconsumer\"} |= \"x\"'}\n",
			wantErr: "consumer_logs: selector: selector has text after '}'"},
		{name: "parser stage refused", block: "    consumer_logs: {selector: '{service=\"devconsumer\"} | json'}\n",
			wantErr: "pipeline stages are not allowed"},
		{name: "metric query refused", block: "    consumer_logs: {selector: 'sum(rate({service=\"devconsumer\"}[1m]))'}\n",
			wantErr: "selector must start with '{'"},
		{name: "unquoted value", block: "    consumer_logs: {selector: '{service=devconsumer}'}\n", wantErr: "value is not a quoted string"},
		{name: "unknown key", block: "    consumer_logs:\n      selecter: '{service=\"devconsumer\"}'\n", wantErr: `consumer_logs: unknown field "selecter"`},
		{name: "extra key", block: "    consumer_logs: {selector: '{a=\"b\"}', url: 'http://loki'}\n", wantErr: `unknown field "url"`},
		{name: "not a mapping", block: "    consumer_logs: '{a=\"b\"}'\n", wantErr: "parse"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o, err := Load(writeTemp(t, withWatch(tc.block)))
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("Load error = %v, want it to contain %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			p, _ := o.Pipeline("orders")
			if got := p.ConsumerLogSelector(); got != tc.want {
				t.Fatalf("ConsumerLogSelector() = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestConsumerLogsRefusedOnBatch(t *testing.T) {
	_, err := loadBatch(t, "    dags: [revenue_daily]\n    consumer_logs: {selector: '{service=\"x\"}'}\n")
	if err == nil || !strings.Contains(err.Error(), "consumer_logs is a cdc field") {
		t.Fatalf("Load = %v, want the cdc-field refusal", err)
	}
}

func TestConsumerLogSelectorZeroValue(t *testing.T) {
	if got := (Pipeline{ID: "x"}).ConsumerLogSelector(); got != "" {
		t.Fatalf("a pipeline built in code: %q, want none", got)
	}
}

// TestConsumerLogSelectorSameRuleAsWorkerSelector: a consumer_logs selector
// is accepted exactly when WORKER_LOGS_SELECTOR would be (both go through
// logql.CheckSelector; workerlogs.CheckSelector delegates to it, and
// workerlogs' TestCheckSelectorDelegates pins that side).
func TestConsumerLogSelectorSameRuleAsWorkerSelector(t *testing.T) {
	for _, s := range []string{
		`{service="devconsumer"}`, ` { service = "c" , container=~"conn.*", level!="DEBUG", x!~` + "`a|b`" + ` } `,
		`{a="}|= \"x\""}`, `{service="c"} |= "x"`, `{service="c"} | json`, `sum(rate({a="b"}[1m]))`,
		`{service=c}`, `{service='c'}`, `{}`, `{a="b"`, `{a="b"} or vector(1)`, "{" + strings.Repeat(`a="b",`, 200) + `a="b"}`,
	} {
		p := &Pipeline{ConsumerLogs: &ConsumerLogsSpec{Selector: s}}
		ownersOK := p.validateConsumerLogs() == nil
		workerOK := logql.CheckSelector(s) == nil
		if ownersOK != workerOK {
			t.Errorf("%q: consumer_logs accepts=%v, worker selector accepts=%v", s, ownersOK, workerOK)
		}
	}
}
