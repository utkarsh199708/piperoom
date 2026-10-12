package main

import (
	"strings"
	"testing"

	"piperoom/internal/alertin"
	"piperoom/internal/config"
	"piperoom/internal/owners"
)

func testOwners() *owners.Owners {
	return &owners.Owners{Pipelines: []owners.Pipeline{{ID: "orders"}, {ID: "billing"}}}
}

func TestParsePagerDutyServices(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    map[string]string
		wantErr string
	}{
		{"empty", "", nil, ""},
		{"blank", "  ", nil, ""},
		{"one", "PF9KMXH=orders", map[string]string{"PF9KMXH": "orders"}, ""},
		{"two with spaces", " PF9KMXH = orders , PABC123=billing ", map[string]string{"PF9KMXH": "orders", "PABC123": "billing"}, ""},
		{"no equals", "PF9KMXH", nil, "SERVICE_ID=pipeline"},
		{"empty pipeline", "PF9KMXH=", nil, "SERVICE_ID=pipeline"},
		{"empty service", "=orders", nil, "SERVICE_ID=pipeline"},
		{"empty entry", "PF9KMXH=orders,,PABC123=billing", nil, "SERVICE_ID=pipeline"},
		{"trailing comma", "PF9KMXH=orders,", nil, "SERVICE_ID=pipeline"},
		{"unknown pipeline", "PF9KMXH=nope", nil, "not in the owners file"},
		{"duplicate service", "PF9KMXH=orders,PF9KMXH=billing", nil, "twice"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parsePagerDutyServices(tt.in, testOwners())
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("err = %v, want it to contain %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			for k, v := range tt.want {
				if got[k] != v {
					t.Fatalf("got %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestAlertConfig(t *testing.T) {
	cfg := config.Config{
		AlertmanagerToken:    "s3cret-am",
		PagerDutyAlertSecret: "s3cret-pd",
		PagerDutyServices:    "PF9KMXH=billing",
		GenericAlertSecret:   "s3cret-gen",
		DefaultAlertPipeline: "orders",
	}
	c, err := alertConfig(cfg, testOwners())
	if err != nil {
		t.Fatalf("alertConfig: %v", err)
	}
	if c.AlertmanagerToken != "s3cret-am" || c.PagerDutySecret != "s3cret-pd" || c.GenericSecret != "s3cret-gen" ||
		c.PagerDutyServices["PF9KMXH"] != "billing" || c.DefaultPipeline != "orders" || c.Owners == nil {
		t.Fatalf("config not carried over: %+v", c)
	}
	if c.GrafanaSecret != "" || c.DatadogToken != "" {
		t.Fatal("unset sources must stay disabled")
	}

	// The log line lists the enabled sources by name and never a secret.
	got := enabledAlertSources(c)
	if got != "alertmanager,pagerduty,generic" {
		t.Fatalf("enabled = %q", got)
	}
	if strings.Contains(got, "s3cret") {
		t.Fatalf("enabled sources line holds a secret: %q", got)
	}
	if got := enabledAlertSources(alertin.Config{}); got != "none" {
		t.Fatalf("no secrets: enabled = %q, want none", got)
	}

	cfg.DefaultAlertPipeline = "nope"
	if _, err := alertConfig(cfg, testOwners()); err == nil || !strings.Contains(err.Error(), "DEFAULT_ALERT_PIPELINE") {
		t.Fatalf("unknown default pipeline: err = %v", err)
	}
	cfg.DefaultAlertPipeline = ""
	cfg.PagerDutyServices = "bad"
	if _, err := alertConfig(cfg, testOwners()); err == nil {
		t.Fatal("bad PAGERDUTY_SERVICES must be refused")
	}
}
