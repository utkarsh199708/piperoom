package clients_test

import (
	"context"
	"encoding/json"
	"slices"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients"
)

// TC1: the external kinds and sources keep their wire names, and a change
// with an ExternalChange in After round-trips.
func TestX4ExternalChangeRoundTrip(t *testing.T) {
	wire := map[string]string{
		clients.ChangeDeploy: "deploy", clients.ChangeMigration: "migration", clients.ChangeVendorIncident: "vendor_incident",
		clients.ChangeSourceWebhook: "webhook", clients.ChangeSourceFlyway: "flyway", clients.ChangeSourceLiquibase: "liquibase",
		clients.ChangeSourceDBT: "dbt", clients.ChangeSourceStatuspage: "statuspage",
		clients.ChangeSourceWatcher: "watcher", clients.ChangeSourceDDLAudit: "ddl_audit",
	}
	for got, want := range wire {
		if got != want {
			t.Errorf("constant = %q, want %q", got, want)
		}
	}

	detail := clients.ExternalChange{URL: "https://github.com/acme/app/actions/runs/1", Service: "checkout-api", Version: "42"}
	after, err := json.Marshal(detail)
	if err != nil {
		t.Fatal(err)
	}
	in := clients.PipelineChange{
		ID: 7, PipelineID: "orders", Kind: clients.ChangeMigration, Object: "checkout-api",
		At: time.Date(2026, 10, 22, 9, 0, 0, 0, time.UTC), Actor: "ci", Source: clients.ChangeSourceWebhook,
		Fingerprint: "fp", After: after, Summary: "drop column legacy_ref",
	}
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), `"before"`) {
		t.Fatalf("empty before must be omitted: %s", raw)
	}
	var out clients.PipelineChange
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	var got clients.ExternalChange
	if err := json.Unmarshal(out.After, &got); err != nil {
		t.Fatal(err)
	}
	if got != detail || out.Kind != in.Kind || out.Source != in.Source || !out.At.Equal(in.At) {
		t.Fatalf("round trip = %+v / %+v", out, got)
	}
	// Zero values stay off the wire.
	if raw, _ := json.Marshal(clients.ExternalChange{}); string(raw) != `{}` {
		t.Fatalf("zero ExternalChange = %s", raw)
	}
}

func TestX4ChangeKinds(t *testing.T) {
	kinds := clients.ChangeKinds()
	// Q7 appends pull_request: a merged fix PR the agent opened, not external.
	want := []string{"ddl", "connector_config", "connector_state", "schema_version", "deploy", "migration", "vendor_incident", "pull_request"}
	if !slices.Equal(kinds, want) {
		t.Fatalf("ChangeKinds() = %v, want %v", kinds, want)
	}
	kinds[0] = "changed"
	if clients.ChangeKinds()[0] != "ddl" {
		t.Fatal("ChangeKinds shares its slice")
	}
	external := want[4:7]
	for _, k := range want {
		if got, ext := clients.IsExternalChange(k), slices.Contains(external, k); got != ext {
			t.Errorf("IsExternalChange(%q) = %v, want %v", k, got, ext)
		}
	}
	if clients.IsExternalChange("") || clients.IsExternalChange("Deploy") {
		t.Fatal("IsExternalChange accepts a non-kind")
	}
}

// E4: caps are enforced at the boundary, one byte over is refused.
func TestX4ExternalChangeValidate(t *testing.T) {
	long := func(n int) string { return strings.Repeat("a", n) }
	urlOf := func(n int) string { p := "https://x.io/"; return p + long(n-len(p)) }
	tests := []struct {
		name string
		in   clients.ExternalChange
		ok   bool
	}{
		{"empty", clients.ExternalChange{}, true},
		{"full", clients.ExternalChange{URL: "http://deploy.local/run/1", Service: "svc", Version: "V42__drop", Status: clients.VendorMonitoring}, true},
		{"url at cap", clients.ExternalChange{URL: urlOf(clients.MaxExternalChangeURLBytes)}, true},
		{"url over cap", clients.ExternalChange{URL: urlOf(clients.MaxExternalChangeURLBytes + 1)}, false},
		{"url javascript", clients.ExternalChange{URL: "javascript:alert(1)"}, false},
		{"url relative", clients.ExternalChange{URL: "/runs/1"}, false},
		{"url no host", clients.ExternalChange{URL: "https:///x"}, false},
		{"url user info", clients.ExternalChange{URL: "https://user:pw@ci.example.com/run"}, false},
		{"url control", clients.ExternalChange{URL: "https://x.io/\x07"}, false},
		{"service at cap", clients.ExternalChange{Service: long(clients.MaxExternalChangeServiceBytes)}, true},
		{"service over cap", clients.ExternalChange{Service: long(clients.MaxExternalChangeServiceBytes + 1)}, false},
		{"service newline", clients.ExternalChange{Service: "a\nb"}, false},
		{"service bad utf8", clients.ExternalChange{Service: "a\xffb"}, false},
		{"version at cap", clients.ExternalChange{Version: long(clients.MaxExternalChangeVersionBytes)}, true},
		{"version over cap", clients.ExternalChange{Version: long(clients.MaxExternalChangeVersionBytes + 1)}, false},
		{"status resolved", clients.ExternalChange{Status: clients.VendorResolved}, true},
		{"status unknown", clients.ExternalChange{Status: "major_outage"}, false},
	}
	for _, tt := range tests {
		if err := tt.in.Validate(); (err == nil) != tt.ok {
			t.Errorf("%s: Validate() = %v, want ok=%v", tt.name, err, tt.ok)
		}
	}
}

// E5: an implementation can satisfy ChangeReader and ChangeWriter with the
// external kinds, unchanged interfaces, no adapter.
type fakeChangeLog struct{ rows []clients.PipelineChange }

func (f *fakeChangeLog) RecordChange(_ context.Context, c clients.PipelineChange) (bool, error) {
	f.rows = append(f.rows, c)
	return true, nil
}

func (f *fakeChangeLog) ListChanges(_ context.Context, pipelineID string, _, _ time.Time, kinds []string, _ int) ([]clients.PipelineChange, error) {
	var out []clients.PipelineChange
	for _, r := range f.rows {
		if r.PipelineID == pipelineID && (kinds == nil || slices.Contains(kinds, r.Kind)) {
			out = append(out, r)
		}
	}
	return out, nil
}

var (
	_ clients.ChangeReader = (*fakeChangeLog)(nil)
	_ clients.ChangeWriter = (*fakeChangeLog)(nil)
)

func TestX4FakeChangeLogExternalKinds(t *testing.T) {
	f := &fakeChangeLog{}
	ctx := context.Background()
	for _, k := range []string{clients.ChangeDeploy, clients.ChangeConnectorState, clients.ChangeVendorIncident} {
		if _, err := f.RecordChange(ctx, clients.PipelineChange{PipelineID: "orders", Kind: k}); err != nil {
			t.Fatal(err)
		}
	}
	ext := slices.DeleteFunc(clients.ChangeKinds(), func(k string) bool { return !clients.IsExternalChange(k) })
	rows, _ := f.ListChanges(ctx, "orders", time.Time{}, time.Time{}, ext, 10)
	if len(rows) != 2 {
		t.Fatalf("external rows = %d, want 2", len(rows))
	}
}
