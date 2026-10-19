package main

import (
	"os"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
)

// TestOwnersBatchLoads keeps dev/owners.batch.yaml loadable and in step with
// the batch profile: the DAG, the models and the sink table it builds.
func TestOwnersBatchLoads(t *testing.T) {
	o, err := owners.Load("../../dev/owners.batch.yaml")
	if err != nil {
		t.Fatalf("owners.Load: %v", err)
	}
	p, ok := o.Pipeline("daily-revenue")
	if !ok {
		t.Fatal("pipeline daily-revenue missing")
	}
	if p.KindOrDefault() != events.PipelineKindBatch {
		t.Errorf("kind = %q, want batch", p.KindOrDefault())
	}
	if !slices.Equal(p.DAGs, []string{"revenue_daily"}) {
		t.Errorf("dags = %v, want [revenue_daily] (dev/batch/dags/revenue_daily.py)", p.DAGs)
	}
	if !slices.Equal(p.Models, []string{"stg_orders", "fct_revenue"}) {
		t.Errorf("models = %v, want the two models of dev/batch/dbt/analytics", p.Models)
	}
	if !slices.Equal(p.SinkTables, []string{"analytics.fct_revenue"}) || p.SinkTSColumn != "loaded_at" {
		t.Errorf("sink = %v / %q, want [analytics.fct_revenue] / loaded_at", p.SinkTables, p.SinkTSColumn)
	}
	for _, m := range p.Models {
		if _, err := os.Stat("../../dev/batch/dbt/analytics/models/" + m + ".sql"); err != nil {
			t.Errorf("model %s: %v", m, err)
		}
	}
}

// TestUsersBatchMatchesOwners runs the server's start-up check on the pair
// dev/batch.md tells you to start the server with.
func TestUsersBatchMatchesOwners(t *testing.T) {
	o, err := owners.Load("../../dev/owners.batch.yaml")
	if err != nil {
		t.Fatalf("owners.Load: %v", err)
	}
	u, err := room.LoadUsers("../../dev/users.batch.yaml")
	if err != nil {
		t.Fatalf("room.LoadUsers: %v", err)
	}
	if err := room.ValidateUsers(u, o); err != nil {
		t.Fatalf("users/owners config: %v", err)
	}
}

// composeFile is the part of dev/docker-compose.yml these checks read.
type composeFile struct {
	Services map[string]struct {
		Profiles    []string          `yaml:"profiles"`
		Image       string            `yaml:"image"`
		Build       any               `yaml:"build"`
		Ports       []string          `yaml:"ports"`
		Volumes     []string          `yaml:"volumes"`
		Environment map[string]string `yaml:"environment"`
	} `yaml:"services"`
}

// TestComposeBatchProfile guards the batch profile's rules: it adds services
// without touching the default ones, its Airflow API (no login) is bound to
// loopback, and its image is pinned.
func TestComposeBatchProfile(t *testing.T) {
	raw, err := os.ReadFile("../../dev/docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}
	var cf composeFile
	if err := yaml.Unmarshal(raw, &cf); err != nil {
		t.Fatalf("parse compose file: %v", err)
	}
	for _, name := range []string{"postgres", "kafka", "schema-registry", "connect", "devconsumer"} {
		if s, ok := cf.Services[name]; !ok || len(s.Profiles) != 0 {
			t.Errorf("default service %s: missing or moved into a profile (%v)", name, s.Profiles)
		}
	}
	af, ok := cf.Services["airflow"]
	if !ok {
		t.Fatal("service airflow missing")
	}
	if !slices.Equal(af.Profiles, []string{"batch"}) {
		t.Errorf("airflow profiles = %v, want [batch]", af.Profiles)
	}
	for _, p := range af.Ports {
		if !strings.HasPrefix(p, "127.0.0.1:") {
			t.Errorf("airflow port %q: want it bound to 127.0.0.1 (the API takes no login)", p)
		}
	}
	if got := af.Environment["AIRFLOW__API__AUTH_BACKENDS"]; got != "airflow.api.auth.backend.default" {
		t.Errorf("AIRFLOW__API__AUTH_BACKENDS = %q", got)
	}
	// Without a public role the no-login API answers 403 (Airflow >= 2.8 authorizes every call).
	if !strings.Contains(strings.Join(af.Volumes, " "), "webserver_config.py:/opt/airflow/webserver_config.py") {
		t.Errorf("airflow volumes = %v: want dev/batch/airflow/webserver_config.py mounted", af.Volumes)
	}
	wc, err := os.ReadFile("../../dev/batch/airflow/webserver_config.py")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(wc), `AUTH_ROLE_PUBLIC = "Viewer"`) {
		t.Error(`webserver_config.py: want AUTH_ROLE_PUBLIC = "Viewer" (read-only, all the client needs)`)
	}
	if strings.HasSuffix(af.Image, ":latest") || !strings.Contains(af.Image, ":") {
		t.Errorf("airflow image %q: want a pinned tag", af.Image)
	}
	df, err := os.ReadFile("../../dev/batch/airflow/Dockerfile")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(df), "FROM apache/airflow:2.") {
		t.Error("dev/batch/airflow/Dockerfile: want an Airflow 2.x base (the client reads REST API v1)")
	}
}
