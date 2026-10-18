package config

import (
	"strings"
	"testing"
	"time"
)

func TestPWaveDefaults(t *testing.T) {
	for _, k := range []string{
		"PIPEROOM_WRITE_MODE", "PIPEROOM_AUTO_APPROVE_DELAY", "SOURCE_DB_WRITE_URL",
		"AIRFLOW_URL", "AIRFLOW_USERNAME", "AIRFLOW_PASSWORD", "AIRFLOW_TOKEN",
		"DBT_ARTIFACTS_DIR", "DBT_CLOUD_URL", "DBT_CLOUD_TOKEN", "DBT_CLOUD_ACCOUNT_ID", "DBT_CLOUD_JOB_ID",
	} {
		t.Setenv(k, "")
	}
	c := Load()
	if on, err := ParseWriteMode(c); err != nil || on {
		t.Errorf("ParseWriteMode = %v, %v; want off (the default for a new install)", on, err)
	}
	if d, err := ParseAutoApproveDelay(c); err != nil || d != 60*time.Second {
		t.Errorf("ParseAutoApproveDelay = %v, %v; want 60s", d, err)
	}
	if c.SourceDBWriteURL != "" {
		t.Errorf("SourceDBWriteURL = %q; want empty (no writer unless configured)", c.SourceDBWriteURL)
	}
	if a, err := ParseAirflow(c); err != nil || a != (Airflow{}) {
		t.Errorf("ParseAirflow = %+v, %v; want off", a, err)
	}
	if d, err := ParseDBT(c); err != nil || d != (DBT{}) {
		t.Errorf("ParseDBT = %+v, %v; want off", d, err)
	}
}

func TestParseWriteMode(t *testing.T) {
	tests := []struct {
		in      string
		want    bool
		wantErr bool
	}{
		{"", false, false},
		{"off", false, false},
		{" off ", false, false},
		{"on", true, false},
		{"On", false, true}, // exact values, like PIPEROOM_DEV_ACK
		{"yes", false, true},
		{"1", false, true},
		{"true", false, true},
	}
	for _, tt := range tests {
		got, err := ParseWriteMode(Config{WriteMode: tt.in})
		if (err != nil) != tt.wantErr || got != tt.want {
			t.Errorf("ParseWriteMode(%q) = %v, %v; want %v, err %v", tt.in, got, err, tt.want, tt.wantErr)
		}
		if err != nil && !strings.Contains(err.Error(), "PIPEROOM_WRITE_MODE") {
			t.Errorf("ParseWriteMode(%q) error does not name the var: %v", tt.in, err)
		}
	}
}

func TestParseAutoApproveDelay(t *testing.T) {
	tests := []struct {
		in      string
		want    time.Duration
		wantErr bool
	}{
		{"", 60 * time.Second, false},
		{"0", 0, false},
		{"2s", 2 * time.Second, false},
		{"5m", 5 * time.Minute, false},
		{"0s", 0, true}, // "0" is the one way to say off
		{"-5s", 0, true},
		{"soon", 0, true},
	}
	for _, tt := range tests {
		got, err := ParseAutoApproveDelay(Config{AutoApproveDelay: tt.in})
		if (err != nil) != tt.wantErr || got != tt.want {
			t.Errorf("ParseAutoApproveDelay(%q) = %v, %v; want %v, err %v", tt.in, got, err, tt.want, tt.wantErr)
		}
		if err != nil && !strings.Contains(err.Error(), "PIPEROOM_AUTO_APPROVE_DELAY") {
			t.Errorf("ParseAutoApproveDelay(%q) error does not name the var: %v", tt.in, err)
		}
	}
}

func TestParseAirflow(t *testing.T) {
	tests := []struct {
		name    string
		c       Config
		wantErr string
	}{
		{"off", Config{}, ""},
		{"basic auth", Config{AirflowURL: "https://airflow.example:8080", AirflowUsername: "ro", AirflowPassword: "pw"}, ""},
		{"token", Config{AirflowURL: "http://airflow:8080", AirflowToken: "tok"}, ""},
		{"no login", Config{AirflowURL: "http://airflow:8080"}, ""},
		{"login without url", Config{AirflowToken: "tok"}, "AIRFLOW_URL is empty"},
		{"user without password", Config{AirflowURL: "http://airflow:8080", AirflowUsername: "ro"}, "go together"},
		{"both logins", Config{AirflowURL: "http://airflow:8080", AirflowUsername: "ro", AirflowPassword: "pw", AirflowToken: "tok"}, "not both"},
		{"user info in url", Config{AirflowURL: "http://ro:pw@airflow:8080"}, "user info"},
		{"not http", Config{AirflowURL: "ftp://airflow"}, "AIRFLOW_URL"},
		{"no host", Config{AirflowURL: "http://"}, "AIRFLOW_URL"},
	}
	for _, tt := range tests {
		_, err := ParseAirflow(tt.c)
		if tt.wantErr == "" {
			if err != nil {
				t.Errorf("%s: %v", tt.name, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
			t.Errorf("%s: err = %v; want %q", tt.name, err, tt.wantErr)
			continue
		}
		// A secret never reaches the error text.
		if strings.Contains(err.Error(), "pw") || strings.Contains(err.Error(), "tok") {
			t.Errorf("%s: error quotes a secret: %v", tt.name, err)
		}
	}
}

func TestParseDBT(t *testing.T) {
	cloud := Config{DBTCloudURL: "https://cloud.getdbt.com", DBTCloudToken: "tok", DBTCloudAccount: "12", DBTCloudJobID: "345"}
	tests := []struct {
		name    string
		c       Config
		wantErr string
	}{
		{"off", Config{}, ""},
		{"artifacts dir", Config{DBTArtifactsDir: "/srv/dbt/target"}, ""},
		{"cloud", cloud, ""},
		{"relative dir", Config{DBTArtifactsDir: "target"}, "absolute"},
		{"both", Config{DBTArtifactsDir: "/srv/dbt/target", DBTCloudURL: "https://cloud.getdbt.com"}, "not both"},
		{"cloud missing job", Config{DBTCloudURL: "https://cloud.getdbt.com", DBTCloudToken: "tok", DBTCloudAccount: "12"}, "together"},
		{"cloud bad account", Config{DBTCloudURL: "https://cloud.getdbt.com", DBTCloudToken: "tok", DBTCloudAccount: "acme", DBTCloudJobID: "345"}, "DBT_CLOUD_ACCOUNT_ID"},
		{"cloud bad url", Config{DBTCloudURL: "cloud.getdbt.com", DBTCloudToken: "tok", DBTCloudAccount: "12", DBTCloudJobID: "345"}, "DBT_CLOUD_URL"},
	}
	for _, tt := range tests {
		_, err := ParseDBT(tt.c)
		if tt.wantErr == "" {
			if err != nil {
				t.Errorf("%s: %v", tt.name, err)
			}
			continue
		}
		if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
			t.Errorf("%s: err = %v; want %q", tt.name, err, tt.wantErr)
		}
	}
	d, err := ParseDBT(Config{DBTArtifactsDir: "/srv/dbt/./target/"})
	if err != nil || d.ArtifactsDir != "/srv/dbt/target" {
		t.Errorf("ParseDBT cleans the dir: %+v, %v", d, err)
	}
}
