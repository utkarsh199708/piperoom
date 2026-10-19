package config_test

import (
	"strings"
	"testing"
	"time"

	"piperoom/internal/config"
)

func TestParseSetupDefaultsOff(t *testing.T) {
	s, err := config.ParseSetup(config.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if s.Enabled {
		t.Error("setup is on by default")
	}
	if s.TokenBudget != 2000000 || s.LocalDelay != 20*time.Second || len(s.PluginHosts) != 1 || s.PluginHosts[0] != "repo1.maven.org" {
		t.Errorf("defaults = %+v", s)
	}
	if s.SecretsDir != "" || s.OwnersDir != "" || s.ProdPatterns != nil {
		t.Errorf("defaults = %+v", s)
	}
}

// PIPEROOM_SETUP_ENABLED=on with write mode off is refused at start, and the
// error names both variables.
func TestParseSetupRequiresWriteMode(t *testing.T) {
	for _, wm := range []string{"", "off"} {
		_, err := config.ParseSetup(config.Config{SetupEnabled: "on", WriteMode: wm, SecretsDir: "/var/lib/piperoom/secrets"})
		if err == nil {
			t.Fatalf("write mode %q: setup on was accepted", wm)
		}
		if !strings.Contains(err.Error(), "PIPEROOM_SETUP_ENABLED") || !strings.Contains(err.Error(), "PIPEROOM_WRITE_MODE") {
			t.Errorf("error does not name both variables: %v", err)
		}
	}
	s, err := config.ParseSetup(config.Config{SetupEnabled: "on", WriteMode: "on", SecretsDir: "/var/lib/piperoom/secrets"})
	if err != nil {
		t.Fatal(err)
	}
	if !s.Enabled || s.SecretsDir != "/var/lib/piperoom/secrets" {
		t.Errorf("setup = %+v", s)
	}
}

func TestParseSetupErrors(t *testing.T) {
	tests := []struct {
		name string
		c    config.Config
		want string
	}{
		{"bad enabled", config.Config{SetupEnabled: "true"}, "PIPEROOM_SETUP_ENABLED"},
		{"on without secrets dir", config.Config{SetupEnabled: "on", WriteMode: "on"}, "PIPEROOM_SECRETS_DIR"},
		{"relative secrets dir", config.Config{SecretsDir: "secrets"}, "PIPEROOM_SECRETS_DIR"},
		{"relative owners dir", config.Config{OwnersDir: "owners"}, "OWNERS_DIR"},
		{"bad budget", config.Config{SetupTokenBudget: "2M"}, "PIPEROOM_SETUP_TOKEN_BUDGET"},
		{"zero budget", config.Config{SetupTokenBudget: "0"}, "PIPEROOM_SETUP_TOKEN_BUDGET"},
		{"bad delay", config.Config{SetupLocalDelay: "soon"}, "PIPEROOM_SETUP_LOCAL_DELAY"},
		{"zero-second delay", config.Config{SetupLocalDelay: "0s"}, `use "0"`},
		{"long delay", config.Config{SetupLocalDelay: "2h"}, "at most"},
		{"plugin host with a path", config.Config{SetupPluginHosts: "repo1.maven.org/maven2"}, "PIPEROOM_SETUP_PLUGIN_HOSTS"},
		{"prod pattern with a space", config.Config{SetupProdPatterns: "prod db"}, "PIPEROOM_SETUP_PROD_PATTERNS"},
	}
	for _, tt := range tests {
		_, err := config.ParseSetup(tt.c)
		if err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("%s: err = %v, want it to contain %q", tt.name, err, tt.want)
		}
	}
}

func TestParseSetupValues(t *testing.T) {
	s, err := config.ParseSetup(config.Config{
		SetupProdPatterns: "*.prod.acme, arn:aws:123, prod-cluster",
		SetupTokenBudget:  "500000",
		SetupLocalDelay:   "0",
		SetupPluginHosts:  "repo1.maven.org,artifacts.acme",
		OwnersDir:         "/etc/piperoom/owners",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(s.ProdPatterns) != 3 || s.ProdPatterns[1] != "arn:aws:123" {
		t.Errorf("prod patterns = %v", s.ProdPatterns)
	}
	if s.TokenBudget != 500000 || s.LocalDelay != 0 || len(s.PluginHosts) != 2 || s.OwnersDir != "/etc/piperoom/owners" {
		t.Errorf("setup = %+v", s)
	}
}

func TestParseSetupRunner(t *testing.T) {
	r, err := config.ParseSetupRunner(config.Config{})
	if err != nil || r != (config.SetupRunner{}) {
		t.Fatalf("empty: %+v, %v", r, err)
	}
	r, err = config.ParseSetupRunner(config.Config{SetupServerURL: "https://piperoom.acme", SetupCode: "ABCD2345", SetupSecretsFile: "/p/.piperoom/secrets.env"})
	if err != nil || r.ServerURL != "https://piperoom.acme" || r.Code != "ABCD2345" || r.SecretsFile != "/p/.piperoom/secrets.env" {
		t.Fatalf("runner = %+v, %v", r, err)
	}
	for _, c := range []config.Config{
		{SetupServerURL: "piperoom.acme"},
		{SetupServerURL: "https://u:p@piperoom.acme"},
		{SetupCode: "ABC"},
	} {
		_, err := config.ParseSetupRunner(c)
		if err == nil {
			t.Errorf("%+v accepted", c)
		} else if strings.Contains(err.Error(), "ABC") {
			t.Errorf("the error quotes the code: %v", err)
		}
	}
}

// Load reads every setup variable so a deployment can set them; the
// defaults are the design's.
func TestLoadSetupVariables(t *testing.T) {
	t.Setenv("PIPEROOM_SETUP_ENABLED", "on")
	t.Setenv("PIPEROOM_WRITE_MODE", "on")
	t.Setenv("PIPEROOM_SECRETS_DIR", "/var/lib/piperoom/secrets")
	t.Setenv("OWNERS_DIR", "/etc/piperoom/owners")
	t.Setenv("PIPEROOM_SETUP_PROD_PATTERNS", "*.prod")
	t.Setenv("PIPEROOM_SETUP_CODE", "ABCD2345")
	c := config.Load()
	if c.SetupEnabled != "on" || c.SecretsDir != "/var/lib/piperoom/secrets" || c.OwnersDir != "/etc/piperoom/owners" || c.SetupProdPatterns != "*.prod" || c.SetupCode != "ABCD2345" {
		t.Errorf("Load = %+v", c)
	}
	if c.SetupTokenBudget != config.DefaultSetupTokenBudget || c.SetupLocalDelay != config.DefaultSetupLocalDelay || c.SetupPluginHosts != config.DefaultSetupPluginHosts {
		t.Errorf("Load defaults = %q %q %q", c.SetupTokenBudget, c.SetupLocalDelay, c.SetupPluginHosts)
	}
	if _, err := config.ParseSetup(c); err != nil {
		t.Fatal(err)
	}
}
