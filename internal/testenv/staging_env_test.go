package testenv

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func TestStagingFromEnv(t *testing.T) {
	env := map[string]string{}
	get := func(k string) string { return env[k] }
	if got := StagingFromEnv(get); got != (Staging{DefaultStagingConnectURL, DefaultStagingRegistryURL}) {
		t.Fatalf("defaults = %+v", got)
	}
	env[StagingConnectEnv] = " http://stg:8083/ "
	env[StagingRegistryEnv] = "https://reg.example:8443"
	if got := StagingFromEnv(get); got != (Staging{"http://stg:8083", "https://reg.example:8443"}) {
		t.Fatalf("overrides = %+v", got)
	}
}

// The defaults are the ports the compose profile publishes.
func TestStagingDefaultsMatchCompose(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "dev", "docker-compose.yml"))
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Services map[string]struct {
			Profiles []string `yaml:"profiles"`
			Ports    []string `yaml:"ports"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	for url, svc := range map[string]string{DefaultStagingConnectURL: "connect-staging", DefaultStagingRegistryURL: "schema-registry-staging"} {
		s := doc.Services[svc]
		port := url[strings.LastIndex(url, ":")+1:]
		if len(s.Profiles) != 1 || s.Profiles[0] != "staging" || len(s.Ports) != 1 || !strings.Contains(s.Ports[0], ":"+port+":") {
			t.Errorf("%s = %+v, want profile staging publishing %s", svc, s, port)
		}
	}
}
