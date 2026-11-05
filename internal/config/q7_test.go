package config

import (
	"strings"
	"testing"
	"time"
)

// Q7: the forge settings of the pull-request fix.

func TestParseForge(t *testing.T) {
	gh := func(mod func(*Config)) Config {
		c := Config{ForgeKind: "github", GitHubAPIURL: DefaultGitHubAPIURL, GitHubAppID: "123",
			GitHubAppInstallationID: "456", GitHubAppPrivateKeyFile: "/run/secrets/app.pem"}
		if mod != nil {
			mod(&c)
		}
		return c
	}
	cases := []struct {
		name    string
		c       Config
		want    Forge
		wantErr string
	}{
		{name: "off", c: Config{}, want: Forge{}},
		{name: "off ignores GITHUB_API_URL (GitHub Actions sets it)", c: Config{GitHubAPIURL: "https://api.github.com"}, want: Forge{}},
		{name: "off with an app id", c: Config{GitHubAppID: "1"}, wantErr: "set FORGE_KIND=github"},
		{name: "off with an installation id", c: Config{GitHubAppInstallationID: "1"}, wantErr: "set FORGE_KIND=github"},
		{name: "off with a key file", c: Config{GitHubAppPrivateKeyFile: "/k.pem"}, wantErr: "set FORGE_KIND=github"},
		{name: "unknown kind", c: Config{ForgeKind: "gitlab"}, wantErr: "FORGE_KIND"},
		{name: "github.com", c: gh(nil), want: Forge{Kind: "github", APIURL: "https://api.github.com", WebHost: "github.com",
			AppID: 123, InstallationID: 456, PrivateKeyFile: "/run/secrets/app.pem"}},
		{name: "kind any case, trailing slash trimmed", c: gh(func(c *Config) { c.ForgeKind = " GitHub "; c.GitHubAPIURL = "https://api.github.com/" }),
			want: Forge{Kind: "github", APIURL: "https://api.github.com", WebHost: "github.com", AppID: 123, InstallationID: 456, PrivateKeyFile: "/run/secrets/app.pem"}},
		{name: "empty API URL is the default", c: gh(func(c *Config) { c.GitHubAPIURL = "" }),
			want: Forge{Kind: "github", APIURL: "https://api.github.com", WebHost: "github.com", AppID: 123, InstallationID: 456, PrivateKeyFile: "/run/secrets/app.pem"}},
		{name: "enterprise server", c: gh(func(c *Config) { c.GitHubAPIURL = "https://GHE.acme.example/api/v3/" }),
			want: Forge{Kind: "github", APIURL: "https://ghe.acme.example/api/v3", WebHost: "ghe.acme.example", AppID: 123, InstallationID: 456, PrivateKeyFile: "/run/secrets/app.pem"}},
		{name: "enterprise server on a port", c: gh(func(c *Config) { c.GitHubAPIURL = "https://ghe.acme.example:8443/api/v3" }),
			want: Forge{Kind: "github", APIURL: "https://ghe.acme.example:8443/api/v3", WebHost: "ghe.acme.example:8443", AppID: 123, InstallationID: 456, PrivateKeyFile: "/run/secrets/app.pem"}},
		{name: "http to loopback", c: gh(func(c *Config) { c.GitHubAPIURL = "http://127.0.0.1:9000" }),
			want: Forge{Kind: "github", APIURL: "http://127.0.0.1:9000", WebHost: "127.0.0.1:9000", AppID: 123, InstallationID: 456, PrivateKeyFile: "/run/secrets/app.pem"}},
		{name: "http to localhost", c: gh(func(c *Config) { c.GitHubAPIURL = "http://localhost:9000/api/v3" }),
			want: Forge{Kind: "github", APIURL: "http://localhost:9000/api/v3", WebHost: "localhost:9000", AppID: 123, InstallationID: 456, PrivateKeyFile: "/run/secrets/app.pem"}},
		{name: "http to a remote host", c: gh(func(c *Config) { c.GitHubAPIURL = "http://ghe.acme.example/api/v3" }), wantErr: "must be https"},
		{name: "other scheme", c: gh(func(c *Config) { c.GitHubAPIURL = "ftp://ghe.acme.example" }), wantErr: "GITHUB_API_URL"},
		{name: "no host", c: gh(func(c *Config) { c.GitHubAPIURL = "api.github.com" }), wantErr: "GITHUB_API_URL"},
		{name: "user info", c: gh(func(c *Config) { c.GitHubAPIURL = "https://x:tok@api.github.com" }), wantErr: "user info"},
		{name: "query", c: gh(func(c *Config) { c.GitHubAPIURL = "https://api.github.com/?a=b" }), wantErr: "query or fragment"},
		{name: "empty query", c: gh(func(c *Config) { c.GitHubAPIURL = "https://api.github.com/?" }), wantErr: "query or fragment"},
		{name: "fragment", c: gh(func(c *Config) { c.GitHubAPIURL = "https://api.github.com/#x" }), wantErr: "query or fragment"},
		{name: "missing app id", c: gh(func(c *Config) { c.GitHubAppID = "" }), wantErr: "GITHUB_APP_ID: required"},
		{name: "zero app id", c: gh(func(c *Config) { c.GitHubAppID = "0" }), wantErr: "GITHUB_APP_ID"},
		{name: "negative app id", c: gh(func(c *Config) { c.GitHubAppID = "-4" }), wantErr: "GITHUB_APP_ID"},
		{name: "non-numeric installation id", c: gh(func(c *Config) { c.GitHubAppInstallationID = "abc" }), wantErr: "GITHUB_APP_INSTALLATION_ID"},
		{name: "missing installation id", c: gh(func(c *Config) { c.GitHubAppInstallationID = " " }), wantErr: "GITHUB_APP_INSTALLATION_ID: required"},
		{name: "missing key file", c: gh(func(c *Config) { c.GitHubAppPrivateKeyFile = "" }), wantErr: "GITHUB_APP_PRIVATE_KEY_FILE"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := ParseForge(c.c)
			if c.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), c.wantErr) {
					t.Fatalf("ParseForge = %+v, %v; want error containing %q", got, err, c.wantErr)
				}
				return
			}
			if err != nil || got != c.want {
				t.Fatalf("ParseForge = %+v, %v; want %+v", got, err, c.want)
			}
		})
	}
}

// The error for user info never quotes the URL, so a token in it stays out of
// the log.
func TestParseForgeErrorHidesCredentials(t *testing.T) {
	_, err := ParseForge(Config{ForgeKind: "github", GitHubAPIURL: "https://x:ghp_secret@api.github.com",
		GitHubAppID: "1", GitHubAppInstallationID: "2", GitHubAppPrivateKeyFile: "/k"})
	if err == nil || strings.Contains(err.Error(), "ghp_secret") {
		t.Fatalf("ParseForge error = %v", err)
	}
}

func TestParseFixPRApplyTimeout(t *testing.T) {
	cases := []struct {
		v       string
		want    time.Duration
		wantErr bool
	}{
		{"", 2 * time.Hour, false},
		{"  ", 2 * time.Hour, false},
		{"2h", 2 * time.Hour, false},
		{"10m", 10 * time.Minute, false},
		{"90m", 90 * time.Minute, false},
		{"168h", 168 * time.Hour, false},
		{"9m59s", 0, true},
		{"168h1s", 0, true},
		{"0", 0, true},
		{"-1h", 0, true},
		{"two hours", 0, true},
		{"24", 0, true},
	}
	for _, c := range cases {
		got, err := ParseFixPRApplyTimeout(Config{FixPRApplyTimeout: c.v})
		if c.wantErr {
			if err == nil || !strings.Contains(err.Error(), "PIPEROOM_FIXPR_APPLY_TIMEOUT") {
				t.Errorf("ParseFixPRApplyTimeout(%q) = %v, %v; want an error naming the variable", c.v, got, err)
			}
			continue
		}
		if err != nil || got != c.want {
			t.Errorf("ParseFixPRApplyTimeout(%q) = %v, %v; want %v", c.v, got, err, c.want)
		}
	}
}

func TestLoadForgeVariables(t *testing.T) {
	t.Setenv("FORGE_KIND", "github")
	t.Setenv("GITHUB_API_URL", "https://ghe.acme.example/api/v3")
	t.Setenv("GITHUB_APP_ID", "11")
	t.Setenv("GITHUB_APP_INSTALLATION_ID", "22")
	t.Setenv("GITHUB_APP_PRIVATE_KEY_FILE", "/k.pem")
	t.Setenv("PIPEROOM_FIXPR_APPLY_TIMEOUT", "3h")
	c := Load()
	if c.ForgeKind != "github" || c.GitHubAPIURL != "https://ghe.acme.example/api/v3" || c.GitHubAppID != "11" ||
		c.GitHubAppInstallationID != "22" || c.GitHubAppPrivateKeyFile != "/k.pem" || c.FixPRApplyTimeout != "3h" {
		t.Fatalf("Load did not read the Q7 variables: %q %q %q %q %q %q", c.ForgeKind, c.GitHubAPIURL, c.GitHubAppID,
			c.GitHubAppInstallationID, c.GitHubAppPrivateKeyFile, c.FixPRApplyTimeout)
	}
	t.Setenv("GITHUB_API_URL", "")
	if got := Load().GitHubAPIURL; got != DefaultGitHubAPIURL {
		t.Fatalf("GITHUB_API_URL default = %q, want %q", got, DefaultGitHubAPIURL)
	}
}
