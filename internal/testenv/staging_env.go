package testenv

// Pure part of the staging helpers (staging.go, build tag integration), kept in
// an untagged file so plain `go test ./internal/testenv/...` covers it.

import "strings"

const (
	// DefaultStagingConnectURL and DefaultStagingRegistryURL are the host ports of
	// the `staging` compose profile (dev/docker-compose.yml, dev/README.md "Staging profile").
	DefaultStagingConnectURL  = "http://localhost:18083"
	DefaultStagingRegistryURL = "http://localhost:18081"

	// StagingConnectEnv and StagingRegistryEnv move them, like devctl's own variables of the same names.
	StagingConnectEnv  = "STAGING_CONNECT_URL"
	StagingRegistryEnv = "STAGING_SCHEMA_REGISTRY_URL"
)

// Staging is where the staging profile's services answer.
type Staging struct {
	ConnectURL        string
	SchemaRegistryURL string
}

// StagingFromEnv reads the staging URLs from getenv, trailing slash trimmed,
// falling back to the profile's host ports.
func StagingFromEnv(getenv func(string) string) Staging {
	pick := func(key, def string) string {
		if v := strings.TrimSpace(getenv(key)); v != "" {
			return strings.TrimRight(v, "/")
		}
		return def
	}
	return Staging{
		ConnectURL:        pick(StagingConnectEnv, DefaultStagingConnectURL),
		SchemaRegistryURL: pick(StagingRegistryEnv, DefaultStagingRegistryURL),
	}
}
