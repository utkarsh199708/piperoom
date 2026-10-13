package wiring

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"piperoom/internal/config"
	"piperoom/internal/owners"
)

// maxConnFileBytes caps a file that holds one connection setting (a DSN, a
// password). Anything larger is not such a file.
const maxConnFileBytes = 64 << 10

// connFields maps each connections: key (owners.ConnectionKeys) to the field
// of config.Config it fills. A test keeps the two lists equal.
var connFields = map[string]func(*config.Config) *string{
	"source_db_url":                 func(c *config.Config) *string { return &c.SourceDBURL },
	"warehouse_db_url":              func(c *config.Config) *string { return &c.WarehouseDBURL },
	"warehouse_kind":                func(c *config.Config) *string { return &c.WarehouseKind },
	"kafka_brokers":                 func(c *config.Config) *string { return &c.KafkaBrokers },
	"kafka_security_protocol":       func(c *config.Config) *string { return &c.KafkaSecurityProtocol },
	"kafka_sasl_mechanism":          func(c *config.Config) *string { return &c.KafkaSASLMechanism },
	"kafka_sasl_username":           func(c *config.Config) *string { return &c.KafkaSASLUsername },
	"kafka_sasl_password":           func(c *config.Config) *string { return &c.KafkaSASLPassword },
	"kafka_tls_ca_file":             func(c *config.Config) *string { return &c.KafkaTLSCAFile },
	"kafka_tls_cert_file":           func(c *config.Config) *string { return &c.KafkaTLSCertFile },
	"kafka_tls_key_file":            func(c *config.Config) *string { return &c.KafkaTLSKeyFile },
	"kafka_tls_server_name":         func(c *config.Config) *string { return &c.KafkaTLSServerName },
	"connect_url":                   func(c *config.Config) *string { return &c.ConnectURL },
	"connect_username":              func(c *config.Config) *string { return &c.ConnectUsername },
	"connect_password":              func(c *config.Config) *string { return &c.ConnectPassword },
	"connect_tls_ca_file":           func(c *config.Config) *string { return &c.ConnectTLSCAFile },
	"connect_tls_cert_file":         func(c *config.Config) *string { return &c.ConnectTLSCertFile },
	"connect_tls_key_file":          func(c *config.Config) *string { return &c.ConnectTLSKeyFile },
	"schema_registry_url":           func(c *config.Config) *string { return &c.SchemaRegistryURL },
	"schema_registry_username":      func(c *config.Config) *string { return &c.SchemaRegistryUsername },
	"schema_registry_password":      func(c *config.Config) *string { return &c.SchemaRegistryPassword },
	"schema_registry_tls_ca_file":   func(c *config.Config) *string { return &c.SchemaRegistryTLSCAFile },
	"schema_registry_tls_cert_file": func(c *config.Config) *string { return &c.SchemaRegistryTLSCertFile },
	"schema_registry_tls_key_file":  func(c *config.Config) *string { return &c.SchemaRegistryTLSKeyFile },
}

// connSource reads what a reference names. Tests pass fakes, so no test needs
// to set process environment variables or write secret files.
type connSource struct {
	lookupEnv func(string) (string, bool)
	readFile  func(string) ([]byte, error)
	// canRead checks that a file (TLS material, which may be a large CA
	// bundle) can be opened; the client reads it later.
	canRead func(string) error
}

var osConnSource = connSource{lookupEnv: os.LookupEnv, readFile: readCapped, canRead: canOpen}

func canOpen(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	return f.Close()
}

// readCapped reads at most maxConnFileBytes of a file and fails when it holds
// more.
func readCapped(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxConnFileBytes+1))
	if err != nil {
		return nil, err
	}
	if len(b) > maxConnFileBytes {
		return nil, fmt.Errorf("larger than %d bytes", maxConnFileBytes)
	}
	return b, nil
}

// errConnRef wraps every failure to resolve a reference.
var errConnRef = errors.New("connection reference")

// pipelineConfig returns the client settings of a pipeline with a
// connections: block. It starts from an empty Config: nothing is taken from
// the process-wide settings, so a key the block leaves out is empty (no TLS,
// no login) and one tenant's broker is never sent another tenant's password.
// Errors name the pipeline, the key and the variable or path, never a value.
func pipelineConfig(p owners.Pipeline, src connSource) (config.Config, error) {
	var pc config.Config
	keys := make([]string, 0, len(p.Connections))
	for k := range p.Connections {
		keys = append(keys, k)
	}
	sort.Strings(keys) // same error for the same file
	for _, k := range keys {
		field, ok := connFields[k]
		if !ok {
			// owners.Load refuses unknown keys; this guards a hand-built Owners.
			return config.Config{}, fmt.Errorf("wiring: pipeline %q: connections: unknown key %q: %w", p.ID, k, errConnRef)
		}
		v, err := resolveRef(k, p.Connections[k], src)
		if err != nil {
			return config.Config{}, fmt.Errorf("wiring: pipeline %q: connections.%s: %w", p.ID, k, err)
		}
		*field(&pc) = v
	}
	return pc, nil
}

// resolveRef returns the value a reference names.
func resolveRef(key string, r owners.ConnRef, src connSource) (string, error) {
	switch {
	case r.Value != "":
		return r.Value, nil
	case r.Env != "":
		v, ok := src.lookupEnv(r.Env)
		if !ok {
			return "", fmt.Errorf("environment variable %s is not set: %w", r.Env, errConnRef)
		}
		if strings.TrimSpace(v) == "" {
			return "", fmt.Errorf("environment variable %s is empty: %w", r.Env, errConnRef)
		}
		return v, nil
	case r.File != "" && owners.IsPathKey(key):
		// TLS material: the client loads the PEM from the path itself.
		if err := src.canRead(r.File); err != nil {
			// The OS error names the path and the reason only.
			return "", fmt.Errorf("open file %s: %w: %w", r.File, errConnRef, err)
		}
		return r.File, nil
	case r.File != "":
		b, err := src.readFile(r.File)
		if err != nil {
			return "", fmt.Errorf("read file %s: %w: %w", r.File, errConnRef, err)
		}
		v := strings.TrimSpace(string(b))
		if v == "" {
			return "", fmt.Errorf("file %s is empty: %w", r.File, errConnRef)
		}
		return v, nil
	}
	return "", fmt.Errorf("no env, file or value: %w", errConnRef)
}
