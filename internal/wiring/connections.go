package wiring

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"sort"
	"strings"

	"piperoom/internal/clients/netsec"
	"piperoom/internal/config"
	"piperoom/internal/owners"
)

// maxConnFileBytes caps a file that holds one connection setting (a DSN, a
// password). Anything larger is not such a file.
const maxConnFileBytes = 64 << 10

// connFields maps each connections: key (owners.ConnectionKeys) to the field
// of config.Config it fills. A test keeps every owners key mapped here; the
// Airflow and dbt keys of a batch pipeline (P4b-1) are mapped first and A3's
// owners follow-up adds them to owners.connKeys, after which the test asks for
// the two lists to be equal again.
var connFields = map[string]func(*config.Config) *string{
	"airflow_url":          func(c *config.Config) *string { return &c.AirflowURL },
	"airflow_username":     func(c *config.Config) *string { return &c.AirflowUsername },
	"airflow_password":     func(c *config.Config) *string { return &c.AirflowPassword },
	"airflow_token":        func(c *config.Config) *string { return &c.AirflowToken },
	"dbt_artifacts_dir":    func(c *config.Config) *string { return &c.DBTArtifactsDir },
	"dbt_cloud_url":        func(c *config.Config) *string { return &c.DBTCloudURL },
	"dbt_cloud_token":      func(c *config.Config) *string { return &c.DBTCloudToken },
	"dbt_cloud_account_id": func(c *config.Config) *string { return &c.DBTCloudAccount },
	"dbt_cloud_job_id":     func(c *config.Config) *string { return &c.DBTCloudJobID },

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
	// homeDir is the directory pgx reads its default files from
	// (os.UserHomeDir, as pgconn does); exists reports whether a path exists.
	homeDir func() (string, error)
	exists  func(string) bool
}

var osConnSource = connSource{
	lookupEnv: os.LookupEnv, readFile: readCapped, canRead: canOpen,
	homeDir: os.UserHomeDir,
	exists: func(p string) bool {
		_, err := os.Stat(p)
		return err == nil
	},
}

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
		r := p.Connections[k]
		v, err := resolveRef(k, r, src)
		if err != nil {
			return config.Config{}, fmt.Errorf("wiring: pipeline %q: connections.%s: %w", p.ID, k, err)
		}
		// The non-secret settings are checked here, before a client sees them:
		// the client's own error would quote the value, and a reference that
		// points at the wrong variable may hold a DSN or a password.
		if owners.IsLiteralKey(k) && !literalOK(k, v) {
			return config.Config{}, fmt.Errorf("wiring: pipeline %q: connections.%s: the value from %s is not %s (value not shown): %w",
				p.ID, k, refName(r), literalWant[k], errConnRef)
		}
		*field(&pc) = v
	}
	return pc, nil
}

// literalWant says, per non-secret key, what literalOK accepts.
var literalWant = map[string]string{
	"warehouse_kind":          "postgres or snowflake",
	"kafka_security_protocol": "PLAINTEXT, SSL, SASL_PLAINTEXT or SASL_SSL",
	"kafka_sasl_mechanism":    "PLAIN, SCRAM-SHA-256, SCRAM-SHA-512 or AWS_MSK_IAM",
	"kafka_tls_server_name":   "a host name",
}

// hostNameRE is an RFC 1123 host name of at most 253 characters (checked
// separately).
var hostNameRE = regexp.MustCompile(`^[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?(\.[A-Za-z0-9]([A-Za-z0-9-]{0,61}[A-Za-z0-9])?)*$`)

// literalOK reports whether v is an allowed value of a non-secret key. The
// sets match what the clients accept: wiring.warehouseKind and netsec
// (protocols and mechanisms, compared without regard to case).
func literalOK(key, v string) bool {
	v = strings.TrimSpace(v)
	switch key {
	case "warehouse_kind":
		return slices.Contains([]string{WarehousePostgres, WarehouseSnowflake}, strings.ToLower(v))
	case "kafka_security_protocol":
		return slices.Contains([]string{netsec.Plaintext, netsec.SSL, netsec.SASLPlaintext, netsec.SASLSSL}, strings.ToUpper(v))
	case "kafka_sasl_mechanism":
		return slices.Contains([]string{netsec.MechPlain, netsec.MechSCRAMSHA256, netsec.MechSCRAMSHA512, netsec.MechAWSMSKIAM}, strings.ToUpper(v))
	case "kafka_tls_server_name":
		return len(v) <= 253 && hostNameRE.MatchString(v)
	}
	return false
}

// refName names a reference for an error: the variable or the path, or
// "value:" for a literal (whose text is not repeated either).
func refName(r owners.ConnRef) string {
	switch {
	case r.Env != "":
		return "environment variable " + r.Env
	case r.File != "":
		return "file " + r.File
	}
	return "value:"
}

// tlsPathRefs are the connection references of the TLS client certificate
// and key files in c: their resolved, cleaned paths (owners.FileRef, after
// filepath.Abs and, when the file exists, filepath.EvalSymlinks). They are
// what a client really opens, whichever variable or file reference named
// them, so two tenants that reach one key file through different names still
// share it (owners.CheckSharedRefs). CA bundles are left out (owners.IsCAKey).
func tlsPathRefs(c config.Config) []string {
	var out []string
	for _, k := range owners.ConnectionKeys() {
		if !owners.IsPathKey(k) || owners.IsCAKey(k) {
			continue
		}
		p := strings.TrimSpace(*connFields[k](&c))
		if p == "" {
			continue
		}
		if abs, err := filepath.Abs(p); err == nil {
			p = abs
		}
		if resolved, err := filepath.EvalSymlinks(p); err == nil {
			p = resolved
		}
		out = append(out, owners.FileRef(p))
	}
	return out
}

// libpqVars are the libpq variables pgx reads for anything a DSN leaves out
// (pgconn.ParseConfig): logins, client certificates and routing.
var libpqVars = []string{
	"PGPASSWORD", "PGPASSFILE", "PGSERVICEFILE", "PGSERVICE", "PGSSLCERT", "PGSSLKEY",
	"PGSSLPASSWORD", "PGUSER", "PGHOST", "PGDATABASE",
}

// libpqHomeFiles are the files pgx reads from the home directory by default
// (pgconn defaultSettings): a password file, a service file that can hold
// logins, and a client key.
var libpqHomeFiles = []string{".pgpass", ".pg_service.conf", filepath.Join(".postgresql", "postgresql.key")}

// checkLibpqDefaults refuses the start when a libpq variable is set (not
// empty) or a libpq home file exists. With per-pipeline connections, a DSN
// that leaves out its password, user, host or certificate would silently get
// the process's one, so one tenant's pipeline could log in as another. The
// error names the variables and files, never their values.
func checkLibpqDefaults(src connSource) error {
	var found []string
	for _, v := range libpqVars {
		if val, ok := src.lookupEnv(v); ok && strings.TrimSpace(val) != "" {
			found = append(found, v)
		}
	}
	if home, err := src.homeDir(); err == nil && home != "" {
		for _, f := range libpqHomeFiles {
			if p := filepath.Join(home, f); src.exists(p) {
				found = append(found, p)
			}
		}
	}
	if len(found) > 0 {
		return fmt.Errorf("wiring: a pipeline has its own connections:, but libpq defaults that pgx would add to its DSN are present (%s); unset or remove them, and put everything a pipeline needs in its own DSN: %w",
			strings.Join(found, ", "), errLibpqDefaults)
	}
	return nil
}

// errLibpqDefaults wraps the checkLibpqDefaults refusal.
var errLibpqDefaults = errors.New("libpq defaults present")

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
