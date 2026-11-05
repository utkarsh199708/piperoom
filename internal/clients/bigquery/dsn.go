package bigquery

import (
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
)

// Scheme is the URL scheme of a BigQuery WAREHOUSE_DB_URL.
const Scheme = "bigquery"

// DefaultMaxBytesBilled caps the bytes one query may bill (10 GiB) unless the
// DSN sets max_bytes_billed. MAX(ts) over a large unpartitioned sink table
// scans the whole column; the cap turns a runaway scan into an error instead
// of a bill.
const DefaultMaxBytesBilled int64 = 10 << 30

var (
	// projectRe is a GCP project ID: 6-30 characters, lower-case letters,
	// digits and hyphens, starting with a letter and not ending in a hyphen.
	// Domain-scoped IDs ("example.com:proj") are not accepted.
	projectRe = regexp.MustCompile(`^[a-z][a-z0-9-]{4,28}[a-z0-9]$`)
	// locationRe is a BigQuery location: a multi-region ("US", "EU") or a
	// region ("europe-west2", "us-central1").
	locationRe = regexp.MustCompile(`^[a-zA-Z]+(-[a-zA-Z0-9]+)*$`)
)

// Config is a parsed WAREHOUSE_DB_URL for BigQuery.
type Config struct {
	// Project holds the sink datasets; "dataset.table" names resolve in it.
	Project string
	// Location is where the datasets and jobs live, lower-cased ("us",
	// "europe-west2"). INFORMATION_SCHEMA.JOBS is read as `region-<location>`.
	Location string
	// BillingProject runs (and pays for) the query jobs. Defaults to Project.
	BillingProject string
	// MaxBytesBilled caps each query; 0 leaves the project default.
	MaxBytesBilled int64
}

// ParseDSN parses
//
//	bigquery://<project>/<location>[?billing_project=<project>][&max_bytes_billed=<n>]
//
// The DSN carries no credentials: the client uses Application Default
// Credentials (GOOGLE_APPLICATION_CREDENTIALS, workload identity or the
// metadata server). A DSN with user:password@, an unknown or repeated
// parameter, a path with more than the location, or a project or location
// that does not match its pattern is refused. Error messages name the part
// that is wrong, never the whole DSN.
func ParseDSN(dsn string) (Config, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return Config{}, errors.New("bigquery: dsn is not a URL; want bigquery://<project>/<location>")
	}
	if !strings.EqualFold(u.Scheme, Scheme) {
		return Config{}, errors.New("bigquery: dsn scheme must be bigquery://")
	}
	if u.User != nil {
		return Config{}, errors.New("bigquery: dsn must not carry credentials (user:password@); use Application Default Credentials")
	}
	if u.Fragment != "" {
		return Config{}, errors.New("bigquery: dsn must not have a #fragment")
	}
	cfg := Config{Project: u.Host, MaxBytesBilled: DefaultMaxBytesBilled}
	if !projectRe.MatchString(cfg.Project) {
		return Config{}, fmt.Errorf("bigquery: dsn project %q is not a GCP project id", cfg.Project)
	}
	loc := strings.TrimPrefix(u.Path, "/")
	if loc == "" {
		return Config{}, errors.New("bigquery: dsn has no location; want bigquery://<project>/<location> (e.g. /us or /europe-west2)")
	}
	if !locationRe.MatchString(loc) {
		return Config{}, fmt.Errorf("bigquery: dsn location %q is not a BigQuery location", loc)
	}
	cfg.Location = strings.ToLower(loc)

	q, err := url.ParseQuery(u.RawQuery)
	if err != nil {
		return Config{}, errors.New("bigquery: dsn query string does not parse")
	}
	for k, v := range q {
		if len(v) != 1 {
			return Config{}, fmt.Errorf("bigquery: dsn parameter %s is repeated", k)
		}
		switch k {
		case "billing_project":
			if !projectRe.MatchString(v[0]) {
				return Config{}, fmt.Errorf("bigquery: dsn billing_project %q is not a GCP project id", v[0])
			}
			cfg.BillingProject = v[0]
		case "max_bytes_billed":
			n, err := strconv.ParseInt(v[0], 10, 64)
			if err != nil || n < 0 {
				return Config{}, errors.New("bigquery: dsn max_bytes_billed must be a non-negative integer (0 = project default)")
			}
			cfg.MaxBytesBilled = n
		default:
			return Config{}, fmt.Errorf("bigquery: dsn parameter %q is not known (allowed: billing_project, max_bytes_billed)", k)
		}
	}
	if cfg.BillingProject == "" {
		cfg.BillingProject = cfg.Project
	}
	return cfg, nil
}

// target names the project and location for a provenance receipt.
func (c Config) target() string {
	return "bigquery " + c.Project + "/" + c.Location
}
