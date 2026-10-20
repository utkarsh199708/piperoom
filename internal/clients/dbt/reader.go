package dbt

import (
	"fmt"

	"piperoom/internal/clients"
	"piperoom/internal/clients/netsec"
	"piperoom/internal/config"
)

// The readers' types are the P0 contract types (internal/clients/clients.go,
// P0 item 8), named here so callers of this package can keep the short names.
type (
	// Reader is the read interface the dbt tools take.
	Reader = clients.TransformReader
	// RunResults is the latest run_results.json. Results keeps dbt's order
	// and every node kind dbt ran (models, tests, seeds, snapshots).
	RunResults = clients.RunResults
	// ModelResult is one node's result. Rows is -1 when the adapter
	// response has no rows_affected.
	ModelResult = clients.ModelResult
	// Lineage is one node's direct parents and children, sorted unique_ids.
	Lineage = clients.Lineage
	// ModelSQL is one model's SQL from manifest.json. SQL is untrusted file
	// text; Compiled is false when it is the raw (Jinja) code.
	ModelSQL = clients.ModelSQL
)

// Both readers implement the optional clients.TransformSQLReader next to
// Reader; the dbt_model_sql tool asserts it at use time.
var (
	_ Reader                     = (*Dir)(nil)
	_ Reader                     = (*Cloud)(nil)
	_ clients.TransformSQLReader = (*Dir)(nil)
	_ clients.TransformSQLReader = (*Cloud)(nil)
)

// FromConfig returns the reader the dbt settings select (config.ParseDBT's
// result): a *Dir for ArtifactsDir, a *Cloud for CloudURL, or nil and no
// error when dbt is off, so wiring can hold a nil optional reader. opts
// apply to the Cloud reader only.
func FromConfig(d config.DBT, opts ...CloudOption) (Reader, error) {
	switch {
	case d.ArtifactsDir != "":
		r, err := NewDir(d.ArtifactsDir)
		if err != nil {
			return nil, err
		}
		return r, nil
	case d.CloudURL != "":
		r, err := NewCloud(d.CloudURL, d.CloudAccount, d.CloudJobID, netsec.Secret(d.CloudToken), opts...)
		if err != nil {
			return nil, err
		}
		return r, nil
	case d.CloudToken != "" || d.CloudAccount != "" || d.CloudJobID != "":
		return nil, fmt.Errorf("dbt: %w: dbt Cloud settings without DBT_CLOUD_URL", netsec.ErrInvalid)
	}
	return nil, nil
}
