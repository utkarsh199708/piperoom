// Package dbt reads dbt artifacts for the batch tools (P4d): the latest
// run_results.json (status, message, rows and time per node) and the
// parent_map/child_map of manifest.json (one model's direct lineage). Two
// readers implement clients.TransformReader:
//
//   - *Dir reads run_results.json and manifest.json from DBT_ARTIFACTS_DIR
//     (dbt's target/ directory), re-read on every call so a new dbt run is
//     seen without a restart.
//   - *Cloud reads the same two files from dbt Cloud's Administrative API v2
//     for the newest finished run of one job.
//
// dbt Cloud paths used (each segment url.PathEscape'd and checked by
// netsec.CheckPath):
//
//	GET /api/v2/accounts/{a}/runs/?job_definition_id={j}&order_by=-id&limit=10
//	GET /api/v2/accounts/{a}/runs/{run_id}/artifacts/run_results.json
//	GET /api/v2/accounts/{a}/runs/{run_id}/artifacts/manifest.json
//
// Both readers cap each artifact at MaxArtifactBytes (16 MiB) and check its
// metadata.dbt_schema_version before using it: an unsupported version is a
// *VersionError (errors.Is ErrUnsupportedVersion), a truncated or malformed
// file wraps ErrMalformed, an oversized one ErrTooLarge, a missing one
// ErrNoArtifact. A bad file is always an error, never a partial result.
//
// Read-only: files are opened for reading and dbt Cloud gets GET requests
// only, so a read-only service token (Job Viewer) is enough. Every text
// field dbt wrote (messages, node names) is untrusted: the reader returns it
// as read and the tool wraps, redacts and scans it.
package dbt
