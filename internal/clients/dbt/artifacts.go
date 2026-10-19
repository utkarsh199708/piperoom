package dbt

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"piperoom/internal/clients"
)

// MaxArtifactBytes caps one artifact read (run_results.json or
// manifest.json): a manifest of a few thousand models stays well below it,
// and the whole file is decoded in memory.
const MaxArtifactBytes = 16 << 20

// Artifact file names, as dbt writes them to target/ and dbt Cloud serves them.
const (
	RunResultsFile = "run_results.json"
	ManifestFile   = "manifest.json"
)

// Supported dbt_schema_version numbers per artifact. run-results v4 is dbt
// 1.0–1.4, v5 1.5–1.7, v6 1.8 and later; manifest v9 is dbt 1.5 up to v12
// for 1.8 and later. The fields read here (results[].unique_id, status,
// message, execution_time, adapter_response.rows_affected; nodes[].name,
// resource_type; parent_map; child_map) have the same shape in all of them.
var (
	supportedRunResults = []int{4, 5, 6}
	supportedManifest   = []int{9, 10, 11, 12}
)

const (
	kindRunResults = "run-results"
	kindManifest   = "manifest"
	maxErrorText   = 200
)

// ErrUnsupportedVersion is matched (errors.Is) by a *VersionError.
var ErrUnsupportedVersion = errors.New("dbt: unsupported artifact schema version")

// ErrMalformed is wrapped when an artifact is not valid JSON of the expected
// shape, including a file cut short while dbt was still writing it.
var ErrMalformed = errors.New("dbt: malformed artifact")

// ErrTooLarge is wrapped when an artifact is larger than MaxArtifactBytes.
var ErrTooLarge = errors.New("dbt: artifact larger than 16 MiB")

// ErrNoArtifact is wrapped when the artifact does not exist: no
// run_results.json in the directory yet, or a dbt Cloud run (or job) without
// that artifact. An unknown model is clients.ErrNotFound instead.
var ErrNoArtifact = errors.New("dbt: artifact not available")

// ErrAmbiguous is wrapped when a short model name matches models of more
// than one package or version; the caller retries with the unique_id. The
// error is an *AmbiguousError, which lists the candidates.
var ErrAmbiguous = errors.New("dbt: model name is ambiguous")

// AmbiguousError is a short model name that matches more than one model.
// Candidates are the matching unique_ids, sorted; they are manifest text
// (package names), so a caller shows them as untrusted data.
type AmbiguousError struct {
	Name       string
	Candidates []string
}

// Error shows the name and the candidates, each part cut to 200 bytes
// without control characters.
func (e *AmbiguousError) Error() string {
	return fmt.Sprintf("dbt: model %q matches %d nodes (%s): %v",
		errorText(e.Name, maxErrorText), len(e.Candidates), errorText(strings.Join(e.Candidates, ", "), maxErrorText), ErrAmbiguous)
}

// Is makes an *AmbiguousError match ErrAmbiguous.
func (e *AmbiguousError) Is(target error) bool { return target == ErrAmbiguous }

// VersionError is an artifact whose metadata.dbt_schema_version is missing,
// not a dbt schema URL, of the other artifact kind, or a version this
// package does not read.
type VersionError struct {
	File      string // RunResultsFile or ManifestFile
	Version   string // as found; file text, untrusted
	Supported []int
}

// Error shows at most 200 bytes of Version without control characters.
func (e *VersionError) Error() string {
	nums := make([]string, len(e.Supported))
	for i, n := range e.Supported {
		nums[i] = "v" + strconv.Itoa(n)
	}
	got := errorText(e.Version, maxErrorText)
	if got == "" {
		got = "(none)"
	}
	return fmt.Sprintf("dbt: %s: dbt_schema_version %s: supported %s", e.File, got, strings.Join(nums, ", "))
}

// Is makes a *VersionError match ErrUnsupportedVersion.
func (e *VersionError) Is(target error) bool { return target == ErrUnsupportedVersion }

// schemaURL is dbt's dbt_schema_version, e.g.
// https://schemas.getdbt.com/dbt/run-results/v6.json.
var schemaURL = regexp.MustCompile(`^https://schemas\.getdbt\.com/dbt/(run-results|manifest)/v([0-9]{1,3})\.json$`)

func checkVersion(file, kind, version string, supported []int) error {
	m := schemaURL.FindStringSubmatch(version)
	if m == nil || m[1] != kind {
		return &VersionError{File: file, Version: version, Supported: supported}
	}
	n, err := strconv.Atoi(m[2])
	if err != nil || !slices.Contains(supported, n) {
		return &VersionError{File: file, Version: version, Supported: supported}
	}
	return nil
}

type metadata struct {
	Version     string    `json:"dbt_schema_version"`
	GeneratedAt time.Time `json:"generated_at"`
}

// readCapped reads all of r, failing with ErrTooLarge past MaxArtifactBytes
// rather than returning a cut file.
func readCapped(r io.Reader, file string) ([]byte, error) {
	b, err := io.ReadAll(io.LimitReader(r, MaxArtifactBytes+1))
	if err != nil {
		return nil, fmt.Errorf("dbt: read %s: %w", file, err)
	}
	if len(b) > MaxArtifactBytes {
		return nil, fmt.Errorf("dbt: %s: %w", file, ErrTooLarge)
	}
	return b, nil
}

// malformed wraps a decode error. json errors quote at most a type name and
// an offset, never file content.
func malformed(file string, err error) error {
	return fmt.Errorf("dbt: %s: %w: %v", file, ErrMalformed, err)
}

// parseRunResults decodes run_results.json. The version is checked before the
// results are decoded, so a newer dbt's changed shape reads as a version
// error rather than a confusing decode error.
func parseRunResults(b []byte) (RunResults, error) {
	var head struct {
		Metadata *metadata       `json:"metadata"`
		Results  json.RawMessage `json:"results"`
	}
	if err := json.Unmarshal(b, &head); err != nil {
		return RunResults{}, malformed(RunResultsFile, err)
	}
	if head.Metadata == nil {
		return RunResults{}, &VersionError{File: RunResultsFile, Supported: supportedRunResults}
	}
	if err := checkVersion(RunResultsFile, kindRunResults, head.Metadata.Version, supportedRunResults); err != nil {
		return RunResults{}, err
	}
	if head.Results == nil || string(head.Results) == "null" {
		return RunResults{}, malformed(RunResultsFile, errors.New("no results array"))
	}
	var raw []struct {
		UniqueID        string  `json:"unique_id"`
		Status          string  `json:"status"`
		Message         *string `json:"message"` // null for a skipped node
		ExecutionTime   float64 `json:"execution_time"`
		AdapterResponse struct {
			RowsAffected *int64 `json:"rows_affected"`
		} `json:"adapter_response"`
	}
	if err := json.Unmarshal(head.Results, &raw); err != nil {
		return RunResults{}, malformed(RunResultsFile, err)
	}
	out := RunResults{GeneratedAt: head.Metadata.GeneratedAt, Results: make([]ModelResult, 0, len(raw))}
	for i, r := range raw {
		if r.UniqueID == "" {
			return RunResults{}, malformed(RunResultsFile, fmt.Errorf("results[%d] has no unique_id", i))
		}
		m := ModelResult{Model: r.UniqueID, Status: r.Status, Rows: -1, Seconds: r.ExecutionTime}
		if r.Message != nil {
			m.Message = *r.Message
		}
		if r.AdapterResponse.RowsAffected != nil {
			m.Rows = *r.AdapterResponse.RowsAffected
		}
		out.Results = append(out.Results, m)
	}
	return out, nil
}

// manifest is the part of manifest.json lineage and model SQL need.
type manifest struct {
	Nodes     map[string]manifestNode `json:"nodes"`
	ParentMap map[string][]string     `json:"parent_map"`
	ChildMap  map[string][]string     `json:"child_map"`
}

type manifestNode struct {
	Name         string `json:"name"`
	ResourceType string `json:"resource_type"`
	Path         string `json:"original_file_path"`
	Language     string `json:"language"`
	// CompiledCode is absent (nil) until dbt compiled the node: a manifest
	// written by "dbt parse" has raw_code only.
	CompiledCode *string `json:"compiled_code"`
	RawCode      string  `json:"raw_code"`
}

// parseManifest decodes manifest.json, version first (see parseRunResults).
func parseManifest(b []byte) (manifest, error) {
	var head struct {
		Metadata *metadata `json:"metadata"`
	}
	if err := json.Unmarshal(b, &head); err != nil {
		return manifest{}, malformed(ManifestFile, err)
	}
	if head.Metadata == nil {
		return manifest{}, &VersionError{File: ManifestFile, Supported: supportedManifest}
	}
	if err := checkVersion(ManifestFile, kindManifest, head.Metadata.Version, supportedManifest); err != nil {
		return manifest{}, err
	}
	var m manifest
	if err := json.Unmarshal(b, &m); err != nil {
		return manifest{}, malformed(ManifestFile, err)
	}
	// dbt writes both maps (they are null only in a manifest from a partial
	// parse); without them there is no lineage to give.
	if m.ParentMap == nil || m.ChildMap == nil {
		return manifest{}, malformed(ManifestFile, errors.New("no parent_map or child_map"))
	}
	return m, nil
}

// lineage resolves model, a unique_id ("model.analytics.fct_revenue", also a
// source or seed id) or a model's short name ("fct_revenue"), and returns its
// direct parents and children, sorted.
func (m manifest) lineage(model string) (Lineage, error) {
	model = strings.TrimSpace(model)
	if model == "" {
		return Lineage{}, fmt.Errorf("dbt: model lineage: empty model name")
	}
	id, err := m.resolve(model)
	if err != nil {
		return Lineage{}, err
	}
	return Lineage{Model: id, Parents: sortedCopy(m.ParentMap[id]), Children: sortedCopy(m.ChildMap[id])}, nil
}

// modelSQL resolves model like lineage and returns its compiled SQL, or its
// raw code when the manifest has none compiled. A node that is not a model
// (a source, seed or test unique_id) is clients.ErrNotFound.
func (m manifest) modelSQL(model string) (ModelSQL, error) {
	model = strings.TrimSpace(model)
	if model == "" {
		return ModelSQL{}, fmt.Errorf("dbt: model sql: empty model name")
	}
	id, err := m.resolve(model)
	if err != nil {
		return ModelSQL{}, err
	}
	n, ok := m.Nodes[id]
	if !ok || n.ResourceType != "model" {
		return ModelSQL{}, fmt.Errorf("dbt: %q is not a model in %s: %w", errorText(model, maxErrorText), ManifestFile, clients.ErrNotFound)
	}
	out := ModelSQL{Model: id, Path: n.Path, Language: n.Language, SQL: n.RawCode}
	if n.CompiledCode != nil {
		out.Compiled, out.SQL = true, *n.CompiledCode
	}
	return out, nil
}

func (m manifest) resolve(model string) (string, error) {
	if _, ok := m.ParentMap[model]; ok {
		return model, nil
	}
	if _, ok := m.ChildMap[model]; ok {
		return model, nil
	}
	var ids []string
	for id, n := range m.Nodes {
		if n.ResourceType == "model" && n.Name == model {
			ids = append(ids, id)
		}
	}
	switch len(ids) {
	case 0:
		return "", fmt.Errorf("dbt: model %q: %w", errorText(model, maxErrorText), clients.ErrNotFound)
	case 1:
		return ids[0], nil
	}
	slices.Sort(ids)
	return "", &AmbiguousError{Name: model, Candidates: ids}
}

// sortedCopy never returns nil (the contract's Lineage has no nil slices).
func sortedCopy(s []string) []string {
	out := make([]string, len(s))
	copy(out, s)
	slices.Sort(out)
	return out
}

// errorText makes untrusted text safe for one error line: control characters
// become spaces, space runs collapse, and the result is cut to at most limit
// bytes on a rune boundary, ending in "…" when cut.
func errorText(s string, limit int) string {
	s = strings.Join(strings.Fields(strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, s)), " ")
	if len(s) <= limit {
		return s
	}
	const marker = "…"
	cut := limit - len(marker)
	for cut > 0 && !utf8.RuneStart(s[cut]) {
		cut--
	}
	return s[:cut] + marker
}
