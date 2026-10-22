package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/events"
)

// Limits of the connector config versions (X5c, migration 00020) beyond the
// contract's caps (clients.MaxConfigKeys, clients.MaxConfigVersionList).
const (
	// MaxConfigVersionsPerConnector is how many versions one connector keeps;
	// the oldest go when a newer one is written, so a connector whose config
	// flaps every poll cannot grow the table between two retention runs.
	MaxConfigVersionsPerConnector = 1000
	// maxConfigVersionName bounds a pipeline id, a connector name and By.
	maxConfigVersionName = 512
	// configVersionByPrefix starts ConnectorConfigVersion.By when it is set.
	configVersionByPrefix = "proposal:"
)

// ConfigVersionStore is the connector config history (X5c): the two client
// interfaces plus the age-based purge the change-log retention loop calls
// (PIPEROOM_CHANGES_RETENTION_DAYS). Both *PG and memstore.Store implement
// it. It is separate from Store like ChangeStore.
type ConfigVersionStore interface {
	clients.ConfigVersionWriter
	clients.ConfigVersionReader
	// PurgeConfigVersionsBefore deletes every version whose at is before
	// cutoff, in all pipelines, except the newest version of each connector
	// (the current config stays diffable), and returns how many.
	PurgeConfigVersionsBefore(ctx context.Context, cutoff time.Time) (int64, error)
}

var _ ConfigVersionStore = (*PG)(nil)

// ErrSecretValue is returned (wrapped) for a config version in which an
// actions.LooksSecret key holds anything but actions.RedactedValue: the
// caller did not pass actions.RedactConfig's form, and nothing is written.
// The error names the key, never the value.
var ErrSecretValue = errors.New("store: config version holds a value for a credential key")

// ErrConfigVersion is returned (wrapped) for any other config version the
// store refuses before I/O: no pipeline, connector or time, a By that is not
// "proposal:<id>", too many keys, a value longer than
// actions.MaxConfigValueBytes, or a list limit outside 1..MaxConfigVersionList.
var ErrConfigVersion = errors.New("store: bad config version")

// NormalizeConfigVersion checks v the way both stores do and returns the form
// they keep: At in UTC cut to microseconds, Config passed through a JSON
// round trip (so invalid UTF-8 reads back as stored), Hash recomputed as
// actions.ConfigHash(Config) (a caller's Hash is ignored), ChangedKeys sorted,
// without repeats, never nil, and with every name actions.LooksSecret matches
// replaced by events.SecretKeyChanged. A credential key holding anything but
// actions.RedactedValue is ErrSecretValue; the other refusals are
// ErrConfigVersion or ErrUnstorable.
func NormalizeConfigVersion(v clients.ConnectorConfigVersion) (clients.ConnectorConfigVersion, error) {
	switch {
	case v.PipelineID == "" || v.Connector == "":
		return v, fmt.Errorf("%w: needs a pipeline id and a connector", ErrConfigVersion)
	case len(v.PipelineID) > maxConfigVersionName || len(v.Connector) > maxConfigVersionName || len(v.By) > maxConfigVersionName:
		return v, fmt.Errorf("%w: pipeline id, connector or by longer than %d bytes", ErrConfigVersion, maxConfigVersionName)
	case v.At.IsZero():
		return v, fmt.Errorf("%w: needs an at time", ErrConfigVersion)
	case v.By != "" && (!strings.HasPrefix(v.By, configVersionByPrefix) || len(v.By) == len(configVersionByPrefix)):
		return v, fmt.Errorf("%w: by must be empty or proposal:<id>", ErrConfigVersion)
	case len(v.Config) > clients.MaxConfigKeys || len(v.ChangedKeys) > clients.MaxConfigKeys:
		return v, fmt.Errorf("%w: more than %d keys", ErrConfigVersion, clients.MaxConfigKeys)
	}
	if err := CheckText(v.PipelineID, v.Connector, v.By); err != nil {
		return v, err
	}
	for k, val := range v.Config {
		if actions.LooksSecret(k) && val != actions.RedactedValue {
			return v, fmt.Errorf("%w: key %q", ErrSecretValue, clip(k))
		}
		if len(val) > actions.MaxConfigValueBytes {
			return v, fmt.Errorf("%w: the value of key %q is over %d bytes (not actions.RedactConfig's form)", ErrConfigVersion, clip(k), actions.MaxConfigValueBytes)
		}
	}
	cfg, err := roundTripConfig(v.Config)
	if err != nil {
		return v, err
	}
	keys := make([]string, 0, len(v.ChangedKeys))
	for _, k := range v.ChangedKeys {
		if actions.LooksSecret(k) {
			k = events.SecretKeyChanged
		}
		keys = append(keys, strings.ToValidUTF8(k, "�"))
	}
	slices.Sort(keys)
	v.ChangedKeys = slices.Compact(keys)
	v.Config = cfg
	v.Hash = actions.ConfigHash(cfg)
	v.At = v.At.UTC().Truncate(time.Microsecond)
	return v, nil
}

// clip cuts a key name for an error message.
func clip(k string) string {
	if len(k) > 80 {
		return strings.ToValidUTF8(k[:80], "") + "…"
	}
	return k
}

// roundTripConfig is cfg as it reads back from the JSON text column: never
// nil, invalid UTF-8 replaced the way encoding/json writes it. The hash of
// the result equals actions.ConfigHash(cfg).
func roundTripConfig(cfg map[string]string) (map[string]string, error) {
	b, err := json.Marshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("%w: encode config: %w", ErrConfigVersion, err)
	}
	out := map[string]string{}
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, fmt.Errorf("%w: decode config: %w", ErrConfigVersion, err)
	}
	return out, nil
}

// CheckConfigVersionLimit refuses a list limit outside 1..MaxConfigVersionList.
func CheckConfigVersionLimit(limit int) error {
	if limit < 1 || limit > clients.MaxConfigVersionList {
		return fmt.Errorf("%w: limit %d is not 1..%d", ErrConfigVersion, limit, clients.MaxConfigVersionList)
	}
	return nil
}

// RecordConfigVersion implements clients.ConfigVersionWriter: insert unless
// the connector's newest version (by at, then id) has the same hash and v has
// no ChangedKeys, then drop the connector's versions beyond the newest
// MaxConfigVersionsPerConnector. Both statements share a transaction. Two
// writers racing on one connector (a watcher leadership change) can both
// insert the same version; the list then shows it twice, which is harmless.
func (s *PG) RecordConfigVersion(ctx context.Context, v clients.ConnectorConfigVersion) (bool, error) {
	v, err := NormalizeConfigVersion(v)
	if err != nil {
		return false, err
	}
	cfg, err := json.Marshal(v.Config)
	if err != nil {
		return false, fmt.Errorf("store: record config version: %w", err)
	}
	keys, err := json.Marshal(v.ChangedKeys)
	if err != nil {
		return false, fmt.Errorf("store: record config version: %w", err)
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("store: record config version: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var id int64
	err = tx.QueryRow(ctx,
		`INSERT INTO connector_config_versions (pipeline_id, connector, hash, at, actor, changed_keys, config)
		 SELECT $1::text, $2::text, $3::text, $4::timestamptz, $5::text, $6::text, $7::text
		 WHERE $8::boolean OR NOT EXISTS (
		   SELECT 1 FROM (
		     SELECT hash FROM connector_config_versions
		     WHERE pipeline_id = $1 AND connector = $2
		     ORDER BY at DESC, id DESC LIMIT 1) latest
		   WHERE latest.hash = $3)
		 RETURNING id`,
		v.PipelineID, v.Connector, v.Hash, v.At, v.By, string(keys), string(cfg), len(v.ChangedKeys) > 0).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: record config version: %w", classify(err))
	}
	if _, err := tx.Exec(ctx,
		`DELETE FROM connector_config_versions WHERE id IN (
		   SELECT id FROM connector_config_versions
		   WHERE pipeline_id = $1 AND connector = $2
		   ORDER BY at DESC, id DESC OFFSET $3)`,
		v.PipelineID, v.Connector, MaxConfigVersionsPerConnector); err != nil {
		return false, fmt.Errorf("store: cap config versions: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("store: record config version: %w", err)
	}
	return true, nil
}

// ConfigVersions implements clients.ConfigVersionReader: newest first by (at,
// id), at most limit, Config empty. None is an empty list.
func (s *PG) ConfigVersions(ctx context.Context, pipelineID, connector string, limit int) ([]clients.ConnectorConfigVersion, error) {
	if err := CheckConfigVersionLimit(limit); err != nil {
		return nil, err
	}
	if err := CheckText(pipelineID, connector); err != nil {
		return nil, err
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	rows, err := s.pool.Query(ctx,
		`SELECT hash, at, actor, changed_keys FROM connector_config_versions
		 WHERE pipeline_id = $1 AND connector = $2
		 ORDER BY at DESC, id DESC LIMIT $3`,
		pipelineID, connector, limit)
	if err != nil {
		return nil, fmt.Errorf("store: list config versions: %w", err)
	}
	defer rows.Close()
	out := []clients.ConnectorConfigVersion{}
	for rows.Next() {
		v := clients.ConnectorConfigVersion{PipelineID: pipelineID, Connector: connector}
		var keys string
		if err := rows.Scan(&v.Hash, &v.At, &v.By, &keys); err != nil {
			return nil, fmt.Errorf("store: scan config version: %w", err)
		}
		if err := json.Unmarshal([]byte(keys), &v.ChangedKeys); err != nil || v.ChangedKeys == nil {
			v.ChangedKeys = []string{}
		}
		v.At = v.At.UTC()
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list config versions: %w", err)
	}
	return out, nil
}

// ConfigVersion implements clients.ConfigVersionReader: the newest version
// of the connector with that hash, Config filled; none is clients.ErrNotFound.
func (s *PG) ConfigVersion(ctx context.Context, pipelineID, connector, hash string) (clients.ConnectorConfigVersion, error) {
	if err := CheckText(pipelineID, connector, hash); err != nil {
		return clients.ConnectorConfigVersion{}, err
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	v := clients.ConnectorConfigVersion{PipelineID: pipelineID, Connector: connector}
	var keys, cfg string
	err := s.pool.QueryRow(ctx,
		`SELECT hash, at, actor, changed_keys, config FROM connector_config_versions
		 WHERE pipeline_id = $1 AND connector = $2 AND hash = $3
		 ORDER BY at DESC, id DESC LIMIT 1`,
		pipelineID, connector, hash).Scan(&v.Hash, &v.At, &v.By, &keys, &cfg)
	if errors.Is(err, pgx.ErrNoRows) {
		return v, fmt.Errorf("store: config version %s of %s/%s: %w", clip(hash), pipelineID, connector, clients.ErrNotFound)
	}
	if err != nil {
		return v, fmt.Errorf("store: get config version: %w", err)
	}
	if err := json.Unmarshal([]byte(keys), &v.ChangedKeys); err != nil || v.ChangedKeys == nil {
		v.ChangedKeys = []string{}
	}
	if err := json.Unmarshal([]byte(cfg), &v.Config); err != nil {
		return v, fmt.Errorf("store: decode config version: %w", err)
	}
	if v.Config == nil {
		v.Config = map[string]string{}
	}
	v.At = v.At.UTC()
	return v, nil
}

// PurgeConfigVersionsBefore implements ConfigVersionStore.
func (s *PG) PurgeConfigVersionsBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx,
		`DELETE FROM connector_config_versions v
		 WHERE v.at < $1 AND EXISTS (
		   SELECT 1 FROM connector_config_versions n
		   WHERE n.pipeline_id = v.pipeline_id AND n.connector = v.connector
		     AND (n.at, n.id) > (v.at, v.id))`,
		cutoff.UTC())
	if err != nil {
		return 0, fmt.Errorf("store: purge config versions: %w", err)
	}
	return tag.RowsAffected(), nil
}
