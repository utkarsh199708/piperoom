package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/clients"
)

// Limits of the pipeline change log (tasks/F8-what-changed-checklist.md, 1).
const (
	// MaxChangesPerPipeline is how many rows one pipeline keeps; the oldest go
	// when a newer row is written, so a flapping task cannot grow the table.
	MaxChangesPerPipeline = 5000
	// MaxChangeJSONBytes bounds Before and After of one row each.
	MaxChangeJSONBytes = 4096
	// MaxChangeSummaryRunes bounds PipelineChange.Summary.
	MaxChangeSummaryRunes = 300
	// MaxListChanges bounds one ListChanges call.
	MaxListChanges = 1000
)

// ChangeStore is the pipeline change log surface: the two client interfaces
// plus the age-based purge the retention loop calls. Both *PG and
// memstore.Store implement it. It is separate from Store like SpendStore.
type ChangeStore interface {
	clients.ChangeReader
	clients.ChangeWriter
	// PurgeChangesBefore deletes every row whose at is before cutoff, in all
	// pipelines, and returns how many.
	PurgeChangesBefore(ctx context.Context, cutoff time.Time) (int64, error)
}

var _ ChangeStore = (*PG)(nil)

// NormalizeChange validates c and returns the form both stores keep: UTC time
// cut to microseconds (what Postgres keeps), a one-line summary of at most
// MaxChangeSummaryRunes runes without control characters, and Before/After
// replaced by {"truncated": true} when over MaxChangeJSONBytes or not JSON.
// A row without pipeline, kind, object, source or fingerprint is refused: it
// could never be deduplicated or shown.
func NormalizeChange(c clients.PipelineChange) (clients.PipelineChange, error) {
	// clients.ChangeKinds is the one list (X4 added deploy, migration and
	// vendor_incident), so a new kind needs no edit here.
	if !slices.Contains(clients.ChangeKinds(), c.Kind) {
		return c, fmt.Errorf("store: change kind %q is not one of the known kinds", c.Kind)
	}
	if c.PipelineID == "" || c.Object == "" || c.Source == "" || c.Fingerprint == "" {
		return c, errors.New("store: change needs pipeline_id, object, source and fingerprint")
	}
	if c.At.IsZero() {
		return c, errors.New("store: change needs an at time")
	}
	c.At = c.At.UTC().Truncate(time.Microsecond)
	c.Summary = cleanSummary(c.Summary)
	c.Actor = cleanSummary(c.Actor)
	c.Before = capChangeJSON(c.Before)
	c.After = capChangeJSON(c.After)
	return c, nil
}

// cleanSummary removes control characters (a newline becomes a space) and cuts
// to MaxChangeSummaryRunes runes. The text came from a customer system.
func cleanSummary(s string) string {
	s = strings.Map(func(r rune) rune {
		switch {
		case r == '\n' || r == '\r' || r == '\t':
			return ' '
		case unicode.IsControl(r):
			return -1
		}
		return r
	}, s)
	if r := []rune(s); len(r) > MaxChangeSummaryRunes {
		s = string(r[:MaxChangeSummaryRunes])
	}
	return s
}

var truncatedJSON = json.RawMessage(`{"truncated":true}`)

func capChangeJSON(b json.RawMessage) json.RawMessage {
	if len(b) == 0 {
		return nil
	}
	if len(b) > MaxChangeJSONBytes || !json.Valid(b) {
		return truncatedJSON
	}
	return b
}

// RecordChange implements clients.ChangeWriter: insert unless the same
// (pipeline, kind, object, fingerprint) exists, then drop the pipeline's rows
// beyond the newest MaxChangesPerPipeline. Both statements share a transaction.
func (s *PG) RecordChange(ctx context.Context, c clients.PipelineChange) (bool, error) {
	c, err := NormalizeChange(c)
	if err != nil {
		return false, err
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return false, fmt.Errorf("store: record change: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var id int64
	err = tx.QueryRow(ctx,
		`INSERT INTO pipeline_changes (pipeline_id, kind, object, at, actor, source, fingerprint, before, after, summary)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		 ON CONFLICT (pipeline_id, kind, object, fingerprint) DO NOTHING
		 RETURNING id`,
		c.PipelineID, c.Kind, c.Object, c.At, c.Actor, c.Source, c.Fingerprint, nullJSON(c.Before), nullJSON(c.After), c.Summary).Scan(&id)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("store: record change: %w", err)
	}
	if _, err := tx.Exec(ctx,
		`DELETE FROM pipeline_changes WHERE id IN (
		   SELECT id FROM pipeline_changes WHERE pipeline_id = $1
		   ORDER BY at DESC, id DESC OFFSET $2)`, c.PipelineID, MaxChangesPerPipeline); err != nil {
		return false, fmt.Errorf("store: cap changes: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return false, fmt.Errorf("store: record change: %w", err)
	}
	return true, nil
}

// ListChanges implements clients.ChangeReader. A zero since or until leaves
// that side open; since is inclusive, until exclusive.
func (s *PG) ListChanges(ctx context.Context, pipelineID string, since, until time.Time, kinds []string, limit int) ([]clients.PipelineChange, error) {
	limit = clampChangeLimit(limit)
	if limit == 0 {
		return []clients.PipelineChange{}, nil
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	var sinceArg, untilArg any
	if !since.IsZero() {
		sinceArg = since.UTC()
	}
	if !until.IsZero() {
		untilArg = until.UTC()
	}
	rows, err := s.pool.Query(ctx,
		`SELECT id, pipeline_id, kind, object, at, actor, source, fingerprint, before, after, summary
		 FROM pipeline_changes
		 WHERE pipeline_id = $1
		   AND ($2::timestamptz IS NULL OR at >= $2)
		   AND ($3::timestamptz IS NULL OR at < $3)
		   AND (cardinality($4::text[]) = 0 OR kind = ANY($4))
		 ORDER BY at DESC, id DESC LIMIT $5`,
		pipelineID, sinceArg, untilArg, append([]string{}, kinds...), limit)
	if err != nil {
		return nil, fmt.Errorf("store: list changes: %w", err)
	}
	defer rows.Close()
	out := []clients.PipelineChange{}
	for rows.Next() {
		var c clients.PipelineChange
		var before, after []byte
		if err := rows.Scan(&c.ID, &c.PipelineID, &c.Kind, &c.Object, &c.At, &c.Actor, &c.Source, &c.Fingerprint, &before, &after, &c.Summary); err != nil {
			return nil, fmt.Errorf("store: scan change: %w", err)
		}
		c.At = c.At.UTC()
		if len(before) > 0 {
			c.Before = json.RawMessage(before)
		}
		if len(after) > 0 {
			c.After = json.RawMessage(after)
		}
		out = append(out, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("store: list changes: %w", err)
	}
	return out, nil
}

// ClampChangeLimit bounds a ListChanges limit to 0..MaxListChanges.
func ClampChangeLimit(limit int) int { return clampChangeLimit(limit) }

func clampChangeLimit(limit int) int {
	switch {
	case limit <= 0:
		return 0
	case limit > MaxListChanges:
		return MaxListChanges
	}
	return limit
}

// PurgeChangesBefore implements ChangeStore.
func (s *PG) PurgeChangesBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	tag, err := s.pool.Exec(ctx, `DELETE FROM pipeline_changes WHERE at < $1`, cutoff.UTC())
	if err != nil {
		return 0, fmt.Errorf("store: purge changes: %w", err)
	}
	return tag.RowsAffected(), nil
}
