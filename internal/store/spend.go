package store

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
)

// SpendStore keeps model spend (docs/design/cost.md section 3.2, migration
// 00013). It is separate from Store like ShadowStore. Both *PG and
// memstore.Store implement it.
type SpendStore interface {
	// AddSpend adds c.Tokens and c.CostMicros to the room's total and to the
	// pipeline's total for c.Day, and c.Call (when set) to both per-model
	// breakdowns, in one transaction. It returns the two totals after the
	// change. The room is locked before the pipeline-day, by every caller. An
	// unknown room is an error (the rooms foreign key).
	AddSpend(ctx context.Context, c SpendChange) (SpendTotals, error)
	// RoomSpend returns the room's charged total and its finished requests by
	// model. A room with no spend yet answers zeros.
	RoomSpend(ctx context.Context, roomID string) (SpendReport, error)
	// PipelineSpend is RoomSpend for one pipeline on one UTC day ("2006-01-02").
	PipelineSpend(ctx context.Context, pipelineID, day string) (SpendReport, error)
}

// SpendChange is one change to a room's and a pipeline-day's spend. Tokens and
// CostMicros may be negative: an estimate taken back or settled lower.
type SpendChange struct {
	RoomID     string
	PipelineID string
	Day        string // UTC date, "2006-01-02"
	Tokens     int64
	CostMicros int64 // millionths of a USD
	Call       *SpendCall
}

// SpendCall is one finished model request for the per-model breakdowns.
type SpendCall struct {
	Model                    string
	InputTokens              int64
	CacheCreationInputTokens int64
	CacheReadInputTokens     int64
	OutputTokens             int64
	CostMicros               int64
	// Priced is false when the model had no price; the request then counts in
	// unpriced_calls and adds no cost.
	Priced bool
}

// SpendTotals are the charged totals after a change.
type SpendTotals struct {
	RoomTokens         int64
	RoomCostMicros     int64
	PipelineTokens     int64
	PipelineCostMicros int64
}

// SpendUsage is one row of a breakdown: the finished requests of one model, or
// their sum.
type SpendUsage struct {
	Model                    string
	Calls                    int64
	UnpricedCalls            int64
	InputTokens              int64
	CacheCreationInputTokens int64
	CacheReadInputTokens     int64
	OutputTokens             int64
	CostMicros               int64
}

// Tokens is the four token counts added up, the unit of the token caps.
func (u SpendUsage) Tokens() int64 {
	return u.InputTokens + u.CacheCreationInputTokens + u.CacheReadInputTokens + u.OutputTokens
}

// Add adds o's counts to u (the model is kept).
func (u *SpendUsage) Add(o SpendUsage) {
	u.Calls += o.Calls
	u.UnpricedCalls += o.UnpricedCalls
	u.InputTokens += o.InputTokens
	u.CacheCreationInputTokens += o.CacheCreationInputTokens
	u.CacheReadInputTokens += o.CacheReadInputTokens
	u.OutputTokens += o.OutputTokens
	u.CostMicros += o.CostMicros
}

// UsageOf is the breakdown row one finished request adds.
func UsageOf(c SpendCall) SpendUsage {
	u := SpendUsage{
		Model:                    c.Model,
		Calls:                    1,
		InputTokens:              c.InputTokens,
		CacheCreationInputTokens: c.CacheCreationInputTokens,
		CacheReadInputTokens:     c.CacheReadInputTokens,
		OutputTokens:             c.OutputTokens,
	}
	if c.Priced {
		u.CostMicros = c.CostMicros
	} else {
		u.UnpricedCalls = 1
	}
	return u
}

// SpendReport is a scope's spend: the charged totals the caps check and the
// finished requests by model, sorted by model id (byte order).
type SpendReport struct {
	ChargedTokens     int64
	ChargedCostMicros int64
	ByModel           []SpendUsage
}

// Totals adds up the breakdown.
func (r SpendReport) Totals() SpendUsage {
	var t SpendUsage
	for _, u := range r.ByModel {
		t.Add(u)
	}
	return t
}

// SortByModel orders a breakdown by model id.
func SortByModel(us []SpendUsage) {
	sort.Slice(us, func(i, j int) bool { return us[i].Model < us[j].Model })
}

// Check is the pre-check of a SpendChange: a room, a valid UTC day, and a
// model for a finished request; text that Postgres can store.
func (c SpendChange) Check() error {
	if c.RoomID == "" {
		return errors.New("store: spend: room id is empty")
	}
	if err := CheckDay(c.Day); err != nil {
		return err
	}
	if err := CheckText(c.RoomID, c.PipelineID); err != nil {
		return err
	}
	if c.Call != nil {
		if c.Call.Model == "" {
			return errors.New("store: spend: model is empty")
		}
		if err := CheckText(c.Call.Model); err != nil {
			return err
		}
	}
	return nil
}

// CheckDay accepts exactly a "2006-01-02" date.
func CheckDay(day string) error {
	t, err := time.Parse(time.DateOnly, day)
	if err != nil || t.Format(time.DateOnly) != day {
		return fmt.Errorf("store: spend: day %q is not a YYYY-MM-DD date", day)
	}
	return nil
}

var _ SpendStore = (*PG)(nil)

// AddSpend implements SpendStore. Each total is one INSERT ... ON CONFLICT DO
// UPDATE that adds the change to the stored value and returns the result, so
// concurrent changes never lose an update; the room row is locked before the
// pipeline-day row in every transaction, so they cannot deadlock.
func (s *PG) AddSpend(ctx context.Context, c SpendChange) (SpendTotals, error) {
	if err := c.Check(); err != nil {
		return SpendTotals{}, err
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	var t SpendTotals
	err := pgx.BeginFunc(ctx, s.pool, func(tx pgx.Tx) error {
		if err := tx.QueryRow(ctx,
			`INSERT INTO room_spend (room_id, tokens, cost_micros) VALUES ($1, $2, $3)
			 ON CONFLICT (room_id) DO UPDATE SET tokens = room_spend.tokens + EXCLUDED.tokens,
			     cost_micros = room_spend.cost_micros + EXCLUDED.cost_micros, updated_at = now()
			 RETURNING tokens, cost_micros`,
			c.RoomID, c.Tokens, c.CostMicros).Scan(&t.RoomTokens, &t.RoomCostMicros); err != nil {
			return fmt.Errorf("room total: %w", err)
		}
		if err := tx.QueryRow(ctx,
			`INSERT INTO pipeline_spend_daily (pipeline_id, day, tokens, cost_micros) VALUES ($1, $2::date, $3, $4)
			 ON CONFLICT (pipeline_id, day) DO UPDATE SET tokens = pipeline_spend_daily.tokens + EXCLUDED.tokens,
			     cost_micros = pipeline_spend_daily.cost_micros + EXCLUDED.cost_micros, updated_at = now()
			 RETURNING tokens, cost_micros`,
			c.PipelineID, c.Day, c.Tokens, c.CostMicros).Scan(&t.PipelineTokens, &t.PipelineCostMicros); err != nil {
			return fmt.Errorf("pipeline total: %w", err)
		}
		if c.Call == nil {
			return nil
		}
		u := UsageOf(*c.Call)
		if _, err := tx.Exec(ctx,
			`INSERT INTO room_spend_models (room_id, model, calls, unpriced_calls, input_tokens,
			     cache_creation_input_tokens, cache_read_input_tokens, output_tokens, cost_micros)
			 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			 ON CONFLICT (room_id, model) DO UPDATE SET
			     calls = room_spend_models.calls + EXCLUDED.calls,
			     unpriced_calls = room_spend_models.unpriced_calls + EXCLUDED.unpriced_calls,
			     input_tokens = room_spend_models.input_tokens + EXCLUDED.input_tokens,
			     cache_creation_input_tokens = room_spend_models.cache_creation_input_tokens + EXCLUDED.cache_creation_input_tokens,
			     cache_read_input_tokens = room_spend_models.cache_read_input_tokens + EXCLUDED.cache_read_input_tokens,
			     output_tokens = room_spend_models.output_tokens + EXCLUDED.output_tokens,
			     cost_micros = room_spend_models.cost_micros + EXCLUDED.cost_micros`,
			c.RoomID, u.Model, u.Calls, u.UnpricedCalls, u.InputTokens,
			u.CacheCreationInputTokens, u.CacheReadInputTokens, u.OutputTokens, u.CostMicros); err != nil {
			return fmt.Errorf("room breakdown: %w", err)
		}
		if _, err := tx.Exec(ctx,
			`INSERT INTO pipeline_spend_daily_models (pipeline_id, day, model, calls, unpriced_calls, input_tokens,
			     cache_creation_input_tokens, cache_read_input_tokens, output_tokens, cost_micros)
			 VALUES ($1, $2::date, $3, $4, $5, $6, $7, $8, $9, $10)
			 ON CONFLICT (pipeline_id, day, model) DO UPDATE SET
			     calls = pipeline_spend_daily_models.calls + EXCLUDED.calls,
			     unpriced_calls = pipeline_spend_daily_models.unpriced_calls + EXCLUDED.unpriced_calls,
			     input_tokens = pipeline_spend_daily_models.input_tokens + EXCLUDED.input_tokens,
			     cache_creation_input_tokens = pipeline_spend_daily_models.cache_creation_input_tokens + EXCLUDED.cache_creation_input_tokens,
			     cache_read_input_tokens = pipeline_spend_daily_models.cache_read_input_tokens + EXCLUDED.cache_read_input_tokens,
			     output_tokens = pipeline_spend_daily_models.output_tokens + EXCLUDED.output_tokens,
			     cost_micros = pipeline_spend_daily_models.cost_micros + EXCLUDED.cost_micros`,
			c.PipelineID, c.Day, u.Model, u.Calls, u.UnpricedCalls, u.InputTokens,
			u.CacheCreationInputTokens, u.CacheReadInputTokens, u.OutputTokens, u.CostMicros); err != nil {
			return fmt.Errorf("pipeline breakdown: %w", err)
		}
		return nil
	})
	if err != nil {
		return SpendTotals{}, fmt.Errorf("store: add spend for room %s: %w", c.RoomID, classify(err))
	}
	return t, nil
}

const spendUsageCols = `model, calls, unpriced_calls, input_tokens, cache_creation_input_tokens,
	cache_read_input_tokens, output_tokens, cost_micros`

// RoomSpend implements SpendStore.
func (s *PG) RoomSpend(ctx context.Context, roomID string) (SpendReport, error) {
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	var r SpendReport
	err := s.pool.QueryRow(ctx, `SELECT tokens, cost_micros FROM room_spend WHERE room_id = $1`, roomID).
		Scan(&r.ChargedTokens, &r.ChargedCostMicros)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return SpendReport{}, fmt.Errorf("store: room spend %s: %w", roomID, err)
	}
	r.ByModel, err = s.spendRows(ctx, `SELECT `+spendUsageCols+` FROM room_spend_models WHERE room_id = $1 ORDER BY model COLLATE "C"`, roomID)
	if err != nil {
		return SpendReport{}, fmt.Errorf("store: room spend %s: %w", roomID, err)
	}
	return r, nil
}

// PipelineSpend implements SpendStore.
func (s *PG) PipelineSpend(ctx context.Context, pipelineID, day string) (SpendReport, error) {
	if err := CheckDay(day); err != nil {
		return SpendReport{}, err
	}
	ctx, cancel := dbCtx(ctx)
	defer cancel()
	var r SpendReport
	err := s.pool.QueryRow(ctx, `SELECT tokens, cost_micros FROM pipeline_spend_daily WHERE pipeline_id = $1 AND day = $2::date`,
		pipelineID, day).Scan(&r.ChargedTokens, &r.ChargedCostMicros)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return SpendReport{}, fmt.Errorf("store: pipeline spend %s %s: %w", pipelineID, day, err)
	}
	r.ByModel, err = s.spendRows(ctx, `SELECT `+spendUsageCols+` FROM pipeline_spend_daily_models
		WHERE pipeline_id = $1 AND day = $2::date ORDER BY model COLLATE "C"`, pipelineID, day)
	if err != nil {
		return SpendReport{}, fmt.Errorf("store: pipeline spend %s %s: %w", pipelineID, day, err)
	}
	return r, nil
}

func (s *PG) spendRows(ctx context.Context, sql string, args ...any) ([]SpendUsage, error) {
	rows, err := s.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []SpendUsage{}
	for rows.Next() {
		var u SpendUsage
		if err := rows.Scan(&u.Model, &u.Calls, &u.UnpricedCalls, &u.InputTokens, &u.CacheCreationInputTokens,
			&u.CacheReadInputTokens, &u.OutputTokens, &u.CostMicros); err != nil {
			return nil, err
		}
		out = append(out, u)
	}
	return out, rows.Err()
}
