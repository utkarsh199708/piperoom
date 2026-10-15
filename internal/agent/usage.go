package agent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"

	"github.com/anthropics/anthropic-sdk-go"

	"piperoom/internal/events"
)

// maxPricesFileBytes caps the PIPEROOM_MODEL_PRICES file; a price table is a
// few hundred bytes.
const maxPricesFileBytes = 1 << 20

// maxPricePerMTok rejects a price that is surely a typo (USD per million
// tokens), and keeps every cost far inside an int64 of millionths of a USD.
const maxPricePerMTok = 1_000_000

// ModelPrice is one model's price in USD per million tokens
// (docs/design/cost.md 2.1). CacheWrite is the 5-minute cache write price.
type ModelPrice struct {
	Input      float64 `json:"input"`
	Output     float64 `json:"output"`
	CacheWrite float64 `json:"cache_write"`
	CacheRead  float64 `json:"cache_read"`
}

// Prices maps a model id, exactly as a request names it, to its price. A nil
// table prices nothing, so every cost is omitted.
type Prices map[string]ModelPrice

// Has reports whether the table prices model.
func (p Prices) Has(model string) bool {
	_, ok := p[model]
	return ok
}

// LoadModelPrices reads the PIPEROOM_MODEL_PRICES file. An empty path means no
// table (nil, nil). A file that is not a valid table is an error, so a typo
// cannot silently drop or zero a price.
func LoadModelPrices(path string) (Prices, error) {
	if path == "" {
		return nil, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("agent: PIPEROOM_MODEL_PRICES: %w", err)
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, maxPricesFileBytes+1))
	if err != nil {
		return nil, fmt.Errorf("agent: PIPEROOM_MODEL_PRICES: read %s: %w", path, err)
	}
	if len(b) > maxPricesFileBytes {
		return nil, fmt.Errorf("agent: PIPEROOM_MODEL_PRICES: %s is larger than %d bytes", path, maxPricesFileBytes)
	}
	p, err := ParseModelPrices(b)
	if err != nil {
		return nil, fmt.Errorf("agent: PIPEROOM_MODEL_PRICES %s: %w", path, err)
	}
	return p, nil
}

// rawPrice detects a missing field: a price left out must not read as 0.
type rawPrice struct {
	Input      *float64 `json:"input"`
	Output     *float64 `json:"output"`
	CacheWrite *float64 `json:"cache_write"`
	CacheRead  *float64 `json:"cache_read"`
}

// ParseModelPrices parses a price table: a JSON object of model id to an
// object with exactly input, output, cache_write and cache_read, each a
// finite number >= 0.
func ParseModelPrices(b []byte) (Prices, error) {
	// encoding/json keeps the last of two equal keys; a price given twice is
	// a typo that must not pick a winner silently.
	if err := noDuplicateKeys(b); err != nil {
		return nil, err
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()
	var raw map[string]rawPrice
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("not a price table: %w", err)
	}
	if dec.More() {
		return nil, errors.New("not a price table: data after the object")
	}
	if len(raw) == 0 {
		return nil, errors.New("the table names no model")
	}
	out := make(Prices, len(raw))
	for model, r := range raw {
		if model == "" {
			return nil, errors.New("a model id is empty")
		}
		fields := []struct {
			name string
			v    *float64
		}{{"input", r.Input}, {"output", r.Output}, {"cache_write", r.CacheWrite}, {"cache_read", r.CacheRead}}
		for _, f := range fields {
			switch {
			case f.v == nil:
				return nil, fmt.Errorf("model %q: %s is missing", model, f.name)
			case math.IsNaN(*f.v) || *f.v < 0 || *f.v > maxPricePerMTok:
				return nil, fmt.Errorf("model %q: %s must be between 0 and %d USD per million tokens", model, f.name, maxPricePerMTok)
			}
		}
		out[model] = ModelPrice{Input: *r.Input, Output: *r.Output, CacheWrite: *r.CacheWrite, CacheRead: *r.CacheRead}
	}
	return out, nil
}

// noDuplicateKeys reports the first object key that appears twice in the same
// JSON object, at any depth. Malformed JSON is left to the decoder.
func noDuplicateKeys(b []byte) error {
	dec := json.NewDecoder(bytes.NewReader(b))
	// encoding/json matches field names ignoring case, so inside a model's
	// object "Input" and "input" are the same price. Model ids stay exact.
	var walk func(path string, depth int) error
	walk = func(path string, depth int) error {
		tok, err := dec.Token()
		if err != nil {
			return nil
		}
		switch tok {
		case json.Delim('{'):
			seen := map[string]bool{}
			for dec.More() {
				kt, err := dec.Token()
				if err != nil {
					return nil
				}
				k, _ := kt.(string)
				if depth > 0 {
					k = strings.ToLower(k)
				}
				if seen[k] {
					return fmt.Errorf("key %q appears twice%s", k, path)
				}
				seen[k] = true
				if err := walk(fmt.Sprintf(" in %q", k), depth+1); err != nil {
					return err
				}
			}
			_, _ = dec.Token() // the closing brace
		case json.Delim('['):
			for dec.More() {
				if err := walk(path, depth); err != nil {
					return err
				}
			}
			_, _ = dec.Token()
		}
		return nil
	}
	return walk("", 0)
}

// callTokens is the four token counts of one request, or of its compaction
// pass.
type callTokens struct {
	input, cacheCreation, cacheRead, output int64
}

func (c callTokens) total() int64 { return c.input + c.cacheCreation + c.cacheRead + c.output }

func (c callTokens) plus(o callTokens) callTokens {
	return callTokens{c.input + o.input, c.cacheCreation + o.cacheCreation, c.cacheRead + o.cacheRead, c.output + o.output}
}

// splitUsage separates a response's own usage from its compaction passes. The
// API reports compaction iterations outside the top-level fields, and
// messageUsage counts both, so the two always add up to messageUsage.
func splitUsage(m anthropic.BetaMessage) (own, compaction callTokens, compacted bool) {
	u := m.Usage
	own = callTokens{u.InputTokens, u.CacheCreationInputTokens, u.CacheReadInputTokens, u.OutputTokens}
	for _, it := range u.Iterations {
		if it.Type == "compaction" {
			compacted = true
			compaction = compaction.plus(callTokens{it.InputTokens, it.CacheCreationInputTokens, it.CacheReadInputTokens, it.OutputTokens})
		}
	}
	return own, compaction, compacted
}

// costMicros is the estimated cost of c on model in millionths of a USD. A
// price per million tokens is exactly a price in millionths per token. ok is
// false when the table has no price for model; the cost is then unknown, not
// 0.
func (p Prices) costMicros(model string, c callTokens) (int64, bool) {
	mp, ok := p[model]
	if !ok {
		return 0, false
	}
	v := float64(c.input)*mp.Input + float64(c.cacheCreation)*mp.CacheWrite +
		float64(c.cacheRead)*mp.CacheRead + float64(c.output)*mp.Output
	return int64(math.Round(v)), true
}

// CostUSD is the estimated cost of one request's four token counts on model,
// rounded to whole millionths of a USD like the agent.usage event. ok is false
// when the table has no price for model.
func (p Prices) CostUSD(model string, input, cacheCreation, cacheRead, output int64) (usd float64, ok bool) {
	micros, ok := p.costMicros(model, callTokens{input, cacheCreation, cacheRead, output})
	return float64(micros) / 1e6, ok
}

// estimateMicros is the least an estimate of est input tokens can cost on
// model: every token at the cache read price, the lowest input rate. 0 when
// the model has no price.
func (p Prices) estimateMicros(model string, est int64) int64 {
	mp, ok := p[model]
	if !ok || est <= 0 {
		return 0
	}
	return int64(math.Round(float64(est) * mp.CacheRead))
}

// usagePayloads builds the agent.usage events of one sent request: one for the
// request with role, and one more for its compaction pass when it ran one.
// They carry counts and the model id only, never request or response text.
func usagePayloads(model, role string, m anthropic.BetaMessage, took time.Duration, prices Prices) []events.AgentUsagePayload {
	own, comp, compacted := splitUsage(m)
	out := []events.AgentUsagePayload{usagePayload(model, role, own, took.Milliseconds(), prices)}
	if compacted {
		// The pass runs inside the same request; its time is in the request's.
		out = append(out, usagePayload(model, events.UsageRoleCompaction, comp, 0, prices))
	}
	return out
}

func usagePayload(model, role string, c callTokens, latencyMS int64, prices Prices) events.AgentUsagePayload {
	p := events.AgentUsagePayload{
		Model:                    model,
		Role:                     role,
		InputTokens:              c.input,
		CacheCreationInputTokens: c.cacheCreation,
		CacheReadInputTokens:     c.cacheRead,
		OutputTokens:             c.output,
		LatencyMS:                latencyMS,
	}
	if micros, ok := prices.costMicros(model, c); ok {
		usd := float64(micros) / 1e6
		p.CostUSD = &usd
	}
	return p
}

// usageEmitTimeout bounds writing one agent.usage event.
const usageEmitTimeout = 5 * time.Second

// detached returns a context that outlives ctx, bounded by usageEmitTimeout:
// a request still in flight when the room closes was billed, so its
// agent.usage event must still be written, or the log and the spend tables
// would disagree.
func detached(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), usageEmitTimeout)
}

// errNotSent marks an error returned before a request reached the API, so no
// agent.usage is reported for it. A *BudgetError means the same.
var errNotSent = errors.New("agent: request not sent")

// wasSent reports whether a model call that returned err reached the API.
func wasSent(err error) bool {
	if _, refused := asBudgetError(err); refused {
		return false
	}
	return !errors.Is(err, errNotSent)
}

// reportingModel reports the usage of every request it sends through report.
// The hand-off uses it: the investigation loop reports from runner.callModel,
// which knows each turn's role.
type reportingModel struct {
	inner  model
	role   string
	prices Prices
	report func(ctx context.Context, p events.AgentUsagePayload)
}

func (m *reportingModel) Turn(ctx context.Context, params anthropic.BetaMessageNewParams) (anthropic.BetaMessage, error) {
	start := time.Now()
	msg, err := m.inner.Turn(ctx, params)
	if wasSent(err) {
		ectx, cancel := detached(ctx)
		defer cancel()
		for _, p := range usagePayloads(params.Model, m.role, msg, time.Since(start), m.prices) {
			m.report(ectx, p)
		}
	}
	return msg, err
}
