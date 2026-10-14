package memstore

import (
	"context"
	"fmt"

	"piperoom/internal/store"
)

var _ store.SpendStore = (*Store)(nil)

// spendTotal is a charged total: room_spend or pipeline_spend_daily.
type spendTotal struct {
	tokens, costMicros int64
}

// pipelineDay keys the pipeline tables.
type pipelineDay struct {
	pipelineID, day string
}

// spendTables is the in-memory form of migration 00013. All of it sits under
// Store.mu, so one AddSpend is atomic like the Postgres transaction.
type spendTables struct {
	room           map[string]*spendTotal
	roomModels     map[string]map[string]*store.SpendUsage
	pipeline       map[pipelineDay]*spendTotal
	pipelineModels map[pipelineDay]map[string]*store.SpendUsage
}

func newSpendTables() spendTables {
	return spendTables{
		room:           map[string]*spendTotal{},
		roomModels:     map[string]map[string]*store.SpendUsage{},
		pipeline:       map[pipelineDay]*spendTotal{},
		pipelineModels: map[pipelineDay]map[string]*store.SpendUsage{},
	}
}

// AddSpend implements store.SpendStore with the Postgres semantics: the room
// must exist (the foreign key), the change is added to both totals and, for a
// finished request, to both breakdowns, all under one lock.
func (s *Store) AddSpend(ctx context.Context, c store.SpendChange) (store.SpendTotals, error) {
	if err := ctx.Err(); err != nil {
		return store.SpendTotals{}, err
	}
	if err := c.Check(); err != nil {
		return store.SpendTotals{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.rooms[c.RoomID]; !ok {
		return store.SpendTotals{}, fmt.Errorf("memstore: add spend: room %q: %w", c.RoomID, store.ErrNotFound)
	}
	sp := &s.spend
	rt := sp.room[c.RoomID]
	if rt == nil {
		rt = &spendTotal{}
		sp.room[c.RoomID] = rt
	}
	key := pipelineDay{c.PipelineID, c.Day}
	pt := sp.pipeline[key]
	if pt == nil {
		pt = &spendTotal{}
		sp.pipeline[key] = pt
	}
	rt.tokens += c.Tokens
	rt.costMicros += c.CostMicros
	pt.tokens += c.Tokens
	pt.costMicros += c.CostMicros
	if c.Call != nil {
		u := store.UsageOf(*c.Call)
		addUsage(sp.roomModels, c.RoomID, u)
		addUsage(sp.pipelineModels, key, u)
	}
	return store.SpendTotals{
		RoomTokens: rt.tokens, RoomCostMicros: rt.costMicros,
		PipelineTokens: pt.tokens, PipelineCostMicros: pt.costMicros,
	}, nil
}

func addUsage[K comparable](m map[K]map[string]*store.SpendUsage, key K, u store.SpendUsage) {
	byModel := m[key]
	if byModel == nil {
		byModel = map[string]*store.SpendUsage{}
		m[key] = byModel
	}
	row := byModel[u.Model]
	if row == nil {
		row = &store.SpendUsage{Model: u.Model}
		byModel[u.Model] = row
	}
	row.Add(u)
}

// RoomSpend implements store.SpendStore.
func (s *Store) RoomSpend(ctx context.Context, roomID string) (store.SpendReport, error) {
	if err := ctx.Err(); err != nil {
		return store.SpendReport{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return report(s.spend.room[roomID], s.spend.roomModels[roomID]), nil
}

// PipelineSpend implements store.SpendStore.
func (s *Store) PipelineSpend(ctx context.Context, pipelineID, day string) (store.SpendReport, error) {
	if err := ctx.Err(); err != nil {
		return store.SpendReport{}, err
	}
	if err := store.CheckDay(day); err != nil {
		return store.SpendReport{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := pipelineDay{pipelineID, day}
	return report(s.spend.pipeline[key], s.spend.pipelineModels[key]), nil
}

// report copies a scope out, so callers never share the stored rows.
func report(t *spendTotal, byModel map[string]*store.SpendUsage) store.SpendReport {
	r := store.SpendReport{ByModel: []store.SpendUsage{}}
	if t != nil {
		r.ChargedTokens, r.ChargedCostMicros = t.tokens, t.costMicros
	}
	for _, u := range byModel {
		r.ByModel = append(r.ByModel, *u)
	}
	store.SortByModel(r.ByModel)
	return r
}

// dropRoomSpendLocked removes a purged room's spend, as the Postgres foreign
// key cascade does. Caller holds s.mu.
func (s *Store) dropRoomSpendLocked(roomID string) {
	delete(s.spend.room, roomID)
	delete(s.spend.roomModels, roomID)
}
