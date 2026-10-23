package owners

import (
	"fmt"
	"slices"

	"piperoom/internal/events"
)

// This file holds the per-pipeline `source_kind:` of owners.yaml (Q8d,
// docs/design/mysql-source.md §4.2): which database a cdc pipeline streams
// from.
//
//	Pipeline.SourceKind / SourceKindOrDefault   postgres (the default) or mysql
//	Pipeline.SourceLayer                        the layer of the source database
//	Pipeline.Layers                             the source layer at index 0 (kind.go)
//
// The source kinds, the mysql layer, SourceLayer and LayersOfSource are Q0
// item 7 contract items (design §5.1) that have not landed in
// internal/events yet. They are mirrored here under the design's names
// (SourceKindPostgres, SourceKindMySQL, LayerMySQL, and the unexported
// sourceLayer and layersOfSource), so the contract PR only turns them into
// the events ones (Q8d's contract change request). Until then the mysql
// layer is known to this package through KnownLayers, not events.Layers.

// Source kinds of a cdc pipeline (design §5.1; the same strings as
// events.SetupRequest.SourceKind).
const (
	SourceKindPostgres = "postgres"
	SourceKindMySQL    = "mysql"
)

// LayerMySQL is the source layer of a pipeline whose source_kind is mysql: it
// takes the place of "postgres" in the pipeline's layers.
const LayerMySQL = "mysql"

// layerPostgres is the source layer of a Postgres pipeline (events.Layers[0]).
const layerPostgres = "postgres"

// KnownLayers returns every layer an owners.yaml entry may name: events.Layers
// plus LayerMySQL while the contract does not list it yet (then it is
// events.Layers unchanged, in its order). New slice per call.
func KnownLayers() []string {
	out := slices.Clone(events.Layers)
	if !slices.Contains(out, LayerMySQL) {
		out = append(out, LayerMySQL)
	}
	return out
}

// sourceLayer returns the layer of a source database of the given kind ("" =
// postgres), or "" for an unknown kind, which fails closed: no owner owns "".
func sourceLayer(sourceKind string) string {
	switch sourceKind {
	case "", SourceKindPostgres:
		return layerPostgres
	case SourceKindMySQL:
		return LayerMySQL
	}
	return ""
}

// layersOfSource returns the layers of a pipeline of kind and sourceKind,
// upstream first: events.LayersOf(kind) with the source layer of a cdc
// pipeline (index 0) replaced by sourceLayer(sourceKind). A batch pipeline
// ignores sourceKind; an unknown sourceKind on a cdc pipeline answers nil.
// New slice per call.
func layersOfSource(kind, sourceKind string) []string {
	l := events.LayersOf(kind)
	if kind != "" && kind != events.PipelineKindCDC {
		return l
	}
	sl := sourceLayer(sourceKind)
	if sl == "" || len(l) == 0 {
		return nil
	}
	l[0] = sl
	return l
}

// SourceKindOrDefault returns the pipeline's source kind: SourceKindPostgres
// when the file names none (every file written before MySQL), else SourceKind
// as written. Load refuses any other value, and the key on a batch pipeline.
// A nil pipeline is the default.
func (p *Pipeline) SourceKindOrDefault() string {
	if p == nil || p.SourceKind == "" {
		return SourceKindPostgres
	}
	return p.SourceKind
}

// SourceLayer returns the layer of the pipeline's source database: "postgres"
// or "mysql" for a cdc pipeline, "" for a batch pipeline (it has no source
// database) and for a source kind Load would refuse.
func (p *Pipeline) SourceLayer() string {
	if p.isBatch() {
		return ""
	}
	return sourceLayer(p.SourceKindOrDefault())
}

// IsMySQL reports whether the pipeline is a cdc pipeline on a MySQL source.
func (p *Pipeline) IsMySQL() bool {
	return p != nil && !p.isBatch() && p.SourceKind == SourceKindMySQL
}

// validateSourceKind checks source_kind on a cdc pipeline (validateBatch
// refuses the key on a batch one) and, on a MySQL pipeline, refuses the
// Postgres-only watch fields: there is no replication slot and no WAL, so the
// block would promise a check that never runs. The errors do not name the
// pipeline; the caller adds it.
func (p *Pipeline) validateSourceKind() error {
	switch p.SourceKind {
	case "", SourceKindPostgres:
		return nil
	case SourceKindMySQL:
	default:
		return fmt.Errorf("source_kind %s: want %s or %s (leave it out for %s)",
			quoteName(p.SourceKind), SourceKindPostgres, SourceKindMySQL, SourceKindPostgres)
	}
	w := p.WatchSpec
	if w == nil {
		return nil
	}
	switch {
	case w.Slot != "":
		return fmt.Errorf("watch.slot: a %s source has no replication slot; remove the key", SourceKindMySQL)
	case w.WALHeadroomBytes != "":
		return fmt.Errorf("watch.wal_headroom_bytes: a %s source has no WAL; remove the key", SourceKindMySQL)
	}
	for _, s := range []string{SignalSlotInactive, SignalSlotWAL} {
		if _, ok := w.Signals[s]; ok {
			return fmt.Errorf("watch.signals.%s: a %s source has no replication slot; remove the signal", s, SourceKindMySQL)
		}
	}
	return nil
}

// noSlotSignals turns the slot signals off in w, for a MySQL pipeline: the
// watcher must never read replication slots for it (they answer
// ErrNotSupported).
func noSlotSignals(w *WatchConfig) {
	w.SlotInactive.Enabled = false
	w.SlotWAL.Enabled = false
}
