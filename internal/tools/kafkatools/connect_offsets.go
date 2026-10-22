package kafkatools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/redact"
	"piperoom/internal/tools"
)

// Caps on what connect_source_offsets shows. A source connector usually
// stores one offset; the caps keep a hostile or synthetic connector from
// filling the summary or the room log with an arbitrary map.
const (
	maxSourceOffsetEntries = 100 // per connector
	maxSourceOffsetsTotal  = 500 // across the pipeline's connectors
	maxOffsetMapKeys       = 32  // per partition or offset map
	maxOffsetKeyBytes      = 64
	maxOffsetValueBytes    = 256
)

// connectSourceOffsets shows the stored offsets of the room's source
// connectors. Read-only: it calls only SourceOffsets, never a lifecycle write.
type connectSourceOffsets struct{ d Deps }

func (t *connectSourceOffsets) Name() string     { return "connect_source_offsets" }
func (t *connectSourceOffsets) Kind() tools.Kind { return tools.KindRead }
func (t *connectSourceOffsets) InputSchema() map[string]any {
	return objectSchema(map[string]any{
		"connector": strProp("optional connector name; default: every connector in this room's pipeline"),
	})
}
func (t *connectSourceOffsets) Description() string {
	return "Returns the stored offsets of the room's Kafka Connect source connectors, read with the KIP-875 offsets API (Kafka Connect 3.5+): " +
		"each partition and its source position (for Debezium Postgres the LSN and transaction id, for MySQL the binlog file and position). " +
		"Positions only; never record data. Use it to see where a connector will resume, or whether it still points at an old database after a repoint. " +
		"On a Connect without the endpoint it reports the Kafka version that adds it."
}

func (t *connectSourceOffsets) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	var p struct {
		Connector string `json:"connector"`
	}
	if err := json.Unmarshal(input, &p); err != nil {
		return tools.Result{}, fmt.Errorf("%w: %v", tools.ErrBadInput, err)
	}
	sc, err := t.d.scope(ctx)
	if err != nil {
		return tools.Result{}, err
	}
	reader, ok := t.d.Connect.(connect.SourceOffsetReader)
	if !ok {
		return tools.Result{}, fmt.Errorf("%w: this Connect client cannot read connector offsets", clients.ErrNotSupported)
	}
	targets, err := t.targets(sc, p.Connector)
	if err != nil {
		return tools.Result{}, err
	}
	if len(targets) == 0 {
		return res("this pipeline has no connectors", map[string]any{"connectors": []any{}}), nil
	}

	cctx, cancel := withTimeout(ctx)
	defer cancel()

	var rows []connectorOffsetsRow
	total := 0
	cut := false
	for _, name := range targets {
		offs, err := reader.SourceOffsets(cctx, name)
		if err != nil {
			if errors.Is(err, clients.ErrNotSupported) {
				return tools.Result{}, fmt.Errorf("kafkatools: connect_source_offsets: %w (%s)", err, t.unsupportedNote(cctx))
			}
			return tools.Result{}, fmt.Errorf("kafkatools: connect_source_offsets: %s: %w", name, err)
		}
		row := t.row(name, offs, maxSourceOffsetsTotal-total)
		total += len(row.Offsets)
		cut = cut || row.Omitted > 0 || row.ValuesOmitted > 0
		rows = append(rows, row)
	}
	summary, summaryCut := buildOffsetsSummary(rows)
	data := map[string]any{"connectors": rows}
	r := res(summary, data)
	if cut || summaryCut {
		r.Truncated = true
	}
	return r, nil
}

// targets resolves the connectors to read: the one the model named (after the
// room scope check) or, with no name, every connector of the room's pipeline.
// The pipeline order is sorted so the summary is stable.
func (t *connectSourceOffsets) targets(sc tools.Scope, named string) ([]string, error) {
	if named != "" {
		if err := sc.CheckConnector(named); err != nil {
			return nil, err
		}
		return []string{named}, nil
	}
	if sc.Unscoped() {
		return nil, fmt.Errorf("%w: connector is required when the call has no room pipeline", tools.ErrBadInput)
	}
	names := append([]string(nil), sc.Pipeline().Connectors...)
	sort.Strings(names)
	// A duplicated name in owners.yaml must not read the same connector twice.
	out := names[:0]
	for i, n := range names {
		if i == 0 || n != names[i-1] {
			out = append(out, n)
		}
	}
	return out, nil
}

// connectorOffsetsRow is one connector in Data.
type connectorOffsetsRow struct {
	Connector string            `json:"connector"`
	Offsets   []sourceOffsetRow `json:"offsets"`
	// Omitted counts whole offsets cut by the caps; ValuesOmitted counts
	// non-position (nested) values dropped from a shown offset. Both mean the
	// result is partial.
	Omitted       int `json:"omitted,omitempty"`
	ValuesOmitted int `json:"values_omitted,omitempty"`
}

// sourceOffsetRow is one {partition, offset} pair in Data. Offset is null when
// Connect stored none for that partition.
type sourceOffsetRow struct {
	Partition map[string]any `json:"partition"`
	Offset    map[string]any `json:"offset"`
}

// row sanitizes one connector's offsets and caps it at limit entries. The
// whole entries it cut and the map values it dropped are in the row itself.
func (t *connectSourceOffsets) row(name string, offs []connect.SourceOffset, limit int) connectorOffsetsRow {
	sorted := append([]connect.SourceOffset(nil), offs...)
	sort.SliceStable(sorted, func(i, j int) bool {
		return partitionKey(sorted[i].Partition) < partitionKey(sorted[j].Partition)
	})
	row := connectorOffsetsRow{Connector: name}
	if limit < 0 {
		limit = 0
	}
	for i, o := range sorted {
		if i >= maxSourceOffsetEntries || len(row.Offsets) >= limit {
			row.Omitted = len(sorted) - i
			break
		}
		part, pOmitted := positionMap(o.Partition)
		off, oOmitted := positionMap(o.Offset)
		row.Offsets = append(row.Offsets, sourceOffsetRow{Partition: part, Offset: off})
		row.ValuesOmitted += pOmitted + oOmitted
	}
	return row
}

// partitionKey is a stable sort key for a ConnectorOffset partition map, so
// the order of a connector's offsets does not depend on the Connect response.
func partitionKey(m map[string]any) string { return formatPosition(m) }

// summaryBudget leaves room for the closing notes inside tools.MaxSummaryBytes.
const summaryBudget = 6000

// buildOffsetsSummary renders every connector's positions until the summary
// budget is spent; the Data keeps what the summary leaves out. It reports
// whether it cut a line, so the Result says it is partial.
func buildOffsetsSummary(rows []connectorOffsetsRow) (string, bool) {
	var sb strings.Builder
	for _, row := range rows {
		// The cap note comes first: it must survive the budget.
		if row.Omitted > 0 {
			appendLine(&sb, fmt.Sprintf("%s: %d offsets not shown (cap)", row.Connector, row.Omitted))
		}
		if len(row.Offsets) == 0 {
			appendLine(&sb, row.Connector+": no stored offsets")
			continue
		}
		for _, o := range row.Offsets {
			line := row.Connector + " partition " + formatPosition(o.Partition)
			if len(o.Offset) == 0 {
				line += ": no stored offset for this partition"
			} else {
				line += ": " + formatPosition(o.Offset)
			}
			if sb.Len()+len(line)+1 > summaryBudget {
				return sb.String(), true
			}
			appendLine(&sb, line)
		}
		if row.ValuesOmitted > 0 {
			appendLine(&sb, fmt.Sprintf("%s: %d values in the offsets are not positions and are not shown", row.Connector, row.ValuesOmitted))
		}
	}
	return sb.String(), false
}

// formatPosition renders a sanitized position map with sorted keys, so two
// calls over the same offsets produce the same summary.
func formatPosition(m map[string]any) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for i, k := range keys {
		if i > 0 {
			sb.WriteString(", ")
		}
		fmt.Fprintf(&sb, "%s=%v", k, m[k])
	}
	if sb.Len() == 0 {
		return "{}"
	}
	return sb.String()
}

// positionMap keeps only scalar entries of a partition or offset map: a
// position is a string, number, bool or null. A nested object or array is never
// a position (it could be a record value in a synthetic connector), so it is
// dropped and counted. Strings are redacted (a credential pasted into an
// offset must not reach the prompt) and capped; keys are capped too.
func positionMap(m map[string]any) (map[string]any, int) {
	out := make(map[string]any, min(len(m), maxOffsetMapKeys))
	if m == nil {
		return nil, 0
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	omitted := 0
	for _, k := range keys {
		if len(out) >= maxOffsetMapKeys || !offsetKeyOK(k) {
			omitted++
			continue
		}
		// A sensitive key is shown for its presence only: redacting the bare
		// value would not know it is a credential ("password": "hunter2").
		if sensitiveKey(k) {
			out[k] = "***"
			continue
		}
		v, ok := positionValue(m[k])
		if !ok {
			omitted++
			continue
		}
		out[k] = v
	}
	return out, omitted
}

// positionValue returns v when it is a scalar position value, redacted and
// capped, or false for a nested object or array.
func positionValue(v any) (any, bool) {
	switch x := v.(type) {
	case nil:
		return nil, true
	case string:
		s, _ := redact.Text(x)
		// A position must stay on one summary line: a line break in an
		// untrusted value could forge a second row.
		s = strings.NewReplacer("\r", " ", "\n", " ").Replace(s)
		if len(s) > maxOffsetValueBytes {
			s = truncateRunes(s, maxOffsetValueBytes) + "…"
		}
		return s, true
	case json.Number:
		return x, true
	case float64, bool:
		return x, true
	default:
		return nil, false
	}
}

// offsetKeyOK accepts a short, printable key and no line break: a key is
// untrusted text from the connector.
func offsetKeyOK(k string) bool {
	if k == "" || len(k) > maxOffsetKeyBytes {
		return false
	}
	return !strings.ContainsFunc(k, func(r rune) bool {
		return unicode.IsControl(r)
	})
}

// unsupportedNote names the running Kafka version when the endpoint is
// missing. The error from the client already names the version that adds the
// endpoint; this adds what the Connect in front of us reports.
func (t *connectSourceOffsets) unsupportedNote(ctx context.Context) string {
	const need = "reading connector offsets needs Kafka Connect 3.5 or later (KIP-875)"
	if r, ok := t.d.Connect.(connect.ConnectInfoReader); ok {
		if info, err := r.ServerInfo(ctx); err == nil && info.Version != "" {
			return fmt.Sprintf("this Connect reports Kafka %s; %s", info.Version, need)
		}
	}
	return need
}
