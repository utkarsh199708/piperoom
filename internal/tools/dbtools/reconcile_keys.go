package dbtools

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// windowKeysReader is the optional read the key-level comparison needs: the
// distinct key values of one table in a time window, plus the exact total
// (which may exceed limit). The Postgres source and warehouse clients
// implement it; a client without it (Snowflake, Databricks, a fake) leaves
// the key section out with a note instead of failing the call, like
// pkColumner.
type windowKeysReader interface {
	WindowKeys(ctx context.Context, table, keyColumn, tsColumn string, since, until time.Time, limit int) ([]string, int64, error)
}

// reconcileKeyDiff is the outcome of the key-level comparison: the data, the
// reason it was not compared, or the input error that fails the call. A key
// read that fails is a note, not an error: the bucket table still answers.
type reconcileKeyDiff struct {
	keys ReconcileKeys
	have bool
	note string
	err  error
}

// reconcileKeyDiff compares the sets of primary keys in the source and in the
// sink over the same window. One key is one row, however often the row was
// updated, so this is a row comparison while the topic column is an event
// count. The diff is left out with a note rather than truncated past
// reconcileKeyCap, so "no more missing keys" is never read from a cut read
// (#409) and the bucket table is still returned.
func (d Deps) reconcileKeyDiff(ctx context.Context, srcTable, sinkTable, tsCol, keyCol string, since, until time.Time) reconcileKeyDiff {
	src, ok := d.Src.(windowKeysReader)
	if !ok {
		return reconcileKeyDiff{note: "keys not compared: the source client cannot list keys"}
	}
	snk, ok := d.Wh.(windowKeysReader)
	if !ok {
		return reconcileKeyDiff{note: "keys not compared: the warehouse client cannot list keys"}
	}
	srcKey, note, err := d.keyColumnOf(ctx, d.Src, srcTable, keyCol, "source")
	if err != nil {
		return reconcileKeyDiff{err: err}
	}
	if note != "" {
		return reconcileKeyDiff{note: note}
	}
	sinkKey, note, err := d.keyColumnOf(ctx, d.Wh, sinkTable, keyCol, "sink")
	if err != nil {
		return reconcileKeyDiff{err: err}
	}
	if note != "" {
		return reconcileKeyDiff{note: note}
	}

	var (
		wg                 sync.WaitGroup
		srcKeys, snkKeys   []string
		srcTotal, snkTotal int64
		srcErr, snkErr     error
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		srcKeys, srcTotal, srcErr = src.WindowKeys(ctx, srcTable, srcKey, tsCol, since, until, reconcileKeyCap)
	}()
	go func() {
		defer wg.Done()
		snkKeys, snkTotal, snkErr = snk.WindowKeys(ctx, sinkTable, sinkKey, tsCol, since, until, reconcileKeyCap)
	}()
	wg.Wait()
	if srcErr != nil {
		return reconcileKeyDiff{note: fmt.Sprintf("keys not compared: reading the source keys of %s failed (%v)", srcTable, srcErr)}
	}
	if snkErr != nil {
		return reconcileKeyDiff{note: fmt.Sprintf("keys not compared: reading the sink keys of %s failed (%v)", sinkTable, snkErr)}
	}
	if srcTotal > reconcileKeyCap || snkTotal > reconcileKeyCap {
		return reconcileKeyDiff{note: fmt.Sprintf("keys not compared: %d source / %d sink keys exceed the %d-key cap; narrow since/until for a key diff", srcTotal, snkTotal, reconcileKeyCap)}
	}
	missing, extra := diffKeys(srcKeys, snkKeys)
	return reconcileKeyDiff{have: true, keys: ReconcileKeys{
		Column:      srcKey,
		SinkColumn:  sinkKey,
		Source:      srcTotal,
		Sink:        snkTotal,
		Missing:     int64(len(missing)),
		Extra:       int64(len(extra)),
		MissingKeys: sampleKeys(missing),
		ExtraKeys:   sampleKeys(extra),
	}}
}

// keyColumnOf returns the key column to compare for table: the table's
// single-column primary key when the client reports one, otherwise the
// caller's choice. key_column may not stand in for a primary key: any other
// column (an email, a tax id) would be read for the whole window and sampled
// into the room log, exactly what pg_row refuses. A note and no error means
// the table cannot be compared by key (no primary key, a composite one, a
// client that does not report it, or a read failure), which is not a failure
// of the call; only a key_column that is not the primary key is ErrBadInput.
func (d Deps) keyColumnOf(ctx context.Context, c any, table, chosen, side string) (string, string, error) {
	pk, ok := c.(pkColumner)
	if !ok {
		// No primary key is reported: the caller's column is the only key
		// there is to compare.
		if chosen != "" {
			return chosen, "", nil
		}
		return "", fmt.Sprintf("keys not compared: the %s client does not report the primary key of %s; pass key_column", side, table), nil
	}
	col, err := pk.PrimaryKeyColumn(ctx, table)
	switch {
	case errors.Is(err, clients.ErrNotFound):
		// No primary key exists, so there is no key a chosen column could
		// bypass; the caller names the only row identity there is.
		if chosen != "" {
			return chosen, "", nil
		}
		return "", fmt.Sprintf("keys not compared: %s has no primary key; pass key_column", table), nil
	case errors.Is(err, clients.ErrNotSupported):
		// A composite key has no single column to compare, and no key_column
		// may stand in for it: that would make any column readable.
		if chosen != "" {
			return "", "", fmt.Errorf("%w: key_column is not allowed for %s: it has a composite primary key, so there is no single-column key to compare", tools.ErrBadInput, table)
		}
		return "", fmt.Sprintf("keys not compared: %s has a composite primary key; reconcile compares one key column", table), nil
	case err != nil:
		return "", fmt.Sprintf("keys not compared: the primary key of %s could not be read (%v)", table, err), nil
	}
	// The client reads the name from the catalog, but a name is still checked
	// before it reaches another client's SQL.
	if !validColumn(col) {
		return "", fmt.Sprintf("keys not compared: the primary key of %s is not a plain identifier", table), nil
	}
	// Snowflake folds unquoted names to upper case and Databricks to lower
	// case; both report the key in their own case, so "ID" and "id" are the
	// same column there. Postgres is case-sensitive (warehouse_row's rule).
	fold := side == "sink" && (d.isSnowflake() || d.isDatabricks())
	if chosen != "" && chosen != col && !(fold && strings.EqualFold(chosen, col)) {
		return "", "", fmt.Errorf("%w: key_column must be the primary key column of %s (%q)", tools.ErrBadInput, table, col)
	}
	// The canonical name, so the client reads the exact catalog column.
	return col, "", nil
}

// diffKeys returns the keys of source that are not in sink and the keys of
// sink that are not in source. Both inputs are sorted as text (the clients
// return them that way) and are deduplicated first, so a fake that ignores
// DISTINCT cannot make a key look missing twice.
func diffKeys(source, sink []string) (missing, extra []string) {
	source, sink = sortedUnique(source), sortedUnique(sink)
	i, j := 0, 0
	for i < len(source) && j < len(sink) {
		switch {
		case source[i] < sink[j]:
			missing = append(missing, source[i])
			i++
		case source[i] > sink[j]:
			extra = append(extra, sink[j])
			j++
		default:
			i++
			j++
		}
	}
	missing = append(missing, source[i:]...)
	extra = append(extra, sink[j:]...)
	return missing, extra
}

func sortedUnique(keys []string) []string {
	out := slices.Clone(keys)
	sort.Strings(out)
	return slices.Compact(out)
}

// sampleKeys returns at most reconcileKeySample keys: enough for the agent to
// confirm one with warehouse_row, small enough for the summary.
func sampleKeys(keys []string) []string {
	if len(keys) <= reconcileKeySample {
		return keys
	}
	return keys[:reconcileKeySample]
}

// summary renders the key section, or the reason it is absent. It is appended
// to the bucket table; every number in it is a row count.
func (k reconcileKeyDiff) summary() string {
	if k.err != nil {
		return ""
	}
	if !k.have {
		if k.note == "" {
			return ""
		}
		return "\n" + k.note
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "\nkeys by %s: source %d, sink %d; missing %d", k.keys.Column, k.keys.Source, k.keys.Sink, k.keys.Missing)
	if len(k.keys.MissingKeys) > 0 {
		fmt.Fprintf(&sb, " (e.g. %s)", strings.Join(k.keys.MissingKeys, ", "))
	}
	fmt.Fprintf(&sb, "; extra %d", k.keys.Extra)
	if len(k.keys.ExtraKeys) > 0 {
		fmt.Fprintf(&sb, " (e.g. %s)", strings.Join(k.keys.ExtraKeys, ", "))
	}
	sb.WriteString("\n")
	switch {
	case k.keys.Missing > 0:
		// A key absent from the sink window can also be a stale row the lagging
		// sink has not updated yet, so the line says both.
		fmt.Fprintf(&sb, "keys: %d source %s not in the sink's window (missing, or not yet updated there); confirm one with warehouse_row (source row present, sink row absent) before naming a writer", k.keys.Missing, rowsAre(k.keys.Missing))
	case k.keys.Extra > 0:
		fmt.Fprintf(&sb, "keys: %d sink %s not among the source rows in this window (a later source update moves a row out of the window); confirm one with warehouse_row", k.keys.Extra, rowsAre(k.keys.Extra))
	case k.keys.Source > 0:
		fmt.Fprintf(&sb, "keys: the same %d %s in source and sink for this window (no drift)", k.keys.Source, rowsAre(k.keys.Source))
	default:
		sb.WriteString("keys: neither table has rows in this window")
	}
	sb.WriteString("\n")
	return sb.String()
}

// rowsAre keeps the verdict readable for a single row.
func rowsAre(n int64) string {
	if n == 1 {
		return "row is"
	}
	return "rows are"
}
