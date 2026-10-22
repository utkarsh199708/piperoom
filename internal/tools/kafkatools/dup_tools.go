package kafkatools

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"piperoom/internal/clients/kafka"
	"piperoom/internal/tools"
)

// ---- kafka_dup_keys (X1c) ----

// dupScanner is the local adapter for the Kafka client's DupScan until a
// reader for it lands in internal/clients/clients.go (contract change
// request in the X1c PR). The tool is registered only for a client that has
// it, so fakes and the eval replay get no tool the model could call in vain.
type dupScanner interface {
	DupScan(ctx context.Context, topic string, since time.Time, maxScan int) (kafka.DupReport, error)
}

// dupMaxListed caps the findings kafka_dup_keys lists in Summary; Data has
// up to kafka.MaxDupFindings.
const dupMaxListed = 10

type dupKeys struct{ d Deps }

func (t *dupKeys) Name() string     { return "kafka_dup_keys" }
func (t *dupKeys) Kind() tools.Kind { return tools.KindRead }
func (t *dupKeys) InputSchema() map[string]any {
	return objectSchema(map[string]any{
		"topic": strProp("one of the pipeline's CDC topics"),
		"since": strProp("RFC 3339 timestamp; scan from here"),
		"max_scan": map[string]any{
			"type":        "integer",
			"description": "max records over all partitions, 1 to " + strconv.Itoa(kafka.DupScanMaxScan) + "; default " + strconv.Itoa(kafka.DupScanMaxScan),
		},
	}, "topic", "since")
}

func (t *dupKeys) Description() string {
	return "Checks one of the pipeline's Debezium Postgres CDC topics since a time for duplicate change events (the same key and source.lsn more than once) " +
		"and per-key order regressions (source.lsn going backwards within a partition). Counts and offsets only; keys appear as sha256 fingerprints, never values."
}

func (t *dupKeys) Run(ctx context.Context, input json.RawMessage) (tools.Result, error) {
	var p struct {
		Topic   string `json:"topic"`
		Since   string `json:"since"`
		MaxScan *int   `json:"max_scan"`
	}
	if err := json.Unmarshal(input, &p); err != nil || p.Topic == "" || p.Since == "" {
		return tools.Result{}, fmt.Errorf("%w: topic and since are required", tools.ErrBadInput)
	}
	since, err := time.Parse(time.RFC3339, p.Since)
	if err != nil {
		return tools.Result{}, fmt.Errorf("%w: since must be RFC 3339: %v", tools.ErrBadInput, err)
	}
	if since.Before(time.Unix(0, 0)) {
		since = time.Unix(0, 0).UTC() // Kafka reads negative times as "latest"/"earliest"
	}
	maxScan := kafka.DupScanMaxScan
	if p.MaxScan != nil {
		if *p.MaxScan < 1 {
			return tools.Result{}, fmt.Errorf("%w: max_scan must be between 1 and %d", tools.ErrBadInput, kafka.DupScanMaxScan)
		}
		maxScan = min(*p.MaxScan, kafka.DupScanMaxScan)
	}
	sc, err := t.d.scope(ctx)
	if err != nil {
		return tools.Result{}, err
	}
	if err := sc.CheckTopic(p.Topic); err != nil {
		return tools.Result{}, err
	}
	ds, ok := t.d.Kafka.(dupScanner)
	if !ok {
		return tools.Result{}, fmt.Errorf("kafka_dup_keys: this Kafka client cannot scan for duplicates")
	}
	cctx, cancel := withTimeout(ctx)
	defer cancel()
	rep, err := ds.DupScan(cctx, p.Topic, since, maxScan)
	if err != nil {
		return tools.Result{}, fmt.Errorf("kafka_dup_keys: %w", err)
	}
	return res(dupSummary(rep), rep), nil
}

// dupSummary is built from numbers, fixed words, the scoped topic name and
// key fingerprints: no record text can reach it.
func dupSummary(rep kafka.DupReport) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s: %s across %s, %s in %s", rep.Topic,
		plural(rep.Duplicates, "duplicate event"), plural(rep.Keys, "key"),
		plural(rep.OrderRegressions, "order regression"), plural(rep.Scanned, "record"))
	if rep.OrderRegressions > 0 {
		fmt.Fprintf(&sb, " (regressions on %s)", plural(rep.RegressionKeys, "key"))
	}
	var open []int32
	for _, p := range rep.Partitions {
		if !p.Done {
			open = append(open, p.Partition)
		}
	}
	switch {
	case rep.Scanned == 0 && !rep.Partial:
		appendLine(&sb, "no records since "+rep.Since.Format(time.RFC3339))
	case rep.Capped:
		appendLine(&sb, fmt.Sprintf("scanned %s (capped): max_scan stopped the scan before the end of partition(s) %v; newer records were not checked, use a later since", group(rep.Scanned), open))
	}
	if rep.Partial {
		appendLine(&sb, fmt.Sprintf("partial: the time limit cut the scan before the end of partition(s) %v; the counts cover the %s read", open, plural(rep.Scanned, "record")))
	}
	if rep.Undecodable > 0 {
		reasons := make([]string, 0, len(rep.UndecodableReasons))
		for r, n := range rep.UndecodableReasons {
			reasons = append(reasons, r+" "+group(n))
		}
		slices.Sort(reasons)
		appendLine(&sb, fmt.Sprintf("%s had no usable key or source.lsn (%s) and were not checked; only Debezium Postgres change events carry source.lsn",
			plural(rep.Undecodable, "record"), strings.Join(reasons, ", ")))
	}
	if rep.Tombstones > 0 {
		appendLine(&sb, plural(rep.Tombstones, "tombstone")+" skipped")
	}
	for i, f := range rep.Findings {
		if i == dupMaxListed {
			break
		}
		if f.Kind == "regression" {
			appendLine(&sb, fmt.Sprintf("  regression: key %s, partition %d offset %d has lsn %d after lsn %d at offset %d", f.Key, f.Partition, f.Offset, f.LSN, f.PrevLSN, f.FirstOffset))
			continue
		}
		appendLine(&sb, fmt.Sprintf("  duplicate: key %s, partition %d offset %d repeats lsn %d of partition %d offset %d", f.Key, f.Partition, f.Offset, f.LSN, f.FirstPartition, f.FirstOffset))
	}
	if n := len(rep.Findings) + rep.FindingsOmitted - min(len(rep.Findings), dupMaxListed); n > 0 {
		appendLine(&sb, fmt.Sprintf("%d more finding(s) not listed; Data.findings has the first %d", n, kafka.MaxDupFindings))
	}
	return sb.String()
}

// plural is "1 key", "2 keys", with thousands grouped.
func plural(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return group(n) + " " + noun + "s"
}

// group writes n with a space between thousands ("41 200").
func group(n int) string {
	s := strconv.Itoa(n)
	if n < 0 {
		return s
	}
	var b strings.Builder
	for i, c := range s {
		if i > 0 && (len(s)-i)%3 == 0 {
			b.WriteByte(' ')
		}
		b.WriteRune(c)
	}
	return b.String()
}
