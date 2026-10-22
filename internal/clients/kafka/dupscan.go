package kafka

import (
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"time"

	"piperoom/internal/clients"
)

// DupScan's caps and deadline (X1c). Callers and tests use these constants.
const (
	// DupScanMaxScan caps DupScan's maxScan (records over all partitions); a
	// larger one is cut to it and maxScan <= 0 means it.
	DupScanMaxScan = 50_000
	// DupScanTimeout is DupScan's deadline when ctx has none or a later one.
	DupScanTimeout = 10 * time.Second
	// MaxDupFindings caps DupReport.Findings; the rest are counted.
	MaxDupFindings = 20
)

// Why a record has no usable (key, source.lsn): the keys of
// DupReport.UndecodableReasons. Fixed words, never record text.
const (
	UndecodableNoKey     = "no_key"         // empty record key
	UndecodableNoLSN     = "no_lsn"         // an envelope without source.lsn (not Postgres, or null)
	UndecodableFormat    = "not_envelope"   // neither a JSON object nor Confluent Avro
	UndecodableMalformed = "malformed"      // JSON or Avro that does not parse
	UndecodableRegistry  = "registry"       // no schema registry, or it was unreachable
	UndecodableSchema    = "schema_refused" // schema missing, too large or unparseable
)

// DupReport is what DupScan found on one topic: counts, offsets, source
// positions and key fingerprints only. A record key or value never enters
// it; Findings name a key by KeyFingerprint.
type DupReport struct {
	Topic   string    `json:"topic"`
	Since   time.Time `json:"since"`
	MaxScan int       `json:"max_scan"`
	// Scanned is the data records read (transaction markers excluded).
	Scanned int `json:"scanned"`
	// Capped: the scan stopped at MaxScan with records left to read.
	Capped bool `json:"capped"`
	// Partial: the deadline cut the scan; the counts cover what was read.
	Partial bool `json:"partial"`
	// Duplicates is the records whose (key, source.lsn) was already seen;
	// Keys is the distinct keys among them.
	Duplicates int `json:"duplicates"`
	Keys       int `json:"keys"`
	// OrderRegressions is the records whose source.lsn is below one the
	// same key already had earlier in the same partition (a duplicate is
	// not also a regression); RegressionKeys is the distinct keys.
	OrderRegressions int `json:"order_regressions"`
	RegressionKeys   int `json:"regression_keys"`
	// Undecodable is the records with no usable key or source.lsn, by reason
	// (the Undecodable* constants). Tombstones (empty values) are counted
	// apart: they carry no position.
	Undecodable        int            `json:"undecodable"`
	UndecodableReasons map[string]int `json:"undecodable_reasons,omitempty"`
	Tombstones         int            `json:"tombstones"`
	Partitions         []DupPartition `json:"partitions"`
	// Findings are the first MaxDupFindings duplicates and regressions in
	// read order; FindingsOmitted counts the rest.
	Findings        []DupFinding `json:"findings,omitempty"`
	FindingsOmitted int          `json:"findings_omitted,omitempty"`
}

// DupPartition is one partition's part of a DupScan.
type DupPartition struct {
	Partition int32 `json:"partition"`
	From      int64 `json:"from"` // first offset at or after since
	Next      int64 `json:"next"` // first offset not read
	End       int64 `json:"end"`  // readable end when the scan started
	Scanned   int   `json:"scanned"`
	Done      bool  `json:"done"`
}

// DupFinding is one duplicate or order regression. First* point at the
// earlier record: the first with the same (key, lsn) for a duplicate, the
// one with the higher PrevLSN for a regression.
type DupFinding struct {
	Kind           string `json:"kind"` // "duplicate" or "regression"
	Key            string `json:"key"`  // KeyFingerprint of the record key
	Partition      int32  `json:"partition"`
	Offset         int64  `json:"offset"`
	FirstPartition int32  `json:"first_partition"`
	FirstOffset    int64  `json:"first_offset"`
	LSN            int64  `json:"lsn"`
	PrevLSN        int64  `json:"prev_lsn,omitempty"`
}

// KeyFingerprint names a record key without showing it: "sha256:" and the
// first 8 bytes of the SHA-256 of the raw key bytes, in hex.
func KeyFingerprint(key []byte) string {
	sum := sha256.Sum256(key)
	return "sha256:" + hex.EncodeToString(sum[:8])
}

// DupScan reads topic from the first offset at or after since, up to the
// readable end of each partition as it was at the start, and reports
// duplicate (key, source.lsn) pairs and per-key source.lsn regressions of
// Debezium Postgres change events. It reads through ReadRecords
// (clients.RecordReader) a chunk per partition in turn, so a cut leaves
// every partition partly read; maxScan records at most (<= 0 or above
// DupScanMaxScan means DupScanMaxScan). The value is decoded only to find
// source.lsn (JSON, or Confluent Avro with the schema from the registry, by
// a bounded walker, never a generic decoder); keys are compared as bytes.
// No key, value or decoded field leaves this method: the report has counts,
// offsets, positions and KeyFingerprint only.
//
// Deadline DupScanTimeout: a scan it cuts returns the partial report with
// Partial set and no error. An unknown topic is clients.ErrNotFound. Read
// tools scope the topic before they call it.
func (c *Client) DupScan(ctx context.Context, topic string, since time.Time, maxScan int) (DupReport, error) {
	if maxScan <= 0 || maxScan > DupScanMaxScan {
		maxScan = DupScanMaxScan
	}
	rep := DupReport{Topic: topic, Since: since.UTC(), MaxScan: maxScan, Partitions: []DupPartition{}}
	ms, err := afterMilli(since)
	if err != nil {
		return rep, err
	}
	ctx, cancel := withDefaultTimeout(ctx, DupScanTimeout)
	defer cancel()
	tparam := "topic=" + topic
	start := time.Now()
	offs, err := c.lister().ListOffsetsAfterMilli(ctx, ms, topic)
	c.record(ctx, start, "kadm.ListOffsetsAfterMilli(topic, since)", tparam, "since="+since.UTC().Format(time.RFC3339))
	if err != nil {
		return rep, fmt.Errorf("kafka: dup scan %s: offsets after since: %w", topic, err)
	}
	if topicMissing(offs[topic]) {
		return rep, fmt.Errorf("kafka: dup scan: topic %s: %w", topic, clients.ErrNotFound)
	}
	start = time.Now()
	ends, err := c.lister().ListEndOffsets(ctx, topic)
	c.record(ctx, start, "kadm.ListEndOffsets(topic)", tparam)
	if err != nil {
		return rep, fmt.Errorf("kafka: dup scan %s: end offsets: %w", topic, err)
	}
	var wins []clients.PartitionOffsets
	for p, lo := range offs[topic] {
		if lo.Err != nil {
			return rep, fmt.Errorf("kafka: dup scan %s/%d: offset after since: %w", topic, p, lo.Err)
		}
		if lo.Offset < 0 {
			continue
		}
		e, ok := ends.Lookup(topic, p)
		if !ok {
			return rep, fmt.Errorf("kafka: dup scan %s/%d: end offset missing from response", topic, p)
		}
		if e.Err != nil {
			return rep, fmt.Errorf("kafka: dup scan %s/%d: end offset: %w", topic, p, e.Err)
		}
		wins = append(wins, clients.PartitionOffsets{Partition: p, Start: lo.Offset, End: e.Offset})
	}
	if wins, err = c.clampToStable(ctx, topic, wins); err != nil {
		return rep, fmt.Errorf("kafka: dup scan %s: %w", topic, err)
	}
	rr := c.dupReader
	if rr == nil {
		rr = NewReplayReader(c)
	}
	var down error // the scan's first registry failure (see decodeKeyIn)
	lsn := func(v []byte) (int64, string) { return c.lsnOf(ctx, v, &down) }
	if err := scanDups(ctx, rr, lsn, wins, &rep); err != nil {
		return rep, fmt.Errorf("kafka: dup scan %s: %w", topic, err)
	}
	return rep, nil
}

// dupPair is one (key, lsn); the key is its SHA-256 prefix, so no key
// bytes are held past the record that carried them.
type dupPair struct {
	key [16]byte
	lsn int64
}

type dupPos struct {
	partition int32
	offset    int64
	lsn       int64
}

type dupState struct {
	seen    map[dupPair]dupPos
	newest  map[int32]map[[16]byte]dupPos // per partition: the key's highest lsn so far
	dupKeys map[[16]byte]bool
	regKeys map[[16]byte]bool
}

// scanDups reads wins round-robin, a chunk of at most maxReadRecords per
// partition per turn, until every window is read, rep.MaxScan records were
// read (Capped) or ctx's deadline (Partial, nil error).
func scanDups(ctx context.Context, rr clients.RecordReader, lsnOf func([]byte) (int64, string), wins []clients.PartitionOffsets, rep *DupReport) error {
	slices.SortFunc(wins, func(a, b clients.PartitionOffsets) int { return cmp.Compare(a.Partition, b.Partition) })
	parts := make([]DupPartition, 0, len(wins))
	for _, w := range wins {
		parts = append(parts, DupPartition{Partition: w.Partition, From: w.Start, Next: w.Start, End: w.End, Done: w.End <= w.Start})
	}
	defer func() { rep.Partitions = parts }()
	st := &dupState{seen: map[dupPair]dupPos{}, newest: map[int32]map[[16]byte]dupPos{}, dupKeys: map[[16]byte]bool{}, regKeys: map[[16]byte]bool{}}
	for {
		open := false
		for i := range parts {
			p := &parts[i]
			if p.Done {
				continue
			}
			open = true
			if rep.Scanned >= rep.MaxScan {
				rep.Capped = true
				return nil
			}
			if ctx.Err() != nil {
				return cutDup(ctx, rep)
			}
			limit := min(maxReadRecords, rep.MaxScan-rep.Scanned)
			recs, err := rr.ReadRecords(ctx, rep.Topic, p.Partition, p.Next, limit)
			for _, r := range recs {
				if r.Offset < p.Next {
					continue
				}
				if r.Offset >= p.End {
					p.Done = true
					break
				}
				st.add(rep, r, lsnOf)
				p.Next, p.Scanned = r.Offset+1, p.Scanned+1
			}
			if err != nil {
				if ctx.Err() != nil {
					return cutDup(ctx, rep)
				}
				return fmt.Errorf("read partition %d from %d: %w", p.Partition, p.Next, err)
			}
			if len(recs) == 0 || p.Next >= p.End {
				p.Done = true
			}
		}
		if !open {
			return nil
		}
	}
}

// cutDup ends a scan whose ctx ended: its own deadline is a partial report,
// a caller's cancel is an error.
func cutDup(ctx context.Context, rep *DupReport) error {
	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		rep.Partial = true
		return nil
	}
	return ctx.Err()
}

func (st *dupState) add(rep *DupReport, r clients.Record, lsnOf func([]byte) (int64, string)) {
	rep.Scanned++
	if len(r.Value) == 0 {
		rep.Tombstones++
		return
	}
	why := ""
	var lsn int64
	if len(r.Key) == 0 {
		why = UndecodableNoKey
	} else {
		lsn, why = lsnOf(r.Value)
	}
	if why != "" {
		rep.Undecodable++
		if rep.UndecodableReasons == nil {
			rep.UndecodableReasons = map[string]int{}
		}
		rep.UndecodableReasons[why]++
		return
	}
	sum := sha256.Sum256(r.Key)
	var k [16]byte
	copy(k[:], sum[:16])
	at := dupPos{partition: r.Partition, offset: r.Offset, lsn: lsn}
	pair := dupPair{key: k, lsn: lsn}
	if first, ok := st.seen[pair]; ok {
		rep.Duplicates++
		if !st.dupKeys[k] {
			st.dupKeys[k] = true
			rep.Keys++
		}
		rep.finding(DupFinding{Kind: "duplicate", Key: KeyFingerprint(r.Key), Partition: r.Partition, Offset: r.Offset,
			FirstPartition: first.partition, FirstOffset: first.offset, LSN: lsn})
		return
	}
	st.seen[pair] = at
	newest := st.newest[r.Partition]
	if newest == nil {
		newest = map[[16]byte]dupPos{}
		st.newest[r.Partition] = newest
	}
	prev, ok := newest[k]
	switch {
	case ok && lsn < prev.lsn:
		rep.OrderRegressions++
		if !st.regKeys[k] {
			st.regKeys[k] = true
			rep.RegressionKeys++
		}
		rep.finding(DupFinding{Kind: "regression", Key: KeyFingerprint(r.Key), Partition: r.Partition, Offset: r.Offset,
			FirstPartition: prev.partition, FirstOffset: prev.offset, LSN: lsn, PrevLSN: prev.lsn})
	case !ok || lsn > prev.lsn:
		newest[k] = at
	}
}

func (rep *DupReport) finding(f DupFinding) {
	if len(rep.Findings) >= MaxDupFindings {
		rep.FindingsOmitted++
		return
	}
	rep.Findings = append(rep.Findings, f)
}

// lsnOf returns the source.lsn of one Debezium change event value, or the
// Undecodable* reason it has none. down is the scan's registry state.
func (c *Client) lsnOf(ctx context.Context, v []byte, down *error) (int64, string) {
	if len(v) >= 5 && v[0] == 0x00 {
		env, why := c.envelopeSchema(ctx, int(binary.BigEndian.Uint32(v[1:5])), down)
		if why != "" {
			return 0, why
		}
		return env.lsn(v[5:])
	}
	return jsonLSN(v)
}
