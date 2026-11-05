//go:build xwavegen

// Generator for the synthesised X1e silent-corruption scenarios (kept for re-recording; see tasks/X-followups.md).
// It builds the state each X1g `devctl break` leaves, runs the REAL tools,
// topology and watcher over fake clients, and fills alert.pipeline_map and
// recordings of the scenario files. Based on evals/gen.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"slices"
	"sort"
	"strconv"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/clients/pgconv"
)

var fixedNow = time.Date(2026, 10, 21, 9, 40, 0, 0, time.UTC)

type clock struct{ now time.Time }

func (c clock) ago(d time.Duration) time.Time { return c.now.Add(-d) }

// at converts a fixed-clock time to this clock.
func (c clock) at(t time.Time) time.Time { return c.now.Add(t.Sub(fixedNow)) }

type call struct {
	tool     string
	input    map[string]any
	free     []string
	wildcard bool
	only     []string
	seq      map[string]int
}

type ago time.Duration

// ---- the orders world: rows, change events, sink ----

const placeholder = "__debezium_unavailable_value"

type orow struct {
	id       int64
	customer int64
	amount   int64
	status   string
	upd      time.Time // fixed clock
	updNull  bool      // sink only
}

type tevent struct {
	part   int32
	offset int64
	at     time.Time // fixed clock
	id     int64
	lsn    int64
	dup    bool
	first  int64 // offset of the first copy for a dup
}

type xstate struct {
	src, snk    map[int64]*orow
	events      []tevent
	base        [3]int64 // offsets before the simulated events
	next        [3]int64
	lsn         int64
	replica     string
	toastB      int64
	statusAvg   int
	ddl         []clients.DDLChange // fixed clock, newest first
	nextID      int64
	lastHistory time.Time
}

func newState() *xstate {
	s := &xstate{src: map[int64]*orow{}, snk: map[int64]*orow{}, replica: "FULL", statusAvg: 7, lsn: 0x3B4BF00000}
	return s
}

// history: rows loaded before the window, mirrored in the sink, no events.
func (s *xstate) history(n int, from, to time.Time, rng *rand.Rand) {
	span := to.Sub(from)
	for i := 0; i < n; i++ {
		s.nextID++
		at := from.Add(time.Duration(int64(span) * int64(i) / int64(n))).Add(time.Duration(rng.IntN(900)) * time.Millisecond)
		st := "placed"
		if rng.IntN(3) == 0 {
			st = "shipped"
		}
		r := &orow{id: s.nextID, customer: int64(rng.IntN(1000)), amount: int64(1 + rng.IntN(5000)), status: st, upd: at.Truncate(time.Microsecond)}
		s.src[r.id] = r
		c := *r
		s.snk[r.id] = &c
		s.emit(r.id, at)
		s.lastHistory = at
	}
}

func (s *xstate) emit(id int64, at time.Time) tevent {
	p := int32(id % 3)
	s.lsn += 0x1A8 + int64(id%7)*8
	e := tevent{part: p, offset: s.next[p], at: at.Add(180 * time.Millisecond).Truncate(time.Millisecond), id: id, lsn: s.lsn}
	s.next[p]++
	s.events = append(s.events, e)
	return e
}

// sinkFn decides what the sink stores for a source row after a change.
type sinkFn func(r orow, op string) (orow, bool)

func goodSink(r orow, _ string) (orow, bool) { return r, true }

func (s *xstate) insert(at time.Time, customer, amount int64, status string, sink sinkFn) int64 {
	s.nextID++
	r := orow{id: s.nextID, customer: customer, amount: amount, status: status, upd: at.Truncate(time.Microsecond)}
	s.src[r.id] = &r
	s.emit(r.id, at)
	if v, ok := sink(r, "c"); ok {
		s.snk[r.id] = &v
	}
	return r.id
}

func (s *xstate) update(at time.Time, id int64, f func(*orow), sink sinkFn) {
	r := s.src[id]
	f(r)
	r.upd = at.Truncate(time.Microsecond)
	s.emit(id, at)
	if v, ok := sink(*r, "u"); ok {
		s.snk[id] = &v
	}
}

// republish appends a copy of event e (same key, same lsn).
func (s *xstate) republish(e tevent, at time.Time) {
	c := tevent{part: e.part, offset: s.next[e.part], at: at.Truncate(time.Millisecond), id: e.id, lsn: e.lsn, dup: true, first: e.offset}
	s.next[e.part]++
	s.events = append(s.events, c)
}

// ---- world on a clock ----

type xw struct {
	*world
	st  *xstate
	c   clock
	now time.Time // tool "now" on this clock
}

func (x *xw) t(fixed time.Time) time.Time { return x.c.at(fixed) }

// fixedOf converts a time on this clock back to the fixed clock.
func (x *xw) fixedOf(t time.Time) time.Time { return t.Add(fixedNow.Sub(x.c.now)) }

func (x *xw) rows(table string) map[int64]*orow {
	switch table {
	case "public.orders":
		return x.st.src
	case "analytics.orders":
		return x.st.snk
	}
	return nil
}

func rowTS(r *orow) (time.Time, bool) { return r.upd, !r.updNull }

type xsrc struct {
	fakeSource
	x *xw
}

type xwh struct {
	fakeWarehouse
	x *xw
}

type xkafka struct {
	fakeKafka
	x *xw
}

func (x *xw) row(table, pk string) (map[string]any, bool, error) {
	rs := x.rows(table)
	if rs == nil {
		return nil, false, fmt.Errorf("table %s: %w", table, clients.ErrNotFound)
	}
	id, err := strconv.ParseInt(pk, 10, 64)
	if err != nil {
		return nil, false, nil
	}
	r, ok := rs[id]
	if !ok {
		return nil, false, nil
	}
	m := map[string]any{"id": pgconv.ToJSON(r.id), "customer_id": pgconv.ToJSON(r.customer), "amount": pgconv.ToJSON(r.amount), "status": r.status}
	if r.updNull {
		m["updated_at"] = nil
	} else {
		m["updated_at"] = pgconv.ToJSON(x.t(r.upd))
	}
	if table == "analytics.orders" {
		m["loaded_at"] = pgconv.ToJSON(x.t(r.upd.Add(400 * time.Millisecond)))
	}
	return m, true, nil
}

func (s xsrc) Row(_ context.Context, table, _, pk string) (map[string]any, bool, error) {
	return s.x.row(table, pk)
}
func (h xwh) Row(_ context.Context, table, _, pk string) (map[string]any, bool, error) {
	return h.x.row(table, pk)
}

func (s xsrc) Freshness(ctx context.Context, table, ts string) (time.Time, error) {
	return xwh{fakeWarehouse{s.x.world}, s.x}.Freshness(ctx, table, ts)
}

func (s xsrc) PrimaryKeyColumn(context.Context, string) (string, error) { return "id", nil }
func (h xwh) PrimaryKeyColumn(context.Context, string) (string, error)  { return "id", nil }

func (h xwh) Freshness(_ context.Context, table, _ string) (time.Time, error) {
	rs := h.x.rows(table)
	if rs == nil {
		return time.Time{}, fmt.Errorf("warehouse: table %s: %w", table, clients.ErrNotFound)
	}
	var newest time.Time
	for _, r := range rs {
		if t, ok := rowTS(r); ok && t.After(newest) {
			newest = t
		}
	}
	return h.x.t(newest), nil
}

func (x *xw) count(table string, from, to time.Time) int64 {
	var n int64
	for _, r := range x.rows(table) {
		t, ok := rowTS(r)
		if !ok {
			continue
		}
		t = x.t(t)
		if !t.Before(from) && t.Before(to) {
			n++
		}
	}
	return n
}

func (x *xw) buckets(table string, since, until time.Time, width time.Duration) []clients.MinuteCount {
	m := map[time.Time]int64{}
	for _, r := range x.rows(table) {
		t, ok := rowTS(r)
		if !ok {
			continue
		}
		t = x.t(t)
		if t.Before(since) || !t.Before(until) {
			continue
		}
		b := since.Add(t.Sub(since) / width * width)
		m[b]++
	}
	var out []clients.MinuteCount
	for b, n := range m {
		out = append(out, clients.MinuteCount{Bucket: b, Count: n})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Bucket.Before(out[j].Bucket) })
	return out
}

func (x *xw) keysIn(table string, since, until time.Time) []*orow {
	var out []*orow
	for _, r := range x.rows(table) {
		t, ok := rowTS(r)
		if !ok {
			continue
		}
		t = x.t(t)
		if !t.Before(since) && t.Before(until) {
			out = append(out, r)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].upd.Equal(out[j].upd) {
			return out[i].upd.After(out[j].upd)
		}
		return out[i].id > out[j].id
	})
	return out
}

func (s xsrc) CountByBucket(_ context.Context, table, _ string, since, until time.Time, width time.Duration) ([]clients.MinuteCount, error) {
	return s.x.buckets(table, since, until, width), nil
}
func (h xwh) CountByBucket(_ context.Context, table, _ string, since, until time.Time, width time.Duration) ([]clients.MinuteCount, error) {
	return h.x.buckets(table, since, until, width), nil
}
func (s xsrc) CountBetween(_ context.Context, table, _ string, from, to time.Time) (int64, error) {
	return s.x.count(table, from, to), nil
}
func (h xwh) CountBetween(_ context.Context, table, _ string, from, to time.Time) (int64, error) {
	return h.x.count(table, from, to), nil
}
func sample(x *xw, table string, since, until time.Time, n int) []string {
	var out []string
	for _, r := range x.keysIn(table, since, until) {
		if len(out) == n {
			break
		}
		out = append(out, strconv.FormatInt(r.id, 10))
	}
	return out
}
func (s xsrc) SampleKeys(_ context.Context, table, _, _ string, since, until time.Time, n int) ([]string, error) {
	return sample(s.x, table, since, until, n), nil
}
func (h xwh) SampleKeys(_ context.Context, table, _, _ string, since, until time.Time, n int) ([]string, error) {
	return sample(h.x, table, since, until, n), nil
}
func rowTimes(x *xw, table string, keys []string) map[string]map[string]time.Time {
	out := map[string]map[string]time.Time{}
	rs := x.rows(table)
	for _, k := range keys {
		id, _ := strconv.ParseInt(k, 10, 64)
		r, ok := rs[id]
		if !ok {
			continue
		}
		m := map[string]time.Time{}
		if t, ok := rowTS(r); ok {
			m["updated_at"] = x.t(t)
		}
		out[k] = m
	}
	return out
}
func (s xsrc) RowTimes(_ context.Context, table, _ string, keys, _ []string) (map[string]map[string]time.Time, error) {
	return rowTimes(s.x, table, keys), nil
}
func (h xwh) RowTimes(_ context.Context, table, _ string, keys, _ []string) (map[string]map[string]time.Time, error) {
	return rowTimes(h.x, table, keys), nil
}
func windowKeys(x *xw, table string, since, until time.Time, limit int) ([]string, int64, error) {
	rs := x.keysIn(table, since, until)
	var out []string
	for i, r := range rs {
		if i == limit {
			break
		}
		out = append(out, strconv.FormatInt(r.id, 10))
	}
	return out, int64(len(rs)), nil
}
func (s xsrc) WindowKeys(_ context.Context, table, _, _ string, since, until time.Time, limit int) ([]string, int64, error) {
	return windowKeys(s.x, table, since, until, limit)
}
func (h xwh) WindowKeys(_ context.Context, table, _, _ string, since, until time.Time, limit int) ([]string, int64, error) {
	return windowKeys(h.x, table, since, until, limit)
}

func (s xsrc) ToastColumns(_ context.Context, tables []string) ([]clients.ToastTable, bool, error) {
	var out []clients.ToastTable
	for _, t := range tables {
		out = append(out, clients.ToastTable{
			Table: t, Found: true, ReplicaIdentity: s.x.st.replica, ToastBytes: s.x.st.toastB,
			Columns: []clients.ToastColumn{{Name: "status", Type: "text", Storage: "x", AvgWidth: s.x.st.statusAvg}},
		})
	}
	return out, false, nil
}

func (h xwh) CountPlaceholder(_ context.Context, table, column, ph, _ string, since, until time.Time) (int64, error) {
	var n int64
	for _, r := range h.x.rows(table) {
		t, ok := rowTS(r)
		if !ok {
			continue
		}
		t = h.x.t(t)
		if r.status == ph && !t.Before(since) && t.Before(until) {
			n++
		}
	}
	return n, nil
}

// ---- kafka over the event log ----

const ordersTopic = "pg.public.orders"

func (k xkafka) offsets() []clients.PartitionOffsets {
	out := make([]clients.PartitionOffsets, 3)
	for p := int32(0); p < 3; p++ {
		var last time.Time
		for _, e := range k.x.st.events {
			if e.part == p && e.at.After(last) {
				last = e.at
			}
		}
		if last.IsZero() {
			last = k.x.st.lastHistory
		}
		out[p] = clients.PartitionOffsets{Partition: p, Start: 0, End: k.x.st.next[p], LastTimestamp: k.x.t(last)}
	}
	return out
}

func (k xkafka) TopicOffsets(_ context.Context, topic string) ([]clients.PartitionOffsets, error) {
	if topic != ordersTopic {
		return nil, fmt.Errorf("kafka: topic %s: %w", topic, clients.ErrNotFound)
	}
	return k.offsets(), nil
}

func (k xkafka) GroupLag(_ context.Context, group string) (clients.GroupLag, error) {
	if group != "order-sync" {
		return clients.GroupLag{}, fmt.Errorf("kafka: group %s: %w", group, clients.ErrNotFound)
	}
	g := clients.GroupLag{Group: group, State: "Stable", Members: 1}
	for p := int32(0); p < 3; p++ {
		g.Partitions = append(g.Partitions, clients.PartitionLag{Topic: ordersTopic, Partition: p, Committed: k.x.st.next[p], End: k.x.st.next[p]})
	}
	return g, nil
}

func (k xkafka) ConsumerGroups(context.Context) ([]string, error) { return []string{"order-sync"}, nil }

func (k xkafka) PartitionRates(_ context.Context, since time.Time, topics ...string) ([]clients.PartitionRate, error) {
	var out []clients.PartitionRate
	for _, topic := range topics {
		if topic != ordersTopic {
			return nil, fmt.Errorf("kafka: topic %s: %w", topic, clients.ErrNotFound)
		}
		for p := int32(0); p < 3; p++ {
			first := int64(-1)
			for _, e := range k.x.st.events {
				if e.part == p && !k.x.t(e.at).Before(since) {
					first = e.offset
					break
				}
			}
			r := clients.PartitionRate{Topic: topic, Partition: p, End: k.x.st.next[p], OffsetAtStart: first}
			if first >= 0 {
				r.Records = r.End - first
			}
			out = append(out, r)
		}
	}
	return out, nil
}

// avroKey is the Confluent-framed Avro key {id: long} (schema id 1).
func avroKey(id int64) []byte {
	b := []byte{0, 0, 0, 0, 1}
	z := uint64((id << 1) ^ (id >> 63))
	for z >= 0x80 {
		b = append(b, byte(z)|0x80)
		z >>= 7
	}
	return append(b, byte(z))
}

func fingerprint(id int64) string {
	h := sha256.Sum256(avroKey(id))
	return "sha256:" + hex.EncodeToString(h[:8])
}

func (k xkafka) DupScan(_ context.Context, topic string, since time.Time, maxScan int) (clients.DupReport, error) {
	if topic != ordersTopic {
		return clients.DupReport{}, fmt.Errorf("kafka: topic %s: %w", topic, clients.ErrNotFound)
	}
	if maxScan <= 0 || maxScan > clients.DupScanMaxScan {
		maxScan = clients.DupScanMaxScan
	}
	rep := clients.DupReport{Topic: topic, Since: since, MaxScan: maxScan}
	type kl struct{ id, lsn int64 }
	seen := map[kl]tevent{}
	dupKeys := map[int64]bool{}
	for p := int32(0); p < 3; p++ {
		dp := clients.DupPartition{Partition: p, From: -1, End: k.x.st.next[p], Done: true}
		for _, e := range k.x.st.events {
			if e.part != p || k.x.t(e.at).Before(since) {
				continue
			}
			if dp.From < 0 {
				dp.From = e.offset
			}
			dp.Scanned++
		}
		if dp.From < 0 {
			dp.From = k.x.st.next[p]
		}
		dp.Next = k.x.st.next[p]
		rep.Partitions = append(rep.Partitions, dp)
	}
	// Read order: by time (the round-robin of the real scan reads every
	// partition to its end; for findings the earlier copy comes first).
	evs := slices.Clone(k.x.st.events)
	sort.SliceStable(evs, func(i, j int) bool { return evs[i].at.Before(evs[j].at) })
	for _, e := range evs {
		if k.x.t(e.at).Before(since) {
			continue
		}
		rep.Scanned++
		key := kl{e.id, e.lsn}
		if f, ok := seen[key]; ok {
			rep.Duplicates++
			dupKeys[e.id] = true
			if len(rep.Findings) < clients.MaxDupFindings {
				rep.Findings = append(rep.Findings, clients.DupFinding{Kind: clients.DupKindDuplicate, Key: fingerprint(e.id),
					Partition: e.part, Offset: e.offset, FirstPartition: f.part, FirstOffset: f.offset, LSN: e.lsn})
			} else {
				rep.FindingsOmitted++
			}
			continue
		}
		seen[key] = e
	}
	rep.Keys = len(dupKeys)
	return rep, nil
}

func (k xkafka) FindByKey(_ context.Context, topic, key string, since time.Time, _ int) ([]clients.RecordRef, error) {
	if topic != ordersTopic {
		return nil, fmt.Errorf("kafka: topic %s: %w", topic, clients.ErrNotFound)
	}
	id, err := strconv.ParseInt(key, 10, 64)
	if err != nil {
		return nil, nil
	}
	var out []clients.RecordRef
	for _, e := range k.x.st.events {
		if e.id == id && !k.x.t(e.at).Before(since) {
			out = append(out, clients.RecordRef{Topic: topic, Partition: e.part, Offset: e.offset, Key: key, Timestamp: k.x.t(e.at)})
		}
	}
	return out, nil
}

func (k xkafka) FindByKeyScan(ctx context.Context, topic, key string, since time.Time, maxScan int) (clients.KeyScan, error) {
	refs, err := k.FindByKey(ctx, topic, key, since, maxScan)
	if err != nil {
		return clients.KeyScan{}, err
	}
	return clients.KeyScan{Refs: refs}, nil
}

func (k xkafka) DescribeTopicConfigs(_ context.Context, topic string) (kafka.TopicConfig, error) {
	if topic != ordersTopic {
		return kafka.TopicConfig{}, fmt.Errorf("kafka: describe topic configs %s: %w", topic, clients.ErrNotFound)
	}
	return kafka.TopicConfig{Topic: topic, Partitions: 3, ReplicationFactor: 1, RetentionMS: "604800000", RetentionBytes: "-1", CleanupPolicy: "delete", MinInsyncReplicas: "1"}, nil
}

func (s xsrc) RecentDDL(_ context.Context, since time.Time, limit int) ([]clients.DDLChange, error) {
	var out []clients.DDLChange
	for _, c := range s.x.st.ddl {
		c.At = s.x.t(c.At)
		if !c.At.Before(since) && len(out) < limit {
			out = append(out, c)
		}
	}
	return out, nil
}

func (s xsrc) ReplicationSlots(context.Context) ([]clients.ReplicationSlot, error) {
	return append([]clients.ReplicationSlot(nil), s.x.world.slots...), nil
}
