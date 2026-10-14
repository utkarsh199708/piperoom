package runbooks

import (
	"slices"
	"strings"
	"testing"
	"unicode"
)

// labelled is a realistic query and the section a good lookup must rank first.
// Section ids are "<runbook id>/<section heading>". Many queries are error text
// and alert titles copied from evals/scenarios/*.yaml fixtures.
type labelled struct{ query, want string }

var labelledQueries = []labelled{
	// schema-incompatible
	{`Schema being registered is incompatible with an earlier schema for subject "pg.public.orders-value"`, "schema-incompatible/Symptoms"},
	{`RestClientException: incompatible, error code: 409`, "schema-incompatible/Find it with Piperoom tools"},
	{`MISSING_UNION_BRANCH`, "schema-incompatible/Symptoms"},
	{`Caused by: org.apache.kafka.common.errors.SerializationException: Error registering Avro schema`, "schema-incompatible/Symptoms"},
	{`how do I set BACKWARD_TRANSITIVE or FULL compatibility on a subject`, "schema-incompatible/Safe checks"},
	{`decimal.handling.mode precise bytes logical type for numeric columns`, "schema-incompatible/Safe checks"},
	{`set the subject compatibility to NONE for the change, then restore it`, "schema-incompatible/Fixes"},
	{`test a schema against the latest version without registering it, verbose reasons`, "schema-incompatible/Safe checks"},
	// source-ddl
	{`column dropped from the source table after a migration, warehouse write fails`, "source-ddl/Symptoms"},
	{`REPLICA IDENTITY FULL old row for update and delete`, "source-ddl/Safe checks"},
	{`pg_recent_ddl who ran the ALTER TABLE`, "source-ddl/Find it with Piperoom tools"},
	{`rename column shows as removed field plus added field`, "source-ddl/Safe checks"},
	{`revert the DDL or migrate the warehouse table to the new column set`, "source-ddl/Fixes"},
	{`REPLICA IDENTITY FULL makes every update write the whole old row to WAL`, "source-ddl/Risks"},
	// slot-wal
	{`orders-db: pg_wal 48.6 GiB, data disk 81% full (PostgresWALDiskUsageHigh on orders-db-primary)`, "slot-wal/Symptoms"},
	{`replication slot inactive, restart_lsn is not moving`, "slot-wal/Symptoms"},
	{`slot invalidated wal_status lost needs a new snapshot, max_slot_wal_keep_size`, "slot-wal/Risks"},
	{`drop the slot with pg_drop_replication_slot`, "slot-wal/Fixes"},
	{`connector STOPPED, resume it with PUT /connectors/pg-orders-connector/resume`, "slot-wal/Fixes"},
	{`session idle in transaction pins the slot and WAL keeps growing`, "slot-wal/Symptoms"},
	{`SELECT slot_name, active, restart_lsn, confirmed_flush_lsn, wal_status FROM pg_replication_slots`, "slot-wal/Safe checks"},
	{`heartbeat.interval.ms heartbeat.action.query so the connector can confirm a newer LSN`, "slot-wal/Fixes"},
	{`pg_terminate_backend rolls back the open transaction`, "slot-wal/Risks"},
	// connector-failed
	{`pg-orders-connector task 0 FAILED since 09:12 UTC`, "connector-failed/Symptoms"},
	{`ConnectException: Unable to update filtered publication dbz_publication for "public"."orders"`, "connector-failed/Fixes"},
	{`PSQLException: ERROR: must be owner of table orders`, "connector-failed/Fixes"},
	{`FATAL: no pg_hba.conf entry for replication connection from host`, "connector-failed/Fixes"},
	{`role lacks the REPLICATION attribute, check rolreplication and rolcanlogin`, "connector-failed/Safe checks"},
	{`publication.autocreate.mode filtered disabled all_tables`, "connector-failed/Safe checks"},
	{`ALTER PUBLICATION dbz_publication ADD TABLE`, "connector-failed/Fixes"},
	{`restarting the task before the cause is fixed fails again in the same way`, "connector-failed/Risks"},
	// consumer-lag
	{`consumer group order-sync state Empty, members 0, lag growing while Connect is RUNNING`, "consumer-lag/Symptoms"},
	{`max.poll.interval.ms exceeded, member leaves the group and a rebalance starts`, "consumer-lag/Fixes"},
	{`static membership group.instance.id to avoid a rebalance when a member restarts`, "consumer-lag/Fixes"},
	{`CooperativeStickyAssignor partition.assignment.strategy`, "consumer-lag/Fixes"},
	{`kafka-consumer-groups.sh --describe --members to see who owns the partitions`, "consumer-lag/Safe checks"},
	{`raising max.poll.interval.ms delays noticing a consumer that is truly stuck`, "consumer-lag/Risks"},
	{`warehouse is falling behind, the consumers are behind by millions of messages`, "consumer-lag/Symptoms"},
	// poison-pill
	{`order-sync lag 13908 on pg.public.orders partition 1; pod restarted 9 times in 15 min`, "poison-pill/Symptoms"},
	{`RecordDeserializationException: Error deserializing key/value for partition pg.public.orders-1 at offset 25377154`, "poison-pill/Symptoms"},
	{`cannot process order 1648 at pg.public.orders/2@799 (poison pill)`, "poison-pill/Symptoms"},
	{`devconsumer: key at pg.public.orders/1@25377154: not confluent wire format`, "poison-pill/Symptoms"},
	{`skip one bad record: kafka-consumer-groups --reset-offsets --to-offset`, "poison-pill/Fixes"},
	{`errors.tolerance=all with errors.deadletterqueue.topic.name for a sink connector`, "poison-pill/Fixes"},
	{`an offset reset skips the record for good`, "poison-pill/Risks"},
	// retention-loss
	{`orders reconciliation: 1,181,433 of 7,912,440 orders changed 01:00-03:00 UTC differ in analytics.orders`, "retention-loss/Symptoms"},
	{`OffsetOutOfRangeException: Fetch position is out of range for partition pg.public.orders-0, auto.offset.reset`, "retention-loss/Symptoms"},
	{`committed offset is below the log start offset, records were deleted before the consumer read them`, "retention-loss/Symptoms"},
	{`retention.ms default 604800000 7 days, retention.bytes -1`, "retention-loss/Safe checks"},
	{`backfill the gap with a Debezium execute-snapshot signal`, "retention-loss/Fixes"},
	{`longer retention uses more broker disk`, "retention-loss/Risks"},
	{`kafka-configs.sh --describe --entity-type topics to see the topic retention settings`, "retention-loss/Safe checks"},
	// paraphrased symptoms and alert wording, as an on-call engineer would type them
	{`Failed to serialize Avro data from topic pg.public.orders`, "schema-incompatible/Symptoms"},
	{`ConnectException: Tolerance exceeded in error handler. Caused by: DataException: Failed to serialize Avro data from topic pg.public.orders. Caused by: SerializationException: Error registering Avro schema`, "schema-incompatible/Symptoms"},
	{`avro schema rejected by the registry after an alter table`, "schema-incompatible/Symptoms"},
	{`dropping a column breaks downstream consumers`, "source-ddl/Symptoms"},
	{`SQLSTATE 42501 permission denied, cannot create publication`, "connector-failed/Fixes"},
	{`disk filling up on the primary, pg_wal keeps growing`, "slot-wal/Symptoms"},
	{`consumer keeps crashing and restarting on the same message`, "poison-pill/Symptoms"},
	{`send unprocessable messages to a DLQ and carry on`, "poison-pill/Fixes"},
	{`rebalancing storm in the consumer group`, "consumer-lag/Symptoms"},
	{`lagging consumers, the warehouse is stale`, "consumer-lag/Symptoms"},
	{`kafka log start offset moved past the group's committed offset`, "retention-loss/Symptoms"},
	{`messages aged out of the topic before they were consumed`, "retention-loss/Symptoms"},
	{`dead-letter topic only works for sink connectors`, "poison-pill/Fixes"},
}

// sectionID is the id a labelled query refers to.
func sectionID(runbook, heading string) string { return runbook + "/" + heading }

// metrics scores a ranker over the labelled set.
type metrics struct{ r1, r3, mrr float64 }

func evaluate(rank func(query string) []string) (m metrics, misses []string) {
	for _, l := range labelledQueries {
		pos := slices.Index(rank(l.query), l.want)
		switch {
		case pos == 0:
			m.r1++
			fallthrough
		case pos > 0 && pos < 3:
			m.r3++
		}
		if pos >= 0 {
			m.mrr += 1 / float64(pos+1)
		} else {
			misses = append(misses, l.query)
		}
	}
	n := float64(len(labelledQueries))
	m.r1, m.r3, m.mrr = m.r1/n, m.r3/n, m.mrr/n
	return m, misses
}

func TestRankingQuality(t *testing.T) {
	if len(labelledQueries) < 40 {
		t.Fatalf("%d labelled queries, want at least 40", len(labelledQueries))
	}
	rbs := embedded(t)
	valid := map[string]bool{}
	for _, rb := range rbs {
		for _, s := range rb.Sections {
			valid[sectionID(rb.ID, s.Heading)] = true
		}
	}
	for _, l := range labelledQueries {
		if !valid[l.want] {
			t.Fatalf("query %q expects unknown section %q", l.query, l.want)
		}
	}

	tl := New(rbs)
	newRank := func(q string) []string {
		var ids []string
		for _, h := range tl.search(q, 10) {
			ids = append(ids, sectionID(h.Runbook, h.Section))
		}
		return ids
	}
	old := newOldIndex(rbs)
	oldRank := func(q string) []string { return old.search(q, 10) }

	om, _ := evaluate(oldRank)
	nm, misses := evaluate(newRank)
	t.Logf("%d labelled queries\n%-12s %8s %8s %8s\n%-12s %8.3f %8.3f %8.3f\n%-12s %8.3f %8.3f %8.3f",
		len(labelledQueries), "scorer", "recall@1", "recall@3", "MRR",
		"old ad-hoc", om.r1, om.r3, om.mrr, "BM25", nm.r1, nm.r3, nm.mrr)
	for _, q := range misses {
		t.Logf("BM25 missed top 10: %q", q)
	}
	if nm.r3 <= om.r3 {
		t.Errorf("recall@3 %.3f does not beat the old scorer's %.3f", nm.r3, om.r3)
	}
	if nm.mrr <= om.mrr {
		t.Errorf("MRR %.3f does not beat the old scorer's %.3f", nm.mrr, om.mrr)
	}
}

func TestSearchDeterministic(t *testing.T) {
	tl := New(embedded(t))
	for _, l := range labelledQueries {
		a, b := tl.search(l.query, 5), tl.search(l.query, 5)
		if !slices.EqualFunc(a, b, func(x, y hit) bool { return x.Runbook == y.Runbook && x.Section == y.Section && x.Score == y.Score }) {
			t.Fatalf("%q: results differ between runs", l.query)
		}
	}
}

// ---- baseline: the scorer this PR replaces, copied unchanged -----------------

type oldUnit struct {
	rb       int
	section  int
	runbook  map[string]bool
	heading  map[string]bool
	textFreq map[string]int
}

type oldIndex struct {
	rbs   []Runbook
	units []oldUnit
}

func newOldIndex(rbs []Runbook) *oldIndex {
	t := &oldIndex{rbs: rbs}
	for i, rb := range rbs {
		rbTerms := set(oldTerms(rb.Title + " " + strings.Join(rb.Keywords, " ")))
		for j, s := range rb.Sections {
			freq := map[string]int{}
			for _, w := range oldTerms(s.Text) {
				freq[w]++
			}
			t.units = append(t.units, oldUnit{rb: i, section: j, runbook: rbTerms, heading: set(oldTerms(s.Heading)), textFreq: freq})
		}
	}
	return t
}

// search returns section ids, best first.
func (t *oldIndex) search(query string, n int) []string {
	q := dedupe(oldTerms(query))
	if len(q) > maxQueryTerms {
		q = q[:maxQueryTerms]
	}
	type scored struct {
		u     oldUnit
		score int
	}
	var res []scored
	for _, u := range t.units {
		score := 0
		for _, w := range q {
			if u.runbook[w] {
				score += 3
			}
			if u.heading[w] {
				score += 2
			}
			score += min(u.textFreq[w], 3)
		}
		if score > 0 {
			res = append(res, scored{u, score})
		}
	}
	slices.SortStableFunc(res, func(a, b scored) int { return b.score - a.score })
	var ids []string
	for _, r := range res[:min(n, len(res))] {
		rb := t.rbs[r.u.rb]
		ids = append(ids, sectionID(rb.ID, rb.Sections[r.u.section].Heading))
	}
	return ids
}

func oldTerms(text string) []string {
	var out []string
	add := func(w string) {
		if len(w) >= 2 && !stopwords[w] {
			out = append(out, w)
		}
	}
	for _, tok := range strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r) && r != '_' && r != '.'
	}) {
		tok = strings.Trim(tok, ".")
		add(tok)
		if strings.ContainsAny(tok, "_.") {
			for _, part := range strings.FieldsFunc(tok, func(r rune) bool { return r == '_' || r == '.' }) {
				add(part)
			}
		}
	}
	return out
}
