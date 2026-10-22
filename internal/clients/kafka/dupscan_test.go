package kafka

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hamba/avro/v2"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/clients"
)

// dbzJSON is a JsonConverter Debezium value (schemas.enable=true) carrying lsn.
func dbzJSON(id int, lsn int64) []byte {
	return fmt.Appendf(nil, `{"schema":{"type":"struct"},"payload":{"before":null,"after":{"id":%d,"email":"secret-%d@example.com"},"source":{"connector":"postgresql","lsn":%d},"op":"u"}}`, id, id, lsn)
}

// TestDupScanKFake (TC1): 100 records over two partitions, 3 of them repeat
// an earlier (key, lsn), on two keys; the scan finds exactly those.
func TestDupScanKFake(t *testing.T) {
	const topic = "pg.public.orders"
	cl, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(2, topic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	raw := rawClient(t, cl.ListenAddrs())
	var recs []*kgo.Record
	lsn := int64(1000)
	for i := range 97 {
		lsn += 10
		recs = append(recs, &kgo.Record{Topic: topic, Partition: int32(i % 2), Key: fmt.Appendf(nil, `{"id":%d}`, i%20), Value: dbzJSON(i%20, lsn)})
	}
	// Key 4 (partition 0) twice, key 7 (partition 1) once, each with an lsn
	// it already had.
	recs = append(recs,
		&kgo.Record{Topic: topic, Partition: 0, Key: recs[4].Key, Value: recs[4].Value},
		&kgo.Record{Topic: topic, Partition: 0, Key: recs[4].Key, Value: recs[4].Value},
		&kgo.Record{Topic: topic, Partition: 1, Key: recs[7].Key, Value: recs[7].Value})
	if err := raw.ProduceSync(unscopedCtx(t), recs...).FirstErr(); err != nil {
		t.Fatal(err)
	}
	c, err := New(cl.ListenAddrs(), nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	rep, err := c.DupScan(unscopedCtx(t), topic, epoch, 0)
	if err != nil {
		t.Fatal(err)
	}
	if rep.Scanned != 100 || rep.Duplicates != 3 || rep.Keys != 2 || rep.OrderRegressions != 0 || rep.Undecodable != 0 || rep.Capped || rep.Partial {
		t.Fatalf("report = %+v, want 100 scanned, 3 duplicates on 2 keys, nothing else", rep)
	}
	if rep.MaxScan != DupScanMaxScan || len(rep.Partitions) != 2 || !rep.Partitions[0].Done || !rep.Partitions[1].Done {
		t.Fatalf("report = %+v, want default max_scan and both partitions done", rep)
	}
	if len(rep.Findings) != 3 || rep.Findings[0].Key != KeyFingerprint(recs[4].Key) {
		t.Fatalf("findings = %+v", rep.Findings)
	}
	// TC6: nothing of a key or a value in the report.
	b, _ := json.Marshal(rep)
	for _, leak := range []string{`{\"id\"`, "secret-", "example.com", "postgresql", `"after"`} {
		if bytes.Contains(b, []byte(leak)) {
			t.Errorf("report JSON holds %q: %s", leak, b)
		}
	}

	// An unknown topic is ErrNotFound, not an empty report.
	if _, err := c.DupScan(unscopedCtx(t), "nope", epoch, 0); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("unknown topic: err = %v, want ErrNotFound", err)
	}
}

// seqReader is a RecordReader over generated records: per partition, the
// record at offset o comes from gen(p, o); partitions in stall block until
// ctx ends.
type seqReader struct {
	end   int64
	gen   func(p int32, o int64) clients.Record
	stall map[int32]bool
	calls atomic.Int32
	// deadline is the first call's ctx deadline.
	deadline atomic.Pointer[time.Time]
}

func (s *seqReader) ReadRecords(ctx context.Context, topic string, p int32, from int64, limit int) ([]clients.Record, error) {
	s.calls.Add(1)
	if d, ok := ctx.Deadline(); ok {
		s.deadline.CompareAndSwap(nil, &d)
	}
	if s.stall[p] {
		<-ctx.Done()
		return nil, fmt.Errorf("read incomplete after 0 records: %w", ctx.Err())
	}
	var out []clients.Record
	for o := from; o < s.end && len(out) < limit; o++ {
		r := s.gen(p, o)
		r.Topic, r.Partition, r.Offset = topic, p, o
		out = append(out, r)
	}
	return out, nil
}

// readerClient is a Client whose offset listings say every partition in
// parts runs [0, end) and whose record reads go to rr.
func readerClient(rr clients.RecordReader, end int64, parts ...int32) *Client {
	m := map[int32]int64{}
	z := map[int32]int64{}
	for _, p := range parts {
		m[p], z[p] = end, 0
	}
	return &Client{offs: &fakeLister{starts: merge(listed("t", z)), ends: merge(listed("t", m))}, dupReader: rr}
}

func jsonRec(key string, lsn int64) clients.Record {
	return clients.Record{Key: []byte(key), Value: dbzJSON(1, lsn)}
}

// TestDupScanOrderRegression (TC2): one key's lsn goes backwards once; a
// repeat of the same pair is a duplicate, not a second regression; the same
// key in another partition is ordered on its own.
func TestDupScanOrderRegression(t *testing.T) {
	plan := map[int32][]clients.Record{
		0: {jsonRec("a", 10), jsonRec("b", 11), jsonRec("a", 30), jsonRec("a", 20), jsonRec("a", 20), jsonRec("b", 40)},
		1: {jsonRec("a", 5)},
	}
	rr := &seqReader{end: 6, gen: func(p int32, o int64) clients.Record {
		if int(o) >= len(plan[p]) {
			return clients.Record{Key: []byte("pad"), Value: dbzJSON(1, 1_000_000+o+int64(p)*1000)}
		}
		return plan[p][o]
	}}
	rep, err := readerClient(rr, 6, 0, 1).DupScan(context.Background(), "t", epoch, 0)
	if err != nil {
		t.Fatal(err)
	}
	if rep.OrderRegressions != 1 || rep.RegressionKeys != 1 || rep.Duplicates != 1 || rep.Keys != 1 {
		t.Fatalf("report = %+v, want 1 regression and 1 duplicate on key a", rep)
	}
	var reg *DupFinding
	for i := range rep.Findings {
		if rep.Findings[i].Kind == "regression" {
			reg = &rep.Findings[i]
		}
	}
	if reg == nil || reg.Offset != 3 || reg.LSN != 20 || reg.PrevLSN != 30 || reg.FirstOffset != 2 || reg.Key != KeyFingerprint([]byte("a")) {
		t.Fatalf("regression finding = %+v", reg)
	}
}

// TestDupScanUndecodable (TC3): records without a usable key or source.lsn
// are counted by reason, never a panic, and never a duplicate.
func TestDupScanUndecodable(t *testing.T) {
	vals := []clients.Record{
		{Key: []byte("k"), Value: []byte(`{"id":1}`)},                                  // JSON, no source
		{Key: []byte("k"), Value: []byte(`{"id":1}`)},                                  // same again: still no duplicate
		{Key: []byte("k"), Value: []byte(`{"payload":{"source":{"lsn":null}}}`)},       // null lsn
		{Key: []byte("k"), Value: []byte(`{"source":{"file":"mysql-bin.1","pos":4}}`)}, // MySQL: no lsn
		{Key: []byte("k"), Value: []byte(`{"source":{"lsn":"12"}}`)},                   // not a number
		{Key: []byte("k"), Value: []byte(`{"source":{"lsn":1.5}}`)},                    // not an integer
		{Key: []byte("k"), Value: []byte(`{"source":`)},                                // truncated
		{Key: []byte("k"), Value: []byte("plain text")},                                // not an envelope
		{Key: []byte("k"), Value: []byte{0, 0, 0, 0, 9, 1, 2}},                         // Avro, no registry
		{Key: nil, Value: dbzJSON(1, 5)},                                               // no key
		{Key: []byte("k"), Value: nil},                                                 // tombstone
	}
	rr := &seqReader{end: int64(len(vals)), gen: func(_ int32, o int64) clients.Record { return vals[o] }}
	rep, err := readerClient(rr, int64(len(vals)), 0).DupScan(context.Background(), "t", epoch, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{UndecodableNoLSN: 4, UndecodableMalformed: 3, UndecodableFormat: 1, UndecodableRegistry: 1, UndecodableNoKey: 1}
	if rep.Undecodable != 10 || rep.Tombstones != 1 || rep.Duplicates != 0 || fmt.Sprint(rep.UndecodableReasons) != fmt.Sprint(want) {
		t.Fatalf("report = %+v, want reasons %v and 1 tombstone", rep, want)
	}
}

// TestDupScanCap (TC4): a maxScan above the cap is cut to it, and the scan
// stops there with Capped set.
func TestDupScanCap(t *testing.T) {
	rr := &seqReader{end: 1 << 40, gen: func(p int32, o int64) clients.Record { return jsonRec(fmt.Sprint(o%5000), o) }}
	rep, err := readerClient(rr, 1<<40, 0, 1, 2).DupScan(context.Background(), "t", epoch, 1_000_000)
	if err != nil {
		t.Fatal(err)
	}
	if rep.MaxScan != 50_000 || rep.Scanned != 50_000 || !rep.Capped || rep.Partial {
		t.Fatalf("report = max_scan %d scanned %d capped %v partial %v, want 50000, 50000, capped", rep.MaxScan, rep.Scanned, rep.Capped, rep.Partial)
	}
	// Round-robin: every partition got part of the budget.
	for _, p := range rep.Partitions {
		if p.Scanned == 0 || p.Done {
			t.Errorf("partition %+v: want scanned and not done", p)
		}
	}
}

// TestDupScanStalledPartition (TC7): a partition that never answers ends the
// scan at the deadline with what was read and Partial set; DupScan gives a
// caller without a deadline DupScanTimeout.
func TestDupScanStalledPartition(t *testing.T) {
	rr := &seqReader{end: 10, stall: map[int32]bool{1: true}, gen: func(p int32, o int64) clients.Record { return jsonRec("k"+fmt.Sprint(o), o) }}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	start := time.Now()
	rep, err := readerClient(rr, 10, 0, 1).DupScan(ctx, "t", epoch, 0)
	if err != nil {
		t.Fatalf("err = %v, want a partial report", err)
	}
	if !rep.Partial || rep.Scanned != 10 || rep.Partitions[1].Done || !rep.Partitions[0].Done {
		t.Fatalf("report = %+v, want partition 0 read, partition 1 open, partial", rep)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("took %s, want the 300ms deadline", took)
	}

	// No caller deadline: DupScanTimeout applies.
	rr = &seqReader{end: 1, gen: func(int32, int64) clients.Record { return jsonRec("k", 1) }}
	start = time.Now()
	if _, err := readerClient(rr, 1, 0).DupScan(context.Background(), "t", epoch, 0); err != nil {
		t.Fatal(err)
	}
	d := rr.deadline.Load()
	if d == nil || d.Sub(start) > DupScanTimeout+time.Second || d.Sub(start) < DupScanTimeout-time.Second {
		t.Fatalf("reader deadline = %v after start, want %s", d, DupScanTimeout)
	}

	// A caller's cancel is an error, not a partial report.
	cctx, ccancel := context.WithCancel(context.Background())
	rr = &seqReader{end: 10, stall: map[int32]bool{0: true}}
	go func() { time.Sleep(50 * time.Millisecond); ccancel() }()
	if _, err := readerClient(rr, 10, 0).DupScan(cctx, "t", epoch, 0); err == nil {
		t.Fatal("cancelled scan: nil error")
	}
}

// TestDupScanReadErrorWrapped (E5): a backend error is returned, wrapped.
func TestDupScanReadErrorWrapped(t *testing.T) {
	c := readerClient(errReader{}, 5, 0)
	if _, err := c.DupScan(context.Background(), "t", epoch, 0); !errors.Is(err, clients.ErrNotFound) || !strings.Contains(err.Error(), "dup scan t") {
		t.Fatalf("err = %v, want the read error wrapped", err)
	}
	if _, err := c.DupScan(context.Background(), "t", time.Unix(-5, 0), 0); err == nil {
		t.Fatal("since before the epoch: nil error")
	}
}

type errReader struct{}

func (errReader) ReadRecords(context.Context, string, int32, int64, int) ([]clients.Record, error) {
	return nil, fmt.Errorf("offset deleted: %w", clients.ErrNotFound)
}

// envelopeSchemaText is a cut-down Debezium Postgres envelope.
const envelopeSchemaText = `{"type":"record","name":"Envelope","namespace":"pg.public.orders","fields":[
 {"name":"before","type":["null",{"type":"record","name":"Value","fields":[
   {"name":"id","type":"long"},{"name":"email","type":["null","string"]},
   {"name":"tags","type":{"type":"array","items":"string"}},
   {"name":"attrs","type":{"type":"map","values":"double"}},
   {"name":"flag","type":"boolean"},{"name":"f","type":"float"},{"name":"h","type":{"type":"fixed","name":"H","size":2}},
   {"name":"kind","type":{"type":"enum","name":"K","symbols":["A","B"]}},{"name":"blob","type":"bytes"}]}],"default":null},
 {"name":"after","type":["null","Value"],"default":null},
 {"name":"source","type":{"type":"record","name":"Source","namespace":"io.debezium.connector.postgresql","fields":[
   {"name":"version","type":"string"},{"name":"ts_ms","type":"long"},
   {"name":"lsn","type":["null","long"],"default":null},{"name":"xmin","type":["null","long"],"default":null}]}},
 {"name":"op","type":"string"}]}`

func avroValue(t testing.TB, id int, lsn *int64) []byte {
	t.Helper()
	row := map[string]any{"id": int64(7), "email": "x@example.com", "tags": []string{"a", "b"}, "attrs": map[string]any{"k": 1.5},
		"flag": true, "f": float32(1), "h": [2]byte{1, 2}, "kind": "B", "blob": []byte{1, 2, 3}}
	v := map[string]any{"before": nil, "after": map[string]any{"pg.public.orders.Value": row}, "source": map[string]any{"version": "3.0", "ts_ms": int64(1), "lsn": lsn, "xmin": nil}, "op": "u"}
	b, err := avro.Marshal(avro.MustParse(envelopeSchemaText), v)
	if err != nil {
		t.Fatal(err)
	}
	hdr := make([]byte, 5)
	binary.BigEndian.PutUint32(hdr[1:], uint32(id))
	return append(hdr, b...)
}

// TestDupScanAvro: Confluent Avro values decode through the registry's
// schema, fetched once per id; a registry failure leaves the records
// undecodable and is not fetched again within the scan.
func TestDupScanAvro(t *testing.T) {
	var fetches atomic.Int32
	down := atomic.Bool{}
	c := testClientWithRegistry(t, func(w http.ResponseWriter, r *http.Request) {
		fetches.Add(1)
		if down.Load() {
			http.Error(w, `{"error_code":50301,"message":"down"}`, http.StatusServiceUnavailable)
			return
		}
		switch r.URL.Path {
		case "/schemas/ids/3":
			b, _ := json.Marshal(map[string]string{"schema": envelopeSchemaText})
			w.Write(b)
		case "/schemas/ids/4":
			w.Write([]byte(`{"schema":"{\"type\":\"record\",\"name\":\"M\",\"fields\":[{\"name\":\"source\",\"type\":{\"type\":\"record\",\"name\":\"S\",\"fields\":[{\"name\":\"pos\",\"type\":\"long\"}]}}]}"}`))
		default:
			http.Error(w, `{"error_code":40403,"message":"Schema not found"}`, http.StatusNotFound)
		}
	})
	l10, l15, l20 := int64(10), int64(15), int64(20)
	vals := []clients.Record{
		{Key: []byte("a"), Value: avroValue(t, 3, &l10)},
		{Key: []byte("a"), Value: avroValue(t, 3, &l20)},
		{Key: []byte("a"), Value: avroValue(t, 3, &l10)}, // duplicate of the first
		{Key: []byte("a"), Value: avroValue(t, 3, &l15)}, // regression below 20
		{Key: []byte("b"), Value: avroValue(t, 3, nil)},  // null lsn
		{Key: []byte("b"), Value: avroValue(t, 4, nil)},  // a source without lsn (schema refused once)
		{Key: []byte("b"), Value: avroValue(t, 4, nil)},
		{Key: []byte("b"), Value: avroValue(t, 9, nil)},       // unknown id
		{Key: []byte("b"), Value: avroValue(t, 3, &l10)[:12]}, // truncated
	}
	rr := &seqReader{end: int64(len(vals)), gen: func(_ int32, o int64) clients.Record { return vals[o] }}
	c.offs = readerClient(nil, int64(len(vals)), 0).offs
	c.dupReader = rr
	rep, err := c.DupScan(context.Background(), "t", epoch, 0)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]int{UndecodableNoLSN: 3, UndecodableSchema: 1, UndecodableMalformed: 1}
	if rep.Duplicates != 1 || rep.OrderRegressions != 1 || fmt.Sprint(rep.UndecodableReasons) != fmt.Sprint(want) {
		t.Fatalf("report = %+v, want 1 duplicate, 1 regression, reasons %v", rep, want)
	}
	if n := fetches.Load(); n != 3 {
		t.Fatalf("registry fetches = %d, want one per id (3)", n)
	}

	// Registry down: one fetch for the scan, every record undecodable.
	down.Store(true)
	fetches.Store(0)
	vals = []clients.Record{{Key: []byte("a"), Value: avroValue(t, 5, &l10)}, {Key: []byte("a"), Value: avroValue(t, 6, &l10)}}
	rr.end = 2
	rep, err = c.DupScan(context.Background(), "t", epoch, 0)
	if err != nil {
		t.Fatal(err)
	}
	if rep.UndecodableReasons[UndecodableRegistry] != 2 || fetches.Load() != 1 {
		t.Fatalf("report = %+v after %d fetches, want 2 registry-undecodable after 1 fetch", rep, fetches.Load())
	}
}

// TestAvroWalkerBounded: hostile values and schemas end quickly as
// malformed: a huge block count of an empty item type, a count beyond the
// bytes, and a schema whose empty records double at each level.
func TestAvroWalkerBounded(t *testing.T) {
	env := parseEnvelope(envelopeSchemaText)
	if env.why != "" {
		t.Fatal(env.why)
	}
	// before: the Value branch, id 0, email null, then an array count of
	// 2^40 strings in a value of a few bytes.
	huge := binary.AppendUvarint([]byte{2, 0, 0}, 1<<41)
	start := time.Now()
	for _, v := range [][]byte{huge, {2, 0, 0, 0, 0xfe, 0xff, 0xff, 0xff, 0x0f}, {}, {4}, {1}} {
		if _, why := env.lsn(v); why != UndecodableMalformed {
			t.Errorf("lsn(%x) = %q, want malformed", v, why)
		}
	}
	// Empty records that double at each level: R0 = {}, Rn = {a: Rn-1,
	// b: Rn-1}; R40 encodes to no bytes but has 2^40 nodes.
	nested := `{"type":"record","name":"R0","fields":[]}`
	for i := 1; i <= 40; i++ {
		nested = fmt.Sprintf(`{"type":"record","name":"R%d","fields":[{"name":"a","type":%s},{"name":"b","type":"R%d"}]}`, i, nested, i-1)
	}
	schema := `{"type":"record","name":"E","fields":[{"name":"x","type":` + nested + `},{"name":"y","type":{"type":"array","items":"R40"}},` +
		`{"name":"source","type":{"type":"record","name":"S","fields":[{"name":"lsn","type":"long"}]}}]}`
	env = parseEnvelope(schema)
	if env.why != "" {
		t.Fatalf("schema refused: %s", env.why)
	}
	if _, why := env.lsn(binary.AppendUvarint(nil, 1<<62)); why != UndecodableMalformed {
		t.Errorf("doubling schema: %q, want malformed", why)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("hostile values took %s", took)
	}
}

// FuzzDupScan (TC3): no value, JSON or Avro, makes the decoder panic or run
// long; a decoded lsn always comes with no reason.
func FuzzDupScan(f *testing.F) {
	l := int64(42)
	f.Add(dbzJSON(1, 42))
	f.Add([]byte(`{"source":{"lsn":-1}}`))
	f.Add([]byte(`{"payload":null,"source":{"lsn":9223372036854775807}}`))
	f.Add([]byte(`[1,2,3]`))
	f.Add([]byte(`{"source":{"lsn":1e3}}`))
	f.Add(avroValue(f, 3, &l))
	f.Add(avroValue(f, 3, nil))
	f.Add([]byte{0, 0, 0, 0, 3, 2, 0xff, 0xff, 0xff, 0xff, 0x0f})
	env := parseEnvelope(envelopeSchemaText)
	c := &Client{envCache: map[int]*dbzEnvelope{3: env}}
	f.Fuzz(func(t *testing.T, v []byte) {
		var down error
		n, why := c.lsnOf(context.Background(), v, &down)
		if why == "" && len(v) >= 5 && v[0] == 0 {
			// Re-walk: the same bytes give the same answer.
			if n2, why2 := env.lsn(v[5:]); why2 != "" || n2 != n {
				t.Fatalf("unstable decode: %d/%q then %d/%q", n, why, n2, why2)
			}
		}
	})
}

// TestDupScanSlowValuesHonourDeadline (review of X1c): values whose schema
// makes every walk spend the whole ops budget end the scan at the deadline,
// not at the end of a 1000-record chunk; the records decoded so far count.
func TestDupScanSlowValuesHonourDeadline(t *testing.T) {
	nested := `{"type":"record","name":"R0","fields":[]}`
	for i := 1; i <= 40; i++ {
		nested = fmt.Sprintf(`{"type":"record","name":"R%d","fields":[{"name":"a","type":%s},{"name":"b","type":"R%d"}]}`, i, nested, i-1)
	}
	env := parseEnvelope(`{"type":"record","name":"E","fields":[{"name":"x","type":` + nested + `},` +
		`{"name":"source","type":{"type":"record","name":"S","fields":[{"name":"lsn","type":"long"}]}}]}`)
	if env.why != "" {
		t.Fatal(env.why)
	}
	hostile := append([]byte{0, 0, 0, 0, 9}, binary.AppendUvarint(nil, 1<<62)...)
	rr := &seqReader{end: 1000, gen: func(int32, int64) clients.Record { return clients.Record{Key: []byte("k"), Value: hostile} }}
	c := readerClient(rr, 1000, 0)
	c.envCache = map[int]*dbzEnvelope{9: env}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	rep, err := c.DupScan(ctx, "t", epoch, 0)
	if err != nil {
		t.Fatal(err)
	}
	if took := time.Since(start); took > 2*time.Second {
		t.Fatalf("took %s, want about the 150ms deadline", took)
	}
	if !rep.Partial || rep.Scanned == 0 || rep.Scanned >= 1000 || rep.UndecodableReasons[UndecodableMalformed] != rep.Scanned {
		t.Fatalf("report = %+v, want a partial scan of malformed values", rep)
	}
}

// staleReader returns only records before from: a reader that never moves
// forward must not spin the scan until the deadline.
type staleReader struct{ calls atomic.Int32 }

func (s *staleReader) ReadRecords(_ context.Context, topic string, p int32, from int64, _ int) ([]clients.Record, error) {
	s.calls.Add(1)
	return []clients.Record{{Topic: topic, Partition: p, Offset: from - 1, Key: []byte("k"), Value: dbzJSON(1, 1)}}, nil
}

func TestDupScanNoProgressEndsPartition(t *testing.T) {
	rr := &staleReader{}
	rep, err := readerClient(rr, 10, 0).DupScan(context.Background(), "t", epoch, 0)
	if err != nil {
		t.Fatal(err)
	}
	if n := rr.calls.Load(); n != 1 || rep.Partial || rep.Scanned != 0 || !rep.Partitions[0].Done {
		t.Fatalf("calls = %d, report = %+v; want one read and the partition ended", n, rep)
	}
}
