package kafka

import (
	"context"
	"errors"
	"log/slog"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// A read_committed consumer drops aborted records and never delivers a
// marker beyond the last stable offset. When every offset left in a read's
// window is such a record (transaction C aborted below transaction A, still
// open: #300), no record ever reaches the window's end and the read would wait
// out its deadline. kgo exposes neither its fetch position nor fetches that
// carry no record, so a read that sees nothing for stallAfter asks the broker
// directly through a prober.

// stallAfter is how long a read polls without a record before it probes.
const stallAfter = 500 * time.Millisecond

const (
	// probePartitionBytes bounds one partition's share of a probe response,
	// as FetchMaxPartitionBytes bounds the consumer's. KIP-74 still returns at
	// least one whole batch, which is enough to tell readable from not.
	probePartitionBytes = 1 << 20
	// probeMaxBytes caps one probe request, as FetchMaxBytes caps the
	// consumer's. A single partition gets a request limit strictly above its
	// partition limit: a broker that reaches the request limit may leave out
	// the aborted-transaction list, and the probe then sees aborted records
	// as readable and makes no progress (safe, but useless).
	probeMaxBytes = 4 << 20
)

// errProbe is returned when the broker gives a probe nothing to decide on.
var errProbe = errors.New("probe: no usable fetch response")

// readWindow is what a read still waits on in one partition: offsets [from, end).
type readWindow struct{ from, end int64 }

// pollOrIdle polls for at most stallAfter. idle is true when nothing but the
// poll's own timeout came back while ctx is still live.
func pollOrIdle(ctx context.Context, cons pollConsumer) (fetches kgo.Fetches, idle bool) {
	pctx, cancel := context.WithTimeout(ctx, stallAfter)
	defer cancel()
	fetches = cons.PollFetches(pctx)
	if ctx.Err() != nil || pctx.Err() == nil || fetches.NumRecords() > 0 {
		return fetches, false
	}
	for _, fe := range fetches.Errors() {
		if !errors.Is(fe.Err, context.DeadlineExceeded) && !errors.Is(fe.Err, context.Canceled) {
			return fetches, false
		}
	}
	return nil, true
}

// prober asks the broker about one read's windows. It lives for one read:
// it keeps the topic's metadata between rounds and counts its requests, and
// record adds them as one provenance receipt, after the read's own.
type prober struct {
	c     *Client
	topic string
	md    *kadm.TopicDetail
	reqs  int
	start time.Time
}

func (c *Client) newProber(topic string) *prober { return &prober{c: c, topic: topic} }

// record adds one receipt for every probe request of the read, if any.
func (pr *prober) record(ctx context.Context) {
	if pr.reqs > 0 {
		pr.c.record(ctx, pr.start, "kmsg.FetchRequest(topic, read_committed) probes", "topic="+pr.topic, "requests="+strconv.Itoa(pr.reqs))
	}
}

// advance moves each window's from to the fetch position the broker reached
// with nothing readable before it, probing again at once while some window
// keeps moving and is not done. A window whose from reaches its end holds no
// readable record: the caller counts the partition done. A window with a
// readable record (in it or just past it) keeps its from; the consumer
// delivers that record next.
func (pr *prober) advance(ctx context.Context, wins map[int32]readWindow) {
	for ctx.Err() == nil {
		open := map[int32]readWindow{}
		for p, w := range wins {
			if w.from < w.end {
				open[p] = w
			}
		}
		if len(open) == 0 {
			return
		}
		moved := false
		for p, next := range pr.round(ctx, open) {
			if w := wins[p]; next > w.from {
				w.from = min(next, w.end)
				wins[p] = w
				moved = true
			}
		}
		if !moved {
			return
		}
	}
}

// round is one probe of the given windows: per partition with no readable
// record (data or marker) in [from, next), the offset next it reached. A
// partition with a readable record before its end or just past it, or whose
// probe failed, is left out. Failures are logged, not returned, because they
// change nothing the read would otherwise do: it keeps waiting on its
// consumer, bounded by its own deadline.
func (pr *prober) round(ctx context.Context, wins map[int32]readWindow) map[int32]int64 {
	if pr.c.probeFn != nil {
		return pr.c.probeFn(ctx, pr.topic, wins)
	}
	at := make(map[int32]int64, len(wins))
	for p, w := range wins {
		at[p] = w.from
	}
	out := map[int32]int64{}
	for p, f := range pr.fetch(ctx, at) {
		if next, ok := unreadableUpTo(f, wins[p]); ok {
			out[p] = next
		}
	}
	return out
}

// fetched is one partition's probe result: its readable records (data and
// markers) at or after the probed offset, oldest first, and the offset the
// next fetch would start at.
type fetched struct {
	recs []*kgo.Record
	next int64
}

// fetch sends one read_committed Fetch per partition leader at the given
// offsets and decodes each partition as a read_committed consumer would. A
// partition whose probe failed is absent.
func (pr *prober) fetch(ctx context.Context, at map[int32]int64) map[int32]fetched {
	if pr.c.adm == nil || pr.c.cl == nil {
		return nil // zero-value test client: no broker to ask
	}
	if pr.start.IsZero() {
		pr.start = time.Now()
	}
	if pr.md == nil {
		pr.reqs++
		md, err := pr.c.adm.Metadata(ctx, pr.topic)
		if err != nil {
			slog.DebugContext(ctx, "kafka: read probe metadata failed", "topic", pr.topic, "err", err)
			return nil
		}
		td, ok := md.Topics[pr.topic]
		if !ok || td.Err != nil {
			return nil
		}
		pr.md = &td
	}
	byLeader := map[int32][]int32{}
	for p := range at {
		if pd, ok := pr.md.Partitions[p]; ok && pd.Err == nil && pd.Leader >= 0 {
			byLeader[pd.Leader] = append(byLeader[pd.Leader], p)
		}
	}
	out := map[int32]fetched{}
	for leader, parts := range byLeader {
		req := kmsg.NewPtrFetchRequest()
		req.MaxBytes = int32(min(int64(len(parts)+1)*probePartitionBytes, probeMaxBytes))
		req.IsolationLevel = 1 // read_committed: the broker lists the aborted transactions
		rt := kmsg.NewFetchRequestTopic()
		rt.Topic, rt.TopicID = pr.topic, pr.md.ID // the name up to v12, the id from v13
		for _, p := range parts {
			rp := kmsg.NewFetchRequestTopicPartition()
			rp.Partition, rp.FetchOffset, rp.PartitionMaxBytes = p, at[p], probePartitionBytes
			rt.Partitions = append(rt.Partitions, rp)
		}
		req.Topics = append(req.Topics, rt)
		pr.reqs++
		resp, err := req.RequestWith(ctx, pr.c.cl.Broker(int(leader)))
		if err != nil {
			pr.md = nil // a moved leader shows up as a failure; look it up again next round
			slog.DebugContext(ctx, "kafka: read probe fetch failed", "topic", pr.topic, "broker", leader, "err", err)
			continue
		}
		for i := range resp.Topics {
			for j := range resp.Topics[i].Partitions {
				rp := &resp.Topics[i].Partitions[j]
				from, ok := at[rp.Partition]
				if !ok || rp.ErrorCode != 0 {
					continue
				}
				fp, next := kgo.ProcessFetchPartition(kgo.ProcessFetchPartitionOpts{
					KeepControlRecords: true,
					Offset:             from,
					IsolationLevel:     kgo.ReadCommitted(),
					Topic:              pr.topic,
					Partition:          rp.Partition,
				}, rp, kgo.DefaultDecompressor(), nil)
				if fp.Err != nil {
					continue
				}
				out[rp.Partition] = fetched{recs: fp.Records, next: next}
			}
		}
	}
	return out
}

// unreadableUpTo returns the fetch position f reached and true when no
// readable record lies at or after w.from in f; false when one does, inside
// the window or past it. A readable record past the window is left to the
// consumer, which delivers it next: it finishes the partition, and it is the
// stand-in a record-timestamp read promises.
func unreadableUpTo(f fetched, w readWindow) (int64, bool) {
	for _, r := range f.recs {
		if r.Offset >= w.from {
			return 0, false
		}
	}
	return f.next, true
}

// newestReadable returns the timestamp of the newest readable record (data
// or marker) in [lo, end) of partition p, and false when there is none. It
// reads backwards in growing spans, each read forward to end, so a short
// aborted tail costs one or two requests. A probe that cannot decide is an
// error: the caller reports it rather than a false "never".
func (pr *prober) newestReadable(ctx context.Context, p int32, lo, end int64) (time.Time, bool, error) {
	for span := int64(64); ; span *= 8 {
		from := max(lo, end-span)
		var newest time.Time
		found := false
		for pos := from; pos < end; {
			f, ok := pr.fetch(ctx, map[int32]int64{p: pos})[p]
			if !ok {
				return time.Time{}, false, errProbe
			}
			for _, r := range f.recs {
				if r.Offset >= pos && r.Offset < end {
					newest, found = r.Timestamp, true
				}
			}
			if f.next <= pos {
				return time.Time{}, false, errProbe
			}
			pos = f.next
		}
		if found {
			return newest, true, nil
		}
		if from == lo {
			return time.Time{}, false, nil
		}
		if err := ctx.Err(); err != nil {
			return time.Time{}, false, err
		}
	}
}
