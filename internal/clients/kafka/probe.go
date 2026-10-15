package kafka

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// A read_committed consumer drops aborted records and never delivers a
// marker beyond the last stable offset. When every offset left in a read's
// window is such a record (transaction C aborted below transaction A, still
// open: #300), no record ever reaches the window's end and the read would wait
// out its deadline. kgo exposes neither its fetch position nor fetches that
// carry no record, so a read that sees nothing for stallAfter asks the broker
// directly with probeWindows.

// stallAfter is how long a read polls without a record before it probes.
const stallAfter = 500 * time.Millisecond

// probeMaxBytes bounds one partition's probe response. KIP-74 still returns at
// least one whole batch, which is enough to tell readable from not.
const probeMaxBytes = 64 << 10

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

// advance probes the windows a stalled read still waits on and moves each
// window's from to the probe's fetch position. A window whose from reaches its
// end holds no readable record: the caller counts the partition done. A
// window with a readable record keeps its from; the consumer delivers it.
func (c *Client) advance(ctx context.Context, topic string, wins map[int32]readWindow) {
	if len(wins) == 0 {
		return
	}
	probe := c.probeFn
	if probe == nil {
		probe = c.probeWindows
	}
	for p, next := range probe(ctx, topic, wins) {
		if w, ok := wins[p]; ok && next > w.from {
			w.from = next
			wins[p] = w
		}
	}
}

// probeWindows sends one read_committed Fetch per partition leader at each
// window's from and returns, per partition with no readable record (data or
// marker) in [from, next), the offset next it reached. A partition with a
// readable record below its end, or whose probe failed, is left out: the read
// then keeps waiting on its consumer, bounded by its own deadline. Probe
// failures are logged, not returned, because they change nothing the read
// would otherwise do.
func (c *Client) probeWindows(ctx context.Context, topic string, wins map[int32]readWindow) map[int32]int64 {
	if c.adm == nil || c.cl == nil {
		return nil // zero-value test client: no broker to ask
	}
	start := time.Now()
	md, err := c.adm.Metadata(ctx, topic)
	c.record(ctx, start, "kadm.Metadata(topic)", "topic="+topic)
	if err != nil {
		slog.DebugContext(ctx, "kafka: read probe metadata failed", "topic", topic, "err", err)
		return nil
	}
	td, ok := md.Topics[topic]
	if !ok || td.Err != nil {
		return nil
	}
	byLeader := map[int32][]int32{}
	for p := range wins {
		if pd, ok := td.Partitions[p]; ok && pd.Err == nil && pd.Leader >= 0 {
			byLeader[pd.Leader] = append(byLeader[pd.Leader], p)
		}
	}
	out := map[int32]int64{}
	for leader, parts := range byLeader {
		req := kmsg.NewPtrFetchRequest()
		req.MaxBytes = int32(len(parts)) * probeMaxBytes
		req.IsolationLevel = 1 // read_committed: the broker lists the aborted transactions
		rt := kmsg.NewFetchRequestTopic()
		rt.Topic, rt.TopicID = topic, td.ID // the name up to v12, the id from v13
		for _, p := range parts {
			rp := kmsg.NewFetchRequestTopicPartition()
			rp.Partition, rp.FetchOffset, rp.PartitionMaxBytes = p, wins[p].from, probeMaxBytes
			rt.Partitions = append(rt.Partitions, rp)
		}
		req.Topics = append(req.Topics, rt)
		start := time.Now()
		resp, err := req.RequestWith(ctx, c.cl.Broker(int(leader)))
		c.record(ctx, start, "kmsg.FetchRequest(topic, read_committed)", "topic="+topic)
		if err != nil {
			slog.DebugContext(ctx, "kafka: read probe fetch failed", "topic", topic, "broker", leader, "err", err)
			continue
		}
		for i := range resp.Topics {
			for j := range resp.Topics[i].Partitions {
				rp := &resp.Topics[i].Partitions[j]
				w, ok := wins[rp.Partition]
				if !ok || rp.ErrorCode != 0 {
					continue
				}
				if next, ok := unreadableUpTo(topic, rp, w); ok {
					out[rp.Partition] = next
				}
			}
		}
	}
	return out
}

// unreadableUpTo decodes one probed partition as a read_committed consumer
// would. It returns the fetch position the response reached and true when no
// readable record lies in [w.from, w.end) before it; false when one does or
// the response cannot be decoded.
func unreadableUpTo(topic string, rp *kmsg.FetchResponseTopicPartition, w readWindow) (int64, bool) {
	fp, next := kgo.ProcessFetchPartition(kgo.ProcessFetchPartitionOpts{
		KeepControlRecords: true,
		Offset:             w.from,
		IsolationLevel:     kgo.ReadCommitted(),
		Topic:              topic,
		Partition:          rp.Partition,
	}, rp, kgo.DefaultDecompressor(), nil)
	if fp.Err != nil {
		return 0, false
	}
	for _, r := range fp.Records {
		if r.Offset >= w.end {
			return w.end, true // the first readable offset is past the window
		}
		if r.Offset >= w.from {
			return 0, false
		}
	}
	return next, true
}
