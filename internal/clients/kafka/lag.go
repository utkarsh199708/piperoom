package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"piperoom/internal/clients"
)

// Lag anomaly kinds reported by GroupLagReport.
const (
	// AnomalyTopicMissing: the group has a commit on a topic or partition
	// the cluster no longer has (deleted or recreated topic).
	AnomalyTopicMissing = "topic_missing"
	// AnomalyOffsetsUnavailable: the commit or the partition's offsets could
	// not be read (authorization, leader election, ...).
	AnomalyOffsetsUnavailable = "offsets_unavailable"
	// AnomalyCommitBeyondEnd: the commit is past the log end (topic recreated
	// or truncated); the reported lag of 0 is not real.
	AnomalyCommitBeyondEnd = "commit_beyond_end"
	// AnomalyCommitBelowLogStart: retention already deleted the records at
	// the commit; on its next fetch the consumer resets per
	// auto.offset.reset and the records in between are lost to it.
	AnomalyCommitBelowLogStart = "commit_below_log_start"
)

// LagAnomaly is one partition whose committed offset cannot be taken at face
// value. It lives in this package until clients.PartitionLag can carry it
// (contract change request in the PR).
type LagAnomaly struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Kind      string `json:"kind"`
	Committed int64  `json:"committed"`       // -1 if nothing committed
	Start     int64  `json:"start"`           // log start offset; -1 if unknown
	End       int64  `json:"end"`             // log end offset; -1 if unknown
	Error     string `json:"error,omitempty"` // broker error, for the unavailable kinds
}

// LagReport is a group's lag plus the partitions whose commit is suspect.
// GroupLag holds what clients.KafkaAdmin.GroupLag returns: partitions whose
// commit or offsets could not be read are only in Anomalies.
type LagReport struct {
	clients.GroupLag
	Anomalies []LagAnomaly `json:"anomalies,omitempty"`
}

// GroupLagReport is GroupLag plus the anomalies GroupLag cannot express:
// commits on deleted topics, commits past the log end and commits below the
// log start offset. A group the cluster does not know is an error wrapping
// clients.ErrNotFound.
func (c *Client) GroupLagReport(ctx context.Context, group string) (LagReport, error) {
	dgs, err := boundCtx(ctx, func(ctx context.Context) (kadm.DescribedGroupLags, error) {
		return c.adm.Lag(ctx, group)
	})
	if err != nil {
		return LagReport{}, fmt.Errorf("kafka: group lag %s: %w", group, mapGroupNotFound(err))
	}
	dg, ok := dgs[group]
	if !ok {
		return LagReport{}, fmt.Errorf("kafka: group %s: %w", group, clients.ErrNotFound)
	}
	return lagReportFrom(dg)
}

// lagReportFrom maps kadm's lag onto the report. Split out so the mapping is
// tested without a broker.
func lagReportFrom(dg kadm.DescribedGroupLag) (LagReport, error) {
	if dg.DescribeErr != nil {
		return LagReport{}, fmt.Errorf("kafka: describe %s: %w", dg.Group, mapGroupNotFound(dg.DescribeErr))
	}
	if dg.FetchErr != nil {
		return LagReport{}, fmt.Errorf("kafka: fetch offsets %s: %w", dg.Group, mapGroupNotFound(dg.FetchErr))
	}
	// Brokers before KIP-848 describe an unknown group as "Dead" with no
	// error; with no members and no commit it is indistinguishable from absent.
	if dg.State == "Dead" && len(dg.Members) == 0 && noPartitions(dg.Lag) {
		return LagReport{}, fmt.Errorf("kafka: group %s: %w", dg.Group, clients.ErrNotFound)
	}
	rep := LagReport{GroupLag: clients.GroupLag{
		Group:    dg.Group,
		State:    dg.State,
		Members:  len(dg.Members),
		TotalLag: dg.Lag.Total(),
	}}
	for _, l := range dg.Lag.Sorted() {
		if a, ok := lagAnomaly(l); ok {
			rep.Anomalies = append(rep.Anomalies, a)
		}
		if l.Err != nil {
			continue // in Anomalies; no lag to report
		}
		rep.Partitions = append(rep.Partitions, clients.PartitionLag{
			Topic:     l.Topic,
			Partition: l.Partition,
			Committed: l.Commit.At,
			End:       l.End.Offset,
			Lag:       l.Lag,
		})
	}
	sort.Slice(rep.Partitions, func(i, j int) bool {
		if rep.Partitions[i].Topic != rep.Partitions[j].Topic {
			return rep.Partitions[i].Topic < rep.Partitions[j].Topic
		}
		return rep.Partitions[i].Partition < rep.Partitions[j].Partition
	})
	sort.Slice(rep.Anomalies, func(i, j int) bool {
		if rep.Anomalies[i].Topic != rep.Anomalies[j].Topic {
			return rep.Anomalies[i].Topic < rep.Anomalies[j].Topic
		}
		return rep.Anomalies[i].Partition < rep.Anomalies[j].Partition
	})
	return rep, nil
}

// noPartitions reports whether l has no partition at all. (GroupLag.IsEmpty
// means something else: that the group is in the Empty state.)
func noPartitions(l kadm.GroupLag) bool {
	for _, ps := range l {
		if len(ps) > 0 {
			return false
		}
	}
	return true
}

// lagAnomaly classifies one partition's lag entry.
func lagAnomaly(l kadm.GroupMemberLag) (LagAnomaly, bool) {
	a := LagAnomaly{Topic: l.Topic, Partition: l.Partition, Committed: l.Commit.At, Start: -1, End: -1}
	if l.Start.Err == nil {
		a.Start = l.Start.Offset
	}
	if l.Err != nil {
		a.Error = l.Err.Error()
		a.Kind = AnomalyOffsetsUnavailable
		if errors.Is(l.Err, kerr.UnknownTopicOrPartition) || errors.Is(l.Err, kerr.UnknownTopicID) {
			a.Kind = AnomalyTopicMissing
		}
		return a, true
	}
	a.End = l.End.Offset
	switch {
	case l.Commit.At < 0:
		return LagAnomaly{}, false
	case l.Commit.At > l.End.Offset:
		a.Kind = AnomalyCommitBeyondEnd
		return a, true
	case a.Start >= 0 && l.Commit.At < a.Start:
		a.Kind = AnomalyCommitBelowLogStart
		return a, true
	}
	return LagAnomaly{}, false
}

// mapGroupNotFound turns the broker's GROUP_ID_NOT_FOUND into
// clients.ErrNotFound, keeping the broker error in the chain.
func mapGroupNotFound(err error) error {
	if errors.Is(err, kerr.GroupIDNotFound) {
		return fmt.Errorf("%w: %w", clients.ErrNotFound, err)
	}
	return err
}
