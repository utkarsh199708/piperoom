package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"

	"piperoom/internal/clients"
)

var _ clients.ClusterReader = (*Client)(nil)

// clusterAdmin is the part of *kadm.Client that ClusterHealth and
// PartitionRates use; a seam so unit tests run without a broker.
type clusterAdmin interface {
	Metadata(ctx context.Context, topics ...string) (kadm.Metadata, error)
	DescribeTopicConfigs(ctx context.Context, topics ...string) (kadm.ResourceConfigs, error)
	DescribeAllLogDirs(ctx context.Context, s kadm.TopicsSet) (kadm.DescribedAllLogDirs, error)
	ListPartitionReassignments(ctx context.Context, s kadm.TopicsSet) (kadm.ListPartitionReassignmentsResponses, error)
	ListOffsetsAfterMilli(ctx context.Context, millisecond int64, topics ...string) (kadm.ListedOffsets, error)
	ListEndOffsets(ctx context.Context, topics ...string) (kadm.ListedOffsets, error)
	DescribeACLs(ctx context.Context, b *kadm.ACLBuilder) (kadm.DescribeACLsResults, error)
}

// clusterAdm returns the test seam when set, else the real kadm client.
func (c *Client) clusterAdm() clusterAdmin {
	if c.clusterFn != nil {
		return c.clusterFn
	}
	return c.adm
}

// LogDirNotPermitted is the BrokerHealth.LogDirErr text for a principal that
// lacks DescribeConfigs on Cluster; the tool prints it as "log dirs: not permitted".
const LogDirNotPermitted = "not permitted"

// ClusterHealth reads broker and partition health for topics only: one
// metadata request, min.insync.replicas per topic, log dirs and active
// reassignments. Log dirs and reassignments are best-effort (a missing grant
// shows as BrokerHealth.LogDirErr or an unset Reassigning, never an error);
// metadata failing is. Topics the cluster does not know are left out.
func (c *Client) ClusterHealth(ctx context.Context, topics ...string) (clients.ClusterHealth, error) {
	adm := c.clusterAdm()
	var (
		wg      sync.WaitGroup
		cfgs    kadm.ResourceConfigs
		logs    kadm.DescribedAllLogDirs
		logsErr error
	)
	// Configs and log dirs do not depend on metadata: run them alongside it.
	if len(topics) > 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start := time.Now()
			// A failure leaves MinISR 0 = unknown.
			cfgs, _ = adm.DescribeTopicConfigs(ctx, topics...)
			c.record(ctx, start, "kadm.DescribeTopicConfigs(topics)", topicsParam(topics))
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		start := time.Now()
		logs, logsErr = adm.DescribeAllLogDirs(ctx, nil)
		c.record(ctx, start, "kadm.DescribeAllLogDirs(all)")
	}()

	start := time.Now()
	md, err := adm.Metadata(ctx, topics...)
	c.record(ctx, start, "kadm.Metadata(topics)", topicsParam(topics))
	if err != nil {
		wg.Wait()
		return clients.ClusterHealth{}, fmt.Errorf("kafka: cluster metadata: %w", err)
	}

	out := clients.ClusterHealth{ClusterID: md.Cluster, Controller: md.Controller}
	var set kadm.TopicsSet
	for _, name := range sortedTopicNames(md.Topics) {
		td := md.Topics[name]
		if td.Err != nil {
			if errors.Is(td.Err, kerr.UnknownTopicOrPartition) {
				continue
			}
			wg.Wait()
			return clients.ClusterHealth{}, fmt.Errorf("kafka: cluster metadata %s: %w", name, td.Err)
		}
		for _, pd := range td.Partitions.Sorted() {
			out.Partitions = append(out.Partitions, clients.PartitionHealth{
				Topic: name, Partition: pd.Partition, Leader: pd.Leader,
				Replicas: nonNil(pd.Replicas), ISR: nonNil(pd.ISR), OfflineReplicas: nonNil(pd.OfflineReplicas),
			})
			set.Add(name, pd.Partition)
		}
	}

	var reass kadm.ListPartitionReassignmentsResponses
	if len(set) > 0 {
		start = time.Now()
		reass, _ = adm.ListPartitionReassignments(ctx, set) // best-effort
		c.record(ctx, start, "kadm.ListPartitionReassignments(topics)", topicsParam(topics))
	}
	wg.Wait()

	minISR := map[string]int{}
	for _, rc := range cfgs {
		if rc.Err != nil {
			continue
		}
		for i := range rc.Configs {
			if rc.Configs[i].Key == "min.insync.replicas" {
				if n, err := strconv.Atoi(rc.Configs[i].MaybeValue()); err == nil {
					minISR[rc.Name] = n
				}
			}
		}
	}
	for i := range out.Partitions {
		p := &out.Partitions[i]
		p.MinISR = minISR[p.Topic]
		if r, ok := reass[p.Topic][p.Partition]; ok && (len(r.AddingReplicas) > 0 || len(r.RemovingReplicas) > 0) {
			p.Reassigning = true
		}
	}
	out.Brokers = brokerHealth(md, logs, logsErr)
	return out, nil
}

// brokerHealth joins the metadata brokers with their log dirs. Per broker the
// byte counts are summed over its directories; one unreported directory makes
// the sum unknown (-1) rather than a misleading partial figure.
func brokerHealth(md kadm.Metadata, logs kadm.DescribedAllLogDirs, logsErr error) []clients.BrokerHealth {
	brokers := append([]kadm.BrokerDetail(nil), md.Brokers...)
	sort.Slice(brokers, func(i, j int) bool { return brokers[i].NodeID < brokers[j].NodeID })
	out := make([]clients.BrokerHealth, 0, len(brokers))
	for _, b := range brokers {
		bh := clients.BrokerHealth{
			NodeID: b.NodeID, Host: b.Host, Port: b.Port, IsController: b.NodeID == md.Controller,
			LogDirTotalBytes: -1, LogDirUsableBytes: -1,
		}
		dirs, ok := logs[b.NodeID]
		switch {
		case ok && len(dirs) > 0:
			var total, usable int64
			known := true
			for _, d := range dirs.Sorted() {
				if d.Err != nil {
					bh.LogDirErr = logDirErr(d.Err)
					known = false
					continue
				}
				if d.TotalBytes < 0 || d.UsableBytes < 0 {
					known = false
					continue
				}
				total += d.TotalBytes
				usable += d.UsableBytes
			}
			if known {
				bh.LogDirTotalBytes, bh.LogDirUsableBytes = total, usable
			}
		case logsErr != nil:
			bh.LogDirErr = logDirErr(logsErr)
		default:
			bh.LogDirErr = "broker reported no log dirs"
		}
		out = append(out, bh)
	}
	return out
}

// logDirErr shortens a log-dir failure to text safe for the summary.
func logDirErr(err error) string {
	if errors.Is(err, kerr.ClusterAuthorizationFailed) {
		return LogDirNotPermitted
	}
	var ae *kadm.AuthError
	if errors.As(err, &ae) {
		return LogDirNotPermitted
	}
	return "unavailable"
}

func sortedTopicNames(m kadm.TopicDetails) []string {
	names := make([]string, 0, len(m))
	for n := range m {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

func nonNil(s []int32) []int32 {
	if s == nil {
		return []int32{}
	}
	return s
}

func topicsParam(topics []string) string {
	return fmt.Sprintf("topics=%d", len(topics))
}
