//go:build xwavegen

package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/kafka"
	"piperoom/internal/clients/postgres"
)

// world is the state of one recorded stack at one moment: what each fake
// client returns. The real tool code runs on top of it.
type world struct {
	connectors map[string]*connector
	// connectDown makes every Connect call go through the real Connect
	// client to a server that never answers, so the error text is exactly
	// the real client's timeout error.
	connectDown bool

	subjects     map[string][]schema // subject -> versions, oldest first
	subjectLevel map[string]string   // subject-level compatibility overrides
	globalLevel  string

	topics   map[string][]clients.PartitionOffsets
	topicCfg map[string]kafka.TopicConfig
	groups   map[string]clients.GroupLag
	records  map[string][]clients.RecordRef // topic -> records FindByKey can see

	slots   []clients.ReplicationSlot
	ddl     []clients.DDLChange // newest first
	txns    []clients.Transaction
	srcRows map[string]map[string]map[string]any // table -> pk -> row
	pubs    []postgres.PublicationTable

	whFresh map[string]time.Time
	whRows  map[string]map[string]map[string]any
}

type connector struct {
	status clients.ConnectorStatus
	config map[string]string
}

type schema struct {
	id     int
	schema string
}

func newWorld() *world {
	return &world{
		connectors:   map[string]*connector{},
		subjects:     map[string][]schema{},
		subjectLevel: map[string]string{},
		globalLevel:  "BACKWARD",
		topics:       map[string][]clients.PartitionOffsets{},
		topicCfg:     map[string]kafka.TopicConfig{},
		groups:       map[string]clients.GroupLag{},
		records:      map[string][]clients.RecordRef{},
		srcRows:      map[string]map[string]map[string]any{},
		whFresh:      map[string]time.Time{},
		whRows:       map[string]map[string]map[string]any{},
	}
}

// ---- Kafka Connect ----

type fakeConnect struct{ w *world }

func (c fakeConnect) ListConnectors(context.Context) ([]string, error) {
	var out []string
	for n := range c.w.connectors {
		out = append(out, n)
	}
	sort.Strings(out)
	return out, nil
}

func (c fakeConnect) Status(_ context.Context, name string) (clients.ConnectorStatus, error) {
	cn, ok := c.w.connectors[name]
	if !ok {
		return clients.ConnectorStatus{}, fmt.Errorf("connect: GET /connectors/%s/status: %w", name, clients.ErrNotFound)
	}
	return cn.status, nil
}

func (c fakeConnect) Config(_ context.Context, name string) (map[string]string, error) {
	cn, ok := c.w.connectors[name]
	if !ok {
		return nil, fmt.Errorf("connect: GET /connectors/%s/config: %w", name, clients.ErrNotFound)
	}
	out := map[string]string{}
	for k, v := range cn.config {
		out[k] = v
	}
	return out, nil
}

func (fakeConnect) RestartTask(context.Context, string, int) error {
	return fmt.Errorf("evalgen: writes are disabled")
}
func (fakeConnect) RestartConnector(context.Context, string) error {
	return fmt.Errorf("evalgen: writes are disabled")
}

// hangingConnect serves nothing: every request waits until the client gives
// up. Paired with the real Connect client and a short client timeout it
// produces the real "Client.Timeout exceeded" error text.
func hangingConnect() (*httptest.Server, func()) {
	stop := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-stop:
		case <-r.Context().Done():
		}
	}))
	return srv, func() { close(stop); srv.Close() }
}

// ---- Schema Registry ----

type fakeRegistry struct{ w *world }

func (r fakeRegistry) Subjects(context.Context) ([]string, error) {
	var out []string
	for s := range r.w.subjects {
		out = append(out, s)
	}
	sort.Strings(out)
	return out, nil
}

func (r fakeRegistry) Versions(_ context.Context, subject string) ([]int, error) {
	vs, ok := r.w.subjects[subject]
	if !ok {
		return nil, fmt.Errorf("schemareg: GET /subjects/%s/versions: %w: schema registry error 40401: Subject '%s' not found.", subject, clients.ErrNotFound, subject)
	}
	out := make([]int, len(vs))
	for i := range vs {
		out[i] = i + 1
	}
	return out, nil
}

func (r fakeRegistry) Schema(_ context.Context, subject string, version int) (clients.SchemaVersion, error) {
	vs, ok := r.w.subjects[subject]
	if !ok {
		return clients.SchemaVersion{}, fmt.Errorf("schemareg: GET /subjects/%s/versions/%s: %w: schema registry error 40401: Subject '%s' not found.", subject, versionPath(version), clients.ErrNotFound, subject)
	}
	if version == -1 {
		version = len(vs)
	}
	if version < 1 || version > len(vs) {
		return clients.SchemaVersion{}, fmt.Errorf("schemareg: GET /subjects/%s/versions/%d: %w: schema registry error 40402: Version %d not found.", subject, version, clients.ErrNotFound, version)
	}
	v := vs[version-1]
	return clients.SchemaVersion{Subject: subject, Version: version, ID: v.id, Schema: v.schema}, nil
}

func versionPath(v int) string {
	if v == -1 {
		return "latest"
	}
	return fmt.Sprint(v)
}

func (r fakeRegistry) Compatibility(_ context.Context, subject string) (string, bool, error) {
	if l, ok := r.w.subjectLevel[subject]; ok {
		return l, true, nil
	}
	return r.w.globalLevel, false, nil
}

func (fakeRegistry) SetCompatibility(context.Context, string, string) error {
	return fmt.Errorf("evalgen: writes are disabled")
}
func (fakeRegistry) Register(context.Context, string, string) (int, error) {
	return 0, fmt.Errorf("evalgen: writes are disabled")
}

// ---- Kafka ----

type fakeKafka struct{ w *world }

func (k fakeKafka) Topics(context.Context) ([]string, error) {
	var out []string
	for t := range k.w.topics {
		out = append(out, t)
	}
	sort.Strings(out)
	return out, nil
}

func (k fakeKafka) TopicOffsets(_ context.Context, topic string) ([]clients.PartitionOffsets, error) {
	offs, ok := k.w.topics[topic]
	if !ok {
		return nil, fmt.Errorf("kafka: topic %s: %w", topic, clients.ErrNotFound)
	}
	return append([]clients.PartitionOffsets(nil), offs...), nil
}

func (k fakeKafka) ConsumerGroups(context.Context) ([]string, error) {
	var out []string
	for g := range k.w.groups {
		out = append(out, g)
	}
	sort.Strings(out)
	return out, nil
}

func (k fakeKafka) GroupLag(_ context.Context, group string) (clients.GroupLag, error) {
	g, ok := k.w.groups[group]
	if !ok {
		return clients.GroupLag{}, fmt.Errorf("kafka: group %s: %w", group, clients.ErrNotFound)
	}
	return g, nil
}

func (k fakeKafka) FindByKey(_ context.Context, topic, key string, since time.Time, _ int) ([]clients.RecordRef, error) {
	if _, ok := k.w.topics[topic]; !ok {
		return nil, fmt.Errorf("kafka: topic %s: %w", topic, clients.ErrNotFound)
	}
	var out []clients.RecordRef
	for _, r := range k.w.records[topic] {
		if r.Key == key && !r.Timestamp.Before(since) {
			out = append(out, r)
		}
	}
	return out, nil
}

// FindByKeyScan reports a complete scan: the fake holds every record, so
// nothing is truncated. Without it kafka_find_by_key hedges with text the
// production client (a KeyScanReader since #430) never emits.
func (k fakeKafka) FindByKeyScan(ctx context.Context, topic, key string, since time.Time, maxScan int) (clients.KeyScan, error) {
	refs, err := k.FindByKey(ctx, topic, key, since, maxScan)
	if err != nil {
		return clients.KeyScan{}, err
	}
	return clients.KeyScan{Refs: refs}, nil
}

var _ clients.KeyScanReader = (*fakeKafka)(nil)

// DescribeTopicConfigs is kafkatools' local adapter for kafka_topic_config.
func (k fakeKafka) DescribeTopicConfigs(_ context.Context, topic string) (kafka.TopicConfig, error) {
	c, ok := k.w.topicCfg[topic]
	if !ok {
		return kafka.TopicConfig{}, fmt.Errorf("kafka: describe topic configs %s: %w", topic, clients.ErrNotFound)
	}
	return c, nil
}

// ---- Source Postgres ----

type fakeSource struct{ w *world }

func (s fakeSource) ReplicationSlots(context.Context) ([]clients.ReplicationSlot, error) {
	return append([]clients.ReplicationSlot(nil), s.w.slots...), nil
}

func (s fakeSource) RecentDDL(_ context.Context, since time.Time, limit int) ([]clients.DDLChange, error) {
	var out []clients.DDLChange
	for _, c := range s.w.ddl {
		if !c.At.Before(since) && len(out) < limit {
			out = append(out, c)
		}
	}
	return out, nil
}

func (s fakeSource) LongTransactions(_ context.Context, olderThan time.Duration) ([]clients.Transaction, error) {
	var out []clients.Transaction
	for _, t := range s.w.txns {
		if t.Age >= olderThan {
			out = append(out, t)
		}
	}
	return out, nil
}

// LongTransactionsOnTables is the scoped read pg_long_transactions needs since #107.
// The fake world has no lock table, so every long transaction counts as on the
// tables; nothing is "other".
func (s fakeSource) LongTransactionsOnTables(ctx context.Context, olderThan time.Duration, _ []string) (postgres.TableTransactions, error) {
	txns, err := s.LongTransactions(ctx, olderThan)
	if err != nil {
		return postgres.TableTransactions{}, err
	}
	return postgres.TableTransactions{Transactions: txns}, nil
}

func (s fakeSource) Row(_ context.Context, table, _, pk string) (map[string]any, bool, error) {
	r, ok := s.w.srcRows[table][pk]
	return r, ok, nil
}

// PublicationTables is dbtools' local adapter for pg_publication_tables.
func (s fakeSource) PublicationTables(_ context.Context, _, _ string, tables []string) ([]postgres.PublicationTable, error) {
	var out []postgres.PublicationTable
	for _, t := range tables {
		for _, p := range s.w.pubs {
			if p.Schema+"."+p.Table == t {
				out = append(out, p)
			}
		}
	}
	return out, nil
}

// ---- Warehouse ----

type fakeWarehouse struct{ w *world }

func (h fakeWarehouse) Freshness(_ context.Context, table, _ string) (time.Time, error) {
	ts, ok := h.w.whFresh[table]
	if !ok {
		return time.Time{}, fmt.Errorf("warehouse: table %s: %w", table, clients.ErrNotFound)
	}
	return ts, nil
}

func (h fakeWarehouse) Row(_ context.Context, table, _, pk string) (map[string]any, bool, error) {
	r, ok := h.w.whRows[table][pk]
	return r, ok, nil
}

// ---- helpers for building worlds ----

func (w *world) addConnector(name, typ, state, worker string, cfg map[string]string, tasks ...clients.TaskStatus) {
	w.connectors[name] = &connector{
		status: clients.ConnectorStatus{Name: name, Type: typ, State: state, WorkerID: worker, Tasks: tasks},
		config: cfg,
	}
}

func task(id int, state, worker, trace string) clients.TaskStatus {
	return clients.TaskStatus{ID: id, State: state, WorkerID: worker, Trace: strings.TrimSpace(trace)}
}
