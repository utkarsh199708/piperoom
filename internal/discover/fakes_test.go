package discover

import (
	"context"
	"errors"

	"piperoom/internal/clients"
)

// The fakes return lists in reverse order on purpose: the code must sort.

type fakeConnect struct {
	configs  map[string]map[string]string
	errs     map[string]error // per-connector Config error
	listErr  error
	blocking bool // Config blocks until ctx ends
}

func (f *fakeConnect) ListConnectors(context.Context) ([]string, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	names := sortedKeys(f.configs)
	for n := range f.errs {
		names = append(names, n)
	}
	for i, j := 0, len(names)-1; i < j; i, j = i+1, j-1 {
		names[i], names[j] = names[j], names[i]
	}
	return names, nil
}

func (f *fakeConnect) Config(ctx context.Context, name string) (map[string]string, error) {
	if f.blocking {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if err, ok := f.errs[name]; ok {
		return nil, err
	}
	c, ok := f.configs[name]
	if !ok {
		return nil, clients.ErrNotFound
	}
	return c, nil
}

type fakeKafka struct {
	topics    []string
	topicsErr error
	groups    map[string]clients.GroupLag
	groupErrs map[string]error
	listErr   error
	lagCalls  map[string]int
}

func (f *fakeKafka) Topics(context.Context) ([]string, error) {
	if f.topicsErr != nil {
		return nil, f.topicsErr
	}
	return reversed(f.topics), nil
}

func (f *fakeKafka) ConsumerGroups(context.Context) ([]string, error) {
	if f.listErr != nil {
		return nil, f.listErr
	}
	names := sortedKeys(f.groups)
	for n := range f.groupErrs {
		names = append(names, n)
	}
	return reversed(sortedUnique(names)), nil
}

func (f *fakeKafka) GroupLag(_ context.Context, g string) (clients.GroupLag, error) {
	if f.lagCalls == nil {
		f.lagCalls = map[string]int{}
	}
	f.lagCalls[g]++
	if err, ok := f.groupErrs[g]; ok {
		return clients.GroupLag{}, err
	}
	return f.groups[g], nil
}

type fakeRegistry struct {
	subjects []string
	err      error
}

func (f *fakeRegistry) Subjects(context.Context) ([]string, error) {
	return reversed(f.subjects), f.err
}

type fakeSource struct {
	slots []clients.ReplicationSlot
	err   error
}

func (f *fakeSource) ReplicationSlots(context.Context) ([]clients.ReplicationSlot, error) {
	return f.slots, f.err
}

func reversed(in []string) []string {
	out := make([]string, len(in))
	for i, s := range in {
		out[len(in)-1-i] = s
	}
	return out
}

var errBoom = errors.New("boom")

func pgConfig(kv ...string) map[string]string {
	c := map[string]string{"connector.class": pgSourceClass}
	for i := 0; i+1 < len(kv); i += 2 {
		c[kv[i]] = kv[i+1]
	}
	return c
}

func committed(topic string) clients.PartitionLag {
	return clients.PartitionLag{Topic: topic, Committed: 10, End: 12, Lag: 2}
}

// happyDeps is the 3-connector fixture behind the golden file.
func happyDeps() Deps {
	return Deps{
		Connect: &fakeConnect{configs: map[string]map[string]string{
			"pg-orders-connector": pgConfig(
				"topic.prefix", "pg",
				"table.include.list", "public.orders",
				"database.password", "s3cr3t-pw",
				"database.user", "dbz-user-xyz",
				"database.hostname", "db-host-xyz.internal",
			),
			"Legacy Billing": pgConfig("database.server.name", "billing", "transforms", "route"),
			"wh-sink":        {"connector.class": "io.confluent.connect.jdbc.JdbcSinkConnector", "topics": "pg.public.orders", "connection.password": "sink-pw-xyz"},
		}},
		Kafka: &fakeKafka{
			topics: []string{"pg.public.orders", "pg.transaction", "pg.eu.public.x", "billing.public.invoices", "billing.public.payments", "__consumer_offsets"},
			groups: map[string]clients.GroupLag{
				"order-sync":   {Group: "order-sync", State: "Stable", Partitions: []clients.PartitionLag{committed("pg.public.orders")}},
				"billing-sync": {Group: "billing-sync", State: "Empty", Partitions: []clients.PartitionLag{committed("billing.public.invoices")}},
				"never":        {Group: "never", State: "Stable", Partitions: []clients.PartitionLag{{Topic: "pg.public.orders", Committed: -1}}},
			},
		},
		Registry: &fakeRegistry{subjects: []string{"pg.public.orders-value", "billing.public.invoices-value"}},
		Source:   &fakeSource{slots: []clients.ReplicationSlot{{Name: "debezium", Active: true}}},
	}
}
