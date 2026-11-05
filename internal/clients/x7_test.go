package clients_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/kafka"
)

// The kafka client satisfies the X7 reader with the type it always returned:
// kafka.TopicConfig is now an alias of clients.TopicConfig.
var _ clients.TopicConfigReader = (*kafka.Client)(nil)

// fakeCatalog proves ColumnReader and TableRetentionReader can be implemented
// and asserted without a local adapter: it is what X7a/X7c's client fakes and
// X7d's watcher fake look like. scope stands in for the pipeline's tables.
type fakeCatalog struct {
	scope map[string]bool
	cols  map[string][]clients.ColumnInfo
	ret   map[string]clients.TableRetention
	calls int
}

var (
	_ clients.ColumnReader         = (*fakeCatalog)(nil)
	_ clients.TableRetentionReader = (*fakeCatalog)(nil)
)

// errOutOfScope stands in for tools.ErrOutOfScope (clients must not import tools).
var errOutOfScope = errors.New("out of scope")

func (f *fakeCatalog) check(table string) error {
	if !f.scope[table] {
		return fmt.Errorf("fake: table %s: %w", table, errOutOfScope)
	}
	return nil
}

func (f *fakeCatalog) Columns(_ context.Context, table string) ([]clients.ColumnInfo, error) {
	if err := f.check(table); err != nil {
		return nil, err
	}
	f.calls++
	c, ok := f.cols[table]
	if !ok {
		return nil, fmt.Errorf("fake: table %s: %w", table, clients.ErrNotFound)
	}
	return c[:min(len(c), clients.MaxTableColumns)], nil
}

func (f *fakeCatalog) TableRetention(_ context.Context, table string) (clients.TableRetention, error) {
	if err := f.check(table); err != nil {
		return clients.TableRetention{}, err
	}
	f.calls++
	r, ok := f.ret[table]
	if !ok {
		return clients.TableRetention{}, clients.ErrNotSupported
	}
	return r, nil
}

// E2, E5: the readers are optional: plain clients do not have them, the fake
// does, and callers find them by type assertion.
func TestX7ReadersAreOptionalAssertions(t *testing.T) {
	var w clients.WarehouseReader = struct{ clients.WarehouseReader }{}
	if _, ok := w.(clients.ColumnReader); ok {
		t.Fatal("a plain WarehouseReader must not be a ColumnReader")
	}
	if _, ok := w.(clients.TableRetentionReader); ok {
		t.Fatal("a plain WarehouseReader must not be a TableRetentionReader")
	}
	var s clients.SourceReader = struct{ clients.SourceReader }{}
	if _, ok := s.(clients.ColumnReader); ok {
		t.Fatal("a plain SourceReader must not be a ColumnReader")
	}
	var k clients.KafkaAdmin = struct{ clients.KafkaAdmin }{}
	if _, ok := k.(clients.TopicConfigReader); ok {
		t.Fatal("a plain KafkaAdmin must not be a TopicConfigReader")
	}
}

// E3, E4: the fake follows the documented rules: out of scope before I/O,
// unknown table is ErrNotFound, the column cap, ErrNotSupported passes through.
func TestX7FakeCatalogRules(t *testing.T) {
	wide := make([]clients.ColumnInfo, clients.MaxTableColumns+5)
	for i := range wide {
		wide[i] = clients.ColumnInfo{Name: fmt.Sprint("c", i), Type: "text", Position: i + 1}
	}
	f := &fakeCatalog{
		scope: map[string]bool{"public.orders": true, "public.wide": true, "public.gone": true},
		cols: map[string][]clients.ColumnInfo{
			"public.orders": {{Name: "id", Type: "bigint", Position: 1}, {Name: "customer_email", Type: "text", Position: 2, Nullable: true}},
			"public.wide":   wide,
		},
		ret: map[string]clients.TableRetention{"public.orders": {Table: "public.orders", Set: true, After: 30 * 24 * time.Hour, Source: "partition_expiration_days"}},
	}
	ctx := context.Background()

	if _, err := f.Columns(ctx, "other.secrets"); !errors.Is(err, errOutOfScope) || f.calls != 0 {
		t.Fatalf("out of scope: err %v, %d calls", err, f.calls)
	}
	if _, err := f.Columns(ctx, "public.gone"); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("unknown table: %v", err)
	}
	cols, err := f.Columns(ctx, "public.wide")
	if err != nil || len(cols) != clients.MaxTableColumns {
		t.Fatalf("wide table: %d columns, %v", len(cols), err)
	}
	if _, err := f.TableRetention(ctx, "public.wide"); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("no retention: %v", err)
	}
	r, err := f.TableRetention(ctx, "public.orders")
	if err != nil || !r.Set || r.Infinite || r.After != 30*24*time.Hour {
		t.Fatalf("retention = %+v, %v", r, err)
	}
	if clients.MaxTableColumns != 1600 || clients.CatalogTimeout != 10*time.Second {
		t.Fatal("X7 catalog caps changed; docs/api.md and docs/design/lineage.md state them")
	}
}

// E1: the X7 DTOs round-trip with the JSON keys the docs name, zero values
// included; ColumnInfo carries a name, a type and a position, nothing else.
func TestX7DTOsRoundTrip(t *testing.T) {
	for _, v := range []any{
		clients.ColumnInfo{Name: "customer_email", Type: "character varying", Position: 3, Nullable: true},
		clients.ColumnInfo{},
		clients.TableRetention{Table: "analytics.orders", Set: true, After: 7 * 24 * time.Hour, Source: "ttl"},
		clients.TableRetention{Table: "analytics.orders", Set: true, Infinite: true},
		clients.TableRetention{},
		clients.TopicConfig{Topic: "pg.public.orders", Partitions: 3, ReplicationFactor: 3, RetentionMS: "-1",
			RetentionBytes: "-1", CleanupPolicy: "compact", MinInsyncReplicas: "2"},
		clients.TopicConfig{},
	} {
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		ptr := reflect.New(reflect.TypeOf(v))
		if err := json.Unmarshal(raw, ptr.Interface()); err != nil {
			t.Fatal(err)
		}
		if got := ptr.Elem().Interface(); !reflect.DeepEqual(got, v) {
			t.Errorf("round trip %T\n got %#v\nwant %#v", v, got, v)
		}
	}
	if n := reflect.TypeOf(clients.ColumnInfo{}).NumField(); n != 4 {
		t.Fatalf("ColumnInfo has %d fields; a catalog read returns name, type, position and nullable only", n)
	}
	raw, _ := json.Marshal(clients.TopicConfig{Topic: "t"})
	const want = `{"topic":"t","partitions":0,"replication_factor":0,"retention_ms":"","retention_bytes":"","cleanup_policy":"","min_insync_replicas":""}`
	if string(raw) != want {
		t.Fatalf("TopicConfig JSON = %s; the kafka_topic_config tool's Data must not change shape", raw)
	}
}
