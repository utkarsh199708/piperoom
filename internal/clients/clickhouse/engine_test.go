package clickhouse

import (
	"context"
	"errors"
	"strings"
	"testing"

	driver "github.com/ClickHouse/clickhouse-go/v2"

	"piperoom/internal/clients"
)

// kafkaEngineDDL is the shape ClickHouse 25.8 returns for the dev Kafka engine
// table (dev/clickhouse, cmd/devctl/clickhouse.go): the statement is one string
// with the group in its SETTINGS clause.
const kafkaEngineDDL = "CREATE TABLE analytics.`orders_kafka`\n(\n    `id` UInt64\n)\nENGINE = Kafka\n" +
	"SETTINGS kafka_broker_list = 'kafka:9092', kafka_topic_list = 'pg.public.orders', " +
	"kafka_group_name = 'piperoom-clickhouse-orders', kafka_format = 'AvroConfluent'"

func TestEngineGroupParsesKafkaGroupName(t *testing.T) {
	tests := []struct {
		name    string
		ddl     string
		want    string
		wantErr error
	}{
		{"dev engine table", kafkaEngineDDL, "piperoom-clickhouse-orders", nil},
		{"spacing and case", "engine = kafka SETTINGS KAFKA_GROUP_NAME = 'orders.sink_group'", "orders.sink_group", nil},
		{"single setting", "ENGINE = Kafka SETTINGS kafka_group_name='g1'", "g1", nil},
		{"escaped quote is not a plain group id", `ENGINE = Kafka SETTINGS kafka_group_name = 'g\'1'`, "", clients.ErrNotSupported},
		{"not a kafka engine", "CREATE TABLE t (id UInt64) ENGINE = MergeTree ORDER BY id", "", clients.ErrNotSupported},
		{"kafka engine without a group", "ENGINE = Kafka SETTINGS kafka_topic_list = 't'", "", clients.ErrNotSupported},
		{"group with a space", "ENGINE = Kafka SETTINGS kafka_group_name = 'two words'", "", clients.ErrNotSupported},
		{"group with a newline", "ENGINE = Kafka SETTINGS kafka_group_name = 'a\nb'", "", clients.ErrNotSupported},
		{"empty group", "ENGINE = Kafka SETTINGS kafka_group_name = ''", "", clients.ErrNotSupported},
		{"too long", "ENGINE = Kafka SETTINGS kafka_group_name = '" + strings.Repeat("g", 256) + "'", "", clients.ErrNotSupported},
		{"text after the setting", "ENGINE = Kafka SETTINGS kafka_group_name = 'g1', kafka_format = 'JSONEachRow'", "g1", nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := engineGroup(tt.ddl)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("engineGroup error = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil || got != tt.want {
				t.Fatalf("engineGroup = %q, %v; want %q", got, err, tt.want)
			}
		})
	}
}

func TestEngineConsumerGroupReadsShowCreate(t *testing.T) {
	q := &fakeQuerier{responses: []queryResponse{{rows: &fakeRows{columns: []string{"statement"}, data: [][]any{{kafkaEngineDDL}}}}}}
	got, err := newTestClient(q).EngineConsumerGroup(context.Background(), "analytics.orders_kafka")
	if err != nil || got != "piperoom-clickhouse-orders" {
		t.Fatalf("EngineConsumerGroup = %q, %v", got, err)
	}
	if len(q.calls) != 1 || q.calls[0].query != "SHOW CREATE TABLE `analytics`.`orders_kafka`" {
		t.Fatalf("query = %+v, want one SHOW CREATE TABLE on the quoted table", q.calls)
	}
	if len(q.calls[0].args) != 0 {
		t.Fatalf("args = %#v, want none: the table is an identifier", q.calls[0].args)
	}
}

func TestEngineConsumerGroupErrors(t *testing.T) {
	t.Run("missing table", func(t *testing.T) {
		q := &fakeQuerier{responses: []queryResponse{{err: &driver.Exception{Code: errCannotGetCreateTable, Name: "CANNOT_GET_CREATE_TABLE_QUERY"}}}}
		_, err := newTestClient(q).EngineConsumerGroup(context.Background(), "analytics.missing_kafka")
		if !errors.Is(err, clients.ErrNotFound) {
			t.Fatalf("error = %v, want ErrNotFound", err)
		}
	})

	t.Run("access denied", func(t *testing.T) {
		cause := &driver.Exception{Code: errAccessDenied, Name: "ACCESS_DENIED", Message: "Not enough privileges"}
		q := &fakeQuerier{responses: []queryResponse{{err: cause}}}
		_, err := newTestClient(q).EngineConsumerGroup(context.Background(), "analytics.orders_kafka")
		if err == nil {
			t.Fatal("EngineConsumerGroup succeeded without SHOW CREATE privilege")
		}
		var chErr *driver.Exception
		if !errors.As(err, &chErr) || chErr.Code != errAccessDenied {
			t.Fatalf("error = %v, want the server exception kept in the chain", err)
		}
		if strings.Contains(err.Error(), "supersecret") {
			t.Fatalf("error leaked DSN password: %v", err)
		}
	})

	t.Run("target down", func(t *testing.T) {
		q := &fakeQuerier{responses: []queryResponse{{err: errors.New("connection refused for clickhouse://piperoom:supersecret@localhost:9000/analytics")}}}
		_, err := newTestClient(q).EngineConsumerGroup(context.Background(), "analytics.orders_kafka")
		if err == nil || strings.Contains(err.Error(), "supersecret") {
			t.Fatalf("error = %v, want a safe target-down error", err)
		}
	})

	t.Run("not a kafka engine", func(t *testing.T) {
		q := &fakeQuerier{responses: []queryResponse{{rows: &fakeRows{columns: []string{"statement"}, data: [][]any{{"ENGINE = MergeTree ORDER BY id"}}}}}}
		_, err := newTestClient(q).EngineConsumerGroup(context.Background(), "analytics.orders")
		if !errors.Is(err, clients.ErrNotSupported) {
			t.Fatalf("error = %v, want ErrNotSupported", err)
		}
	})

	t.Run("no rows", func(t *testing.T) {
		q := &fakeQuerier{responses: []queryResponse{{rows: &fakeRows{columns: []string{"statement"}}}}}
		_, err := newTestClient(q).EngineConsumerGroup(context.Background(), "analytics.orders_kafka")
		if err == nil {
			t.Fatal("EngineConsumerGroup accepted an empty SHOW CREATE result")
		}
	})

	t.Run("non-string value", func(t *testing.T) {
		q := &fakeQuerier{responses: []queryResponse{{rows: &fakeRows{columns: []string{"statement"}, data: [][]any{{42}}}}}}
		_, err := newTestClient(q).EngineConsumerGroup(context.Background(), "analytics.orders_kafka")
		if err == nil {
			t.Fatal("EngineConsumerGroup accepted a non-string SHOW CREATE result")
		}
	})

	t.Run("scan error", func(t *testing.T) {
		q := &fakeQuerier{responses: []queryResponse{{rows: &fakeRows{columns: []string{"statement"}, data: [][]any{{"x"}}, scanErr: errors.New("bad row")}}}}
		_, err := newTestClient(q).EngineConsumerGroup(context.Background(), "analytics.orders_kafka")
		if err == nil || strings.Contains(err.Error(), "supersecret") {
			t.Fatalf("error = %v, want a safe decode error", err)
		}
	})

	t.Run("bad input does not query", func(t *testing.T) {
		for _, table := range []string{"analytics.orders; DROP TABLE x", "analytics.`orders`", "", "a.b.c"} {
			q := &fakeQuerier{}
			if _, err := newTestClient(q).EngineConsumerGroup(context.Background(), table); err == nil {
				t.Fatalf("EngineConsumerGroup accepted %q", table)
			}
			if len(q.calls) != 0 {
				t.Fatalf("unsafe table %q ran %d queries", table, len(q.calls))
			}
		}
	})
}

func TestUnescapeQuoted(t *testing.T) {
	tests := []struct {
		in, want string
	}{
		{"plain", "plain"},
		{`a\'b`, "a'b"},
		{`a\\b`, `a\b`},
		{`a\nb`, "anb"},
		{`trailing\`, `trailing\`},
	}
	for _, tt := range tests {
		if got := unescapeQuoted(tt.in); got != tt.want {
			t.Errorf("unescapeQuoted(%q) = %q, want %q", tt.in, got, tt.want)
		}
	}
}
