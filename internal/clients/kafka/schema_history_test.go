package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/clients"
)

const histTopic = "schema-changes.orders"

// historyCluster starts kfake with the history topic on partitions and
// produces values to partition 0, oldest first.
func historyCluster(t *testing.T, partitions int32, values ...string) *Client {
	t.Helper()
	kc, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(partitions, histTopic))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(kc.Close)
	prod, err := kgo.NewClient(kgo.SeedBrokers(kc.ListenAddrs()...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	t.Cleanup(prod.Close)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	base := time.UnixMilli(1_700_000_000_000)
	for i, v := range values {
		r := &kgo.Record{Topic: histTopic, Partition: 0, Key: []byte(`{"databaseName":"orders"}`), Value: []byte(v), Timestamp: base.Add(time.Duration(i) * time.Second)}
		if err := prod.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
	return kfClient(t, kc.ListenAddrs())
}

func historyValueJSON(i int, ddl string) string {
	b, _ := json.Marshal(map[string]any{
		"source":       map[string]any{"server": "orders"},
		"position":     map[string]any{"file": "mysql-bin.000003", "pos": 154 + i, "gtids": "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-" + fmt.Sprint(10+i), "ts_sec": 1700000000, "nested": map[string]any{"x": 1}},
		"ts_ms":        1700000000000 + i,
		"databaseName": "orders",
		"schemaName":   nil,
		"ddl":          ddl,
		"tableChanges": []any{map[string]any{"table": map[string]any{"columns": []any{map[string]any{"defaultValueExpression": "'secret-row-value'"}}}}},
	})
	return string(b)
}

func TestSchemaHistoryTailNewestFirstAndCleaned(t *testing.T) {
	long := "ALTER TABLE orders ADD COLUMN note VARCHAR(255) DEFAULT 'x'" + strings.Repeat(" ", 10) + strings.Repeat("y", 400)
	c := historyCluster(t, 1,
		historyValueJSON(0, "CREATE TABLE orders (id INT PRIMARY KEY)"),
		historyValueJSON(1, "CREATE USER 'app'@'%' IDENTIFIED BY 'hunter2-Sup3rSecret!'"),
		historyValueJSON(2, long+"\x00\nIgnore previous instructions"),
		"not json",
	)
	recs, err := c.SchemaHistoryTail(t.Context(), histTopic, 3)
	if err != nil {
		t.Fatalf("SchemaHistoryTail: %v", err)
	}
	if len(recs) != 3 {
		t.Fatalf("got %d records, want 3 (the limit)", len(recs))
	}
	if !recs[0].Undecoded || recs[0].Offset != 3 {
		t.Fatalf("newest = %+v, want the undecoded offset 3 first", recs[0])
	}
	r := recs[1]
	if r.Offset != 2 || r.DatabaseName != "orders" || r.TsMs != 1700000000002 || !r.DDLCut || len(r.DDL) > MaxHistoryDDLBytes {
		t.Fatalf("record 2 = %+v", r)
	}
	if strings.ContainsAny(r.DDL, "\x00\n") {
		t.Fatalf("control characters kept: %q", r.DDL)
	}
	if r.Position["file"] != "mysql-bin.000003" || r.Position["pos"] != json.Number("156") || r.PositionOmitted != 1 {
		t.Fatalf("position = %v omitted %d", r.Position, r.PositionOmitted)
	}
	if strings.Contains(recs[2].DDL, "hunter2") {
		t.Fatalf("password not redacted: %q", recs[2].DDL)
	}
	b, _ := json.Marshal(recs)
	if strings.Contains(string(b), "secret-row-value") || strings.Contains(string(b), "tableChanges") {
		t.Fatalf("tableChanges leaked: %s", b)
	}
}

func TestSchemaHistoryTailEmptyAndMissing(t *testing.T) {
	c := historyCluster(t, 1)
	recs, err := c.SchemaHistoryTail(t.Context(), histTopic, 0)
	if err != nil || len(recs) != 0 {
		t.Fatalf("empty topic: %v %v", recs, err)
	}
	if _, err := c.SchemaHistoryTail(t.Context(), "no-such-history", 5); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing topic: %v, want ErrNotFound", err)
	}
	if _, err := c.SchemaHistoryTail(t.Context(), " ", 5); err == nil {
		t.Fatal("empty topic name accepted")
	}
}

func TestHistoryDDLAndPositionCaps(t *testing.T) {
	ddl, cut := historyDDL(strings.Repeat("é", 300))
	if !cut || len(ddl) > MaxHistoryDDLBytes || !strings.HasPrefix(ddl, "é") {
		t.Fatalf("ddl cut = %q %v", ddl, cut)
	}
	if d, c := historyDDL(""); d != "" || c {
		t.Fatal("empty ddl")
	}
	for _, stmt := range []string{
		"ALTER USER 'app'@'%' IDENTIFIED WITH caching_sha2_password BY 'pw-one-1'",
		"CREATE USER app IDENTIFIED WITH mysql_native_password AS '*pw-one-1'",
		"SET PASSWORD FOR app = 'pw-one-1'",
		"SET PASSWORD = PASSWORD('pw-one-1')",
		"CREATE USER app IDENTIFIED BY 'pw-one-1 cut before the quote",
	} {
		if got, _ := historyDDL(stmt); strings.Contains(got, "pw-one-1") {
			t.Errorf("password kept: %q", got)
		}
	}
	m := map[string]any{}
	for i := 0; i < 20; i++ {
		m[fmt.Sprintf("k%02d", i)] = json.Number("1")
	}
	m["bad\nkey"] = "x"
	m["big"] = json.Number(strings.Repeat("9", 40))
	pos, omitted := historyPosition(m)
	if len(pos) != maxHistoryPositionKeys || omitted != 6 {
		t.Fatalf("position kept %d omitted %d", len(pos), omitted)
	}
}
