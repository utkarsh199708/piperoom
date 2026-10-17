package topology

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// sourceNodeOf builds the graph and returns the source table node.
func sourceNodeOf(t *testing.T, g Graph) Node {
	t.Helper()
	for _, n := range g.Nodes {
		if n.Kind == KindSourceTable {
			return n
		}
	}
	t.Fatalf("no source node in %+v", g.Nodes)
	return Node{}
}

// #569: pipeline_map names the same slot as tools.Scope.Slots (and the
// watcher): watch.slot first, an invalid slot.name is never the default, and
// only a PostgresConnector defaults to "debezium".
func TestSourceSlotMatchesScopeSlots(t *testing.T) {
	slots := []clients.ReplicationSlot{
		{Name: "orders_slot", Active: true, RetainedWALBytes: 10},
		// Another pipeline's slot under the Debezium default name.
		{Name: "debezium", Active: false, WALStatus: "lost"},
	}
	cases := []struct {
		name      string
		watch     string
		cfg       map[string]string
		connErr   error
		wantSlots []string // Scope.Slots; nil with wantErr
		wantErr   bool
		wantFacts map[string]string // the subset that must hold
		wantH     Health
	}{
		{
			name: "watch.slot with Connect down", watch: "    watch:\n      slot: orders_slot\n",
			connErr: errors.New("connect down"), wantSlots: []string{"orders_slot"},
			wantFacts: map[string]string{"slot_active": "true"}, wantH: HealthOK,
		},
		{
			name: "watch.slot with an unresolved config provider", watch: "    watch:\n      slot: orders_slot\n",
			cfg: map[string]string{
				"connector.class": "io.debezium.connector.postgresql.PostgresConnector",
				"slot.name":       "${file:/secrets/slot.properties:name}",
			},
			wantSlots: []string{"orders_slot"},
			wantFacts: map[string]string{"slot_active": "true"}, wantH: HealthOK,
		},
		{
			name: "JDBC source has no slot",
			cfg: map[string]string{
				"connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
				"table.whitelist": "orders",
			},
			wantSlots: nil, wantFacts: map[string]string{"slot": "unknown"}, wantH: HealthUnknown,
		},
		{
			name: "blank slot.name is not the default",
			cfg: map[string]string{
				"connector.class": "io.debezium.connector.postgresql.PostgresConnector",
				"slot.name":       "",
			},
			wantErr: true, wantFacts: map[string]string{"slot": "invalid"}, wantH: HealthUnknown,
		},
		{
			name: "Postgres connector without slot.name uses debezium",
			cfg: map[string]string{
				"connector.class": "io.debezium.connector.postgresql.PostgresConnector",
			},
			wantSlots: []string{"debezium"},
			wantFacts: map[string]string{"wal_status": "lost"}, wantH: HealthFail,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			o := loadOwners(t, fmt.Sprintf(pipelineYAML, tc.watch))
			p, _ := o.Pipeline("orders")
			conn := &fakeConnect{
				config: map[string]map[string]string{"c1": tc.cfg},
				status: map[string]clients.ConnectorStatus{"c1": {State: "RUNNING", Tasks: []clients.TaskStatus{{State: "RUNNING"}}}},
			}
			if tc.connErr != nil {
				conn.configErr = map[string]error{"c1": tc.connErr}
				conn.statusErr = map[string]error{"c1": tc.connErr}
			}

			sc, err := tools.ScopeFor(tools.WithPipeline(context.Background(), p), o)
			if err != nil {
				t.Fatal(err)
			}
			got, err := sc.Slots(context.Background(), conn)
			if (err != nil) != tc.wantErr {
				t.Fatalf("Scope.Slots err = %v, wantErr %v", err, tc.wantErr)
			}
			if fmt.Sprint(got) != fmt.Sprint(tc.wantSlots) {
				t.Fatalf("Scope.Slots = %v, want %v", got, tc.wantSlots)
			}

			b := NewBuilder(o, conn, &fakeKafka{}, &fakeSource{slots: slots}, &fakeWarehouse{})
			g, err := b.Build(context.Background(), "orders")
			if err != nil {
				t.Fatal(err)
			}
			n := sourceNodeOf(t, g)
			if n.Health != tc.wantH {
				t.Fatalf("source health = %s, want %s (facts %v)", n.Health, tc.wantH, n.Facts)
			}
			for k, v := range tc.wantFacts {
				if n.Facts[k] != v {
					t.Fatalf("fact %s = %q, want %q (facts %v)", k, n.Facts[k], v, n.Facts)
				}
			}
		})
	}
}
