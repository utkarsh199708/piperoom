package nettools

import (
	"context"
	"testing"

	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
)

// A MySQL source target without a port defaults to 3306; Postgres keeps 5432
// (Q8q).
func TestSourceDefaultPortMySQL(t *testing.T) {
	pg := owners.Pipeline{ID: "orders", Connectors: []string{"c"}}
	my := owners.Pipeline{ID: "orders-mysql", SourceKind: owners.SourceKindMySQL, Connectors: []string{"c"}}
	tests := []struct {
		name      string
		p         owners.Pipeline
		sourceURL string
		connector map[string]string // nil = no Connect
		want      []string
	}{
		{"mysql URL, no port", pg, "mysql://u:p@db.internal/shop?tls=true", nil, []string{"db.internal:3306"}},
		{"mysql URL, explicit port", pg, "mysql://u:p@db.internal:3307/shop", nil, []string{"db.internal:3307"}},
		{"mysql URL on a mysql pipeline", my, "mysql://u:p@db.internal/shop", nil, []string{"db.internal:3306"}},
		{"bare host on a mysql pipeline", my, "db.internal", nil, []string{"db.internal:3306"}},
		{"bare host on a postgres pipeline", pg, "db.internal", nil, []string{"db.internal:5432"}},
		{"postgres URL unchanged", pg, "postgres://u:p@db.internal/orders", nil, []string{"db.internal:5432"}},
		{"MySQL connector without database.port", pg, "",
			map[string]string{"connector.class": "io.debezium.connector.mysql.MySqlConnector", "database.hostname": "my.internal"},
			[]string{"my.internal:3306"}},
		{"connector on a mysql pipeline without database.port", my, "",
			map[string]string{"connector.class": "C", "database.hostname": "my.internal"},
			[]string{"my.internal:3306"}},
		{"Postgres connector without database.port", pg, "",
			map[string]string{"connector.class": "io.debezium.connector.postgresql.PostgresConnector", "database.hostname": "pg.internal"},
			[]string{"pg.internal:5432"}},
		{"MySQL connector with database.port", pg, "",
			map[string]string{"connector.class": "io.debezium.connector.mysql.MySqlConnector", "database.hostname": "my.internal", "database.port": "3310"},
			[]string{"my.internal:3310"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var src TargetSource
			if tc.connector != nil {
				fc := &fakeConnect{configFn: func(context.Context, string) (map[string]string, error) { return tc.connector, nil }}
				src = RoomTargets(config.Config{SourceDBURL: tc.sourceURL}, fc)
			} else {
				src = RoomTargets(config.Config{SourceDBURL: tc.sourceURL}, nil)
			}
			tgt, err := src.Target(context.Background(), tc.p, LabelSource)
			if err != nil {
				t.Fatal(err)
			}
			var got []string
			for _, e := range tgt.Endpoints {
				got = append(got, e.Addr)
			}
			if len(got) != len(tc.want) || (len(got) > 0 && got[0] != tc.want[0]) {
				t.Fatalf("source endpoints = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestSetupTargetMySQLDefaultPort(t *testing.T) {
	tests := []struct{ kind, addr, want string }{
		{"mysql", "db.internal", "db.internal:3306"},
		{"MySQL", "db.internal", "db.internal:3306"},
		{"mysql", "db.internal:3307", "db.internal:3307"},
		{"postgres", "db.internal", "db.internal:5432"},
		{"connect", "connect.internal", "connect.internal:8083"},
	}
	for _, tc := range tests {
		t.Run(tc.kind+" "+tc.addr, func(t *testing.T) {
			src := TargetsFromSetup([]events.SetupTarget{{Kind: tc.kind, Address: tc.addr}})
			tgt, err := src.Target(context.Background(), owners.Pipeline{}, tc.kind)
			if err != nil {
				t.Fatal(err)
			}
			if len(tgt.Endpoints) != 1 || tgt.Endpoints[0].Addr != tc.want {
				t.Fatalf("endpoints = %+v, want %s", tgt.Endpoints, tc.want)
			}
		})
	}
}
