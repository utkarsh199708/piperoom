package actions

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/clients/mysql"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

const (
	mysqlSourceClass = "io.debezium.connector.mysql.MySqlConnector"
	srvUUID          = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
)

// fakeBinlog is a mysql.BinlogReader with a mutable status.
type fakeBinlog struct {
	st    mysql.BinlogStatus
	err   error
	calls int
}

func (f *fakeBinlog) BinlogStatus(context.Context) (mysql.BinlogStatus, error) {
	f.calls++
	return f.st, f.err
}

func newFakeBinlog() *fakeBinlog {
	return &fakeBinlog{st: mysql.BinlogStatus{
		File: "mysql-bin.000005", Pos: 1000, GTIDMode: "ON",
		GTIDExecuted: srvUUID + ":1-100", GTIDPurged: srvUUID + ":1-20",
		ServerID: 1, ServerUUID: srvUUID,
		Binlogs: []mysql.BinlogFile{{Name: "mysql-bin.000003", Size: 5000}, {Name: "mysql-bin.000004", Size: 7000}, {Name: "mysql-bin.000005", Size: 1000}},
	}}
}

// mysqlLifecycleFake is the lifecycle fake with a Debezium MySQL connector
// storing a binlog position.
func mysqlLifecycleFake() *lifecycleFake {
	f := newLifecycleFake()
	f.configs[lifecycleConnector] = map[string]string{
		"connector.class": mysqlSourceClass, "topic.prefix": "orders", "snapshot.mode": "no_data", "database.server.id": "184054",
	}
	f.offsets[lifecycleConnector] = []connect.SourceOffset{{
		Partition: map[string]any{"server": "orders"},
		Offset: map[string]any{
			"file": "mysql-bin.000004", "pos": json.Number("4000"), "gtids": srvUUID + ":1-80",
			"ts_sec": json.Number("1700000000"), "server_id": json.Number("1"), "row": json.Number("1"), "event": json.Number("2"),
		},
	}}
	return f
}

func mysqlResetIn(file string, pos int, gtids string) string {
	b, _ := json.Marshal(map[string]any{
		"connector": lifecycleConnector, "stop_first": true,
		"position": map[string]any{
			"partition": map[string]any{"server": "orders"},
			"offset": map[string]any{
				"file": file, "pos": pos, "gtids": gtids,
				"ts_sec": 1700000000, "server_id": 1, "row": 1, "event": 2,
			},
		},
	})
	return string(b)
}

func TestResetMySQLPositionValidate(t *testing.T) {
	cases := map[string]struct {
		file   string
		pos    int
		gtids  string
		want   error // nil = accepted
		reason string
	}{
		"in an older file":        {"mysql-bin.000004", 5000, srvUUID + ":1-90", nil, ""},
		"at the current end":      {"mysql-bin.000005", 1000, srvUUID + ":1-100", nil, ""},
		"purged file":             {"mysql-bin.000002", 4, srvUUID + ":1-90", ErrPositionPurged, "is purged"},
		"past a file's end":       {"mysql-bin.000004", 8000, srvUUID + ":1-90", ErrPositionPastEnd, "past the end of mysql-bin.000004"},
		"past the current pos":    {"mysql-bin.000005", 2000, srvUUID + ":1-90", ErrPositionPastEnd, "past the current write position"},
		"a future file":           {"mysql-bin.000009", 4, srvUUID + ":1-90", ErrPositionPastEnd, "after the current one"},
		"another server's file":   {"binlog.000004", 4, srvUUID + ":1-90", ErrSourcePosition, "never existed"},
		"gtids miss gtid_purged":  {"mysql-bin.000004", 5000, srvUUID + ":30-90", ErrPositionPurged, "misses 20 transactions of gtid_purged"},
		"gtids past the end":      {"mysql-bin.000004", 5000, srvUUID + ":1-150", ErrPositionPastEnd, "50 transactions the server has not executed"},
		"gtids of another server": {"mysql-bin.000004", 5000, srvUUID + ":1-90,4f22aa47-71ca-11e1-9e33-c80aa9429562:1-3", ErrPositionPastEnd, "3 transactions"},
		"gtids not a set":         {"mysql-bin.000004", 5000, "garbage", ErrSourcePosition, "not a GTID set"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			f := mysqlLifecycleFake()
			a := resetAction(t, f, nil)
			bl := newFakeBinlog()
			a.d.Binlog = bl
			err := a.Validate(scoped(), json.RawMessage(mysqlResetIn(tc.file, tc.pos, tc.gtids)))
			if tc.want == nil {
				if err != nil {
					t.Fatalf("Validate: %v", err)
				}
				return
			}
			if !errors.Is(err, tc.want) || !errors.Is(err, ErrSourcePosition) || !strings.Contains(err.Error(), tc.reason) {
				t.Fatalf("err = %v, want %v with %q", err, tc.want, tc.reason)
			}
			if bl.calls == 0 {
				t.Fatal("the binlog status was not read")
			}
		})
	}
}

func TestResetMySQLNeedsBinlogReader(t *testing.T) {
	f := mysqlLifecycleFake()
	in := json.RawMessage(mysqlResetIn("mysql-bin.000004", 5000, srvUUID+":1-90"))
	if err := resetAction(t, f, nil).Validate(scoped(), in); !errors.Is(err, clients.ErrNotSupported) || !strings.Contains(err.Error(), "cannot check a MySQL position") {
		t.Fatalf("no binlog reader: %v", err)
	}
	a := resetAction(t, f, nil)
	a.d.Binlog = &fakeBinlog{err: errors.New("Access denied; you need REPLICATION CLIENT")}
	if err := a.Validate(scoped(), in); err == nil || !strings.Contains(err.Error(), "REPLICATION CLIENT") {
		t.Fatalf("binlog read error: %v", err)
	}
	// The source client is used when it is a binlog reader (the MySQL client).
	src := &binlogSource{lifecycleSource: &lifecycleSource{}, fakeBinlog: newFakeBinlog()}
	if err := resetAction(t, f, src).Validate(scoped(), in); err != nil {
		t.Fatalf("binlog reader from Source: %v", err)
	}
	// A Postgres connector never asks for one.
	pg := newLifecycleFake()
	if err := resetAction(t, pg, nil).Validate(scoped(), json.RawMessage(resetAtIn)); err != nil {
		t.Fatalf("postgres reset: %v", err)
	}
}

// binlogSource is a source client that also reads the binlog, as the MySQL
// client does.
type binlogSource struct {
	*lifecycleSource
	*fakeBinlog
}

func TestResetMySQLGTIDModeRules(t *testing.T) {
	f := mysqlLifecycleFake()
	f.offsets[lifecycleConnector] = nil // after a full reset: no stored shape
	a := resetAction(t, f, nil)
	bl := newFakeBinlog()
	a.d.Binlog = bl
	onlyFile := `{"connector":"pg-orders-connector","stop_first":true,"position":{"partition":{"server":"orders"},"offset":{"file":"mysql-bin.000004","pos":5000}}}`
	if err := a.Validate(scoped(), json.RawMessage(onlyFile)); !errors.Is(err, ErrSourcePosition) || !strings.Contains(err.Error(), "must carry gtids") {
		t.Fatalf("gtid_mode ON without gtids: %v", err)
	}
	bl.st.GTIDMode, bl.st.GTIDExecuted, bl.st.GTIDPurged = "OFF", "", ""
	if err := a.Validate(scoped(), json.RawMessage(onlyFile)); err != nil {
		t.Fatalf("file and pos with GTIDs off: %v", err)
	}
	onlyGTID := `{"connector":"pg-orders-connector","stop_first":true,"position":{"partition":{"server":"orders"},"offset":{"gtids":"` + srvUUID + `:1-50"}}}`
	if err := a.Validate(scoped(), json.RawMessage(onlyGTID)); !errors.Is(err, ErrSourcePosition) || !strings.Contains(err.Error(), "must carry file and pos") {
		t.Fatalf("gtids only with GTIDs off: %v", err)
	}
	for name, off := range map[string]string{
		"snapshot key":     `{"file":"mysql-bin.000004","pos":5,"snapshot":true}`,
		"unknown key":      `{"file":"mysql-bin.000004","pos":5,"lsn":5}`,
		"pos without file": `{"pos":5}`,
		"negative pos":     `{"file":"mysql-bin.000004","pos":-5}`,
		"string pos":       `{"file":"mysql-bin.000004","pos":"5"}`,
		"wrong partition":  ``,
	} {
		in := `{"connector":"pg-orders-connector","stop_first":true,"position":{"partition":{"server":"orders"},"offset":` + off + `}}`
		if off == "" {
			in = `{"connector":"pg-orders-connector","stop_first":true,"position":{"partition":{"server":"other"},"offset":{"file":"mysql-bin.000004","pos":5}}}`
		}
		if err := a.Validate(scoped(), json.RawMessage(in)); !errors.Is(err, ErrSourcePosition) {
			t.Errorf("%s: %v, want ErrSourcePosition", name, err)
		}
	}
	// With the binlog off there is no file position to check against.
	bl.st.File = ""
	if err := a.Validate(scoped(), json.RawMessage(onlyFile)); !errors.Is(err, ErrSourcePosition) {
		t.Fatalf("binlog off: %v", err)
	}
}

func TestResetMySQLOmittedNewestBinlogs(t *testing.T) {
	st := newFakeBinlog().st
	st.Binlogs = st.Binlogs[:1] // only the oldest kept; 000004 and 000005 omitted
	st.BinlogsOmitted = 2
	pos := sourcePosition{Partition: map[string]any{"server": "orders"}, Offset: map[string]any{"file": "mysql-bin.000004", "pos": json.Number("99999"), "gtids": srvUUID + ":1-90"}}
	cfg := map[string]string{"connector.class": mysqlSourceClass, "topic.prefix": "orders"}
	if err := checkMySQLPosition("c", cfg, pos, st); err != nil {
		t.Fatalf("a file among the omitted newest ones: %v", err)
	}
	st.BinlogsOmitted = 0
	if err := checkMySQLPosition("c", cfg, pos, st); !errors.Is(err, ErrSourcePosition) || !strings.Contains(err.Error(), "never existed") {
		t.Fatalf("a file in a gap: %v", err)
	}
}

func TestResetMySQLPreviewAndExecuteRecheck(t *testing.T) {
	f := mysqlLifecycleFake()
	a := resetAction(t, f, nil)
	bl := newFakeBinlog()
	a.d.Binlog = bl
	in := json.RawMessage(mysqlResetIn("mysql-bin.000004", 5000, srvUUID+":1-90"))
	res, err := a.Preview(scoped(), in)
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	for _, want := range []string{"current binlog position mysql-bin.000005:1000", "oldest binlog kept mysql-bin.000003", "10 transactions behind gtid_executed", "re-reads 0 and skips 10 transactions"} {
		if !strings.Contains(res.Expected, want) {
			t.Errorf("preview misses %q: %s", want, res.Expected)
		}
	}
	if !res.Reversible || !strings.Contains(res.Rollback, "mysql-bin.000004") {
		t.Fatalf("rollback = %q", res.Rollback)
	}
	if f.refusedWrites() != 0 {
		t.Fatal("Preview wrote")
	}

	// The server purges 000004 between approval and execution: refused, no write.
	f.allowWrites = true
	bl.st.Binlogs = bl.st.Binlogs[2:]
	bl.st.GTIDPurged = srvUUID + ":1-95"
	if _, err := a.Execute(scoped(), in); !errors.Is(err, ErrPositionPurged) {
		t.Fatalf("Execute after a purge: %v", err)
	}
	if len(f.altered) != 0 {
		t.Fatal("Execute wrote a purged position")
	}
	bl.st = newFakeBinlog().st
	if _, err := a.Execute(scoped(), in); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if len(f.altered[lifecycleConnector]) != 1 {
		t.Fatalf("altered = %v", f.altered)
	}
}

func TestResetMySQLClearPreviewSaysSkip(t *testing.T) {
	f := mysqlLifecycleFake()
	res, err := resetAction(t, f, nil).Preview(scoped(), json.RawMessage(resetEmptyIn))
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if !strings.Contains(res.Expected, "SKIPS every change") {
		t.Fatalf("no_data clear preview: %s", res.Expected)
	}
	f.configs[lifecycleConnector]["snapshot.mode"] = "initial"
	res, err = resetAction(t, f, nil).Preview(scoped(), json.RawMessage(resetEmptyIn))
	if err != nil || !strings.Contains(res.Expected, "snapshot re-reads the tables") {
		t.Fatalf("initial clear preview: %v %s", err, res.Expected)
	}
}

// ---- connect_update_connector_config: the MySQL keys ----

// perNameConnect serves other connectors' configs next to the one under test.
type perNameConnect struct {
	*configConnect
	others map[string]map[string]string
}

func (f *perNameConnect) Config(ctx context.Context, name string) (map[string]string, error) {
	if c, ok := f.others[name]; ok {
		return c, nil
	}
	return f.configConnect.Config(ctx, name)
}

func mysqlConfigAction(t *testing.T) (*updateConnectorConfig, *perNameConnect, *fakeBinlog) {
	t.Helper()
	c := newConfigConnect(connectorWithTasks("RUNNING", "RUNNING"))
	c.cfg = map[string]string{"connector.class": mysqlSourceClass, "database.server.id": "184054", "snapshot.mode": "no_data", "database.password": "hunter2"}
	pc := &perNameConnect{configConnect: c, others: map[string]map[string]string{
		"mysql-other": {"connector.class": mysqlSourceClass, "database.server.id": "5400"},
	}}
	bl := newFakeBinlog()
	a := &updateConnectorConfig{d: Deps{Connect: pc, Registry: &fakeRegistry{}, Binlog: bl}}
	guardDeps(t, a.d)
	return a, pc, bl
}

func mysqlRoom() context.Context {
	return tools.WithPipeline(context.Background(), owners.Pipeline{ID: "my-orders", TopicPrefix: "orders", Connectors: []string{"pg-orders-connector", "mysql-other"}, SourceTables: []string{"orders.orders"}})
}

func changesIn(kv string) json.RawMessage {
	return json.RawMessage(`{"connector":"pg-orders-connector","changes":{` + kv + `}}`)
}

func TestServerIDChecks(t *testing.T) {
	a, pc, bl := mysqlConfigAction(t)
	for v, ok := range map[string]bool{"1": false, "0": false, "01": false, "4294967296": false, "-3": false, "x": false, "184055": true, "4294967295": true} {
		err := a.Validate(mysqlRoom(), changesIn(`"database.server.id":"`+v+`"`))
		if ok != (err == nil) {
			t.Errorf("server id %s: %v", v, err)
		}
	}
	if err := a.Validate(mysqlRoom(), changesIn(`"database.server.id":"5400"`)); err == nil || !strings.Contains(err.Error(), "mysql-other") {
		t.Fatalf("clash with another connector: %v", err)
	}
	if err := a.Validate(mysqlRoom(), changesIn(`"database.server.id":"1"`)); !errors.Is(err, tools.ErrBadInput) || !strings.Contains(err.Error(), "@@server_id") {
		t.Fatalf("clash with the server: %v", err)
	}
	bl.err = errors.New("connection refused")
	if err := a.Validate(mysqlRoom(), changesIn(`"database.server.id":"184055"`)); err == nil {
		t.Fatal("a failed @@server_id read was swallowed")
	}
	// Without a binlog reader (a Postgres room) the MySQL keys are neither
	// offered nor described, so a Postgres room's action list is unchanged.
	a.d.Binlog = nil
	if err := a.Validate(mysqlRoom(), changesIn(`"database.server.id":"184055"`)); err == nil || !strings.Contains(err.Error(), "not allowed") {
		t.Fatalf("no binlog reader: %v", err)
	}
	schema, _ := json.Marshal(a.ParamsSchema())
	if strings.Contains(string(schema), keyServerID) || strings.Contains(a.Describe(), "MySQL") {
		t.Fatal("the MySQL keys are offered outside a MySQL room")
	}
	a.d.Binlog = newFakeBinlog()
	schema, _ = json.Marshal(a.ParamsSchema())
	if !strings.Contains(string(schema), keyServerID) || !strings.Contains(string(schema), keySnapshotMode) || !strings.Contains(a.Describe(), "NOW(6)") {
		t.Fatal("the MySQL keys are not offered in a MySQL room")
	}
	a.allow = map[string]map[string]bool{keyServerID: {}}
	a.d.Binlog = nil
	if err := a.Validate(mysqlRoom(), changesIn(`"database.server.id":"184055"`)); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("no binlog reader with the key allowed: %v", err)
	}
	a.allow = nil
	// Execute needs the room's pipeline to compare with the other connectors.
	a.d.Binlog = newFakeBinlog()
	pc.allowWrites = true
	if _, err := a.Execute(context.Background(), changesIn(`"database.server.id":"184055"`)); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("Execute without a pipeline: %v", err)
	}
	pc.others["mysql-other"]["database.server.id"] = "184055" // changed since Validate
	if _, err := a.Execute(mysqlRoom(), changesIn(`"database.server.id":"184055"`)); err == nil {
		t.Fatal("Execute missed a clash that appeared after Validate")
	}
	pc.others["mysql-other"]["database.server.id"] = "5400"
	out, err := a.Execute(mysqlRoom(), changesIn(`"database.server.id":"184055"`))
	if err != nil || !strings.Contains(out, "database.server.id: 184054 → 184055") {
		t.Fatalf("Execute: %v %q", err, out)
	}
}

func TestServerIDPreviewAndRollback(t *testing.T) {
	a, _, _ := mysqlConfigAction(t)
	res, err := a.Preview(mysqlRoom(), changesIn(`"database.server.id":"184055"`))
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	if !res.Reversible || !strings.Contains(res.Rollback, `"database.server.id":"184054"`) {
		t.Fatalf("rollback = %q", res.Rollback)
	}
	if !strings.Contains(strings.Join(res.Downstream, " "), "replica id 184055") {
		t.Fatalf("downstream = %v", res.Downstream)
	}
}

func TestMySQLKeysOnlyOnMySQLSources(t *testing.T) {
	for _, class := range []string{pgSourceClass, "io.confluent.connect.jdbc.JdbcSinkConnector", "io.debezium.connector.mysql.MySqlConnector-but-not"} {
		a, pc, _ := mysqlConfigAction(t)
		pc.cfg["connector.class"] = class
		for _, kv := range []string{`"database.server.id":"184055"`, `"snapshot.mode":"recovery"`} {
			if err := a.Validate(mysqlRoom(), changesIn(kv)); err == nil || !strings.Contains(err.Error(), "MySQL source connector") {
				t.Errorf("%s %s: %v", class, kv, err)
			}
			if _, err := a.Preview(mysqlRoom(), changesIn(kv)); err == nil {
				t.Errorf("%s %s: Preview promised it", class, kv)
			}
		}
	}
}

func TestSnapshotModeRecovery(t *testing.T) {
	a, pc, _ := mysqlConfigAction(t)
	if err := a.Validate(mysqlRoom(), changesIn(`"snapshot.mode":"always"`)); !errors.Is(err, tools.ErrBadInput) {
		t.Fatalf("always: %v", err)
	}
	// Leaving recovery is the rollback only: refused from no_data.
	if err := a.Validate(mysqlRoom(), changesIn(`"snapshot.mode":"initial"`)); err == nil || !strings.Contains(err.Error(), "not in recovery") {
		t.Fatalf("initial from no_data: %v", err)
	}
	res, err := a.Preview(mysqlRoom(), changesIn(`"snapshot.mode":"recovery"`))
	if err != nil {
		t.Fatalf("Preview: %v", err)
	}
	down := strings.Join(res.Downstream, " ")
	if !strings.Contains(down, "safe only if no schema change happened") || !strings.Contains(down, "kafka_schema_history") || !strings.Contains(down, "RUNNING") {
		t.Fatalf("downstream = %v", res.Downstream)
	}
	if !res.Reversible || !strings.Contains(res.Rollback, `"snapshot.mode":"no_data"`) {
		t.Fatalf("rollback = %q", res.Rollback)
	}
	// The rollback runs: from recovery back to no_data.
	pc.cfg["snapshot.mode"] = "recovery"
	if err := a.Validate(mysqlRoom(), changesIn(`"snapshot.mode":"no_data"`)); err != nil {
		t.Fatalf("rollback refused: %v", err)
	}
	// An unset mode rolls back to Debezium's default.
	delete(pc.cfg, "snapshot.mode")
	res, err = a.Preview(mysqlRoom(), changesIn(`"snapshot.mode":"recovery"`))
	if err != nil || !strings.Contains(res.Rollback, `"snapshot.mode":"initial"`) {
		t.Fatalf("unset rollback: %v %q", err, res.Rollback)
	}
}

// pluginConnect adds the worker's plugin list to the MySQL config fake.
type pluginConnect struct {
	*perNameConnect
	version string
}

func (p *pluginConnect) Plugins(context.Context) ([]connect.Plugin, error) {
	return []connect.Plugin{{Class: mysqlSourceClass, Type: "source", Version: p.version}}, nil
}

func TestSnapshotModeRecoveryNameByVersion(t *testing.T) {
	a, pc, _ := mysqlConfigAction(t)
	old := &pluginConnect{perNameConnect: pc, version: "2.5.4.Final"}
	a.d.Connect = old
	if err := a.Validate(mysqlRoom(), changesIn(`"snapshot.mode":"recovery"`)); err == nil || !strings.Contains(err.Error(), "schema_only_recovery") {
		t.Fatalf("recovery on 2.5: %v", err)
	}
	if err := a.Validate(mysqlRoom(), changesIn(`"snapshot.mode":"schema_only_recovery"`)); err != nil {
		t.Fatalf("schema_only_recovery on 2.5: %v", err)
	}
	old.version = "3.0.1.Final"
	res, err := a.Preview(mysqlRoom(), changesIn(`"snapshot.mode":"schema_only_recovery"`))
	if err != nil || !strings.Contains(res.Expected, "deprecates it for recovery") {
		t.Fatalf("schema_only_recovery on 3.0: %v %s", err, res.Expected)
	}
}

func TestHeartbeatDialectByClass(t *testing.T) {
	mysqlQ := "INSERT INTO `orders`.`debezium_heartbeat` (id, ts) VALUES (1, NOW(6)) ON DUPLICATE KEY UPDATE ts = NOW(6)"
	pgQ := `INSERT INTO orders.debezium_heartbeat (id, ts) VALUES (1, now()) ON CONFLICT (id) DO UPDATE SET ts = EXCLUDED.ts`
	shared := `UPDATE orders.debezium_heartbeat SET ts = CURRENT_TIMESTAMP(3) WHERE id = 1`
	q := func(s string) json.RawMessage {
		b, _ := json.Marshal(map[string]any{"connector": "pg-orders-connector", "changes": map[string]string{keyHeartbeatQuery: s}})
		return b
	}
	cases := []struct {
		class, query string
		ok           bool
	}{
		{mysqlSourceClass, mysqlQ, true},
		{mysqlSourceClass, pgQ, false},
		{mysqlSourceClass, shared, true},
		{"io.debezium.connector.mariadb.MariaDbConnector", mysqlQ, true},
		{pgSourceClass, pgQ, true},
		{pgSourceClass, mysqlQ, false},
		{pgSourceClass, shared, true},
		{"io.debezium.connector.sqlserver.SqlServerConnector", pgQ, false},
		{"io.debezium.connector.sqlserver.SqlServerConnector", shared, true},
	}
	for _, tc := range cases {
		a, pc, _ := mysqlConfigAction(t)
		pc.cfg["connector.class"] = tc.class
		err := a.Validate(mysqlRoom(), q(tc.query))
		if tc.ok != (err == nil) {
			t.Errorf("%s / %s: %v", tc.class, tc.query, err)
		}
		if err != nil && !errors.Is(err, tools.ErrBadInput) {
			t.Errorf("%s / %s: %v, want ErrBadInput", tc.class, tc.query, err)
		}
	}
	for _, bad := range []string{"NOW(7)", "NOW(66)", "NOW(a)", "CURRENT_TIMESTAMP()", "CLOCK_TIMESTAMP(6)"} {
		if _, err := parseHeartbeatQuery(`UPDATE orders.hb SET ts = ` + bad); err == nil {
			t.Errorf("%s accepted", bad)
		}
	}
}
