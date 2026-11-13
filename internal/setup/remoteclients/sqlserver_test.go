package remoteclients

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/clients/sqlserver"
	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
	"piperoom/internal/setup/protocol"
	mssqldriver "piperoom/internal/setup/sources/sqlserver"
)

// The U4b driver's checks run over the runner in a SQL Server setup room: the
// remote SQLServer is the driver's whole Reader (row U4e).
var _ mssqldriver.Reader = (*SQLServer)(nil)

// The op names this package sends are the ones the runner's SQL Server
// source registers, and the U4b driver names as its reads.
func TestSQLServerOpNamesMatchTheCatalogue(t *testing.T) {
	for sent, registered := range map[string]string{
		OpSourceServerInfo:        ops.OpSourceServerInfo,
		OpSourceAgentState:        ops.OpSourceAgentState,
		OpSourceDatabaseCDC:       ops.OpSourceDatabaseCDC,
		OpSourceTableCDC:          ops.OpSourceTableCDC,
		OpSourceRoleInfo:          ops.OpSourceRoleInfo,
		OpSourceCreatePublication: ops.OpSourceCreatePublication,
		OpSourceCreateCanaryTable: ops.OpSourceCreateCanaryTable,
		OpSourceInsertCanary:      ops.OpSourceInsertCanary,
		OpSourceCreateReadRole:    ops.OpSourceCreateReadRole,
	} {
		if sent != registered {
			t.Errorf("remoteclients sends %q, the catalogue registers %q", sent, registered)
		}
	}
}

// mssqlSource is the runner's SQL Server in the end-to-end tests: a ready
// 2022 Developer source by default, which a test mutates. It records every
// read so a test can show a refused name never reaches it.
type mssqlSource struct {
	mu     sync.Mutex
	reads  int
	server sqlserver.ServerInfo
	agent  sqlserver.AgentState
	dbCDC  bool
	dbErr  error
	tables []sqlserver.TableCDC
	login  sqlserver.RoleInfo
}

func readyMSSQL() *mssqlSource {
	return &mssqlSource{
		server: sqlserver.ServerInfo{Version: "16.0.4135.4", Edition: "Developer Edition (64-bit)"},
		agent:  sqlserver.AgentState{Running: true, Status: "Running"},
		dbCDC:  true,
		tables: []sqlserver.TableCDC{
			{Table: "dbo.orders", Exists: true, CDCEnabled: true, HasPrimaryKey: true},
			{Table: mssqldriver.DefaultCanaryTable, Exists: true, CDCEnabled: true, HasPrimaryKey: true},
		},
		login: sqlserver.RoleInfo{Login: "piperoom_cdc", Exists: true, HasDBUser: true, CDCSelect: true, ViewDBState: true,
			Selectable: map[string]bool{"dbo.orders": true, mssqldriver.DefaultCanaryTable: true}},
	}
}

func (m *mssqlSource) hit() {
	m.mu.Lock()
	m.reads++
	m.mu.Unlock()
}

func (m *mssqlSource) ServerInfo(context.Context) (sqlserver.ServerInfo, error) {
	m.hit()
	return m.server, nil
}

func (m *mssqlSource) AgentState(context.Context) (sqlserver.AgentState, error) {
	m.hit()
	return m.agent, nil
}

func (m *mssqlSource) DatabaseCDC(context.Context, string) (bool, error) {
	m.hit()
	return m.dbCDC, m.dbErr
}

func (m *mssqlSource) TableCDC(_ context.Context, tables []string) ([]sqlserver.TableCDC, error) {
	m.hit()
	var out []sqlserver.TableCDC
	for _, want := range tables {
		for _, tc := range m.tables {
			if tc.Table == want {
				out = append(out, tc)
			}
		}
	}
	return out, nil
}

func (m *mssqlSource) RoleInfo(_ context.Context, login string, _ []string) (sqlserver.RoleInfo, error) {
	m.hit()
	info := m.login
	info.Login = login
	return info, nil
}

// mssqlCatalogue is the real runner catalogue of a SQL Server room: the
// generic ops plus the SQL Server source over src and w.
func mssqlCatalogue(t *testing.T, src sqlserver.SettingsReader, w clients.SetupSourceWriter) *ops.Registry {
	t.Helper()
	cat, err := ops.New(ops.Config{
		ProjectDir: t.TempDir(),
		Targets:    []events.SetupTarget{{Kind: "postgres", Address: "mssql.acme:1433"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = cat.Close() })
	if err := ops.RegisterSQLServerSource(cat, ops.SQLServerSource{Reader: src, Writer: w}); err != nil {
		t.Fatalf("RegisterSQLServerSource: %v", err)
	}
	return cat
}

func mssqlDriver() mssqldriver.Driver {
	return mssqldriver.Driver{Database: "orders", Login: "piperoom_cdc", CaptureInstance: "orders_mssql",
		Tables: []string{"dbo.orders", mssqldriver.DefaultCanaryTable}, Target: "compose"}
}

// TestSQLServerDriverThroughTheRealRunner (row U4e "Done when"): every U4b
// check runs through a paired runner and the real catalogue — a ready source
// passes all of them, and each broken prerequisite fails exactly its own
// check with the fix the driver names, as it does over the direct client.
func TestSQLServerDriverThroughTheRealRunner(t *testing.T) {
	d := mssqlDriver()
	if err := d.Validate(); err != nil {
		t.Fatalf("driver: %v", err)
	}

	ready := readyMSSQL()
	results, err := d.Run(roomCtx(t), New(startRunner(t, mssqlCatalogue(t, ready, nil), false), "r_setup").SQLServer())
	if err != nil {
		t.Fatalf("Run over the runner: %v", err)
	}
	if len(results) != len(d.Checks()) {
		t.Fatalf("%d results, want one per check (%d)", len(results), len(d.Checks()))
	}
	for _, r := range results {
		if !r.OK || r.Err != nil {
			t.Errorf("%s failed on a ready source over the runner: %v", r.Check.ID, r.Err)
		}
	}

	broken := []struct {
		id     string
		mutate func(*mssqlSource)
	}{
		{mssqldriver.CheckVersionEdition, func(m *mssqlSource) { m.server.Edition = "Express Edition (64-bit)" }},
		{mssqldriver.CheckAgentRunning, func(m *mssqlSource) { m.agent = sqlserver.AgentState{Status: "Stopped"} }},
		{mssqldriver.CheckDatabaseCDC, func(m *mssqlSource) { m.dbCDC = false }},
		{mssqldriver.CheckDatabaseCDC, func(m *mssqlSource) { m.dbErr = fmt.Errorf("x: %w", clients.ErrNotFound) }},
		{mssqldriver.CheckTableCDC, func(m *mssqlSource) { m.tables[0].CDCEnabled = false }},
		{mssqldriver.CheckCaptureLogin, func(m *mssqlSource) { m.login.Disabled = true }},
		{mssqldriver.CheckCaptureLoginGrant, func(m *mssqlSource) { m.login.CDCSelect = false }},
	}
	for _, b := range broken {
		src := readyMSSQL()
		b.mutate(src)
		ex := startRunner(t, mssqlCatalogue(t, src, nil), false)
		remote, err := d.Run(roomCtx(t), New(ex, "r_setup").SQLServer())
		if err != nil {
			t.Fatalf("%s: Run over the runner: %v", b.id, err)
		}
		direct, err := d.Run(context.Background(), src)
		if err != nil {
			t.Fatalf("%s: Run over the direct reader: %v", b.id, err)
		}
		for i := range remote {
			rFix, rAsk := remote[i].Check.Fix(d, remote[i].Evidence)
			dFix, dAsk := direct[i].Check.Fix(d, direct[i].Evidence)
			if remote[i].OK != direct[i].OK || rFix != dFix || rAsk != dAsk {
				t.Errorf("%s: check %s differs over the runner: remote ok=%v fix=%q ask=%q, direct ok=%v fix=%q ask=%q",
					b.id, remote[i].Check.ID, remote[i].OK, rFix, rAsk, direct[i].OK, dFix, dAsk)
			}
			if remote[i].Check.ID == b.id && remote[i].OK {
				t.Errorf("%s: the broken prerequisite passed over the runner", b.id)
			}
		}
	}
}

// A name the identifier rule refuses never reaches the runner's SQL Server,
// and a room that registered no source database refuses every SQL Server
// read as a target refusal.
func TestSQLServerRunnerRefusals(t *testing.T) {
	src := readyMSSQL()
	ex := startRunner(t, mssqlCatalogue(t, src, nil), false)
	c := New(ex, "r_setup").SQLServer()
	if _, err := c.DatabaseCDC(roomCtx(t), "orders; DROP DATABASE orders"); err == nil {
		t.Fatal("a non-identifier database name was accepted")
	}
	if _, err := c.RoleInfo(roomCtx(t), "x]; DROP LOGIN sa; --", nil); err == nil {
		t.Fatal("a non-identifier login was accepted")
	}
	if _, err := c.TableCDC(roomCtx(t), []string{"orders"}); err == nil {
		t.Fatal("a table without a schema was accepted")
	}
	if src.reads != 0 {
		t.Fatalf("the source was read %d times for refused names", src.reads)
	}

	noTarget, err := ops.New(ops.Config{ProjectDir: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = noTarget.Close() })
	if err := ops.RegisterSQLServerSource(noTarget, ops.SQLServerSource{Reader: src}); err != nil {
		t.Fatal(err)
	}
	c = New(startRunner(t, noTarget, false), "r_setup").SQLServer()
	if _, err := c.ServerInfo(roomCtx(t)); err == nil {
		t.Fatal("a SQL Server read ran in a room with no source database target")
	}
	if src.reads != 0 {
		t.Fatalf("the source was read %d times without a target", src.reads)
	}
}

// A runner with no SQL Server connection answers not_supported, which the
// client maps to clients.ErrNotSupported.
func TestSQLServerRunnerWithoutAConnection(t *testing.T) {
	c := New(startRunner(t, mssqlCatalogue(t, nil, nil), false), "r_setup").SQLServer()
	if _, err := c.ServerInfo(roomCtx(t)); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("err = %v, want clients.ErrNotSupported", err)
	}
}

// mssqlWriter is the runner's sqlserver.SetupWriter in the write test: it
// records the calls and returns a secret name.
type mssqlWriter struct {
	mu    sync.Mutex
	calls []string
}

func (w *mssqlWriter) rec(s string) {
	w.mu.Lock()
	w.calls = append(w.calls, s)
	w.mu.Unlock()
}

func (w *mssqlWriter) CreatePublication(_ context.Context, name string, tables []string) error {
	w.rec(fmt.Sprintf("publication %s %v", name, tables))
	return nil
}

func (w *mssqlWriter) CreateCanaryTable(_ context.Context, table string) error {
	w.rec("canary " + table)
	return nil
}

func (w *mssqlWriter) InsertCanary(_ context.Context, table, nonce string) error {
	w.rec("insert " + table + " " + nonce)
	return nil
}

func (w *mssqlWriter) CreateReadRole(_ context.Context, role string, tables []string) (string, error) {
	w.rec(fmt.Sprintf("role %s %v", role, tables))
	return "mssql_" + role, nil
}

func (w *mssqlWriter) AlterSystem(context.Context, string, string) (bool, error) {
	return false, clients.ErrNotSupported
}

// TestSQLServerWritesThroughTheRealRunner: the four setup writes of a SQL
// Server room run through the real runner's step binding at their T2 floors,
// reach the runner's SQL Server writer once each with the frozen params, and
// CreateReadRole returns only the secret name. The U4b driver's Writer wraps
// the remote writer unchanged.
func TestSQLServerWritesThroughTheRealRunner(t *testing.T) {
	w := &mssqlWriter{}
	cat := mssqlCatalogue(t, readyMSSQL(), w)
	for op, want := range map[string]string{
		OpSourceCreatePublication: protocol.TierT2, OpSourceCreateCanaryTable: protocol.TierT2,
		OpSourceInsertCanary: protocol.TierT2, OpSourceCreateReadRole: protocol.TierT2,
	} {
		if got, ok := cat.Floor(op); !ok || got != want {
			t.Fatalf("%s floor = %q (registered %v), want %q", op, got, ok, want)
		}
	}
	ex := startRunner(t, cat, false)
	remote := New(ex, "r_setup").SQLServer().Writer()
	if _, err := mssqldriver.NewWriter(remote); err != nil {
		t.Fatalf("the U4b driver does not take the remote writer: %v", err)
	}

	steps := []struct {
		action string
		params string
		run    func(context.Context) error
		want   string
	}{
		{OpSourceCreatePublication, `{"publication":"orders_mssql","tables":["dbo.orders"]}`, func(ctx context.Context) error {
			return remote.CreatePublication(ctx, "orders_mssql", []string{"dbo.orders"})
		}, "publication orders_mssql [dbo.orders]"},
		{OpSourceCreateCanaryTable, `{"table":"dbo.piperoom_canary"}`, func(ctx context.Context) error {
			return remote.CreateCanaryTable(ctx, "dbo.piperoom_canary")
		}, "canary dbo.piperoom_canary"},
		{OpSourceInsertCanary, `{"table":"dbo.piperoom_canary","nonce":"n0nce_42"}`, func(ctx context.Context) error {
			return remote.InsertCanary(ctx, "dbo.piperoom_canary", "n0nce_42")
		}, "insert dbo.piperoom_canary n0nce_42"},
		{OpSourceCreateReadRole, `{"role":"piperoom_cdc","tables":["dbo.orders"]}`, func(ctx context.Context) error {
			name, err := remote.CreateReadRole(ctx, "piperoom_cdc", []string{"dbo.orders"})
			if err == nil && name != "mssql_piperoom_cdc" {
				return fmt.Errorf("secret name = %q", name)
			}
			return err
		}, "role piperoom_cdc [dbo.orders]"},
	}
	for i, s := range steps {
		ctx := frozenStepCtx(t, fmt.Sprintf("p_mssql_%d", i), protocol.TierT2, s.action, s.params)
		if err := s.run(ctx); err != nil {
			t.Fatalf("%s through the runner: %v", s.action, err)
		}
		w.mu.Lock()
		got := w.calls[len(w.calls)-1]
		w.mu.Unlock()
		if got != s.want {
			t.Fatalf("%s reached the writer as %q, want %q", s.action, got, s.want)
		}
	}
	if len(w.calls) != len(steps) {
		t.Fatalf("the writer ran %d times, want %d", len(w.calls), len(steps))
	}
}
