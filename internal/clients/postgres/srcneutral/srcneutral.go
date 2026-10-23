// Package srcneutral lets the source-neutral tools and actions (dbtools'
// source_* tools, sourceactions; Q8n) read a MySQL source through the same
// item 2 interfaces they read Postgres through.
//
// Q0 item 2 (SourceSettingsReader, ScopedBlockerReader and their DTOs) was
// meant to move into internal/clients/clients.go with the "[Q8-0] contracts:
// item 7" PR (docs/design/mysql-source.md §5.2 (a)). That move has not
// landed: the Postgres shapes live in internal/clients/postgres (settings.go)
// and the MySQL client (Q8b) declares its own identical copies in
// internal/clients/mysql (types.go), because it may not import the postgres
// package. The two sets of types are distinct Go types, so *mysql.DB does not
// satisfy an interface written over the postgres DTOs.
//
// Wrap closes that gap without a contract edit: it returns a MySQL reader
// re-typed onto the postgres DTOs (which carry the additive MySQL fields,
// Engine, Accounts and Privileges), field for field, and every other client
// unchanged. When the contract move lands, both DTO sets become aliases of
// the clients ones, Wrap becomes the identity, and this package is deleted.
package srcneutral

import (
	"context"
	"time"

	"piperoom/internal/clients/mysql"
	"piperoom/internal/clients/postgres"
)

// Source kinds, as owners.yaml source_kind spells them (Q8d).
const (
	KindPostgres = "postgres"
	KindMySQL    = "mysql"
)

// KindOf reports the kind of a source client from the item 2 surface it
// implements: KindMySQL for the MySQL reader (or a Wrap of it), KindPostgres
// for the Postgres one, "" for a client with neither (a fake, the replay).
func KindOf(src any) string {
	switch src.(type) {
	case *mysqlReader, *mysqlScopedReader, mysql.SourceSettingsReader:
		return KindMySQL
	case postgres.SourceSettingsReader:
		return KindPostgres
	}
	return ""
}

// Wrap returns src re-typed for the postgres-typed item 2 interfaces when it
// is a MySQL reader (mysql.SourceSettingsReader, plus BlockersOnTables when
// it has mysql.ScopedBlockerReader), and src itself otherwise. Callers type
// assert the result against their own reader interfaces exactly as they
// asserted src before; nothing else of src is reachable through a wrapped
// value, so only the item 2 reads go through it.
func Wrap(src any) any {
	r, ok := src.(mysql.SourceSettingsReader)
	if !ok {
		return src
	}
	base := &mysqlReader{r: r}
	if s, ok := src.(mysql.ScopedBlockerReader); ok {
		return &mysqlScopedReader{mysqlReader: base, s: s}
	}
	return base
}

// mysqlReader is a mysql.SourceSettingsReader seen as a
// postgres.SourceSettingsReader.
type mysqlReader struct{ r mysql.SourceSettingsReader }

var (
	_ postgres.SourceSettingsReader = (*mysqlReader)(nil)
	_ postgres.SourceSettingsReader = (*mysqlScopedReader)(nil)
)

// Settings implements postgres.SourceSettingsReader.
func (m *mysqlReader) Settings(ctx context.Context) ([]postgres.DBSetting, error) {
	in, err := m.r.Settings(ctx)
	if err != nil {
		return nil, err
	}
	out := make([]postgres.DBSetting, len(in))
	for i, s := range in {
		out[i] = postgres.DBSetting{Name: s.Name, Value: s.Value, Unit: s.Unit, PendingRestart: s.PendingRestart}
	}
	return out, nil
}

// TableReplication implements postgres.SourceSettingsReader.
func (m *mysqlReader) TableReplication(ctx context.Context, tables []string) ([]postgres.TableReplication, error) {
	in, err := m.r.TableReplication(ctx, tables)
	if err != nil {
		return nil, err
	}
	out := make([]postgres.TableReplication, len(in))
	for i, t := range in {
		out[i] = postgres.TableReplication{Table: t.Table, HasPrimaryKey: t.HasPrimaryKey, ReplicaIdentity: t.ReplicaIdentity, Engine: t.Engine}
	}
	return out, nil
}

// RoleInfo implements postgres.SourceSettingsReader.
func (m *mysqlReader) RoleInfo(ctx context.Context, role string, tables []string) (postgres.RoleInfo, error) {
	in, err := m.r.RoleInfo(ctx, role, tables)
	if err != nil {
		return postgres.RoleInfo{}, err
	}
	return postgres.RoleInfo{
		Role: in.Role, Exists: in.Exists, CanLogin: in.CanLogin, Replication: in.Replication,
		Selectable: in.Selectable, Accounts: in.Accounts, Privileges: in.Privileges,
	}, nil
}

// Blockers implements postgres.SourceSettingsReader.
func (m *mysqlReader) Blockers(ctx context.Context, olderThan time.Duration) ([]postgres.DBSession, error) {
	in, err := m.r.Blockers(ctx, olderThan)
	if err != nil {
		return nil, err
	}
	return sessions(in), nil
}

// mysqlScopedReader adds BlockersOnTables for a reader that has it.
type mysqlScopedReader struct {
	*mysqlReader
	s mysql.ScopedBlockerReader
}

// BlockersOnTables is the pipeline-scoped Blockers, re-typed.
func (m *mysqlScopedReader) BlockersOnTables(ctx context.Context, olderThan time.Duration, tables []string) ([]postgres.DBSession, error) {
	in, err := m.s.BlockersOnTables(ctx, olderThan, tables)
	if err != nil {
		return nil, err
	}
	return sessions(in), nil
}

// sessions copies every field, the unexported-in-JSON identity flags
// included: source_end_session needs OldXact, SameDB and ClientBackend.
func sessions(in []mysql.DBSession) []postgres.DBSession {
	out := make([]postgres.DBSession, len(in))
	for i, s := range in {
		out[i] = postgres.DBSession{
			PID: s.PID, StartedAt: s.StartedAt, XactStart: s.XactStart, User: s.User, App: s.App,
			State: s.State, Query: s.Query, BlocksSlot: s.BlocksSlot, InScope: s.InScope,
			OldXact: s.OldXact, SameDB: s.SameDB, ClientBackend: s.ClientBackend,
		}
	}
	return out
}
