package srcneutral

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"piperoom/internal/clients/mysql"
	"piperoom/internal/clients/postgres"
)

type fakeMySQL struct {
	err      error
	settings []mysql.DBSetting
	tables   []mysql.TableReplication
	role     mysql.RoleInfo
	sessions []mysql.DBSession
}

func (f *fakeMySQL) Settings(context.Context) ([]mysql.DBSetting, error) { return f.settings, f.err }
func (f *fakeMySQL) TableReplication(context.Context, []string) ([]mysql.TableReplication, error) {
	return f.tables, f.err
}
func (f *fakeMySQL) RoleInfo(context.Context, string, []string) (mysql.RoleInfo, error) {
	return f.role, f.err
}
func (f *fakeMySQL) Blockers(context.Context, time.Duration) ([]mysql.DBSession, error) {
	return f.sessions, f.err
}

type fakeScopedMySQL struct{ *fakeMySQL }

func (f fakeScopedMySQL) BlockersOnTables(context.Context, time.Duration, []string) ([]mysql.DBSession, error) {
	return f.sessions, f.err
}

type fakePostgres struct{}

func (fakePostgres) Settings(context.Context) ([]postgres.DBSetting, error) { return nil, nil }
func (fakePostgres) TableReplication(context.Context, []string) ([]postgres.TableReplication, error) {
	return nil, nil
}
func (fakePostgres) RoleInfo(context.Context, string, []string) (postgres.RoleInfo, error) {
	return postgres.RoleInfo{}, nil
}
func (fakePostgres) Blockers(context.Context, time.Duration) ([]postgres.DBSession, error) {
	return nil, nil
}

type scoped interface {
	BlockersOnTables(ctx context.Context, olderThan time.Duration, tables []string) ([]postgres.DBSession, error)
}

func TestKindOf(t *testing.T) {
	m := &fakeMySQL{}
	for _, tc := range []struct {
		name string
		src  any
		want string
	}{
		{"mysql reader", m, KindMySQL},
		{"wrapped mysql", Wrap(m), KindMySQL},
		{"wrapped scoped mysql", Wrap(fakeScopedMySQL{m}), KindMySQL},
		{"postgres reader", fakePostgres{}, KindPostgres},
		{"neither", struct{}{}, ""},
		{"nil", nil, ""},
	} {
		if got := KindOf(tc.src); got != tc.want {
			t.Errorf("%s: KindOf = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestWrapLeavesOtherClients(t *testing.T) {
	pg := fakePostgres{}
	if Wrap(pg) != any(pg) {
		t.Fatal("Wrap changed a Postgres reader")
	}
	if Wrap(nil) != nil {
		t.Fatal("Wrap(nil) is not nil")
	}
	// A MySQL reader without the scoped read gets no BlockersOnTables: the
	// tools must then refuse a scoped call, never read in full.
	if _, ok := Wrap(&fakeMySQL{}).(scoped); ok {
		t.Fatal("wrapped unscoped MySQL reader claims BlockersOnTables")
	}
	if _, ok := Wrap(fakeScopedMySQL{&fakeMySQL{}}).(scoped); !ok {
		t.Fatal("wrapped scoped MySQL reader lacks BlockersOnTables")
	}
}

func TestWrapCopiesEveryField(t *testing.T) {
	start := time.Date(2026, 10, 23, 9, 0, 0, 0, time.UTC)
	f := &fakeMySQL{
		settings: []mysql.DBSetting{{Name: "binlog_expire_logs_seconds", Value: "86400", Unit: "s", PendingRestart: true}},
		tables:   []mysql.TableReplication{{Table: "shop.orders", HasPrimaryKey: true, Engine: "InnoDB"}},
		role: mysql.RoleInfo{Role: "debezium", Exists: true, CanLogin: true, Replication: true,
			Selectable: map[string]bool{"shop.orders": true}, Accounts: []string{"debezium@%"}, Privileges: map[string]bool{"RELOAD": true}},
		sessions: []mysql.DBSession{{PID: 7, StartedAt: start, XactStart: start, User: "app", App: "api", State: "Sleep", Query: "SELECT 1",
			BlocksSlot: true, InScope: true, OldXact: true, SameDB: true, ClientBackend: true}},
	}
	r := Wrap(fakeScopedMySQL{f}).(postgres.SourceSettingsReader)
	ctx := context.Background()

	s, _ := r.Settings(ctx)
	sameFields(t, f.settings[0], s[0])
	tr, _ := r.TableReplication(ctx, nil)
	sameFields(t, f.tables[0], tr[0])
	ri, _ := r.RoleInfo(ctx, "debezium", nil)
	sameFields(t, f.role, ri)
	b, _ := r.Blockers(ctx, time.Minute)
	sameFields(t, f.sessions[0], b[0])
	bs, _ := r.(scoped).BlockersOnTables(ctx, time.Minute, nil)
	sameFields(t, f.sessions[0], bs[0])
}

func TestWrapPassesErrors(t *testing.T) {
	boom := errors.New("boom")
	r := Wrap(fakeScopedMySQL{&fakeMySQL{err: boom}}).(postgres.SourceSettingsReader)
	ctx := context.Background()
	if _, err := r.Settings(ctx); !errors.Is(err, boom) {
		t.Errorf("Settings err = %v", err)
	}
	if _, err := r.TableReplication(ctx, nil); !errors.Is(err, boom) {
		t.Errorf("TableReplication err = %v", err)
	}
	if _, err := r.RoleInfo(ctx, "x", nil); !errors.Is(err, boom) {
		t.Errorf("RoleInfo err = %v", err)
	}
	if _, err := r.Blockers(ctx, 0); !errors.Is(err, boom) {
		t.Errorf("Blockers err = %v", err)
	}
	if _, err := r.(scoped).BlockersOnTables(ctx, 0, nil); !errors.Is(err, boom) {
		t.Errorf("BlockersOnTables err = %v", err)
	}
}

// sameFields checks that every field of the MySQL DTO exists on the postgres
// DTO with the same type, JSON tag and value: a field added to one copy and
// not the other fails here, not silently in a room.
func sameFields(t *testing.T, from, to any) {
	t.Helper()
	fv, tv := reflect.ValueOf(from), reflect.ValueOf(to)
	ft := fv.Type()
	for i := range ft.NumField() {
		f := ft.Field(i)
		g, ok := tv.Type().FieldByName(f.Name)
		if !ok {
			t.Errorf("%s.%s has no counterpart on %s", ft.Name(), f.Name, tv.Type().Name())
			continue
		}
		if g.Type != f.Type || g.Tag.Get("json") != f.Tag.Get("json") {
			t.Errorf("%s.%s: type %s tag %q, postgres has %s %q", ft.Name(), f.Name, f.Type, f.Tag.Get("json"), g.Type, g.Tag.Get("json"))
		}
		if !reflect.DeepEqual(fv.Field(i).Interface(), tv.FieldByName(f.Name).Interface()) {
			t.Errorf("%s.%s not copied: %v != %v", ft.Name(), f.Name, fv.Field(i).Interface(), tv.FieldByName(f.Name).Interface())
		}
	}
	if tv.NumField() != fv.NumField() {
		t.Errorf("%s has %d fields, %s %d", ft.Name(), fv.NumField(), tv.Type().Name(), tv.NumField())
	}
}
