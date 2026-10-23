package srcneutral

import (
	"context"
	"testing"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/clients/mysql"
	"piperoom/internal/clients/postgres"
)

// fakeSettings is the item 2 settings reader both kinds share.
type fakeSettings struct{}

func (fakeSettings) Settings(context.Context) ([]clients.DBSetting, error) { return nil, nil }
func (fakeSettings) TableReplication(context.Context, []string) ([]clients.TableReplication, error) {
	return nil, nil
}
func (fakeSettings) RoleInfo(context.Context, string, []string) (clients.RoleInfo, error) {
	return clients.RoleInfo{}, nil
}
func (fakeSettings) Blockers(context.Context, time.Duration) ([]clients.DBSession, error) {
	return nil, nil
}

// fakeMySQL adds the binlog reader, which only the MySQL client has.
type fakeMySQL struct{ fakeSettings }

func (fakeMySQL) BinlogStatus(context.Context) (clients.BinlogStatus, error) {
	return clients.BinlogStatus{}, nil
}

var (
	_ clients.SourceSettingsReader = fakeSettings{}
	_ clients.BinlogReader         = fakeMySQL{}
)

func TestKindOf(t *testing.T) {
	for _, tc := range []struct {
		name string
		src  any
		want string
	}{
		{"mysql client", (*mysql.DB)(nil), KindMySQL},
		{"binlog reader", fakeMySQL{}, KindMySQL},
		{"postgres client", (*postgres.DB)(nil), KindPostgres},
		{"settings reader only", fakeSettings{}, KindPostgres},
		{"neither", struct{}{}, ""},
		{"nil", nil, ""},
	} {
		if got := KindOf(tc.src); got != tc.want {
			t.Errorf("%s: KindOf = %q, want %q", tc.name, got, tc.want)
		}
	}
}
