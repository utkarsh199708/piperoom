package wiring

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/mysql"
	"piperoom/internal/config"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

func mysqlPipe(id string) owners.Pipeline {
	return owners.Pipeline{ID: id, SourceKind: owners.SourceKindMySQL, TopicPrefix: "shop", SourceTables: []string{"shop.orders"}, Connectors: []string{id + "-c"}}
}

const (
	mysqlReadURL  = "mysql://cdc_reader:secret-read@db.internal:3306/shop"
	mysqlWriteURL = "mysql://piperoom_writer:secret-write@db.internal/shop"
)

// binlogSource is a MySQL source fake: a SourceDB with the binlog reader.
type binlogSource struct{ downSource }

func (binlogSource) BinlogStatus(context.Context) (mysql.BinlogStatus, error) {
	return mysql.BinlogStatus{File: "binlog.000001", Pos: 4}, nil
}

// fakeAdminWriter is a MySQL source writer fake (the admin methods only).
type fakeAdminWriter struct {
	*fakeWriter
	readURL string
}

func (fakeAdminWriter) SetReplicaIdentity(context.Context, string, string, string) error {
	return clients.ErrNotSupported
}
func (fakeAdminWriter) GrantSelect(context.Context, string, []string) error { return nil }
func (fakeAdminWriter) CreateHeartbeatTable(context.Context, string) error  { return nil }
func (fakeAdminWriter) EndSession(context.Context, int64, time.Time) error  { return nil }

// mysqlOpeners are rec's openers plus a MySQL source and writer that record
// their calls.
func mysqlOpeners(t *testing.T, rec *openRecorder, admins *[]fakeAdminWriter) clientOpeners {
	op := rec.openers(t)
	op.mysql = func(context.Context, string) (clients.SourceDB, func(), error) {
		rec.calls = append(rec.calls, "mysql")
		return binlogSource{}, func() {}, nil
	}
	op.writers.mysql = func(_ context.Context, dsn, readURL string) (closingAdminWriter, error) {
		rec.calls = append(rec.calls, "mysql writer")
		w := fakeAdminWriter{fakeWriter: &fakeWriter{kind: "mysql", dsn: dsn}, readURL: readURL}
		*admins = append(*admins, w)
		return w, nil
	}
	return op
}

// A scheme that disagrees with source_kind fails Open before any client is
// built, naming the pipeline and both values, never a URL (design §6.1, §12).
func TestOpenRefusesSourceKindSchemeMismatch(t *testing.T) {
	for _, tt := range []struct {
		name       string
		o          *owners.Owners
		read, wr   string
		wantInErr  []string
		wantIsKind bool
	}{
		{"mysql pipeline, postgres URL", ownersOf(mysqlPipe("orders-mysql")), readerDSN, "",
			[]string{"pipeline orders-mysql", "source_kind mysql", "source database URL", "postgres:// URL"}, false},
		{"postgres pipeline, mysql URL", ownersOf(cdcPipe("orders")), mysqlReadURL, "",
			[]string{"pipeline orders", "source_kind postgres", "mysql:// URL"}, false},
		{"mysql pipeline, postgres write URL", ownersOf(mysqlPipe("m")), mysqlReadURL, writerDSN,
			[]string{"pipeline m", "source_kind mysql", "source write URL", "postgres:// URL"}, false},
		{"postgres pipeline, mysql write URL", ownersOf(cdcPipe("p")), readerDSN, mysqlWriteURL,
			[]string{"pipeline p", "source_kind postgres", "source write URL", "mysql:// URL"}, false},
		{"mariadb scheme", ownersOf(mysqlPipe("m")), "mariadb://u:secret-read@h/db", "",
			[]string{"pipeline m", "mariadb:// URL", "want postgres:// or mysql://"}, false},
		{"two source kinds in one set", ownersOf(cdcPipe("p"), mysqlPipe("m")), readerDSN, "",
			[]string{"more than one source kind", "source_kind mysql (m)", "source_kind postgres (p)"}, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rec := &openRecorder{}
			var admins []fakeAdminWriter
			cfg := devA2Config()
			cfg.SourceDBURL, cfg.SourceDBWriteURL = tt.read, tt.wr
			_, err := openClientsWith(context.Background(), cfg, nil, tt.o, true, mysqlOpeners(t, rec, &admins))
			if err == nil {
				t.Fatal("Open succeeded")
			}
			for _, want := range tt.wantInErr {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("err %q lacks %q", err, want)
				}
			}
			for _, secret := range []string{"secret-read", "secret-write", "db.internal"} {
				if strings.Contains(err.Error(), secret) {
					t.Errorf("err %q leaks %q", err, secret)
				}
			}
			if len(rec.calls) != 0 {
				t.Fatalf("clients opened before the refusal: %v", rec.calls)
			}
		})
	}
}

// The driver is chosen by the source kind (which the scheme must match):
// mysql.New for mysql://, postgres.New for postgres://, postgresql:// and a
// keyword DSN.
func TestOpenChoosesDriverBySourceKind(t *testing.T) {
	for _, tt := range []struct {
		name string
		o    *owners.Owners
		url  string
		want []string
		kind string
	}{
		{"mysql", ownersOf(mysqlPipe("m")), mysqlReadURL, []string{"a2", "mysql", "warehouse"}, "mysql"},
		{"MYSQL scheme case", ownersOf(mysqlPipe("m")), "MySQL://u:p@h/db", []string{"a2", "mysql", "warehouse"}, "mysql"},
		{"postgres", ownersOf(cdcPipe("p")), readerDSN, []string{"a2", "source", "warehouse"}, "postgres"},
		{"postgresql", ownersOf(cdcPipe("p")), "postgresql://u:p@h/db", []string{"a2", "source", "warehouse"}, "postgres"},
		{"keyword DSN", ownersOf(cdcPipe("p")), "host=h user=u dbname=db", []string{"a2", "source", "warehouse"}, "postgres"},
		{"batch only opens no source", ownersOf(batchPipe("b")), mysqlReadURL, []string{"airflow", "dbt", "warehouse"}, "postgres"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			rec := &openRecorder{}
			var admins []fakeAdminWriter
			cfg := devA2Config()
			cfg.SourceDBURL = tt.url
			w, err := openClientsWith(context.Background(), cfg, nil, tt.o, false, mysqlOpeners(t, rec, &admins))
			if err != nil {
				t.Fatal(err)
			}
			defer w.Close()
			if !slices.Equal(rec.calls, tt.want) {
				t.Fatalf("opened %v, want %v", rec.calls, tt.want)
			}
			if w.source != tt.kind {
				t.Fatalf("set source kind %q, want %q", w.source, tt.kind)
			}
		})
	}
}

// With write mode on, a MySQL set's writer is mysql.NewWriter's on the write
// URL, told the read URL (its user's sessions are protected), held as the
// admin writer; no Postgres writer is built.
func TestOpenMySQLWriter(t *testing.T) {
	rec := &openRecorder{}
	var admins []fakeAdminWriter
	cfg := devA2Config()
	cfg.SourceDBURL, cfg.SourceDBWriteURL = mysqlReadURL, mysqlWriteURL
	w, err := openClientsWith(context.Background(), cfg, nil, ownersOf(mysqlPipe("m")), true, mysqlOpeners(t, rec, &admins))
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(rec.calls, []string{"a2", "mysql", "warehouse", "mysql writer"}) {
		t.Fatalf("opened %v", rec.calls)
	}
	if len(admins) != 1 || admins[0].dsn != mysqlWriteURL || admins[0].readURL != mysqlReadURL {
		t.Fatalf("MySQL writer built with %+v", admins)
	}
	for _, fw := range rec.writers {
		if fw.kind == "source" {
			t.Fatal("a Postgres writer was built for a MySQL set")
		}
	}
	if w.writers.source != nil {
		t.Fatal("MySQL set holds a Postgres source writer")
	}
	if adminWriter(w) == nil {
		t.Fatal("the MySQL writer is not the set's admin writer")
	}
	w.Close()
	if !admins[0].closed.Load() {
		t.Fatal("Close did not close the MySQL writer")
	}

	// Write mode off: nothing is built from the write URL.
	rec, admins = &openRecorder{}, nil
	w, err = openClientsWith(context.Background(), cfg, nil, ownersOf(mysqlPipe("m")), false, mysqlOpeners(t, rec, &admins))
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	if len(admins) != 0 || adminWriter(w) != nil {
		t.Fatal("a MySQL writer was built with write mode off")
	}
}

func TestOpenMySQLWriterErrorClosesEverything(t *testing.T) {
	rec := &openRecorder{}
	var admins []fakeAdminWriter
	op := mysqlOpeners(t, rec, &admins)
	errBoom := errors.New("boom")
	op.writers.mysql = func(context.Context, string, string) (closingAdminWriter, error) { return nil, errBoom }
	cfg := devA2Config()
	cfg.SourceDBURL, cfg.SourceDBWriteURL = mysqlReadURL, mysqlWriteURL
	_, err := openClientsWith(context.Background(), cfg, nil, ownersOf(mysqlPipe("m")), true, op)
	if !errors.Is(err, errBoom) || !strings.Contains(err.Error(), "source mysql writer") {
		t.Fatalf("err = %v", err)
	}
	for _, fw := range rec.writers {
		if !fw.closed.Load() {
			t.Fatalf("%s writer left open", fw.kind)
		}
	}
}

// The write URL may not log in as the read URL's account on MySQL either
// (same user, host and port; another database or password does not count).
func TestMySQLWriteURLIsNotTheReadRole(t *testing.T) {
	for _, tt := range []struct {
		read, write string
		same        bool
	}{
		{mysqlReadURL, "mysql://cdc_reader:other@db.internal/other_db", true},
		{"mysql://u:p@H:3306/a", "mysql://u:q@h/b", true},
		{mysqlReadURL, mysqlWriteURL, false},
		{mysqlReadURL, "mysql://cdc_reader:p@db.internal:3307/shop", false},
		{mysqlReadURL, "mysql://cdc_reader:p@replica.internal/shop", false},
		{readerDSN, mysqlWriteURL, false},
	} {
		if got := writeIsReadRole(config.Config{SourceDBURL: tt.read, SourceDBWriteURL: tt.write}); got != tt.same {
			t.Errorf("writeIsReadRole(%s, %s) = %v, want %v", tt.read, tt.write, got, tt.same)
		}
	}
	err := checkSourceWriteURL(config.Config{SourceDBURL: mysqlReadURL, SourceDBWriteURL: "mysql://cdc_reader:x@db.internal/shop"})
	if !errors.Is(err, errSameSourceRole) {
		t.Fatalf("checkSourceWriteURL = %v", err)
	}
}

func TestMySQLWriterOptionsProtectTheReader(t *testing.T) {
	if n := len(mysqlWriterOptions(mysqlReadURL)); n != 1 {
		t.Fatalf("mysqlWriterOptions(read URL) = %d options, want WithReaderUser", n)
	}
	for _, bad := range []string{"", "postgres://u:p@h/db", "mysql://h/db", "::"} {
		if n := len(mysqlWriterOptions(bad)); n != 0 {
			t.Errorf("mysqlWriterOptions(%q) = %d options", bad, n)
		}
	}
	if u, h, p, ok := mysqlURLRole(mysqlWriteURL); !ok || u != "piperoom_writer" || h != "db.internal" || p != "3306" {
		t.Fatalf("mysqlURLRole = %q %q %q %v", u, h, p, ok)
	}
}

// The Connect actions get the MySQL client as their binlog reader on a MySQL
// set only; a Postgres set's source is never one, whatever methods it has.
func TestBinlogReaderOnlyOnMySQLSet(t *testing.T) {
	my := &Wired{Source: binlogSource{}, Owners: ownersOf(mysqlPipe("m"))}
	if _, err := my.sourceKind(); err != nil {
		t.Fatal(err)
	}
	if binlogReader(my) == nil {
		t.Fatal("MySQL set: no binlog reader")
	}
	pg := &Wired{Source: binlogSource{}, Owners: ownersOf(cdcPipe("p"))}
	if _, err := pg.sourceKind(); err != nil {
		t.Fatal(err)
	}
	if binlogReader(pg) != nil {
		t.Fatal("Postgres set got a binlog reader")
	}
	if binlogReader(&Wired{source: events.SourceKindMySQL}) != nil {
		t.Fatal("no source must be a nil reader")
	}
}

// Registration by source kind: a MySQL set registers no pg_* tool or action,
// a Postgres set no mysql_* one; a hand-built set mixing kinds is refused.
func TestRegisterToolsBySourceKind(t *testing.T) {
	my := &Wired{Source: binlogSource{}, Owners: ownersOf(mysqlPipe("m"))}
	reg, areg := registered(t, my)
	names := regNames(reg)
	for _, want := range []string{"mysql_binlog_status", "source_row", "source_long_transactions", "source_settings"} {
		if !slices.Contains(names, want) {
			t.Errorf("MySQL set lacks %s: %v", want, names)
		}
	}
	for _, n := range names {
		if strings.HasPrefix(n, "pg_") {
			t.Errorf("MySQL set registers %s", n)
		}
	}
	for _, a := range areg.All() {
		if strings.HasPrefix(a.Name(), "pg_") {
			t.Errorf("MySQL set registers action %s", a.Name())
		}
	}
	if !slices.ContainsFunc(areg.All(), func(a actions.Action) bool { return a.Name() == "source_grant_select" }) {
		t.Error("MySQL set lacks source_grant_select")
	}

	pg := &Wired{Source: binlogSource{}, Owners: ownersOf(cdcPipe("p"))}
	reg, areg = registered(t, pg)
	for _, n := range regNames(reg) {
		if strings.HasPrefix(n, "mysql_") || n == "source_row" || n == "source_long_transactions" || n == "kafka_schema_history" {
			t.Errorf("Postgres set registers %s", n)
		}
	}
	if !slices.ContainsFunc(areg.All(), func(a actions.Action) bool { return a.Name() == "pg_set_replica_identity" }) {
		t.Error("Postgres set lost pg_set_replica_identity")
	}

	err := RegisterTools(tools.NewReadOnlyRegistry(), actions.NewRegistry(), &Wired{Owners: ownersOf(cdcPipe("p"), mysqlPipe("m"))})
	if err == nil || !strings.Contains(err.Error(), "more than one source kind") {
		t.Fatalf("mixed set: err = %v", err)
	}
}
