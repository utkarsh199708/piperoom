package mysql

import (
	"context"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/clients"
)

// recordingWriter is the wrapped clients.SetupSourceWriter; it records what the
// Writer let through.
type recordingWriter struct {
	calls  []string
	secret string
	err    error
}

func (r *recordingWriter) CreatePublication(context.Context, string, []string) error {
	r.calls = append(r.calls, "publication")
	return r.err
}
func (r *recordingWriter) CreateCanaryTable(_ context.Context, table string) error {
	r.calls = append(r.calls, "canary_table "+table)
	return r.err
}
func (r *recordingWriter) InsertCanary(_ context.Context, table, nonce string) error {
	r.calls = append(r.calls, "canary_insert "+table+" "+nonce)
	return r.err
}
func (r *recordingWriter) CreateReadRole(_ context.Context, role string, _ []string) (string, error) {
	r.calls = append(r.calls, "read_role "+role)
	return r.secret, r.err
}
func (r *recordingWriter) AlterSystem(_ context.Context, name, _ string) (bool, error) {
	r.calls = append(r.calls, "alter_system "+name)
	return true, r.err
}

const goodNonce = "0123456789abcdef0123456789abcdef"

func TestNewWriterNeedsAWriter(t *testing.T) {
	if _, err := NewWriter(nil); !errors.Is(err, ErrNoWriter) {
		t.Fatalf("NewWriter(nil) = %v", err)
	}
	var w *Writer
	if err := w.CreateCanaryTable(context.Background(), "shop.c"); !errors.Is(err, ErrNoWriter) {
		t.Fatalf("nil Writer: %v", err)
	}
}

func TestWriterNotSupported(t *testing.T) {
	rec := &recordingWriter{}
	w, _ := NewWriter(rec)
	if err := w.CreatePublication(context.Background(), "dbz_publication", []string{"shop.orders"}); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("CreatePublication = %v, want ErrNotSupported", err)
	}
	// Every allow-listed Postgres name and every MySQL variable is refused the same.
	for _, name := range []string{"wal_level", "binlog_format", "binlog_row_image", "server_id", "anything"} {
		pending, err := w.AlterSystem(context.Background(), name, "ROW")
		if !errors.Is(err, clients.ErrNotSupported) || pending {
			t.Fatalf("AlterSystem(%s) = %v, %v; want ErrNotSupported", name, pending, err)
		}
	}
	if len(rec.calls) != 0 {
		t.Fatalf("a refused call reached the wrapped writer: %v", rec.calls)
	}
	// The same answer from a Writer with no wrapped writer: the refusal does not
	// depend on write mode.
	var nilW *Writer
	if _, err := nilW.AlterSystem(context.Background(), "binlog_format", "ROW"); !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("nil Writer AlterSystem = %v", err)
	}
}

func TestWriterValidatesBeforeDelegating(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		run  func(w *Writer) error
		want error
	}{
		{"canary table bad name", func(w *Writer) error { return w.CreateCanaryTable(ctx, "shop.x`y") }, ErrIdentifier},
		{"canary table no db", func(w *Writer) error { return w.CreateCanaryTable(ctx, "canary") }, ErrIdentifier},
		{"canary table three parts", func(w *Writer) error { return w.CreateCanaryTable(ctx, "a.b.c") }, ErrIdentifier},
		{"canary insert bad table", func(w *Writer) error { return w.InsertCanary(ctx, "x", goodNonce) }, ErrIdentifier},
		{"canary insert short nonce", func(w *Writer) error { return w.InsertCanary(ctx, "shop.c", "abc") }, ErrParams},
		{"canary insert upper nonce", func(w *Writer) error { return w.InsertCanary(ctx, "shop.c", strings.ToUpper(goodNonce)) }, ErrParams},
		{"canary insert injection", func(w *Writer) error { return w.InsertCanary(ctx, "shop.c", "x'); DROP TABLE t;--") }, ErrParams},
		{"role bad name", func(w *Writer) error { _, err := w.CreateReadRole(ctx, "a'b", []string{"shop.orders"}); return err }, ErrIdentifier},
		{"role reserved", func(w *Writer) error { _, err := w.CreateReadRole(ctx, "root", []string{"shop.orders"}); return err }, ErrIdentifier},
		{"role no tables", func(w *Writer) error { _, err := w.CreateReadRole(ctx, "debezium", nil); return err }, ErrNoTables},
		{"role bad table", func(w *Writer) error { _, err := w.CreateReadRole(ctx, "debezium", []string{"orders"}); return err }, ErrIdentifier},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := &recordingWriter{secret: "source_read_password"}
			w, _ := NewWriter(rec)
			if err := tc.run(w); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
			if len(rec.calls) != 0 {
				t.Fatalf("an invalid call reached the wrapped writer: %v", rec.calls)
			}
		})
	}
}

func TestWriterDelegates(t *testing.T) {
	ctx := context.Background()
	rec := &recordingWriter{secret: "source_read_password"}
	w, _ := NewWriter(rec)
	if err := w.CreateCanaryTable(ctx, "shop.it_canary"); err != nil {
		t.Fatal(err)
	}
	if err := w.InsertCanary(ctx, "shop.it_canary", goodNonce); err != nil {
		t.Fatal(err)
	}
	name, err := w.CreateReadRole(ctx, "debezium", []string{"shop.orders"})
	if err != nil || name != "source_read_password" {
		t.Fatalf("CreateReadRole = %q, %v", name, err)
	}
	want := []string{"canary_table shop.it_canary", "canary_insert shop.it_canary " + goodNonce, "read_role debezium"}
	if strings.Join(rec.calls, "|") != strings.Join(want, "|") {
		t.Fatalf("calls = %v, want %v", rec.calls, want)
	}
}

func TestWriterWrapsAndRejectsBadSecretNames(t *testing.T) {
	ctx := context.Background()
	boom := errors.New("boom")
	w, _ := NewWriter(&recordingWriter{err: boom})
	if err := w.CreateCanaryTable(ctx, "shop.c"); !errors.Is(err, boom) {
		t.Fatalf("CreateCanaryTable lost the cause: %v", err)
	}
	if err := w.InsertCanary(ctx, "shop.c", goodNonce); !errors.Is(err, boom) {
		t.Fatalf("InsertCanary lost the cause: %v", err)
	}
	if _, err := w.CreateReadRole(ctx, "debezium", []string{"shop.orders"}); !errors.Is(err, boom) {
		t.Fatalf("CreateReadRole lost the cause: %v", err)
	}
	// A writer that returns a secret name that is not a store name (a password
	// by mistake) is refused, and the value is not echoed.
	leaky := "s3cr3t password with spaces"
	w, _ = NewWriter(&recordingWriter{secret: leaky})
	_, err := w.CreateReadRole(ctx, "debezium", []string{"shop.orders"})
	if !errors.Is(err, ErrParams) {
		t.Fatalf("bad secret name accepted: %v", err)
	}
	if strings.Contains(err.Error(), leaky) {
		t.Fatalf("error echoes the returned secret: %v", err)
	}
}

func TestStatementBuilders(t *testing.T) {
	ddl, err := CanaryTableSQL("shop.it_canary")
	if err != nil {
		t.Fatal(err)
	}
	// PRIMARY KEY (nonce): the binlog event's key is the primary key, and the
	// ladder's kafka_find_by_key matches it against the nonce (rungs 5 and 8).
	for _, part := range []string{"CREATE TABLE IF NOT EXISTS `shop`.`it_canary`", "AUTO_INCREMENT", "PRIMARY KEY (nonce)", "nonce VARCHAR(64) NOT NULL", "ENGINE=InnoDB"} {
		if !strings.Contains(ddl, part) {
			t.Errorf("canary DDL lacks %q: %s", part, ddl)
		}
	}
	ins, err := InsertCanarySQL("shop.it_canary")
	if err != nil || ins != "INSERT INTO `shop`.`it_canary` (nonce) VALUES (?)" {
		t.Fatalf("InsertCanarySQL = %q, %v", ins, err)
	}

	grants, err := ReadUserGrants("debezium", []string{"shop.orders", "shop.order_items", "shop.orders"})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"GRANT SELECT ON `shop`.`orders` TO 'debezium'@'%'",
		"GRANT SELECT ON `shop`.`order_items` TO 'debezium'@'%'",
		"GRANT RELOAD, SHOW DATABASES, LOCK TABLES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%'",
	}
	if strings.Join(grants, "\n") != strings.Join(want, "\n") {
		t.Fatalf("grants:\n%s\nwant:\n%s", strings.Join(grants, "\n"), strings.Join(want, "\n"))
	}
	for _, g := range grants {
		u := strings.ToUpper(g)
		for _, bad := range []string{"GRANT OPTION", "ALL PRIVILEGES", "SUPER", "INSERT", "UPDATE", "DELETE", "PROCESS", "FILE"} {
			if strings.Contains(u, bad) {
				t.Errorf("grant %q contains %s", g, bad)
			}
		}
	}

	create, err := CreateUserSQL("debezium", "ab12")
	if err != nil || create != "CREATE USER 'debezium'@'%' IDENTIFIED BY 'ab12'" {
		t.Fatalf("CreateUserSQL = %q, %v", create, err)
	}
	// A password with a quote and a backslash stays one literal.
	create, _ = CreateUserSQL("debezium", `a'b\c`)
	if !strings.HasSuffix(create, `'a''b\\c'`) {
		t.Fatalf("password literal not escaped: %s", create)
	}
	drop, err := DropUserSQL("debezium")
	if err != nil || drop != "DROP USER IF EXISTS 'debezium'@'%'" {
		t.Fatalf("DropUserSQL = %q, %v", drop, err)
	}
	for _, bad := range []string{"", "a'b", "root", "mysql_x", strings.Repeat("a", 33), "1abc"} {
		if _, err := CreateUserSQL(bad, "x"); !errors.Is(err, ErrIdentifier) {
			t.Errorf("CreateUserSQL(%q) = %v", bad, err)
		}
		if _, err := ReadUserGrants(bad, []string{"shop.orders"}); !errors.Is(err, ErrIdentifier) {
			t.Errorf("ReadUserGrants(%q) = %v", bad, err)
		}
	}
	if _, err := ReadUserGrants("debezium", nil); !errors.Is(err, ErrNoTables) {
		t.Errorf("ReadUserGrants with no tables: %v", err)
	}
}

func TestIdentifierRules(t *testing.T) {
	good := []string{"shop.orders", "_a.b1", "A.B", "shop." + strings.Repeat("a", 64)}
	for _, g := range good {
		if _, err := quoteTable(g); err != nil {
			t.Errorf("quoteTable(%q): %v", g, err)
		}
	}
	bad := []string{"", "shop", ".orders", "shop.", "shop.ord ers", "shop.o`x", "shop.o;x", "shop.o-x", "1shop.t", "shop." + strings.Repeat("a", 65), "a.b.c", "shop.ünï"}
	for _, b := range bad {
		if _, err := quoteTable(b); !errors.Is(err, ErrIdentifier) {
			t.Errorf("quoteTable(%q) = %v, want ErrIdentifier", b, err)
		}
	}
	if got := quoteLiteral(`a'b\c`); got != `'a''b\\c'` {
		t.Errorf("quoteLiteral = %s", got)
	}
}
