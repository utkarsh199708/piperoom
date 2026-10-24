package mysql

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"

	gomysql "github.com/go-sql-driver/mysql"
)

var varCols = []string{"gtid_mode", "gtid_executed", "gtid_purged", "server_id", "server_uuid", "expire"}

func binlogServer(version string, files int) *fakeServer {
	srv := (&fakeServer{}).
		on("@@global.gtid_mode", varCols, row(b("ON"), b("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100"), b("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-20"), int64(223344), b("3e11fa47-71ca-11e1-9e33-c80aa9429562"), int64(86400)))
	status := []driver.Value{b("binlog.000007"), int64(1234), b(""), b(""), b("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100")}
	statusCols := []string{"File", "Position", "Binlog_Do_DB", "Binlog_Ignore_DB", "Executed_Gtid_Set"}
	if version >= "8.2" {
		srv.on("SHOW BINARY LOG STATUS", statusCols, status)
	} else {
		srv.on("SHOW MASTER STATUS", statusCols, status)
	}
	var rows [][]driver.Value
	for i := 1; i <= files; i++ {
		rows = append(rows, row(b(fmt.Sprintf("binlog.%06d", i)), int64(1000+i), b("No")))
	}
	srv.on("SHOW BINARY LOGS", []string{"Log_name", "File_size", "Encrypted"}, rows...)
	srv.onErr("rds_show_configuration", &gomysql.MySQLError{Number: 1305})
	return srv
}

func TestBinlogStatus(t *testing.T) {
	for _, version := range []string{"8.0.36", "8.4.3"} {
		srv := binlogServer(version, 3)
		d := newFakeVersion(t, srv, version)
		st, err := d.BinlogStatus(context.Background())
		if err != nil {
			t.Fatalf("%s: %v", version, err)
		}
		want := BinlogStatus{
			File: "binlog.000007", Pos: 1234, GTIDMode: "ON",
			GTIDExecuted: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100", GTIDPurged: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-20",
			ServerID: 223344, ServerUUID: "3e11fa47-71ca-11e1-9e33-c80aa9429562",
			Binlogs:       []BinlogFile{{Name: "binlog.000001", Size: 1001}, {Name: "binlog.000002", Size: 1002}, {Name: "binlog.000003", Size: 1003}},
			ExpireSeconds: 86400,
		}
		if !reflect.DeepEqual(st, want) {
			t.Errorf("%s:\ngot  %+v\nwant %+v", version, st, want)
		}
		for _, c := range srv.queries() {
			if version < "8.2" && strings.Contains(c.query, "SHOW BINARY LOG STATUS") {
				t.Errorf("%s ran SHOW BINARY LOG STATUS", version)
			}
			if version >= "8.2" && strings.Contains(c.query, "SHOW MASTER STATUS") {
				t.Errorf("%s ran SHOW MASTER STATUS", version)
			}
		}
	}
}

// An "unknown statement" falls back once to the other form (an 8.0 build that
// already renamed it, or a version string the probe misread).
func TestBinlogStatusFallsBack(t *testing.T) {
	srv := binlogServer("8.0.36", 1) // only SHOW MASTER STATUS is routed
	srv.routes = append([]route{{substr: "SHOW BINARY LOG STATUS", res: result{err: &gomysql.MySQLError{Number: 1064, Message: "You have an error in your SQL syntax"}}}}, srv.routes...)
	d := newFakeVersion(t, srv, "8.4.0")
	st, err := d.BinlogStatus(context.Background())
	if err != nil || st.File != "binlog.000007" {
		t.Fatalf("st=%+v err=%v", st, err)
	}
}

func TestBinlogStatusBinlogOffAndCap(t *testing.T) {
	srv := (&fakeServer{}).
		on("@@global.gtid_mode", varCols, row(b("OFF"), b(""), b(""), int64(1), b("u"), int64(0))).
		on("SHOW BINARY LOG STATUS", []string{"File", "Position"}).
		onErr("SHOW BINARY LOGS", &gomysql.MySQLError{Number: 1381, Message: "You are not using binary logging"}).
		on("rds_show_configuration", []string{"name", "value"}, row(b("binlog retention hours"), b("48")))
	d := newFake(t, srv)
	st, err := d.BinlogStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if st.File != "" || st.Pos != 0 || st.Binlogs != nil || st.RetentionHours == nil || *st.RetentionHours != 48 {
		t.Fatalf("st = %+v", st)
	}

	srv = binlogServer("8.4.3", MaxBinlogFiles+7)
	d = newFake(t, srv)
	st, err = d.BinlogStatus(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Binlogs) != MaxBinlogFiles || st.BinlogsOmitted != 7 || st.Binlogs[0].Name != "binlog.000001" {
		t.Fatalf("cap: %d files, %d omitted, first %v", len(st.Binlogs), st.BinlogsOmitted, st.Binlogs[0])
	}
}

func TestBinlogStatusErrors(t *testing.T) {
	srv := (&fakeServer{}).
		on("@@global.gtid_mode", varCols, row(b("ON"), b(""), b(""), int64(1), b("u"), int64(0))).
		onErr("SHOW BINARY LOG STATUS", &gomysql.MySQLError{Number: 1227, Message: "Access denied; you need REPLICATION CLIENT"})
	d := newFake(t, srv)
	if _, err := d.BinlogStatus(context.Background()); !errors.Is(err, ErrPrivilege) || !strings.Contains(err.Error(), "REPLICATION CLIENT") {
		t.Fatalf("err = %v", err)
	}

	srv = (&fakeServer{}).
		on("@@global.gtid_mode", varCols, row(b("ON"), b(""), b(""), int64(1), b("u"), int64(0))).
		on("SHOW BINARY LOG STATUS", []string{"File", "Position"}, row(b("binlog.000001"), b("x")))
	d = newFake(t, srv)
	if _, err := d.BinlogStatus(context.Background()); err == nil {
		t.Fatal("a non-numeric position was accepted")
	}

	big := strings.Repeat("a", maxGTIDBytes+1)
	srv = (&fakeServer{}).on("@@global.gtid_mode", varCols, row(b("ON"), b(big), b(""), int64(1), b("u"), int64(0)))
	d = newFake(t, srv)
	if _, err := d.BinlogStatus(context.Background()); err == nil {
		t.Fatal("an oversized GTID set was accepted")
	}
}
