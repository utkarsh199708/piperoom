package mysql

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"piperoom/internal/clients"
)

// TestOpenSetupPoolRefusesBadURLsWithoutLeakingThem: the setup pool parses the
// URL with the client's rules (a mysql:// URL, the parameter allow-list, TLS
// off only on loopback, no driver DSN syntax), and its errors never carry the
// password.
func TestOpenSetupPoolRefusesBadURLsWithoutLeakingThem(t *testing.T) {
	for _, raw := range []string{
		"",
		"postgres://u:" + secret + "@h/db",
		"mysql://u:" + secret + "@h:3306/db?allowAllFiles=true",
		"mysql://u:" + secret + "@example.com:3306/db?tls=false",
		"u:" + secret + "@tcp(h:3306)/db",
		"mysql://u:" + secret + "@h:3306/db#frag",
	} {
		pool, err := OpenSetupPool(context.Background(), raw, nil)
		if pool != nil {
			_ = pool.Close()
		}
		if !errors.Is(err, ErrBadURL) {
			t.Errorf("OpenSetupPool(%s) = %v, want ErrBadURL", redactPW(raw), err)
			continue
		}
		if strings.Contains(err.Error(), secret) {
			t.Errorf("OpenSetupPool(%s): the error leaks the password: %v", redactPW(raw), err)
		}
	}
}

// TestOpenSetupPoolProbesTheServer pins the setup pool to the version floor New
// and NewWriter enforce: a server that does not answer, MariaDB and MySQL
// before 8.0 are refused, and the probe runs under the client's deadline.
func TestOpenSetupPoolProbesTheServer(t *testing.T) {
	versionCols := []string{"v", "c"}
	cases := []struct {
		name string
		srv  *fakeServer
		has  string
		is   error
	}{
		{"server down", &fakeServer{ping: errors.New("connection refused")}, "connect", nil},
		{"version read fails", (&fakeServer{}).onErr("SELECT VERSION()", errors.New("boom")), "read server version", nil},
		{"mariadb", (&fakeServer{}).on("SELECT VERSION()", versionCols, row([]byte("11.4.2-MariaDB"), []byte("mariadb.org binary distribution"))), "MariaDB", clients.ErrNotSupported},
		{"mysql 5.7", (&fakeServer{}).on("SELECT VERSION()", versionCols, row([]byte("5.7.44-log"), []byte("MySQL Community Server (GPL)"))), "not supported", clients.ErrNotSupported},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sdb := sql.OpenDB(fakeConnector{srv: tc.srv})
			defer sdb.Close()
			err := openSetupPool(context.Background(), sdb)
			if err == nil || !strings.Contains(err.Error(), tc.has) {
				t.Fatalf("openSetupPool = %v, want %q", err, tc.has)
			}
			if tc.is != nil && !errors.Is(err, tc.is) {
				t.Fatalf("openSetupPool = %v, want %v in the chain", err, tc.is)
			}
		})
	}

	srv := (&fakeServer{}).on("SELECT VERSION()", versionCols, row([]byte("8.4.3"), []byte("MySQL Community Server - GPL")))
	sdb := sql.OpenDB(fakeConnector{srv: srv})
	defer sdb.Close()
	if err := openSetupPool(context.Background(), sdb); err != nil {
		t.Fatalf("openSetupPool on MySQL 8.4: %v", err)
	}
	// The probe is the only statement, and it ran under the 10 s deadline. The
	// setup package checks writability itself, so no SET or write ran here.
	calls := srv.queries()
	if len(calls) != 1 || !strings.HasPrefix(calls[0].query, "SELECT VERSION()") || calls[0].exec {
		t.Fatalf("statements = %+v, want the one version probe", calls)
	}
	for _, left := range srv.deadlines {
		if left < 0 || left > callTimeout {
			t.Fatalf("the probe ran with %v left to its deadline, want within %v", left, callTimeout)
		}
	}
}
