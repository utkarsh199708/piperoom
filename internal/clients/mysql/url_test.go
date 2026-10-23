package mysql

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients/netsec"
)

const secret = "s3cr3t-pw"

func TestParseURLAccepts(t *testing.T) {
	tests := []struct {
		raw     string
		host    string
		port    int
		db      string
		tls     bool
		ca, sni string
	}{
		{"mysql://pr:" + secret + "@localhost/shop", "localhost", 3306, "shop", false, "", ""},
		{"mysql://pr:" + secret + "@127.0.0.1:3307/", "127.0.0.1", 3307, "", false, "", ""},
		{"mysql://pr@[::1]:3306", "::1", 3306, "", false, "", ""},
		{"mysql://pr:" + secret + "@localhost/shop?tls=true", "localhost", 3306, "shop", true, "", ""},
		{"mysql://pr:" + secret + "@localhost/shop?tls=false", "localhost", 3306, "shop", false, "", ""},
		{"mysql://pr:" + secret + "@db.example.com/shop", "db.example.com", 3306, "shop", true, "", ""},
		{"mysql://pr:" + secret + "@db.example.com:3310/shop?tls=true&tls_ca=/etc/ca.pem&tls_server_name=mysql.internal", "db.example.com", 3310, "shop", true, "/etc/ca.pem", "mysql.internal"},
		{"mysql://pr:p%40ss%3Aw%2Frd@db.example.com/shop", "db.example.com", 3306, "shop", true, "", ""},
	}
	for _, tt := range tests {
		u, err := parseURL(tt.raw)
		if err != nil {
			t.Errorf("parseURL(%s): %v", redactPW(tt.raw), err)
			continue
		}
		if u.host != tt.host || u.port != tt.port || u.db != tt.db || u.tls != tt.tls || u.tlsCA != tt.ca || u.tlsServerName != tt.sni || u.user != "pr" {
			t.Errorf("parseURL(%s) = %+v", redactPW(tt.raw), u)
		}
	}
	u, _ := parseURL("mysql://pr:p%40ss%3Aw%2Frd@db.example.com/shop")
	if u.password != "p@ss:w/rd" {
		t.Errorf("password not decoded: %q", u.password)
	}
	if strings.Contains(u.String(), "p@ss") || strings.Contains(u.target(), "pr") && strings.Contains(u.target(), "p@ss") {
		t.Errorf("String/target leak the password: %s / %s", u, u.target())
	}
	if got := u.target(); got != "mysql shop@db.example.com:3306" {
		t.Errorf("target = %q", got)
	}
}

// Every parameter the driver understands, and any other, is refused (design
// §6.1): a URL is configuration, and configuration must not reach the driver.
func TestParseURLRefusesParameters(t *testing.T) {
	refused := []string{
		"allowAllFiles=true",
		"multiStatements=true",
		"allowCleartextPasswords=true",
		"allowOldPasswords=true",
		"allowNativePasswords=false",
		"allowFallbackToPlaintext=true",
		"interpolateParams=true",
		"parseTime=false",
		"loc=Local",
		"timeout=1h",
		"readTimeout=0",
		"writeTimeout=0",
		"charset=latin1",
		"collation=latin1_swedish_ci",
		"clientFoundRows=true",
		"columnsWithAlias=true",
		"maxAllowedPacket=0",
		"rejectReadOnly=false",
		"serverPubKey=x",
		"connectionAttributes=program_name:x",
		"checkConnLiveness=false",
		"sql_mode=''",
		"transaction_read_only=0",
		"max_execution_time=0",
		"autocommit=0",
		"sslmode=disable",
		"tls=skip-verify",
		"tls=preferred",
		"tls=custom",
		"tls=1",
		"tls=true&tls=false",
		"tls_ca=",
		"tls_server_name=",
		"tls_server_name=a%20b",
	}
	for _, p := range refused {
		raw := "mysql://pr:" + secret + "@localhost:3306/shop?" + p
		_, err := parseURL(raw)
		if !errors.Is(err, ErrBadURL) {
			t.Errorf("parameter %q: err = %v, want ErrBadURL", p, err)
			continue
		}
		if strings.Contains(err.Error(), secret) {
			t.Errorf("parameter %q: error leaks the password: %v", p, err)
		}
	}
}

func TestParseURLRefusesShapes(t *testing.T) {
	refused := []string{
		"",
		"pr:" + secret + "@tcp(localhost:3306)/shop",       // driver DSN syntax
		"pr:" + secret + "@unix(/tmp/mysql.sock)/shop",     // unix socket
		"mysql://pr:" + secret + "@unix(/tmp/mysql.sock)/", // unix socket in a URL
		"mariadb://pr:" + secret + "@localhost/shop",
		"mysqlx://pr:" + secret + "@localhost/shop",
		"postgres://pr:" + secret + "@localhost/shop",
		"mysql:pr:" + secret + "@localhost/shop", // opaque
		"mysql://localhost/shop",                 // no user
		"mysql://:" + secret + "@localhost/shop", // empty user
		"mysql://" + strings.Repeat("u", 33) + ":" + secret + "@localhost/shop",
		"mysql://pr:" + secret + "@/shop",                // no host
		"mysql://pr:" + secret + "@localhost:0/shop",     // port 0
		"mysql://pr:" + secret + "@localhost:70000/shop", // port too big
		"mysql://pr:" + secret + "@localhost:/shop",      // empty port
		"mysql://pr:" + secret + "@localhost/shop/extra",
		"mysql://pr:" + secret + "@localhost/sh%60op", // backtick
		"mysql://pr:" + secret + "@localhost/1shop",
		"mysql://pr:" + secret + "@localhost/" + strings.Repeat("d", 65),
		"mysql://pr:" + secret + "@localhost/shop#frag",
		"mysql://pr:" + secret + "#x@localhost/shop",              // an unencoded # in the password
		"mysql://pr:" + secret + "@db.example.com/shop?tls=false", // clear text off loopback
		"mysql://pr:" + secret + "@localhost/shop?tls_ca=/ca.pem", // tls_ca with TLS off
		"mysql://pr:" + secret + "@localhost/shop?tls=false&tls_server_name=x",
		"mysql://pr:" + secret + "@localhost/shop?%zz", // bad query
		"mysql://pr:" + secret + "@local host/shop",    // url.Parse error
	}
	for _, raw := range refused {
		_, err := parseURL(raw)
		if !errors.Is(err, ErrBadURL) {
			t.Errorf("parseURL(%s) = %v, want ErrBadURL", redactPW(raw), err)
			continue
		}
		if strings.Contains(err.Error(), secret) {
			t.Errorf("parseURL(%s): error leaks the password: %v", redactPW(raw), err)
		}
	}
}

func TestConfigSetsEverySafetySwitch(t *testing.T) {
	u, err := parseURL("mysql://pr:" + secret + "@localhost:3306/shop")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := u.config(readerProgram, readParams, driverLogger{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AllowAllFiles || cfg.MultiStatements || cfg.InterpolateParams || cfg.AllowCleartextPasswords ||
		cfg.AllowOldPasswords || cfg.AllowFallbackToPlaintext {
		t.Errorf("an unsafe switch is on: %+v", cfg)
	}
	if !cfg.ParseTime || cfg.Loc != time.UTC || cfg.Timeout != 5*time.Second || cfg.ReadTimeout != 10*time.Second || cfg.WriteTimeout != 10*time.Second {
		t.Errorf("times: %+v", cfg)
	}
	if cfg.Net != "tcp" || cfg.Addr != "localhost:3306" || cfg.DBName != "shop" || cfg.User != "pr" || cfg.Passwd != secret {
		t.Errorf("connection: net=%s addr=%s db=%s user=%s", cfg.Net, cfg.Addr, cfg.DBName, cfg.User)
	}
	if cfg.ConnectionAttributes != "program_name:piperoom-reader" {
		t.Errorf("ConnectionAttributes = %q", cfg.ConnectionAttributes)
	}
	if len(cfg.Params) != 2 || cfg.Params["transaction_read_only"] != "1" || cfg.Params["max_execution_time"] != "10000" {
		t.Errorf("Params = %v", cfg.Params)
	}
	if cfg.TLS != nil || cfg.TLSConfig != "" {
		t.Errorf("loopback without tls=true should be plain: %v %q", cfg.TLS, cfg.TLSConfig)
	}
	cfg.Params["x"] = "y"
	if _, ok := readParams["x"]; ok {
		t.Fatal("config shares the readParams map")
	}
}

func TestConfigTLS(t *testing.T) {
	u, err := parseURL("mysql://pr:" + secret + "@db.example.com/shop")
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := u.config(readerProgram, readParams, driverLogger{})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.TLS == nil || cfg.TLS.InsecureSkipVerify || cfg.TLS.ServerName != "db.example.com" || cfg.TLS.MinVersion == 0 {
		t.Fatalf("TLS = %+v", cfg.TLS)
	}

	u, _ = parseURL("mysql://pr:" + secret + "@db.example.com/shop?tls_server_name=mysql.internal")
	cfg, err = u.config(readerProgram, readParams, driverLogger{})
	if err != nil || cfg.TLS.ServerName != "mysql.internal" {
		t.Fatalf("tls_server_name: %v %+v", err, cfg.TLS)
	}

	// A missing or empty CA file fails at start-up, not at the first read.
	u, _ = parseURL("mysql://pr:" + secret + "@db.example.com/shop?tls_ca=" + filepath.Join(t.TempDir(), "none.pem"))
	if _, err := u.config(readerProgram, readParams, driverLogger{}); err == nil {
		t.Fatal("missing CA file accepted")
	}
	empty := filepath.Join(t.TempDir(), "empty.pem")
	if err := os.WriteFile(empty, []byte("not pem"), 0o600); err != nil {
		t.Fatal(err)
	}
	u, _ = parseURL("mysql://pr:" + secret + "@db.example.com/shop?tls_ca=" + empty)
	if _, err := u.config(readerProgram, readParams, driverLogger{}); !errors.Is(err, netsec.ErrInvalid) {
		t.Fatalf("CA file without PEM: %v", err)
	}
}

func TestNewRefusesBeforeDialing(t *testing.T) {
	_, err := New(context.Background(), "mysql://pr:"+secret+"@localhost/shop?allowAllFiles=true")
	if !errors.Is(err, ErrBadURL) {
		t.Fatalf("err = %v", err)
	}
}

// Target down: the error says so and never carries the password.
func TestNewTargetDown(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	_, err = New(ctx, "mysql://pr:"+secret+"@"+addr+"/shop")
	if err == nil {
		t.Fatal("New on a closed port succeeded")
	}
	if strings.Contains(err.Error(), secret) || !strings.Contains(err.Error(), "mysql: connect") {
		t.Fatalf("err = %v", err)
	}
}

func redactPW(s string) string { return strings.ReplaceAll(s, secret, "***") }
