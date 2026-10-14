package integrations

import (
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"strings"
	"testing"

	"piperoom/internal/integrations/spec"
)

// sslServer answers every SSLRequest with reply and closes.
func sslServer(t *testing.T, reply byte) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				msg := make([]byte, 8)
				if _, err := io.ReadFull(c, msg); err != nil {
					return
				}
				if binary.BigEndian.Uint32(msg[4:]) == sslRequestCode {
					_, _ = c.Write([]byte{reply})
				}
			}(c)
		}
	}()
	_, p, _ := net.SplitHostPort(ln.Addr().String())
	return p
}

func TestPostgresTLSRefusedByServer(t *testing.T) {
	port := sslServer(t, 'N')
	d := fastDeps()
	// pgconn's own error for an 'N' answer is untyped text.
	d.pgConnect = func(context.Context, pgTarget) (pgSession, error) {
		return nil, errors.New("tls error: server refused TLS connection")
	}
	checks := d.postgresKinds()[0].Test(context.Background(), pgValues(port))
	c := wantStatus(t, checks, "tls", spec.CheckFail)
	if !strings.Contains(c.Detail, "did not use TLS") || !strings.Contains(c.Fix, "Enable ssl") {
		t.Fatalf("tls = %+v", c)
	}
	if l := wantStatus(t, checks, "login", spec.CheckSkipped); strings.Contains(l.Fix, "Check the host") {
		t.Fatalf("login = %+v", l)
	}
}

func TestPostgresSSLProbeAcceptedIsNotRefused(t *testing.T) {
	d := fastDeps()
	if d.sslRefused(context.Background(), "127.0.0.1", sslServer(t, 'S')) {
		t.Fatal("'S' reported as refused")
	}
	if d.sslRefused(context.Background(), "127.0.0.1", closedPort(t)) {
		t.Fatal("closed port reported as refused")
	}
}

func TestPostgresSetupScriptSafeUnedited(t *testing.T) {
	script := pgSetupScript(true)(vals(map[string]string{"user": "piperoom_ro", "database": "orders", "tables": "public.orders"}, nil))
	if strings.Contains(script, "PASSWORD '") {
		t.Fatalf("script carries a usable password literal:\n%s", script)
	}
	if !strings.Contains(script, `\password piperoom_ro`) {
		t.Fatalf("script does not say how to set the password:\n%s", script)
	}
	// The optional audit grants run only when the schema exists.
	for _, line := range strings.Split(script, "\n") {
		if strings.HasPrefix(line, "GRANT") && strings.Contains(line, "piperoom_audit") {
			t.Fatalf("unguarded audit grant %q:\n%s", line, script)
		}
	}
	if !strings.Contains(script, "to_regclass('piperoom_audit.ddl_log') IS NOT NULL") {
		t.Fatalf("audit grants are not guarded:\n%s", script)
	}
}
