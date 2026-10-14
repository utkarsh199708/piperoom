package discover

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"testing"
)

// QA4: a Connect HTTP error quotes the response body, which can echo the
// connector's config (database.password). The warning must not carry it
// verbatim, nor an unbounded body.
func TestConnectorErrorBodyRedactedAndTruncated(t *testing.T) {
	const secret = "Sup3rS3cr3tPassw0rd!"
	body := `{"error_code":500,"message":"Connector config {database.password=` + secret + `, database.user=cdc}"}` + strings.Repeat("x", 10000)
	d := single(pgConfig("topic.prefix", "pg"))
	d.Connect = &fakeConnect{
		configs: map[string]map[string]string{"c": pgConfig("topic.prefix", "pg")},
		errs:    map[string]error{"c": errors.New("connect: GET /connectors/c/config: status 500: " + body)},
	}
	var buf bytes.Buffer
	run(t, d, Options{Logger: slog.New(slog.NewTextHandler(&buf, nil))})
	out := buf.String()
	if !strings.Contains(out, "connector config read failed") {
		t.Fatalf("warning missing: %s", out)
	}
	if strings.Contains(out, secret) {
		t.Fatalf("secret logged verbatim: %s", out)
	}
	if len(out) > 2*maxLoggedErr {
		t.Fatalf("log line is %d bytes, want the error capped near %d", len(out), maxLoggedErr)
	}
}
