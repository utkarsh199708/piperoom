package kafkatools

import (
	"encoding/json"
	"strings"
	"testing"

	"piperoom/internal/clients"
)

// traceRow is one trace text from the #430 review corpus. Each row runs as a
// FAILED connector's trace and as a FAILED task's trace; secrets must reach
// neither Summary nor Data, keep must reach both.
type traceRow struct {
	name    string
	line    string // one trace line, a "Caused by:" line or a top line
	secrets []string
	keep    []string
}

// bothPaths runs line through connect_status as a connector trace and as a
// task trace and returns, per path, Summary and the condensed error in Data.
func bothPaths(t *testing.T, line string) map[string][2]string {
	t.Helper()
	trace := line
	if strings.HasPrefix(line, "Caused by:") {
		trace = "org.apache.kafka.connect.errors.ConnectException: Exiting WorkerSinkTask due to unrecoverable exception.\n" + line
	}
	out := map[string][2]string{}
	r, data := connectStatusData(t, clients.ConnectorStatus{Name: "dw-sink", Type: "sink", State: "FAILED", WorkerID: "w1:8083", Trace: trace})
	var c struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal([]byte(data), &c); err != nil {
		t.Fatal(err)
	}
	out["connector"] = [2]string{r.Summary, c.Error}
	r, data = connectStatusData(t, clients.ConnectorStatus{Name: "dw-sink", Type: "sink", State: "RUNNING",
		Tasks: []clients.TaskStatus{{ID: 0, State: "FAILED", Trace: trace}}})
	var tk struct {
		Tasks []struct {
			Error string `json:"error"`
		} `json:"tasks"`
	}
	if err := json.Unmarshal([]byte(data), &tk); err != nil || len(tk.Tasks) != 1 {
		t.Fatalf("data = %s (%v)", data, err)
	}
	out["task"] = [2]string{r.Summary, tk.Tasks[0].Error}
	return out
}

func checkRows(t *testing.T, rows []traceRow) {
	t.Helper()
	for _, tt := range rows {
		t.Run(tt.name, func(t *testing.T) {
			for path, got := range bothPaths(t, tt.line) {
				for i, where := range []string{"summary", "data error"} {
					for _, s := range tt.secrets {
						if strings.Contains(got[i], s) {
							t.Errorf("%s path, %s carries %q:\n%s", path, where, s, got[i])
						}
					}
					for _, k := range tt.keep {
						if !strings.Contains(got[i], k) {
							t.Errorf("%s path, %s lost %q:\n%s", path, where, k, got[i])
						}
					}
				}
			}
		})
	}
}

// listenerRefused is the exception text DriverManager and the Oracle driver
// put before a URL.
const listenerRefused = "java.sql.SQLException: Listener refused the connection: "

// TestTraceOracleURLs: the Oracle password runs to the last '@' of its token
// (it may hold '@' and '/'), the user may be quoted, and URLs without
// credentials stay whole. Credentials are dummies.
func TestTraceOracleURLs(t *testing.T) {
	checkRows(t, []traceRow{
		{"at in password, sid", listenerRefused + "jdbc:oracle:thin:loader/Pr0d@2024x@wh:1521:ORCL", []string{"Pr0d", "2024x"}, []string{"jdbc:oracle:thin:loader/", "@wh:1521:ORCL"}},
		{"at in password, service", listenerRefused + "jdbc:oracle:thin:etl/S3c@Ret99@//wh:1521/svc", []string{"S3c@", "Ret99"}, []string{"jdbc:oracle:thin:etl/", "@//wh:1521/svc"}},
		{"leading at", listenerRefused + "jdbc:oracle:thin:loader/P@ssw0rd99@wh:1521:ORCL", []string{"ssw0rd99"}, []string{"@wh:1521:ORCL"}},
		{"at mid password, service", listenerRefused + "jdbc:oracle:thin:etl/Ab@cD3fGh!@//wh:1521/svc", []string{"Ab@", "cD3fGh"}, []string{"@//wh:1521/svc"}},
		{"two ats", listenerRefused + "jdbc:oracle:thin:ops/x@y@Zq9Kp2Lm@wh:1521:ORCL", []string{"x@y", "Zq9Kp2Lm"}, []string{"jdbc:oracle:thin:ops/", "@wh:1521:ORCL"}},
		{"slash and at", listenerRefused + "jdbc:oracle:thin:loader/a/B9@x7Qw@//wh:1521/svc", []string{"B9@", "x7Qw"}, []string{"@//wh:1521/svc"}},
		{"quoted user", listenerRefused + "jdbc:oracle:thin:\"Loader\"/QuPw55z@wh:1521:ORCL", []string{"QuPw55z"}, []string{"@wh:1521:ORCL"}},
		{"quoted upper user", listenerRefused + "jdbc:oracle:thin:\"LOADER\"/QuS3rPw77@wh:1521:ORCL", []string{"QuS3rPw77"}, []string{"@wh:1521:ORCL"}},
		{"no host", listenerRefused + "jdbc:oracle:thin:loader/NoAtPw99 (connection refused)", []string{"NoAtPw99"}, []string{"jdbc:oracle:thin:loader/", "(connection refused)"}},
		// No credentials: nothing to mask, the address is the diagnosis.
		{"no credentials, service", listenerRefused + "jdbc:oracle:thin:@//wh:1521/svc", nil, []string{"jdbc:oracle:thin:@//wh:1521/svc"}},
		{"no credentials, sid", listenerRefused + "jdbc:oracle:thin:@wh:1521:ORCL", nil, []string{"jdbc:oracle:thin:@wh:1521:ORCL"}},
		{"no credentials, ezconnect", listenerRefused + "jdbc:oracle:thin:@wh/svc", nil, []string{"jdbc:oracle:thin:@wh/svc"}},
	})
}

// TestTraceSQLServerBracedPassword: MS JDBC writes a value holding ';' in
// braces ('}}' escapes '}'). redact.Text read the braced value as URL user
// info, leaked its tail and masked the port and user instead.
func TestTraceSQLServerBracedPassword(t *testing.T) {
	const nsd = "java.sql.SQLException: No suitable driver found for "
	checkRows(t, []traceRow{
		{"braced with at", nsd + "jdbc:sqlserver://wh:1433;user=loader;password={Br@ce;Pw42x};encrypt=true", []string{"Br@ce", "Pw42x"}, []string{"jdbc:sqlserver://wh:1433", "user=loader", "encrypt=true"}},
		{"braced", nsd + "jdbc:sqlserver://wh:1433;user=loader;password={Br4ce;S3cr3tT4il};encrypt=true", []string{"Br4ce", "S3cr3tT4il"}, []string{"jdbc:sqlserver://wh:1433", "user=loader"}},
		{"braced pwd, escaped brace", nsd + "jdbc:sqlserver://wh:1433;user=loader;pwd={a}}b;Esc4peTail};encrypt=true", []string{"Esc4peTail"}, []string{"jdbc:sqlserver://wh:1433", "user=loader"}},
	})
}

// TestTraceCredentialsBeforeRecordMasking: a line that quotes a record also
// goes through dlq.RedactMessage, which drops the quoted column name a
// key/value rule needs and caps the line at 300 runes. Credentials are masked
// first, so neither can expose them.
func TestTraceCredentialsBeforeRecordMasking(t *testing.T) {
	cut := func(head, url, secret string) string {
		// Pad so the 300-rune cut of dlq.RedactMessage falls inside secret.
		const pre = "Caused by: "
		start := len(pre) + len(head) + 1 + strings.Index(url, secret)
		// Six runes of it before the cut, as in the review's inputs.
		n := 300 - start - 6
		line := pre + head + strings.Repeat("x", n) + " " + url
		if i := strings.Index(line, secret); i >= 300 || i+len(secret) <= 300 {
			t.Fatalf("test setup: %q at [%d,%d) does not straddle rune 300", secret, i, i+len(secret))
		}
		return line
	}
	checkRows(t, []traceRow{
		{"update password column", "Caused by: java.sql.BatchUpdateException: Batch entry 0 UPDATE \"users\" SET \"password\" = 'Hunter2xyzQ' WHERE \"id\" = 7 was aborted", []string{"Hunter2xyzQ"}, []string{"Batch entry 0 UPDATE"}},
		{"update client secret", "Caused by: java.sql.BatchUpdateException: Batch entry 0 UPDATE \"apps\" SET \"client_secret\" = 'Cs_live_8a7b6c5d4e3f2a1b' WHERE \"id\" = 7 was aborted", []string{"Cs_live_8a7b6c5d4e3f2a1b", "8a7b6c5d"}, nil},
		{"update api token", "Caused by: java.sql.BatchUpdateException: Batch entry 0 UPDATE \"apps\" SET \"api_token\" = 'Tk_9f8e7d6c5b4a3f2e1d0c9b8a' WHERE \"id\" = 7 was aborted", []string{"Tk_9f8e7d6c5b4a3f2e1d0c9b8a", "9f8e7d6c"}, nil},
		{"cannot parse, cut in password", cut("org.apache.kafka.connect.errors.DataException: Cannot parse value from ", "postgresql://loader:CutPw1234567@wh/dw", "CutPw1234567"), []string{"CutPw1"}, []string{"Cannot parse value from"}},
		{"batch insert, cut in password", cut("java.sql.BatchUpdateException: Batch entry 0 INSERT INTO audit failed for ", "postgresql://loader:CutBPw9876543@wh/dw", "CutBPw9876543"), []string{"CutBPw"}, []string{"Batch entry 0 INSERT"}},
		{"cannot parse, long password cut", cut("org.apache.kafka.connect.errors.DataException: Cannot parse value from ", "postgresql://loader:LongR3cPassw0rdTail@wh:5432/dw", "LongR3cPassw0rdTail"), []string{"LongR3"}, nil},
		// Record values the dlq masking covers once recordSnippet matches.
		{"mysql duplicate entry", "Caused by: java.sql.SQLIntegrityConstraintViolationException: Duplicate entry '123-45-6789' for key 'customers.ssn_uq'", []string{"123-45-6789"}, []string{"Duplicate entry", "customers.ssn_uq"}},
		{"sqlserver duplicate key", "Caused by: com.microsoft.sqlserver.jdbc.SQLServerException: Violation of PRIMARY KEY constraint 'PK_customers'. Cannot insert duplicate key in object 'dbo.customers'. The duplicate key value is (123-45-6789).", []string{"123-45-6789"}, []string{"Violation of PRIMARY KEY constraint"}},
	})
}

// TestTraceRedactGaps pins today's output for redact.Text gaps the trace path
// exposes. They are A4's to fix (gap: redact.Text, see #455); when a row
// changes, flip it to the masked form in the same PR.
func TestTraceRedactGaps(t *testing.T) {
	for _, tt := range []struct {
		name, line, today string
	}{
		// Leaks.
		{"gap: redact.Text, see #455: config value", "org.apache.kafka.common.config.ConfigException: Invalid value S3cretKs!Pw for configuration ssl.key.password: must be non-empty", "Invalid value S3cretKs!Pw for configuration"},
		{"gap: redact.Text, see #455: bare aws secret", "com.amazonaws.SdkClientException: bad creds key AKIAIOSFODNN7EXAMPLE secret wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY", "wJalrXUtnFEMI"},
		// Over-masking that drops the diagnosis.
		{"gap: redact.Text, see #455: using password NO", "java.sql.SQLException: Access denied for user 'debezium'@'172.18.0.7' (using password: NO)", "(using password: [redacted"},
		{"gap: redact.Text, see #455: using password YES", "java.sql.SQLException: Access denied for user 'debezium'@'172.18.0.7' (using password: YES)", "(using password: [redacted"},
		{"gap: redact.Text, see #455: scram login module", "javax.security.auth.login.LoginException: ScramLoginModule required", "[redacted"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for path, got := range bothPaths(t, tt.line) {
				if !strings.Contains(got[0], tt.today) || !strings.Contains(got[1], tt.today) {
					t.Errorf("%s path no longer gives today's %q (fixed in redact? flip this row):\nsummary: %s\ndata: %s", path, tt.today, got[0], got[1])
				}
			}
		})
	}
}
