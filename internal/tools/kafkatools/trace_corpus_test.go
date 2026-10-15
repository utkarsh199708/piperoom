package kafkatools

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
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
		// Escaped quotes, as in a trace that echoes JSON config, and an SQL
		// doubled quote inside a quoted password.
		{"escaped quoted user", listenerRefused + `jdbc:oracle:thin:\"Loader\"/EscU5erPw9@wh:1521:ORCL`, []string{"EscU5erPw9"}, []string{"@wh:1521:ORCL"}},
		{"escaped quoted password in json", `org.apache.kafka.connect.errors.ConnectException: bad config: {\"connection.url\":\"jdbc:oracle:thin:loader/\"p w0rdJs0nTail\"@wh:1521:ORCL\"}`, []string{"w0rdJs0nTail"}, []string{"jdbc:oracle:thin:loader/", "@wh:1521:ORCL"}},
		{"doubled quote in quoted password", listenerRefused + `jdbc:oracle:thin:loader/"Dq""Pw0rdTail"@wh:1521:ORCL`, []string{"Pw0rdTail"}, []string{"@wh:1521:ORCL"}},
		// A credential after the URL on the same line (compact JSON, EZConnect
		// Plus parameters, a JDBC property) keeps its own mask: the Oracle
		// password must not run into it and eat its key.
		{"json sibling password", `org.apache.kafka.connect.errors.ConnectException: bad config {"connection.url":"jdbc:oracle:thin:loader/Pw9Orc@wh:1521:ORCL","connection.password":"Sec@ret99Tail"}`, []string{"Pw9Orc", "ret99Tail"}, []string{"@wh:1521:ORCL", "connection.password"}},
		{"escaped json sibling password", `org.apache.kafka.connect.errors.ConnectException: bad config {\"connection.url\":\"jdbc:oracle:thin:loader/Pw9Orc@wh:1521:ORCL\",\"connection.password\":\"Sec@ret99Tail\"}`, []string{"Pw9Orc", "ret99Tail"}, []string{"@wh:1521:ORCL", "connection.password"}},
		{"json sibling token", `org.apache.kafka.connect.errors.ConnectException: bad config {"connection.url":"jdbc:oracle:thin:loader/Pw9Orc@wh:1521:ORCL","connection.token":"Tk@n9Zq8Xw7Vu6"}`, []string{"Pw9Orc", "n9Zq8Xw7Vu6"}, []string{"@wh:1521:ORCL"}},
		{"ezconnect wallet password", listenerRefused + "jdbc:oracle:thin:loader/Pw9Orc@tcps://wh:2484/svc?wallet_location=/w&oracle.net.wallet_password=W@llT4il99", []string{"Pw9Orc", "llT4il99"}, []string{"@tcps://wh:2484/svc"}},
		{"braced sibling password", listenerRefused + "jdbc:oracle:thin:loader/Pw9Orc@wh:1521:ORCL;password={Br@ceOrcT4il}", []string{"Pw9Orc", "Br@ce", "ceOrcT4il"}, []string{"@wh:1521:ORCL"}},
		{"double-escaped quoted password", listenerRefused + `jdbc:oracle:thin:loader/\\"DblEsc9Pw\\"@wh:1521:ORCL`, []string{"DblEsc9Pw"}, []string{"@wh:1521:ORCL"}},
		{"doubled quote in quoted user", listenerRefused + `jdbc:oracle:thin:"lo""ader"/NqPw66Zx@wh:1521:ORCL`, []string{"NqPw66Zx"}, []string{"@wh:1521:ORCL"}},
		// cleanTrace masks a second time; a masked password is left alone, so
		// the host survives even with a later '@' glued on.
		{"second pass keeps host", `org.apache.kafka.connect.errors.ConnectException: bad config: {\"url\":\"jdbc:oracle:thin:loader/\"Ej@x EjTail\"@wh:1521:ORCL\",\"owner\":\"ops@corp\"}`, []string{"Ej@x", "EjTail"}, []string{"loader/[redacted]@wh:1521:ORCL"}},
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
		// Compound MS JDBC keys take braced values the same way.
		{"trustStorePassword", nsd + "jdbc:sqlserver://wh:1433;user=loader;password=Pl41nPwZx;trustStorePassword={Tr@st;StPw42Zx};encrypt=true", []string{"Pl41nPwZx", "Tr@st", "StPw42Zx"}, []string{"jdbc:sqlserver://wh:1433", "user=loader", "encrypt=true"}},
		{"keyStorePassword", nsd + "jdbc:sqlserver://wh:1433;user=loader;keyStorePassword={Ks;St0reP4ssTail}", []string{"St0reP4ssTail"}, []string{"jdbc:sqlserver://wh:1433", "user=loader"}},
		{"keyStoreSecret", nsd + "jdbc:sqlserver://wh:1433;user=loader;keyStoreSecret={K3y;Secr3tTailQ}", []string{"Secr3tTailQ"}, []string{"jdbc:sqlserver://wh:1433", "user=loader"}},
		{"clientKeyPassword", nsd + "jdbc:sqlserver://wh:1433;user=loader;clientKeyPassword={Cl1ent@Key;TailW9}", []string{"Cl1ent@Key", "TailW9"}, []string{"jdbc:sqlserver://wh:1433", "user=loader"}},
		{"aadSecurePrincipalSecret", nsd + "jdbc:sqlserver://wh:1433;user=loader;aadSecurePrincipalSecret={Aad;Pr1ncTail8}", []string{"Pr1ncTail8"}, []string{"jdbc:sqlserver://wh:1433", "user=loader"}},
		{"database_password", nsd + "jdbc:sqlserver://wh:1433;user=loader;database_password={Us;Und3rTail}", []string{"Und3rTail"}, []string{"jdbc:sqlserver://wh:1433", "user=loader"}},
		// Malformed: no closing brace; the value runs to the end of the line.
		{"unclosed brace", nsd + "jdbc:sqlserver://wh:1433;user=loader;password={Br@ce;Unc1osedTail", []string{"Br@ce", "Unc1osedTail"}, []string{"jdbc:sqlserver://wh:1433", "user=loader"}},
		// An empty value has nothing to hide.
		{"empty braces", nsd + "jdbc:sqlserver://wh:1433;user=loader;password={};encrypt=true", nil, []string{"password={};encrypt=true"}},
		// Keys that do not end in password, pwd or secret keep their values.
		{"unrelated braced keys", nsd + "jdbc:sqlserver://wh:1433;user=loader;passwordPolicy={min=12};cwd={C:/app};encrypt=true", nil, []string{"jdbc:sqlserver://wh:1433", "user=loader", "passwordPolicy={min=12}", "cwd={C:/app}"}},
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

// sqlsrv is a SQL Server driver error up to the connection properties.
const sqlsrv = "java.sql.SQLException: No suitable driver found for jdbc:sqlserver://wh:1433;user=loader;"

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
		// "must" reads as a value after "ssl.key.password:".
		{"gap: redact.Text, see #455: must be non-empty", "org.apache.kafka.common.config.ConfigException: Missing value for configuration ssl.key.password: must be non-empty", ": [redacted 8] be non-empty"},
		// An unbraced password with '@' reads as URL user info: the port and
		// user are masked and the tail leaks.
		{"gap: redact.Text, see #455: unbraced at password", "java.sql.SQLException: No suitable driver found for jdbc:sqlserver://wh:1433;user=loader;password=P@ssUnbr42Zx;encrypt=true", "@ssUnbr42Zx"},
		// The SQL '' escape ends the quoted value early; the tail leaks.
		{"gap: redact.Text, see #455: sql doubled quote", "Caused by: java.sql.BatchUpdateException: Batch entry 0 UPDATE \"users\" SET \"password\" = 'It''sS3cret99' WHERE \"id\" = 7 was aborted", "sS3cret99"},
		// Braced forms main leaks the same way (#455): text glued after the
		// closing brace, keys that do not end in password/pwd/secret, nested
		// braces.
		{"gap: redact.Text, see #455: glued after {noop}", sqlsrv + "password={noop}Gl0edPwTail;encrypt=true", "Gl0edPwTail"},
		{"gap: redact.Text, see #455: glued after {bcrypt}", sqlsrv + "password={bcrypt}Bcr7ptHashTail;encrypt=true", "Bcr7ptHashTail"},
		{"gap: redact.Text, see #455: glued after {cipher}", sqlsrv + "password={cipher}C1pherTextTail;encrypt=true", "C1pherTextTail"},
		{"gap: redact.Text, see #455: glued after {}", sqlsrv + "password={}Gl0edEmptyTail;encrypt=true", "Gl0edEmptyTail"},
		{"gap: redact.Text, see #455: glued after client.secret", sqlsrv + "client.secret={x}Gl0edSecTail;encrypt=true", "Gl0edSecTail"},
		{"gap: redact.Text, see #455: secretKey braced", sqlsrv + "secretKey={Sk@y;SkKeyT4il77};encrypt=true", "SkKeyT4il77"},
		{"gap: redact.Text, see #455: secret_key braced", sqlsrv + "secret_key={Sk@y;SkT4il77};encrypt=true", "SkT4il77"},
		{"gap: redact.Text, see #455: nested braces", sqlsrv + "password={a{b}c;NestT4il88};encrypt=true", "NestT4il88"},
		// Oracle forms the local mask does not cover (#449).
		{"gap: redact.Text, see #449: unclosed quote", listenerRefused + `jdbc:oracle:thin:loader/"UnclQ UqTail5@wh:1521:ORCL`, "UqTail5"},
		{"gap: redact.Text, see #449: unquoted space", listenerRefused + "jdbc:oracle:thin:loader/Sp Tail7Q@wh:1521:ORCL", "Tail7Q"},
		// dlq.RedactMessage keeps identifier-shaped Key (...)=(...) values (#469).
		{"gap: dlq.RedactMessage, see #469: identifier-shaped key value", "Caused by: org.postgresql.util.PSQLException: ERROR: insert or update violates foreign key Key (customer_ref)=(jdoe_smith) is not present in table \"customers\".", "Key (customer_ref)=(jdoe_smith)"},
		// Known over-mask, no leak: a {{ }} template reads as a brace that
		// '}}' escapes, so the value runs to the end of the line.
		{"known: template over-mask", sqlsrv + "password={{ vault_db_password }};encrypt=true", "user=loader;password=[redacted]"},
		// Not a redact gap: per-line masking cannot follow a quote opened on
		// the line before (main's whole-text scrub did). Contrived; pinned.
		{"known: quote across lines", "org.apache.kafka.connect.errors.ConnectException: bad jaas config: password=\"QHead99\nCaused by: QTail99Zx\";", "QTail99Zx"},
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

// bigSchema is an Avro record schema over redact.MaxScan (64 KiB).
func bigSchema() string {
	var sb strings.Builder
	sb.WriteString(`{"type":"record","name":"Envelope","fields":[`)
	for i := 0; i < 1100; i++ {
		if i > 0 {
			sb.WriteString(",")
		}
		fmt.Fprintf(&sb, `{"name":"field_%06d_padding","type":["null","string"],"default":null}`, i)
	}
	sb.WriteString(`]}`)
	return sb.String()
}

// TestTraceLargeSchemaKeepsTail: schema blobs are stripped before credential
// masking. redact.Text masks text past 64 KiB as one block, and a blob cut
// that way has no closing brace or quote, so stripping it afterwards took the
// error code, compatibility level and subject with it.
func TestTraceLargeSchemaKeepsTail(t *testing.T) {
	big := bigSchema()
	if len(big) <= 64<<10 {
		t.Fatalf("test setup: schema is %d bytes, want over 64 KiB", len(big))
	}
	checkRows(t, []traceRow{
		{"old schema over 64 KiB", "Caused by: io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException: Schema being registered is incompatible with an earlier schema for subject \"orders-value\", details: [{errorType:'MISSING_UNION_BRANCH', description:'The new schema is missing a type'}, {oldSchemaVersion: 1}, {oldSchema: '" + big + "'}, {compatibility: 'BACKWARD'}]; error code: 409",
			nil, []string{"error code: 409", "compatibility: 'BACKWARD'", "orders-value", "oldSchema: <schema>"}},
		{"registered schema over 64 KiB", "org.apache.kafka.connect.errors.DataException: Error registering Avro schema" + big + " for subject orders-value; error code: 409",
			nil, []string{"error code: 409", "orders-value", "Error registering Avro schema<schema>"}},
	})
}

// TestTraceOracleOvermask pins a known over-mask: an unquoted Oracle password
// runs to the last '@' of its token, so text glued after the URL up to a later
// '@' is masked with it and the host reads wrong. Stopping earlier would leak
// a password that holds '@' (TestTraceOracleURLs). A credential in that glued
// text is masked before the Oracle pass runs, so the over-mask cannot eat its
// key ("json sibling password" in TestTraceOracleURLs).
func TestTraceOracleOvermask(t *testing.T) {
	for _, line := range []string{
		`org.apache.kafka.connect.errors.ConnectException: bad config {"url":"jdbc:oracle:thin:loader/Js0nPw9@wh:1521:ORCL","owner":"ops@corp.example"}`,
		listenerRefused + "jdbc:oracle:thin:loader/Js0nPw9@wh:1521:ORCL;principal=dbz@EXAMPLE.COM",
	} {
		for path, got := range bothPaths(t, line) {
			for _, text := range got {
				if strings.Contains(text, "Js0nPw9") || !strings.Contains(text, "loader/[redacted]@") {
					t.Errorf("%s path: want the password masked (over-mask allowed):\n%s", path, text)
				}
			}
		}
	}
}

// TestCleanTraceMasksBeforeCap: cleanTrace masks the whole condensed text
// before its 2000-byte cap, so a URL password across the cut is masked whole.
func TestCleanTraceMasksBeforeCap(t *testing.T) {
	const pre = "org.apache.kafka.connect.errors.ConnectException: Cannot connect to "
	url := "postgresql://loader:CapS3cretPassw0rd@wh:5432/dw"
	pad := strings.Repeat("retry ", (maxTraceText-len(pre)-len("postgresql://loader:")-6)/6)
	in := pre + pad + url
	if i := strings.Index(in, "CapS3cret"); i >= maxTraceText || i+len("CapS3cretPassw0rd") <= maxTraceText {
		t.Fatalf("test setup: secret at %d does not straddle %d", i, maxTraceText)
	}
	if got := cleanTrace(in); strings.Contains(got, "CapS3c") {
		t.Fatalf("cleanTrace leaks the password head:\n...%s", got[len(got)-120:])
	}
}

// TestConnectStatusRejectedSchemaLineMasked: the rejected-schema diff is added
// to a task's condensed trace after condenseTrace, so only cleanTrace masks
// it. Field names come from the trace and the registry: untrusted.
func TestConnectStatusRejectedSchemaLineMasked(t *testing.T) {
	const leak = "postgresql://loader:Fn4meLeakPw@wh/dw"
	trace := "org.apache.kafka.connect.errors.ConnectException: Tolerance exceeded in error handler\n" +
		`Caused by: org.apache.kafka.common.errors.SerializationException: Error registering Avro schema{"type":"record","name":"R","fields":[{"name":"id","type":"long"},{"name":"` + leak + `","type":"string"}]}` + "\n" +
		`Caused by: RestClientException: Schema being registered is incompatible with an earlier schema for subject "pg.public.orders-value"; error code: 409`
	fc := &fakeConnect{statusFn: func(ctx context.Context, n string) (clients.ConnectorStatus, error) {
		return clients.ConnectorStatus{Name: n, Type: "source", State: "RUNNING",
			Tasks: []clients.TaskStatus{{ID: 0, State: "FAILED", Trace: trace}}}, nil
	}}
	fr := &fakeRegistry{schemaFn: func(context.Context, string, int) (clients.SchemaVersion, error) {
		return clients.SchemaVersion{Subject: "pg.public.orders-value", Version: 3, Schema: `{"type":"record","name":"R","fields":[{"name":"id","type":"long"}]}`}, nil
	}}
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Connect: fc, Registry: fr}); err != nil {
		t.Fatal(err)
	}
	tool, _ := reg.Get("connect_status")
	r := runTool(t, tool, `{"connector":"pg-orders"}`)
	if !strings.Contains(r.Summary, "Rejected schema vs pg.public.orders-value v3") {
		t.Fatalf("test setup: no rejected-schema line:\n%s", r.Summary)
	}
	data, _ := json.Marshal(r.Data)
	for _, where := range []struct{ name, text string }{{"summary", r.Summary}, {"data", string(data)}} {
		if strings.Contains(where.text, "Fn4meLeakPw") {
			t.Errorf("%s carries the password from the rejected-schema line:\n%s", where.name, where.text)
		}
	}
}
