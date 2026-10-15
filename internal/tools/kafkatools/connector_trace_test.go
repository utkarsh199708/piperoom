package kafkatools

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// TestConnectStatusConnectorTrace: a connector that fails at startup has no
// tasks, only a connector-level trace; connect_status used to say just
// "FAILED on w1:8083." (#399). The trace is condensed (top line and every
// "Caused by:", no frames), record values masked as for task traces, and
// credentials redacted, in Summary and Data alike. Credentials are dummies.
func TestConnectStatusConnectorTrace(t *testing.T) {
	for _, tt := range []struct {
		name, typ, trace string
		want             []string // the diagnosis that must survive
		secrets          []string // what must not reach Summary or Data
	}{
		{
			name: "debezium postgres, password rotated", typ: "source",
			trace: "io.debezium.DebeziumException: Couldn't obtain encoding for database orders\n" +
				"\tat io.debezium.connector.postgresql.connection.PostgresConnection.determineDatabaseCharset(PostgresConnection.java:745)\n" +
				"\tat io.debezium.connector.postgresql.PostgresConnectorTask.start(PostgresConnectorTask.java:97)\n" +
				"Caused by: org.postgresql.util.PSQLException: FATAL: password authentication failed for user \"debezium\"\n" +
				"\tat org.postgresql.core.v3.ConnectionFactoryImpl.doAuthentication(ConnectionFactoryImpl.java:693)\n" +
				"\t... 14 more\n",
			want: []string{"DebeziumException: Couldn't obtain encoding for database orders",
				"Caused by: org.postgresql.util.PSQLException: FATAL: password authentication failed for user \"debezium\""},
		},
		{
			name: "debezium mysql, access denied", typ: "source",
			trace: "org.apache.kafka.connect.errors.ConnectException: Unable to connect: Access denied for user 'debezium'@'172.18.0.7' (using password: YES)\n" +
				"\tat io.debezium.connector.mysql.MySqlConnector.validateConnection(MySqlConnector.java:92)\n" +
				"Caused by: java.sql.SQLException: Access denied for user 'debezium'@'172.18.0.7'\n" +
				"\tat com.mysql.cj.jdbc.exceptions.SQLError.createSQLException(SQLError.java:130)\n",
			want: []string{"ConnectException: Unable to connect: Access denied for user 'debezium'",
				"Caused by: java.sql.SQLException: Access denied for user 'debezium'@'172.18.0.7'"},
		},
		{
			name: "jdbc sink, password in the connection url", typ: "sink",
			trace: "org.apache.kafka.connect.errors.ConnectException: java.sql.SQLException: No suitable driver found for jdbc:postgresql://warehouse:5432/dw?user=loader&password=Sup3rS3cretPw\n" +
				"\tat io.confluent.connect.jdbc.util.CachedConnectionProvider.getConnection(CachedConnectionProvider.java:59)\n" +
				"Caused by: java.sql.SQLException: No suitable driver found for jdbc:postgresql://warehouse:5432/dw?user=loader&password=Sup3rS3cretPw\n" +
				"\tat java.sql/java.sql.DriverManager.getConnection(DriverManager.java:706)\n",
			want:    []string{"No suitable driver found for jdbc:postgresql://warehouse:5432/dw"},
			secrets: []string{"Sup3rS3cretPw"},
		},
		{
			name: "postgres jdbc source, url password", typ: "source",
			trace: "org.apache.kafka.common.config.ConfigException: Invalid value postgresql://connect:Pa55w0rdLeak@pg-primary:5432/orders for configuration connection.url: Couldn't open connection\n" +
				"\tat io.confluent.connect.jdbc.JdbcSourceConnector.start(JdbcSourceConnector.java:94)\n" +
				"Caused by: org.postgresql.util.PSQLException: FATAL: database \"orders\" does not exist\n" +
				"\tat org.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2713)\n",
			want:    []string{"ConfigException: Invalid value postgresql://connect:", "@pg-primary:5432/orders for configuration connection.url", "FATAL: database \"orders\" does not exist"},
			secrets: []string{"Pa55w0rdLeak"},
		},
		{
			// redact.Text has no rule for Oracle's user/password@ form yet
			// (#449); cleanTrace masks it locally.
			name: "jdbc sink, oracle thin url with sid and service", typ: "sink",
			trace: "org.apache.kafka.connect.errors.ConnectException: java.sql.SQLException: No suitable driver found for jdbc:oracle:thin:loader/Or4clePw77@wh:1521:ORCL\n" +
				"\tat io.confluent.connect.jdbc.util.CachedConnectionProvider.getConnection(CachedConnectionProvider.java:59)\n" +
				"Caused by: java.sql.SQLException: No suitable driver found for jdbc:oracle:thin:etl/Svc0racle!9@//wh:1521/svc\n" +
				"Caused by: java.sql.SQLException: No suitable driver found for jdbc:oracle:thin:ops/\"Qu0ted p@ss\"@//wh:1521/svc\n" +
				"\tat java.sql/java.sql.DriverManager.getConnection(DriverManager.java:706)\n",
			want:    []string{"No suitable driver found for jdbc:oracle:thin:loader/", "@wh:1521:ORCL", "jdbc:oracle:thin:etl/", "@//wh:1521/svc"},
			secrets: []string{"Or4clePw77", "Svc0racle!9", "Qu0ted", "p@ss"},
		},
		{
			name: "jdbc sink, failing record quoted", typ: "sink",
			trace: "org.apache.kafka.connect.errors.ConnectException: Exiting WorkerSinkTask due to unrecoverable exception.\n" +
				"Caused by: java.sql.BatchUpdateException: Batch entry 0 INSERT INTO \"orders\" (\"id\",\"email\") VALUES (42,'carol.leak@example.com') was aborted\n" +
				"Caused by: org.postgresql.util.PSQLException: ERROR: duplicate key value violates unique constraint \"orders_pkey\" Detail: Key (ssn)=(123-45-6789) already exists.\n",
			want:    []string{"BatchUpdateException", "duplicate key value violates unique constraint"},
			secrets: []string{"carol.leak", "123-45-6789"},
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fc := &fakeConnect{statusFn: func(ctx context.Context, n string) (clients.ConnectorStatus, error) {
				return clients.ConnectorStatus{Name: n, Type: tt.typ, State: "FAILED", WorkerID: "w1:8083", Trace: tt.trace}, nil
			}}
			guardWrites(t, fc)
			reg := tools.NewRegistry()
			if err := Register(reg, Deps{Connect: fc}); err != nil {
				t.Fatal(err)
			}
			tool, _ := reg.Get("connect_status")
			r := runTool(t, tool, `{"connector":"pg-orders"}`)
			if !strings.HasPrefix(r.Summary, "pg-orders ("+tt.typ+"): FAILED on w1:8083:\n") {
				t.Fatalf("summary does not lead with the connector's failure:\n%s", r.Summary)
			}
			data, err := json.Marshal(r.Data)
			if err != nil {
				t.Fatal(err)
			}
			var d struct {
				Error string `json:"error"`
			}
			if err := json.Unmarshal(data, &d); err != nil {
				t.Fatal(err)
			}
			for _, w := range tt.want {
				if !strings.Contains(r.Summary, w) {
					t.Errorf("summary misses %q:\n%s", w, r.Summary)
				}
				if !strings.Contains(d.Error, w) {
					t.Errorf("data error misses %q:\n%s", w, d.Error)
				}
			}
			for _, where := range []struct{ name, text string }{{"summary", r.Summary}, {"data", string(data)}} {
				for _, s := range append([]string{"\tat ", ".java:", "more"}, tt.secrets...) {
					if strings.Contains(where.text, s) {
						t.Errorf("%s carries %q:\n%s", where.name, s, where.text)
					}
				}
			}
		})
	}
}

// TestConnectStatusConnectorTraceOnlyWhenFailed: Connect keeps a stale trace
// on a connector that recovered; only a FAILED connector shows one.
func TestConnectStatusConnectorTraceOnlyWhenFailed(t *testing.T) {
	fc := &fakeConnect{statusFn: func(ctx context.Context, n string) (clients.ConnectorStatus, error) {
		return clients.ConnectorStatus{Name: n, Type: "source", State: "RUNNING", WorkerID: "w1:8083",
			Trace: "org.apache.kafka.connect.errors.ConnectException: old failure",
			Tasks: []clients.TaskStatus{{ID: 0, State: "RUNNING"}}}, nil
	}}
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Connect: fc}); err != nil {
		t.Fatal(err)
	}
	tool, _ := reg.Get("connect_status")
	r := runTool(t, tool, `{"connector":"pg-orders"}`)
	if r.Summary != "pg-orders (source): RUNNING on w1:8083.\nTask 0: RUNNING" {
		t.Fatalf("summary = %q", r.Summary)
	}
	data, _ := json.Marshal(r.Data)
	if strings.Contains(string(data), "old failure") {
		t.Fatalf("data carries the stale trace: %s", data)
	}
}

// connectStatusData runs connect_status over one status and returns the result
// and its Data as JSON.
func connectStatusData(t *testing.T, st clients.ConnectorStatus) (tools.Result, string) {
	t.Helper()
	fc := &fakeConnect{statusFn: func(ctx context.Context, n string) (clients.ConnectorStatus, error) { return st, nil }}
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Connect: fc}); err != nil {
		t.Fatal(err)
	}
	tool, _ := reg.Get("connect_status")
	r := runTool(t, tool, `{"connector":"`+st.Name+`"}`)
	data, err := json.Marshal(r.Data)
	if err != nil {
		t.Fatal(err)
	}
	return r, string(data)
}

// TestConnectStatusTaskTraceRedacted: task traces get the same credential
// redaction as connector traces; sink tasks quote their connection URL.
// Record dumps (Connect's SinkRecord/Struct and pgjdbc batch UPDATEs) get
// the record-value masking.
func TestConnectStatusTaskTraceRedacted(t *testing.T) {
	for _, tt := range []struct {
		name, trace string
		want        string
		secrets     []string
	}{
		{"postgres url query password",
			"org.apache.kafka.connect.errors.ConnectException: java.sql.SQLException: Cannot connect to jdbc:postgresql://wh:5432/dw?user=loader&password=Sup3rS3cretPw",
			"Cannot connect to jdbc:postgresql://wh:5432/dw", []string{"Sup3rS3cretPw"}},
		{"postgres url userinfo password",
			"org.apache.kafka.connect.errors.ConnectException: connection refused: postgresql://loader:Pw9x7Leak@wh:5432/dw",
			"connection refused: postgresql://loader:", []string{"Pw9x7Leak"}},
		{"oracle thin",
			"org.apache.kafka.connect.errors.ConnectException: java.sql.SQLException: Listener refused the connection: jdbc:oracle:thin:loader/Or4clePw77@wh:1521:ORCL",
			"Listener refused the connection: jdbc:oracle:thin:loader/", []string{"Or4clePw77"}},
		{"connect sink record dump",
			"org.apache.kafka.connect.errors.ConnectException: Exiting WorkerSinkTask due to unrecoverable exception.\n" +
				"Caused by: org.apache.kafka.connect.errors.DataException: Failed to insert record SinkRecord{kafkaOffset=42, timestampType=CreateTime} ConnectRecord{topic='pg.public.orders', value=Struct{id=42,ssn=123-45-6789,email=dora.leak@example.com}}",
			"DataException: Failed to insert record", []string{"123-45-6789", "dora.leak"}},
		{"pgjdbc batch update",
			"org.apache.kafka.connect.errors.ConnectException: Exiting WorkerSinkTask due to unrecoverable exception.\n" +
				"Caused by: java.sql.BatchUpdateException: Batch entry 0 UPDATE \"customers\" SET \"ssn\" = '123-45-6789',\"name\" = 'Erin Leakwood' WHERE \"id\" = 7 was aborted",
			"BatchUpdateException: Batch entry 0 UPDATE", []string{"123-45-6789", "Erin Leakwood"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			r, data := connectStatusData(t, clients.ConnectorStatus{Name: "dw-sink", Type: "sink", State: "RUNNING",
				Tasks: []clients.TaskStatus{{ID: 0, State: "FAILED", Trace: tt.trace}}})
			if !strings.Contains(r.Summary, tt.want) {
				t.Errorf("summary misses %q:\n%s", tt.want, r.Summary)
			}
			var d struct {
				Tasks []struct {
					Error string `json:"error"`
				} `json:"tasks"`
			}
			if err := json.Unmarshal([]byte(data), &d); err != nil || len(d.Tasks) != 1 || d.Tasks[0].Error == "" {
				t.Fatalf("data = %s (%v)", data, err)
			}
			for _, s := range tt.secrets {
				if strings.Contains(r.Summary, s) {
					t.Errorf("summary carries %q:\n%s", s, r.Summary)
				}
				if strings.Contains(d.Tasks[0].Error, s) {
					t.Errorf("tasks[0].error carries %q:\n%s", s, d.Tasks[0].Error)
				}
			}
		})
	}
}

// TestConnectStatusTraceRedactsBeforeCap: a credential that straddles the
// 2000-byte cap is masked whole. Capped first, the cut would leave a URL with
// no "@host" that the redactor no longer reads as one, and its password head
// would leak.
func TestConnectStatusTraceRedactsBeforeCap(t *testing.T) {
	const head = "org.apache.kafka.connect.errors.ConnectException: Cannot connect to "
	secret := "LongS3cretPassw0rdTail"
	url := "postgresql://loader:" + secret + "@wh:5432/dw"
	// The cut at maxTraceText falls inside the password.
	pad := strings.Repeat("retry ", (maxTraceText-len(head)-len("postgresql://loader:")-6)/6)
	trace := head + pad + url + " after 3 attempts"
	if i := strings.Index(trace, secret); i >= maxTraceText || i+len(secret) <= maxTraceText {
		t.Fatalf("test setup: secret at [%d,%d) does not straddle %d", i, i+len(secret), maxTraceText)
	}
	for _, st := range []clients.ConnectorStatus{
		{Name: "dw-sink", Type: "sink", State: "FAILED", Trace: trace},
		{Name: "dw-sink", Type: "sink", State: "RUNNING", Tasks: []clients.TaskStatus{{ID: 0, State: "FAILED", Trace: trace}}},
	} {
		r, data := connectStatusData(t, st)
		for _, where := range []struct{ name, text string }{{"summary", r.Summary}, {"data", data}} {
			if strings.Contains(where.text, secret[:6]) {
				t.Errorf("%s (connector %s) carries the password head %q", where.name, st.State, secret[:6])
			}
		}
	}
}
