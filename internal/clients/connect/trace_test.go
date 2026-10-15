package connect

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
)

// Connector-level traces as Kafka Connect reports them when the connector
// itself fails to start (no tasks). Credentials are dummies.
const (
	tracePGSource = "io.debezium.DebeziumException: Couldn't obtain encoding for database orders\n" +
		"\tat io.debezium.connector.postgresql.connection.PostgresConnection.determineDatabaseCharset(PostgresConnection.java:745)\n" +
		"\tat io.debezium.connector.postgresql.PostgresConnectorTask.start(PostgresConnectorTask.java:97)\n" +
		"Caused by: org.postgresql.util.PSQLException: FATAL: password authentication failed for user \"debezium\"\n" +
		"\tat org.postgresql.core.v3.ConnectionFactoryImpl.doAuthentication(ConnectionFactoryImpl.java:693)\n" +
		"\t... 14 more\n"
	traceMySQL = "org.apache.kafka.connect.errors.ConnectException: Unable to connect: Access denied for user 'debezium'@'172.18.0.7' (using password: YES)\n" +
		"\tat io.debezium.connector.mysql.MySqlConnector.validateConnection(MySqlConnector.java:92)\n" +
		"Caused by: java.sql.SQLException: Access denied for user 'debezium'@'172.18.0.7' (using password: YES)\n" +
		"\tat com.mysql.cj.jdbc.exceptions.SQLError.createSQLException(SQLError.java:130)\n"
	traceJDBCSink = "org.apache.kafka.connect.errors.ConnectException: java.sql.SQLException: No suitable driver found for jdbc:postgresql://warehouse:5432/dw?user=loader&password=Sup3rS3cretPw\n" +
		"\tat io.confluent.connect.jdbc.util.CachedConnectionProvider.getConnection(CachedConnectionProvider.java:59)\n" +
		"\tat io.confluent.connect.jdbc.sink.JdbcSinkTask.start(JdbcSinkTask.java:46)\n" +
		"Caused by: java.sql.SQLException: No suitable driver found for jdbc:postgresql://warehouse:5432/dw?user=loader&password=Sup3rS3cretPw\n" +
		"\tat java.sql/java.sql.DriverManager.getConnection(DriverManager.java:706)\n"
	tracePGJDBCSource = "org.apache.kafka.common.config.ConfigException: Invalid value postgresql://connect:Pa55w0rdLeak@pg-primary:5432/orders for configuration connection.url: Couldn't open connection\n" +
		"\tat io.confluent.connect.jdbc.JdbcSourceConnector.start(JdbcSourceConnector.java:94)\n" +
		"Caused by: org.postgresql.util.PSQLException: FATAL: database \"orders\" does not exist\n" +
		"\tat org.postgresql.core.v3.QueryExecutorImpl.receiveErrorResponse(QueryExecutorImpl.java:2713)\n"
)

// TestStatusDecodesConnectorTrace: a connector that fails at startup carries
// its trace on the connector object and lists no tasks; it used to be
// dropped, so connect_status had no reason to show (#399).
func TestStatusDecodesConnectorTrace(t *testing.T) {
	for _, tt := range []struct {
		name, typ, state, trace string
	}{
		{"debezium postgres", "source", "FAILED", tracePGSource},
		{"debezium mysql", "source", "FAILED", traceMySQL},
		{"jdbc sink", "sink", "FAILED", traceJDBCSink},
		{"jdbc postgres source", "source", "FAILED", tracePGJDBCSource},
		{"running, no trace", "source", "RUNNING", ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn := map[string]any{"state": tt.state, "worker_id": "w1:8083"}
			if tt.trace != "" {
				conn["trace"] = tt.trace
			}
			body, err := json.Marshal(map[string]any{"name": "pg-orders", "type": tt.typ, "connector": conn, "tasks": []any{}})
			if err != nil {
				t.Fatal(err)
			}
			c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/connectors/pg-orders/status" {
					http.NotFound(w, r)
					return
				}
				w.Write(body)
			})
			cs, err := c.Status(context.Background(), "pg-orders")
			if err != nil {
				t.Fatalf("Status: %v", err)
			}
			if cs.State != tt.state || cs.WorkerID != "w1:8083" || cs.Type != tt.typ || len(cs.Tasks) != 0 {
				t.Fatalf("got %+v", cs)
			}
			// The client keeps the raw text; condensing and masking are the
			// tool's job (clients.ConnectorStatus.Trace).
			if cs.Trace != tt.trace {
				t.Fatalf("Trace = %q, want %q", cs.Trace, tt.trace)
			}
		})
	}
}
