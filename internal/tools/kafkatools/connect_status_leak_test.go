package kafkatools

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// TestConnectStatusMasksRecordValues: sink and converter traces quote the
// failing row (JDBC VALUES tuples, PostgreSQL Detail lines, Jackson's
// [Source:] snippet). connect_status must not carry those values into
// Summary or Data; dlq_peek already masks the same forms.
func TestConnectStatusMasksRecordValues(t *testing.T) {
	const (
		email = "alice.leak@example.com"
		card  = "4242424242424242"
		ssn   = "123-45-6789"
		token = "TOKMARKER9f"
	)
	trace := "org.apache.kafka.connect.errors.ConnectException: Exiting WorkerSinkTask due to unrecoverable exception.\n" +
		"Caused by: java.sql.BatchUpdateException: Batch entry 0 INSERT INTO \"orders\" (\"id\",\"email\",\"card\") VALUES (42,'" + email + "','" + card + "') was aborted\n" +
		"\tat org.postgresql.jdbc.BatchResultHandler.handleError(BatchResultHandler.java:165)\n" +
		"Caused by: org.postgresql.util.PSQLException: ERROR: duplicate key value violates unique constraint \"orders_pkey\" Detail: Key (ssn)=(" + ssn + ") already exists.\n" +
		"Caused by: com.fasterxml.jackson.core.JsonParseException: Unrecognized token '" + token + "': was expecting (JSON String, Number, Array, Object or token 'null', 'true' or 'false') at [Source: (byte[])\"{\"email\":\"" + email + "\"}\"; line: 1, column: 2]\n" +
		"Caused by: org.postgresql.util.PSQLException: ERROR: invalid input syntax for type integer: \"" + email + "\"\n" +
		"Caused by: java.lang.NumberFormatException: For input string: \"" + card + "\"\n" +
		"Caused by: org.apache.kafka.connect.errors.DataException: Cannot parse value \"" + ssn + "\" as timestamp\n" +
		"Caused by: org.postgresql.util.PSQLException: ERROR: new row violates check constraint \"amount_positive\" Failing row contains (7, " + token + ", -1)."
	fc := &fakeConnect{
		statusFn: func(ctx context.Context, n string) (clients.ConnectorStatus, error) {
			return clients.ConnectorStatus{Name: "pg-orders-sink", Type: "sink", State: "RUNNING",
				Tasks: []clients.TaskStatus{{ID: 0, State: "FAILED", Trace: trace}}}, nil
		},
	}
	guardWrites(t, fc)
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Connect: fc}); err != nil {
		t.Fatal(err)
	}
	tool, _ := reg.Get("connect_status")
	r := runTool(t, tool, `{"connector":"pg-orders-sink"}`)
	data, err := json.Marshal(r.Data)
	if err != nil {
		t.Fatal(err)
	}
	for _, where := range []struct{ name, text string }{{"summary", r.Summary}, {"data", string(data)}} {
		for _, m := range []string{"alice.leak", card, ssn, token} {
			if strings.Contains(where.text, m) {
				t.Errorf("%s carries record value %q:\n%s", where.name, m, where.text)
			}
		}
	}
	// The diagnosis survives: exception classes and the constraint name.
	for _, want := range []string{"BatchUpdateException", "duplicate key value violates unique constraint", "JsonParseException",
		"invalid input syntax for type integer", "NumberFormatException: For input string", "Cannot parse value", "violates check constraint"} {
		if !strings.Contains(r.Summary, want) {
			t.Errorf("summary lost %q:\n%s", want, r.Summary)
		}
	}
}
