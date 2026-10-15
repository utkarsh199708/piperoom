package kafkatools

import "testing"

// TestTraceSinkErrorsMaskRecordValues (#382): MySQL, PostgreSQL and
// Elasticsearch sink errors quote the failing value in wordings that are not
// JDBC tuples or "Detail:" lines. The value must reach neither Summary nor
// Data; the error wording, column and type stay. Elasticsearch quotes the
// whole reason, so there only the exception class survives (an over-mask
// org.md §12 allows; it cannot leak).
func TestTraceSinkErrorsMaskRecordValues(t *testing.T) {
	const value = "Alice Leakwood 42" // no e-mail shape: redact.Text alone does not mask it
	checkRows(t, []traceRow{
		{"mysql duplicate entry",
			"Caused by: java.sql.SQLIntegrityConstraintViolationException: Duplicate entry '" + value + "' for key 'users.email'",
			[]string{"Leakwood"}, []string{"SQLIntegrityConstraintViolationException", "Duplicate entry"}},
		{"mysql incorrect string value",
			"Caused by: java.sql.SQLException: Incorrect string value: '" + value + "' for column 'age' at row 1",
			[]string{"Leakwood"}, []string{"java.sql.SQLException", "Incorrect string value"}},
		{"mysql incorrect integer value",
			"Caused by: java.sql.SQLException: Incorrect integer value: '" + value + "' for column 'age' at row 1",
			[]string{"Leakwood"}, []string{"Incorrect integer value"}},
		{"postgres out of range",
			"Caused by: org.postgresql.util.PSQLException: ERROR: value \"" + value + "\" is out of range for type integer",
			[]string{"Leakwood"}, []string{"PSQLException", "is out of range for type integer"}},
		{"postgres invalid enum",
			"Caused by: org.postgresql.util.PSQLException: ERROR: invalid input value for enum status: \"" + value + "\"",
			[]string{"Leakwood"}, []string{"PSQLException", "invalid input value for enum status"}},
		{"elasticsearch preview of field",
			"Caused by: org.apache.kafka.connect.errors.ConnectException: Bulk request failed: [{\"type\":\"mapper_parsing_exception\",\"reason\":\"failed to parse field [age] of type [long] in document with id 'k1'. Preview of field's value: '" + value + "'\"}]",
			[]string{"Leakwood"}, []string{"ConnectException: Bulk request failed"}},
		{"elasticsearch exception preview of field",
			"Caused by: org.elasticsearch.ElasticsearchException: Elasticsearch exception [type=mapper_parsing_exception, reason=failed to parse field [age] of type [long] in document with id 'k1'. Preview of field's value: '" + value + "']",
			[]string{"Leakwood"}, []string{"ElasticsearchException"}},
	})
}
