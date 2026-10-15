package kafkatools

import (
	"strings"
	"testing"
)

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

// TestTraceSinkErrorsKnownGaps pins sink wordings that still carry the record
// value, on main and here: known gaps, not in #382's list, follow-up #505.
// When #505 masks one, move its row to TestTraceSinkErrorsMaskRecordValues.
func TestTraceSinkErrorsKnownGaps(t *testing.T) {
	const value = "Alice Leakwood 42"
	for _, tt := range []struct{ name, line string }{
		{"postgres date/time field value out of range",
			"Caused by: org.postgresql.util.PSQLException: ERROR: date/time field value out of range: \"" + value + "\""},
		{"postgres malformed array literal",
			"Caused by: org.postgresql.util.PSQLException: ERROR: malformed array literal: \"" + value + "\""},
		{"elasticsearch failed to parse date field",
			"Caused by: org.apache.kafka.connect.errors.ConnectException: Bulk request failed: [{\"type\":\"illegal_argument_exception\",\"reason\":\"failed to parse date field [" + value + "] with format [strict_date_optional_time||epoch_millis]\"}]"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for path, got := range bothPaths(t, tt.line) {
				for i, where := range []string{"summary", "data error"} {
					if !strings.Contains(got[i], "Leakwood") {
						t.Errorf("%s path, %s no longer carries the value: #505 fixed it? move the row to the must-mask test:\n%s", path, where, got[i])
					}
				}
			}
		})
	}
}
