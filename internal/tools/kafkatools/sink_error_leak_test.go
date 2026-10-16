package kafkatools

import (
	"strings"
	"testing"
	"time"
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

// TestTraceSinkErrorsMaskMoreValues (#505): PostgreSQL date/time and
// malformed-literal errors and Elasticsearch date-parse errors quote the
// failing value after the error's category words. The category words are
// the diagnosis and must stay; the value is masked to the end of its line,
// since a value can hold any end marker (PR #528 review), so text after the
// value on that line is not asserted. PostgreSQL rows come
// quoted and unquoted, with DETAIL/HINT on the same line or on lines of their
// own, and inside a Connect trace; Elasticsearch rows come with and without
// "Preview of field's value" and with a nested caused_by reason.
func TestTraceSinkErrorsMaskMoreValues(t *testing.T) {
	const value = "Alice Leakwood 42"
	const pg = "Caused by: org.postgresql.util.PSQLException: ERROR: "
	const bulk = "Caused by: org.apache.kafka.connect.errors.ConnectException: Bulk request failed: "
	checkRows(t, []traceRow{
		// The three #505 forms, pinned as gaps by #501.
		{"postgres date/time field value out of range",
			pg + "date/time field value out of range: \"" + value + "\"",
			[]string{"Leakwood"}, []string{"PSQLException", "date/time field value out of range"}},
		{"postgres malformed array literal",
			pg + "malformed array literal: \"" + value + "\"",
			[]string{"Leakwood"}, []string{"PSQLException", "malformed array literal"}},
		{"elasticsearch failed to parse date field",
			bulk + "[{\"type\":\"illegal_argument_exception\",\"reason\":\"failed to parse date field [" + value + "] with format [strict_date_optional_time||epoch_millis]\"}]",
			[]string{"Leakwood"}, []string{"illegal_argument_exception", "failed to parse date field"}},

		// PostgreSQL variants.
		{"postgres date/time unquoted",
			pg + "date/time field value out of range: " + value,
			[]string{"Leakwood"}, []string{"PSQLException", "date/time field value out of range"}},
		{"postgres date/time with hint on the line",
			pg + "date/time field value out of range: \"" + value + "\"  Hint: Perhaps you need a different \"datestyle\" setting.",
			[]string{"Leakwood"}, []string{"date/time field value out of range"}},
		{"postgres date/time unquoted with hint on the line",
			pg + "date/time field value out of range: " + value + "  Hint: Perhaps you need a different \"datestyle\" setting.",
			[]string{"Leakwood"}, []string{"date/time field value out of range"}},
		{"postgres date/time hint on its own line",
			pg + "date/time field value out of range: \"" + value + "\"\n  Hint: Perhaps you need a different \"datestyle\" setting.\n  Position: 52",
			[]string{"Leakwood"}, []string{"date/time field value out of range"}},
		{"postgres date/time batch entry",
			"Caused by: java.sql.BatchUpdateException: Batch entry 0 INSERT INTO \"orders\" (\"id\",\"ts\") VALUES (7,'" + value + "') was aborted: ERROR: date/time field value out of range: \"" + value + "\"  Call getNextException to see other errors in the batch.",
			[]string{"Leakwood"}, []string{"BatchUpdateException", "date/time field value out of range"}},
		{"postgres interval field value out of range",
			pg + "interval field value out of range: \"" + value + "\"",
			[]string{"Leakwood"}, []string{"interval field value out of range"}},
		{"postgres timestamp out of range",
			pg + "timestamp out of range: \"" + value + "\"",
			[]string{"Leakwood"}, []string{"timestamp out of range"}},
		{"postgres date out of range",
			pg + "date out of range: \"" + value + "\"",
			[]string{"Leakwood"}, []string{"date out of range"}},
		{"postgres malformed array literal unquoted",
			pg + "malformed array literal: " + value,
			[]string{"Leakwood"}, []string{"malformed array literal"}},
		// "Detail:" sends the line through main's dlq_peek redaction, which
		// already masks the value (and the rest of the line).
		{"postgres malformed array literal with detail",
			pg + "malformed array literal: \"" + value + "\"  Detail: Array value must start with \"{\" or dimension information.",
			[]string{"Leakwood"}, []string{"malformed array literal"}},
		{"postgres malformed array literal detail on its own line",
			pg + "malformed array literal: \"" + value + "\"\n  Detail: Array value must start with \"{\" or dimension information.",
			[]string{"Leakwood"}, []string{"malformed array literal"}},
		{"postgres malformed record literal",
			pg + "malformed record literal: \"" + value + "\"",
			[]string{"Leakwood"}, []string{"malformed record literal"}},
		{"postgres malformed range literal",
			pg + "malformed range literal: \"" + value + "\"",
			[]string{"Leakwood"}, []string{"malformed range literal"}},
		{"postgres value with a quote inside",
			pg + "malformed array literal: \"Alice \"Leakwood\" 42\"",
			[]string{"Leakwood"}, []string{"malformed array literal"}},
		{"postgres top line",
			"org.apache.kafka.connect.errors.ConnectException: org.postgresql.util.PSQLException: ERROR: date/time field value out of range: \"" + value + "\"",
			[]string{"Leakwood"}, []string{"date/time field value out of range"}},

		// Elasticsearch variants.
		{"elasticsearch date field no format",
			bulk + "[{\"type\":\"illegal_argument_exception\",\"reason\":\"failed to parse date field [" + value + "]\"}]",
			[]string{"Leakwood"}, []string{"failed to parse date field"}},
		{"elasticsearch date field value with brackets",
			bulk + "[{\"type\":\"illegal_argument_exception\",\"reason\":\"failed to parse date field [Alice] Leakwood [42] with format [epoch_millis]\"}]",
			[]string{"Leakwood"}, []string{"failed to parse date field"}},
		{"elasticsearch exception form",
			"Caused by: org.elasticsearch.ElasticsearchException: Elasticsearch exception [type=illegal_argument_exception, reason=failed to parse date field [" + value + "] with format [strict_date_optional_time||epoch_millis]]",
			[]string{"Leakwood"}, []string{"ElasticsearchException", "failed to parse date field"}},
		{"elasticsearch nested caused_by reason",
			bulk + "[{\"type\":\"mapper_parsing_exception\",\"reason\":\"failed to parse field [created] of type [date] in document with id 'k1'\",\"caused_by\":{\"type\":\"illegal_argument_exception\",\"reason\":\"failed to parse date field [" + value + "] with format [strict_date_optional_time||epoch_millis]\",\"caused_by\":{\"type\":\"date_time_parse_exception\",\"reason\":\"Text '" + value + "' could not be parsed at index 0\"}}}]",
			[]string{"Leakwood"}, []string{"mapper_parsing_exception", "failed to parse field [created] of type [date]", "failed to parse date field"}},
		{"elasticsearch exception nested reason",
			"Caused by: org.elasticsearch.ElasticsearchException: Elasticsearch exception [type=illegal_argument_exception, reason=failed to parse date field [" + value + "] with format [epoch_millis]]; nested: Elasticsearch exception [type=date_time_parse_exception, reason=Text '" + value + "' could not be parsed at index 0]",
			[]string{"Leakwood"}, []string{"failed to parse date field"}},
		{"elasticsearch joda invalid format",
			bulk + "[{\"type\":\"illegal_argument_exception\",\"reason\":\"Invalid format: \\\"" + value + "\\\" is malformed at \\\"Leakwood 42\\\"\"}]",
			[]string{"Leakwood"}, []string{"illegal_argument_exception", "Invalid format:"}},
		// With a preview the line already gets main's dlq_peek redaction,
		// which masks the whole quoted reason; this pass cannot unmask it.
		{"elasticsearch date field with preview",
			bulk + "[{\"type\":\"mapper_parsing_exception\",\"reason\":\"failed to parse field [created] of type [date] in document with id 'k1'. Preview of field's value: '" + value + "'\",\"caused_by\":{\"type\":\"illegal_argument_exception\",\"reason\":\"failed to parse date field [" + value + "] with format [epoch_millis]\"}}]",
			[]string{"Leakwood"}, []string{"ConnectException: Bulk request failed"}},

		// PR #528 review: values that contain a field end or end marker.
		{"postgres date/time value containing hint",
			pg + "date/time field value out of range: \"Alice  Hint: Leakwood 42\"",
			[]string{"Leakwood"}, []string{"date/time field value out of range"}},
		{"postgres date/time value containing position",
			pg + "date/time field value out of range: \"Alice  Position: Leakwood 42\"",
			[]string{"Leakwood"}, []string{"date/time field value out of range"}},
		{"postgres date/time value containing where",
			pg + "date/time field value out of range: \"Alice  Where: Leakwood 42\"",
			[]string{"Leakwood"}, []string{"date/time field value out of range"}},
		{"postgres malformed array literal value containing hint",
			pg + "malformed array literal: \"Alice  Hint: Leakwood 42\"",
			[]string{"Leakwood"}, []string{"malformed array literal"}},
		{"postgres malformed array literal value containing position",
			pg + "malformed array literal: \"Alice  Position: Leakwood 42\"",
			[]string{"Leakwood"}, []string{"malformed array literal"}},
		{"postgres malformed array literal value containing where",
			pg + "malformed array literal: \"Alice  Where: Leakwood 42\"",
			[]string{"Leakwood"}, []string{"malformed array literal"}},
		{"elasticsearch date value containing the format marker",
			"Caused by: org.elasticsearch.ElasticsearchException: Elasticsearch exception [type=illegal_argument_exception, reason=failed to parse date field [Alice] with format [Leakwood] with format [epoch_millis]]",
			[]string{"Leakwood"}, []string{"failed to parse date field"}},
		{"java.time value containing the parse marker",
			"Caused by: java.time.format.DateTimeParseException: Text 'Alice' could not be parsed Leakwood' could not be parsed at index 0",
			[]string{"Leakwood"}, []string{"DateTimeParseException", "Text '"}},
		{"elasticsearch date field then nested reason",
			"Caused by: org.elasticsearch.ElasticsearchException: Elasticsearch exception [type=illegal_argument_exception, reason=failed to parse date field [" + value + "]]; nested: Elasticsearch exception [type=date_time_parse_exception, reason=Text '" + value + "' could not be parsed at index 0]",
			[]string{"Leakwood"}, []string{"ElasticsearchException", "failed to parse date field"}},
		// PR #528 review round 2: an earlier, unrelated "text '" must not
		// open the mask.
		{"java.time after an unrelated text quote",
			"ConnectException: Plain text 'mode' unsupported; Text 'Leakwood' could not be parsed at index 0",
			[]string{"Leakwood"}, []string{"Plain text 'mode' unsupported;", "could not be parsed at index 0"}},
	})
}

// TestTraceSinkValueMaskKeepsCategories: lines with the #505 category words
// but no value to mask keep their text.
func TestTraceSinkValueMaskKeepsCategories(t *testing.T) {
	for _, line := range []string{
		"failed to parse field [created] of type [date] in document with id 'k1'",
		"date/time field value out of range",
		"Schema being registered is incompatible with an earlier schema for subject \"orders-value\"",
		// PR #528 review: "Invalid format" and "is malformed at" outside
		// Joda's quoted form are diagnoses main keeps whole.
		"Caused by: org.apache.kafka.common.errors.SerializationException: Invalid format: magic byte 0x07 unknown",
		"DataException: Invalid format: expected Avro, got JSON",
		"SerializationException: Invalid format: magic byte 0x7b  Hint: check the producer's serializer",
		"JSON is malformed at line 3 column 5",
	} {
		if got := sanitizeTrace(line); got != line {
			t.Errorf("sanitizeTrace(%q) = %q, want unchanged", line, got)
		}
	}
}

// TestTraceSinkValueMaskHostile: maskSinkValues on a 64 KiB line of repeated
// openers stays linear, and a value after the last opener is still masked.
// The budget is on this pass; TestSanitizeTraceHostileSpeed times
// sanitizeTrace as a whole (#533).
func TestTraceSinkValueMaskHostile(t *testing.T) {
	for _, tt := range []struct {
		unit   string
		opener bool // the unit ends in an opener, so the trailing "Leak" is a value
	}{
		{"malformed array literal: \"", true},
		{"malformed array literal: \\\"", true},
		{"\"x\" date/time field value out of range: ", true},
		{"date/time field value out of range: ", true},
		{"failed to parse date field [", true},
		{"] failed to parse date field [", true},
		{"Invalid format: \"x\" is malformed at ", true},
		{"failed to parse date field [x] with format [", false},
		{"Text '", false},
		{"Text 'x' could not be parsed ", false},
		{"malformed array literal: \"x\"  Hint: ", false},
		{"\"Text 'x\", ", false},
		{"plain text 'a' Text '", false},
	} {
		line := "Caused by: x: " + strings.Repeat(tt.unit, (64<<10)/len(tt.unit)) + "Leak"
		start := time.Now()
		got := maskSinkValues(line)
		if d := time.Since(start); d > 50*time.Millisecond {
			t.Errorf("%q x 64 KiB took %v, want < 50ms", tt.unit, d)
		}
		if tt.opener && strings.Contains(got, "Leak") {
			t.Errorf("%q: the value after the last opener is not masked: ...%s", tt.unit, got[max(0, len(got)-80):])
		}
	}
}

// TestTraceSinkErrorsKnownGaps pins sink wordings that still carry part of
// the record, on main and here: they have another shape than the #505 forms
// (the value comes before the words, or is a key or raw bytes): known gaps,
// follow-up #527. When one is masked, move its row to a must-mask test.
func TestTraceSinkErrorsKnownGaps(t *testing.T) {
	for _, tt := range []struct{ name, line, leak string }{
		{"postgres to_date invalid value",
			"Caused by: org.postgresql.util.PSQLException: ERROR: invalid value \"Leakwood\" for \"YYYY\"",
			"Leakwood"},
		{"postgres invalid byte sequence",
			"Caused by: org.postgresql.util.PSQLException: ERROR: invalid byte sequence for encoding \"UTF8\": 0x4c 0x65",
			"0x4c 0x65"},
		// The newline is the mask's only terminator, and condenseTrace keeps a
		// "Caused by:" line, so a quoted value holding a newline and
		// "Caused by:" keeps its tail (PR #528 review round 2).
		{"postgres value with a newline and caused by",
			"Caused by: org.postgresql.util.PSQLException: ERROR: malformed array literal: \"Alice\nCaused by: Leakwood 42\"",
			"Leakwood"},
		{"elasticsearch document id without preview",
			"Caused by: org.apache.kafka.connect.errors.ConnectException: Bulk request failed: [{\"type\":\"mapper_parsing_exception\",\"reason\":\"failed to parse field [age] of type [long] in document with id 'Leakwood-42'\"}]",
			"Leakwood-42"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			for path, got := range bothPaths(t, tt.line) {
				for i, where := range []string{"summary", "data error"} {
					if !strings.Contains(got[i], tt.leak) {
						t.Errorf("%s path, %s no longer carries %q: move the row to a must-mask test:\n%s", path, where, tt.leak, got[i])
					}
				}
			}
		})
	}
}
