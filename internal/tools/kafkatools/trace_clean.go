package kafkatools

// CleanTaskTrace is the form of a Connect connector or task trace that may
// leave this process outside a tool result: condensed to the top exception
// and its "Caused by:" lines, Avro schema bodies stripped, credentials and
// the failing record's quoted values masked, and capped at 2000 bytes. It is
// what connect_status shows; actions use it for a stage's failed-task detail
// (X8b) so the two never disagree on what a trace may carry.
func CleanTaskTrace(trace string) string {
	return cleanTrace(condenseTrace(trace))
}
