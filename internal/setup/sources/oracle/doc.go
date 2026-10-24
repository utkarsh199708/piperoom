// Package oracle implements the Oracle setup source driver: read-only
// prerequisite checks, validated delegates to clients.SetupSourceWriter, and
// the Debezium Oracle connector configuration.
//
// Oracle's writer cannot enable ARCHIVELOG on an open instance, and the
// current setup writer contract has no operation for database-level minimal
// supplemental logging. Those checks therefore give the exact DBA action and
// tier instead of advertising a write that the runner cannot safely perform.
package oracle
