// Package oracle is the Oracle prerequisite driver of the setup wave
// (docs/design/setup-agent.md §6; U9b): the ordered reads that say whether a
// source Oracle database can serve the Debezium Oracle connector over
// LogMiner, the fix each failure needs, the ladder's canary table, and the
// connector config with ${secret:} references.
//
// It follows the Postgres driver (internal/setup/sources/postgres, U1-9): a
// driver is data plus checks, never a code path (design §1). Each Check names
// the read that answers it, the condition that passes, the operation that
// fixes a failure (a real runner op) or the documented ask a person carries
// out when no op can, and the tier floor. The room routes a fix through its
// proposal path, so the driver never runs a write itself: it holds the U9a
// client's read-only Reader only.
//
// Oracle differs from Postgres where the driver says so: archive log mode is
// switched only on a MOUNTed instance (a restart), so no runner op can do it
// and the check is an ask, T3 inside a maintenance window on any database this
// room did not create; database-level supplemental logging and the LogMiner
// grants of an existing user are asks too (the setup writer only creates); in
// a CDB the connector logs in as a common user (C##) and names its PDB in
// database.pdb.name. The driver covers the free editions (Oracle Database Free
// 23ai/26ai, Express Edition) and the 19c and 21c releases the Debezium docs
// test; on any other edition or release the edition check fails and says so.
//
// Writer wraps another clients.SetupSourceWriter (the runner's remote client,
// or the U9a client's executing Writer) and refuses identifiers, canary nonces
// and ALTER SYSTEM names outside this package's rules before any I/O.
//
// Every identifier is checked against ^[A-Za-z_][A-Za-z0-9_$#]*$ (the U9a
// client's widening of AGENTS.md §4 rule 6 for Oracle names such as
// C##DBZUSER), must be upper case as the dictionary stores it, and is quoted
// with double quotes before it reaches any statement text this package
// builds; no statement is built from model text.
package oracle
