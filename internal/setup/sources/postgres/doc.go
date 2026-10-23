// Package postgres is the Postgres prerequisite driver of the setup wave
// (docs/design/setup-agent.md §6; U1-9): the ordered reads that say whether a
// source Postgres can serve a Debezium connector, the setup-only writes that
// make it able to (a filtered publication of the pipeline's tables, the
// ladder's canary table, a read role with the permissions.md grants, and
// ALTER SYSTEM on the plan's allow-list), and the connector config with
// ${secret:} references.
//
// A driver is data plus checks, never a code path (design §1): each Check
// names the read that answers it, the condition that passes, the operation
// that fixes a failure, and that operation's tier floor. The room routes the
// fix through its proposal path, so the driver never runs a write itself: it
// holds read-only readers only.
//
// Two types implement clients.SetupSourceWriter: SetupWriter executes the
// writes on a write pool of its own (the runner's local implementation), and
// Writer validates every argument before handing the call to another
// SetupSourceWriter (the runner's remote client in a setup room, or a
// SetupWriter). Both refuse identifiers, ALTER SYSTEM names and values, and
// canary nonces outside this package's rules before any I/O.
//
// Every identifier is checked against ^[a-zA-Z_][a-zA-Z0-9_]*$ (and the 63-byte
// name limit) and quoted with pgx.Identifier.Sanitize before it reaches SQL
// (AGENTS.md §4 rule 6). Every statement here is fixed text plus quoted
// identifiers and $n parameters; no statement is built from model text.
package postgres
