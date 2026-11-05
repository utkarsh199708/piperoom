// Package mongodb is the MongoDB prerequisite driver of the setup wave
// (docs/design/setup-agent.md §6; U7b), the counterpart of
// internal/setup/sources/postgres and internal/setup/sources/sqlserver: the
// ordered reads that say whether a source MongoDB can serve the Debezium
// MongoDB connector (change streams, which need a replica set member or a
// mongos; the capture user; its oplog read; its read on every captured
// collection), the validating wrapper around the setup-only writes (the
// ladder's canary collection and the capture user), and the connector config
// with mongodb.connection.string as a ${secret:} reference.
//
// A driver is data plus checks, never a code path (design §1): each Check
// names the read that answers it, the condition that passes, the operation
// that fixes a failure (or the documented ask when no op can), and that
// operation's tier floor. The room routes the fix through its proposal path,
// so the driver never runs a write itself: it holds read-only readers only.
//
// A standalone mongod has no change streams. Its fix is an op (T1, compose_up
// of the room's own rendered stack, which runs mongod with --replSet) only when
// this room created that mongod, which the server knows by a setup receipt and
// sets in Driver.SourceCreatedByRoom; for anyone else's mongod it is a
// documented ask, since no setup op restarts a database it did not create.
//
// Every database, collection and user name is checked against
// ^[a-zA-Z_][a-zA-Z0-9_]*$ (at most 64 bytes, the MongoDB client's rule) before
// it reaches a reader, a writer or a message (AGENTS.md §4 rule 6), and the
// system databases admin, local and config are never captured or written.
package mongodb
