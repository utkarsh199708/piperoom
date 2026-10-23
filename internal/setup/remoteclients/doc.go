// Package remoteclients implements the client interfaces the Q tools and Q
// actions take (clients.ConnectClient, clients.KafkaReader,
// clients.SchemaRegistryReader, clients.SourceDB,
// postgres.SourceSettingsReader, postgres.SourceAdminWriter,
// clients.SetupSourceWriter and the optional connect readers) over the setup
// runner's typed operation protocol (docs/design/setup-agent.md §2.2, §3.2):
// every client method sends exactly one protocol.OpRequest to the runner paired
// to the room and decodes its OpResult. The Q tools and Q actions are
// constructed over these clients unchanged, so they keep their own names,
// validations, previews and scope checks in a setup room.
//
// # One op per method
//
// A read never forwards its arguments as an opaque blob: it builds the op's
// params from a typed struct whose fields are the method's own arguments (a
// connector name, a table, a role, ...), and it decodes a named field of the
// result. The op names, tiers and target kinds are in ops.go (OpSpec); the
// params and result shapes are documented on each method and pinned by
// methods_test.go (TestOneOpPerMethod, TestWireParams), so the runner
// catalogue and this package cannot drift apart silently.
//
// # Tiers and proposal fields
//
// A read is a T0 op and carries no proposal fields. A write is a T2 op (T3
// for SetupSourceWriter.AlterSystem, which the contract calls out) and is only
// built from a proposal step the room put in ctx with room.WithSetupStep:
// ProposalID, StepIndex, ParamsHash and Class are copied into the OpRequest,
// and Tier is raised to the step's tier. Without a step a write method refuses
// with ErrNoProposal before building a request; nothing is sent.
//
// # A write is its proposal step
//
// The runner binds every T1+ request to the frozen step it names
// (runner.checkStep): the op must execute the step's action and the params
// must be canonically the step's params. So a write sends the step's action as
// its op and the step's frozen params (room.SetupStep.Action and Params) byte
// for byte; the method's arguments are only checked against those params
// (ErrStepMismatch when they differ, or when the step runs another action),
// never used to rebuild them. The runner's check is the boundary; this one
// makes a mismatched write fail before it is sent, with a reason.
//
// A step runs at most once: the runner records it as started before it runs
// it and refuses it again as duplicate_step. A Q action's in-step retry (a
// create or delete retried after a Connect rebalance or a timeout) therefore
// never reaches the runner a second time: the session refuses the second
// write of a step with ErrStepSpent, which wraps the first attempt's error, so
// the action's retry loop ends on that error (its landed-detection reads still
// run). Retrying a write is a new proposal step, approved again; retrying
// inside one op, next to Connect, is the runner op's job.
//
// The runner recomputes ParamsHash from the frozen proposal it was shown with
// the first request of a proposal (protocol.OpRequest.Proposal). room.SetupStep
// does not carry that payload, so the setup action adapter that fills the
// context must add it with WithProposal for the first write of a proposal;
// without it the runner refuses the request with params_hash (a gap recorded in
// the PR, not worked around here).
//
// # Cut and redacted results
//
// A result the runner cut to fit the protocol (OpResult.Truncated) or whose
// Data its literal redaction changed (OpResult.Redacted) is refused with
// ErrTruncated or ErrRedacted instead of being decoded: a cut list is not the
// whole answer, and a ${secret:NAME} the runner put in place of a stored value
// is not the endpoint's value, so a caller comparing it (a config read back
// after a create) or judging it (a plaintext-credential check before a delete)
// would be misled.
//
// # No runner, no I/O
//
// A client built with a nil Executor, or called with a ctx that carries no room
// id (room.RoomID), answers ErrNoRunner before it builds a request. Every op
// that reaches the room's runner gets a deadline (10 s for a read, 30 s for a
// T2 write, 5 min for a T3 write so a person can confirm on the runner's
// terminal; the room bounds a T3 step for it), and responses are capped at
// protocol.MaxDataBytes before they are decoded. Nothing here logs: an op result is data for the caller, and the
// runner already redacted it.
//
// # Contract gaps
//
//   - postgres.SourceSettingsReader, postgres.SourceAdminWriter,
//     connect.ConnectLifecycleWriter, connect.SourceOffsetReader and
//     kafkatools.ConfigValidator are declared in those packages rather than in
//     internal/clients/clients.go (the Q0 gap); this package implements those
//     declarations unchanged. clients.SetupSourceWriter is in the contract.
//     package sourceactions aliases the Q2a DTOs (TableReplication, RoleInfo,
//     DBSession), so the one Source client serves the Q2 tools and actions
//     directly.
//   - a failed op result has no structured reason field. When the runner puts
//     an error_code in the result's Data (see ErrorCode*), the matching
//     sentinel (clients.ErrNotFound, connect.ErrConnectorExists, ...) is
//     wrapped so the Q actions branch as they do on a real client; without one
//     the summary is the error text. The PR asks for the code in
//     protocol.OpResult.
//   - the setup-only writes (CreatePublication, CreateCanaryTable,
//     InsertCanary, CreateReadRole, AlterSystem) have no registered action
//     yet: they check their arguments against the step params under the
//     design's §2.5 names (publication, tables, table, nonce, role, name,
//     value), so an action that names them differently is refused with
//     ErrStepMismatch until the two agree.
//   - the runner catalogue (internal/setup/ops) does not ship the Q op entries
//     yet: this package sends the design's §2.5 names and params, and the ops
//     that answer them land with the catalogue's Q work. Until then a real
//     runner refuses each as unknown_op; the unit tests use a fake runner.
package remoteclients
