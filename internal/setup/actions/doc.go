// Package setupactions holds the setup-only actions (docs/design/setup-agent.md
// §2.5, §3.1; row U1-7): one Adapter per typed runner op, forwarded to the
// room's paired runner, plus the four optional interfaces the room uses on a
// setup step.
//
// An Adapter is built from a Spec (the op name, its tier floor, layer,
// description, bounded params schema, the scope rule and the T0 preview op)
// and implements:
//
//   - actions.Action: Validate checks the params' shape; Execute sends the
//     frozen proposal step's op and params to the runner and returns its
//     Summary. It refuses a call with no proposal step in the context, an op
//     or params that are not the step's, and a target outside the room's
//     provisional pipeline, all before anything is sent.
//   - actions.Scoped: CheckScope applies the spec's scope rule to params and
//     the pipeline, with no I/O: a named resource that is not the pipeline's
//     (a table outside source_tables, a connector that is not the pipeline's)
//     is refused with tools.ErrOutOfScope.
//   - actions.Preview: one T0 request to the spec's preview op, never the
//     write op and never a second op. The runner renders what Execute would do
//     without changing anything.
//   - actions.Tiered: plan.TierOf of the op's floor and the class the room
//     resolved; no I/O.
//   - actions.Receipted: ExecuteReceipt is Execute with the runner's own
//     events.SetupReceipt returned unchanged; this package never synthesizes
//     one.
//
// The write is the proposal step's write. The room puts room.SetupStep into
// the call's context (internal/room/setup_route.go); the adapter sends exactly
// its action, params and proposal fields, so the runner's checkStep can bind
// the request to the approval it came from.
package setupactions
