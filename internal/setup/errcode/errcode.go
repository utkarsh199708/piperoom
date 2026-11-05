// Package errcode holds the error codes a failed runner op may carry in its
// result's Data ("error_code"). It is a leaf: it imports nothing, so every
// setup package (remoteclients, the setup tools and actions) and the packages
// that build them (internal/agent, internal/wiring) can share one set of
// values without importing internal/room or each other.
//
// The runner's OpResult has no reason field yet, so the runner catalogue puts
// one of these in Data for the failures a caller branches on.
// internal/setup/protocol is a contract file and is not edited here; if the
// leads add a reason field to protocol.OpResult, these values move there.
package errcode

// Error codes of a failed op's Data. remoteclients re-exports each one under
// its ErrorCode* name, so both spell the same value.
const (
	NotFound            = "not_found"
	NotSupported        = "not_supported"
	ConnectorExists     = "connector_exists"
	ConnectorNotStopped = "connector_not_stopped"
	Rebalancing         = "rebalancing"
	SessionChanged      = "session_changed"
	SessionProtected    = "session_protected"
	OutOfScope          = "out_of_scope"
)
