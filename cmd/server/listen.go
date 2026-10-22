package main

import (
	"errors"
	"fmt"
	"net"

	"piperoom/internal/config"
	"piperoom/internal/quickstart"
)

// errNonLoopback reports a listen address that another host can reach. In dev
// auth mode anyone who can reach the port can act as any user, so the server
// refuses such an address unless the operator sets
// PIPEROOM_DEV_ALLOW_NONLOOPBACK=1 (or the deprecated --allow-remote flag).
var errNonLoopback = errors.New("listen address is not loopback")

// checkListenAddr decides whether the server may listen on addr in dev auth
// mode. A loopback address (127.0.0.1, ::1 or localhost) is always allowed. Any
// other address needs allowRemote; the returned remote flag is then true so the
// caller can warn loudly that dev auth is reachable.
func checkListenAddr(addr string, allowRemote bool) (remote bool, err error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false, fmt.Errorf("server: parse listen address %q: %w", addr, err)
	}
	if isLoopbackHost(host) {
		return false, nil
	}
	if !allowRemote {
		return false, fmt.Errorf("server: refusing to listen on %q in dev auth mode (anyone who can reach it can act as any user): set PIPEROOM_DEV_ALLOW_NONLOOPBACK=1 and PIPEROOM_DEV_ACK="+config.DevAckValue+" to allow it (--demo needs only the first), or PIPEROOM_AUTH_MODE=oidc: %w", addr, errNonLoopback)
	}
	return true, nil
}

// isLoopbackHost reports whether host names the loopback interface. An empty
// host (":8090") listens on every interface, so it is not loopback. The rule
// itself lives with the quickstart, which refuses a non-loopback address
// whatever the dev switches say.
func isLoopbackHost(host string) bool {
	return quickstart.IsLoopbackHost(host)
}
