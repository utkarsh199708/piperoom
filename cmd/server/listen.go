package main

import (
	"errors"
	"fmt"
	"net"
	"strings"
)

// errNonLoopback reports a listen address that another host can reach. The
// server has no real login yet (X-User is a dev stub), so it refuses such an
// address unless the operator passes --allow-remote.
var errNonLoopback = errors.New("listen address is not loopback")

// checkListenAddr decides whether the server may listen on addr. A loopback
// address (127.0.0.1, ::1 or localhost) is always allowed. Any other address
// needs allowRemote; the returned remote flag is then true so the caller can
// warn that auth is the dev stub.
func checkListenAddr(addr string, allowRemote bool) (remote bool, err error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false, fmt.Errorf("server: parse listen address %q: %w", addr, err)
	}
	if isLoopbackHost(host) {
		return false, nil
	}
	if !allowRemote {
		return false, fmt.Errorf("server: refusing to listen on %q without --allow-remote (auth is the dev X-User stub): %w", addr, errNonLoopback)
	}
	return true, nil
}

// isLoopbackHost reports whether host names the loopback interface. An empty
// host (":8090") listens on every interface, so it is not loopback.
func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
