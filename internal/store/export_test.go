package store

import "time"

// Test hooks for the external package tests (pg_integration_test.go). They live
// in a _test.go file so none of this is part of the production API.

// ListenerPID is the backend pid of the shared LISTEN connection, 0 while it is down.
func (p *PG) ListenerPID() uint32 { return p.hub.pid.Load() }

// SetTimings overrides the subscriber safety poll and the slow-consumer limit.
// Call it before the first Subscribe.
func (p *PG) SetTimings(poll, slowAfter time.Duration) {
	p.poll = poll
	p.slowAfter = slowAfter
}

// SetListenKeepalive overrides how long the hub waits before it health-checks a
// quiet LISTEN connection. Call it before the first Subscribe.
func (p *PG) SetListenKeepalive(d time.Duration) { p.hub.keepalive = d }
