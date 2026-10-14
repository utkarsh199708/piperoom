// Package loadtest measures capacity and isolation of the room service.
//
// The harness (driven by cmd/loadtest) starts real server processes — the
// room service, HTTP API, WebSocket gateway and agent loop on Postgres,
// assembled as cmd/server assembles them — with a fake model endpoint and fake
// tools, then runs named profiles (fairness, throughput, replay, fanout,
// isolation) and writes JSON and markdown results. TestSmokeAllProfiles
// (build tag "integration") runs every profile at its CI size. See
// reviews/capacity-2026-10.md for the method, the commands and the numbers.
//
// The earlier in-process WebSocket fan-out test on the in-memory store
// (ws_load_test.go, build tag "load", reviews/load-test-2026-10.md) was
// removed: the fanout profile supersedes it on Postgres. It is in the history
// at b81248b.
package loadtest
