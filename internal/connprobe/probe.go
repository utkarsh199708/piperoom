// Package connprobe tests a configured connection the way Piperoom will use
// it: it builds a throwaway client from the connection's settings, checks
// reachability, login and every permission Piperoom needs, and says how to fix
// what is missing (docs/design/onboarding.md §6.4). It only reads. A result
// never carries a secret, a raw server error body or a record value: check
// ids, statuses, timings and fixed problem/fix text with object names only.
package connprobe

import (
	"context"
	"time"

	"piperoom/internal/config"
	"piperoom/internal/owners"
)

// Kind is the kind of system a connection reaches.
type Kind string

// Connection kinds (docs/api.md, "Admin: settings and connection tests").
const (
	KindSourceDB       Kind = "source_db"
	KindWarehouse      Kind = "warehouse"
	KindKafka          Kind = "kafka"
	KindConnect        Kind = "connect"
	KindSchemaRegistry Kind = "schema_registry"
)

// Kinds lists every kind in display order.
var Kinds = []Kind{KindSourceDB, KindKafka, KindConnect, KindSchemaRegistry, KindWarehouse}

// Check statuses.
const (
	StatusOK   = "ok"   // the check passed
	StatusFail = "fail" // Piperoom cannot work like this; Fix says what to do
	StatusWarn = "warn" // works, but something should change (e.g. more rights than needed)
	StatusSkip = "skip" // not run: an earlier check failed, or nothing to check
)

// Check is one step of a probe.
type Check struct {
	// ID is a stable, lower-case id such as "reach", "login", "read_slots".
	ID     string `json:"id"`
	Status string `json:"status"`
	// MS is how long the step took, 0 when skipped.
	MS int64 `json:"ms"`
	// Problem says what is wrong in one sentence, Fix says how to fix it
	// (one SQL line, one ACL command, one setting). Both are fixed text with
	// object names only; never a value from the server's error.
	Problem string `json:"problem,omitempty"`
	Fix     string `json:"fix,omitempty"`
	// Detail is a short, non-secret fact for an ok check, e.g.
	// "default_transaction_read_only = on" or "3 brokers".
	Detail string `json:"detail,omitempty"`
}

// Result is a whole probe.
type Result struct {
	Kind Kind `json:"kind"`
	// OK is true when no check failed (warnings allowed).
	OK     bool    `json:"ok"`
	TookMS int64   `json:"took_ms"`
	Checks []Check `json:"checks"`
}

// Target is what to probe: the settings of one connection and the pipelines
// that use it (whose tables, topics, groups and subjects the permission checks
// read). Config holds resolved values; only the fields of Kind matter.
type Target struct {
	Kind      Kind
	Config    config.Config
	Pipelines []owners.Pipeline
}

// DefaultStepTimeout bounds each network step of a probe.
const DefaultStepTimeout = 10 * time.Second

// probeFunc runs one kind of probe. Each kind lives in its own file.
type probeFunc func(ctx context.Context, t Target, g *Guard) Result

// probes maps a kind to its probe. A fixed table, not a registry.
var probes = map[Kind]probeFunc{
	KindSourceDB:       probeSourceDB,
	KindWarehouse:      probeWarehouse,
	KindKafka:          probeKafka,
	KindConnect:        probeConnect,
	KindSchemaRegistry: probeSchemaRegistry,
}

// Run probes t. Every dial goes through g, which refuses metadata and
// link-local addresses (and loopback unless allowed). An unknown kind is one
// failed check, not a panic.
func Run(ctx context.Context, t Target, g *Guard) Result {
	start := time.Now()
	p, ok := probes[t.Kind]
	if !ok {
		r := Result{Kind: t.Kind, Checks: []Check{{ID: "kind", Status: StatusFail, Problem: "unknown connection kind"}}}
		r.TookMS = time.Since(start).Milliseconds()
		return r
	}
	if g == nil {
		g = &Guard{}
	}
	r := p(ctx, t, g)
	r.Kind = t.Kind
	r.OK = true
	for _, c := range r.Checks {
		if c.Status == StatusFail {
			r.OK = false
		}
	}
	if r.Checks == nil {
		r.Checks = []Check{}
	}
	r.TookMS = time.Since(start).Milliseconds()
	return r
}

// step times fn and fills the check's MS. fn returns the finished check.
func step(fn func() Check) Check {
	start := time.Now()
	c := fn()
	c.MS = time.Since(start).Milliseconds()
	return c
}

// skipped returns a skip check for id, used after an earlier step failed.
func skipped(id, why string) Check {
	return Check{ID: id, Status: StatusSkip, Detail: why}
}
