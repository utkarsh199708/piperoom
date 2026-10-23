package nettools

import (
	"context"
	"crypto/tls"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"piperoom/internal/clients"
)

// maxListeners caps the brokers whose advertised listeners one probe reports.
const maxListeners = 12

// listenerChecks reads the brokers' advertised listeners from cluster metadata
// and probes each one that is not also a configured bootstrap address. The
// metadata host:port is what a client is told to use after the bootstrap
// connection, so an address that works only inside the cluster strands every
// client that gets that far: the check reports it, and the probe of the
// advertised address names dns or tcp as the failing step.
func (t *netProbe) listenerChecks(ctx context.Context, endpointChecks []Check, tgt Target) []Check {
	skip := func(why string) []Check {
		return []Check{{ID: "listeners", Status: StatusSkip, Detail: why}}
	}
	if t.d.Kafka == nil {
		return skip("no Kafka client")
	}
	cr, ok := t.d.Kafka.(clients.ClusterReader)
	if !ok {
		return skip("the Kafka client cannot read cluster metadata")
	}
	if !allTCPOK(endpointChecks) {
		// Metadata cannot be read through a broker this probe could not reach;
		// the tcp check already names the failure.
		return skip("the configured brokers were not reachable")
	}
	cctx, cancel := context.WithTimeout(ctx, stepOr(t.d.StepTimeout))
	defer cancel()
	start := time.Now()
	health, err := cr.ClusterHealth(cctx)
	if err != nil {
		c := Check{ID: "listeners", Status: StatusFail, MS: time.Since(start).Milliseconds(),
			Problem: "cannot read the brokers' metadata: " + redactedText(err.Error()),
			Fix:     "check that the probe location may reach a broker and read cluster metadata"}
		if cctx.Err() != nil {
			c.Problem = "reading the brokers' metadata did not finish within the step's time"
		}
		return []Check{c}
	}
	if len(health.Brokers) == 0 {
		return []Check{{ID: "listeners", Status: StatusFail, MS: time.Since(start).Milliseconds(),
			Problem: "the metadata response lists no brokers",
			Fix:     "check the cluster's broker registration and the addresses it advertises"}}
	}
	configured := make([]string, 0, len(tgt.Endpoints))
	for _, e := range tgt.Endpoints {
		configured = append(configured, e.Addr)
	}
	inConfigured := func(addr string) bool {
		for _, a := range configured {
			if strings.EqualFold(a, addr) {
				return true
			}
		}
		return false
	}
	// The advertised addresses are probed with the configured security
	// protocol: a client that bootstraps with TLS keeps using TLS on them.
	tlsOn, tlsCfg := endpointTLS(tgt)

	brokers := append([]clients.BrokerHealth(nil), health.Brokers...)
	sort.Slice(brokers, func(i, j int) bool { return brokers[i].NodeID < brokers[j].NodeID })
	shown := brokers
	if len(shown) > maxListeners {
		shown = shown[:maxListeners]
	}
	prober := Prober{Loc: t.d.Location, StepTimeout: t.d.StepTimeout}
	var out []Check
	var matched, mismatched, unreachable []string
	for _, b := range shown {
		addr := canonicalAddr(b.Host, strconv.Itoa(int(b.Port)))
		if addr == "" {
			mismatched = append(mismatched, fmt.Sprintf("node %d advertises an unusable address", b.NodeID))
			unreachable = append(unreachable, fmt.Sprintf("node %d", b.NodeID))
			continue
		}
		if inConfigured(addr) {
			matched = append(matched, addr)
			continue
		}
		mismatched = append(mismatched, fmt.Sprintf("node %d advertises %s, which is not a configured broker address", b.NodeID, addr))
		checks := prober.Probe(ctx, Target{Label: tgt.Label, Kind: tgt.Kind, Endpoints: []Endpoint{{Addr: addr, TLS: tlsOn, Config: tlsCfg}}})
		out = append(out, checks...)
		// A name that does not resolve here is as unreachable as a refused
		// port: it is the typical listener that exists only inside the
		// cluster (a pod's headless-service name), and its tcp step is then a
		// skip, not a failure.
		reachable := true
		for _, c := range checks {
			if (c.ID == "tcp" || c.ID == "dns") && c.failed() {
				reachable = false
			}
		}
		if !reachable {
			unreachable = append(unreachable, addr)
		}
	}
	detail := fmt.Sprintf("%d brokers", len(brokers))
	if len(brokers) > len(shown) {
		detail = fmt.Sprintf("%d of %d brokers probed", len(shown), len(brokers))
	}
	if len(matched) > 0 {
		detail += ", advertised " + strings.Join(matched, ", ")
	}
	c := Check{ID: "listeners", Status: StatusOK, MS: time.Since(start).Milliseconds(), Detail: detail}
	switch {
	case len(mismatched) == 0:
		return append(out, c)
	case len(unreachable) > 0:
		c.Status = StatusFail
		c.Problem = strings.Join(mismatched, "; ") + "; the address is not reachable from here"
		c.Fix = "set advertised.listeners on each broker to the host:port clients reach it by, or add the advertised address to the broker list; a listener that only exists inside the cluster strands clients after bootstrap"
	default:
		c.Status = StatusWarn
		c.Problem = strings.Join(mismatched, "; ")
		c.Fix = "make the configured broker list name the addresses the brokers advertise, so DNS and every client agree"
	}
	c.MS = time.Since(start).Milliseconds()
	return append(out, c)
}

// allTCPOK reports whether the endpoint checks include at least one TCP check
// and none of them failed.
func allTCPOK(checks []Check) bool {
	seen := false
	for _, c := range checks {
		if c.ID != "tcp" {
			continue
		}
		seen = true
		if c.failed() {
			return false
		}
	}
	return seen
}

// endpointTLS returns the TLS policy of the target's first endpoint, so an
// advertised listener is probed with the same protocol the configured brokers
// are reached with.
func endpointTLS(t Target) (bool, *tls.Config) {
	if len(t.Endpoints) == 0 {
		return false, nil
	}
	return t.Endpoints[0].TLS, t.Endpoints[0].Config
}

// stepOr returns d, or stepTimeout when d is not set.
func stepOr(d time.Duration) time.Duration {
	if d <= 0 {
		return stepTimeout
	}
	return d
}
