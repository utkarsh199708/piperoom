package nettools

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strings"
	"time"

	"piperoom/internal/tools/kafkatools"
)

// connectView asks Kafka Connect to validate the running config of the
// pipeline's source connector, so the database is checked as Connect itself
// reaches it: a host that resolves from the room but not from the Connect
// workers shows up here and nowhere else. It never takes a value from the
// model: the config is the one Connect already holds, sent back to Connect on
// the same authenticated connection, and only host and port messages are kept.
func (t *netProbe) connectView(ctx context.Context, tgt Target) Check {
	const id = "connect_view"
	skip := func(why string) Check { return Check{ID: id, Status: StatusSkip, Detail: why} }
	if t.d.Connect == nil {
		return skip("no Connect client")
	}
	val, ok := t.d.Connect.(kafkatools.ConfigValidator)
	if !ok {
		return skip("the Connect client cannot validate configs")
	}
	name, cfg, why := t.runningSourceConfig(ctx, tgt)
	if name == "" {
		return skip(why)
	}
	class := strings.TrimSpace(cfg["connector.class"])
	if class == "" {
		return Check{ID: id, Status: StatusWarn,
			Problem: "connector " + clip(name) + " has no connector.class, so Connect cannot validate it"}
	}
	cctx, cancel := context.WithTimeout(ctx, stepOr(t.d.StepTimeout))
	defer cancel()
	start := time.Now()
	vr, err := val.ValidateConfig(cctx, class, cfg)
	ms := time.Since(start).Milliseconds()
	if err != nil {
		c := Check{ID: id, Status: StatusFail, MS: ms,
			Problem: "Connect could not validate connector " + clip(name) + "'s config: " + redactedText(err.Error()),
			Fix:     "check that Connect is up, that the plugin class is installed on every worker, and that the Connect login may validate configs"}
		if cctx.Err() != nil {
			c.Problem = "Connect did not finish validating connector " + clip(name) + " within the step's time"
		}
		return c
	}
	detail := fmt.Sprintf("Connect validated %s (%s): %d error(s)", clip(name), clip(class), vr.ErrorCount)
	if vr.ErrorCount == 0 {
		return Check{ID: id, Status: StatusOK, MS: ms, Detail: detail}
	}
	hostProblems, otherKeys := hostMessages(vr.Errors)
	if len(hostProblems) == 0 {
		return Check{ID: id, Status: StatusWarn, MS: ms, Detail: detail,
			Problem: "Connect reports errors on " + strings.Join(otherKeys, ", ") + ", none about the database host"}
	}
	return Check{ID: id, Status: StatusFail, MS: ms, Detail: detail,
		Problem: "Connect cannot use the database as configured: " + strings.Join(hostProblems, "; ") + " (connector " + clip(name) + ")",
		Fix:     "make the database reachable from every Connect worker (DNS, the advertised host, the port and its firewall) and re-run the probe"}
}

// runningSourceConfig picks the connector whose running config the Connect
// check validates: the one that named the target's address, else the first
// source connector whose database.hostname is one of the target's hosts. It
// reads only database.hostname/database.port out of the config itself; the
// whole config is what Connect validates.
func (t *netProbe) runningSourceConfig(ctx context.Context, tgt Target) (string, map[string]string, string) {
	for _, e := range tgt.Endpoints {
		if e.Connector == "" {
			continue
		}
		cfg, err := t.connectorConfig(ctx, e.Connector)
		if err != nil {
			return "", nil, "connector " + clip(e.Connector) + ": config unreadable"
		}
		return e.Connector, cfg, ""
	}
	hosts := map[string]bool{}
	for _, e := range tgt.Endpoints {
		if h, _, err := net.SplitHostPort(e.Addr); err == nil {
			hosts[strings.ToLower(h)] = true
		}
	}
	for _, name := range tgt.Connectors {
		cfg, err := t.connectorConfig(ctx, name)
		if err != nil {
			continue
		}
		h := strings.ToLower(strings.TrimSpace(cfg["database.hostname"]))
		if h != "" && hosts[h] {
			return name, cfg, ""
		}
	}
	return "", nil, "no source connector of the pipeline names this host"
}

func (t *netProbe) connectorConfig(ctx context.Context, name string) (map[string]string, error) {
	cctx, cancel := context.WithTimeout(ctx, stepOr(t.d.StepTimeout))
	defer cancel()
	cfg, err := t.d.Connect.Config(cctx, name)
	if err != nil {
		return nil, fmt.Errorf("net_probe: reading connector %q: %w", name, err)
	}
	return cfg, nil
}

// hostMessages keeps the validation messages of the host and port keys, in key
// order, and names the other keys that have one. Messages are untrusted text:
// each is redacted and clipped, and a value from the connector config never
// reaches them.
func hostMessages(errs map[string][]string) (hostProblems, otherKeys []string) {
	keys := make([]string, 0, len(errs))
	for k := range errs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		msgs := errs[k]
		if len(msgs) == 0 {
			continue
		}
		if !hostKey(k) {
			otherKeys = append(otherKeys, clip(k))
			continue
		}
		shown := msgs
		if len(shown) > 2 {
			shown = shown[:2]
		}
		parts := make([]string, 0, len(shown))
		for _, m := range shown {
			if s := redactedText(m); s != "" {
				parts = append(parts, s)
			}
		}
		hostProblems = append(hostProblems, clip(k)+": "+strings.Join(parts, " / "))
	}
	if len(otherKeys) == 0 {
		otherKeys = []string{"other keys"}
	}
	return hostProblems, otherKeys
}

// hostKey reports whether a config key names the database host or port, the
// keys whose validation messages say whether Connect can reach it. It is not
// actions.LooksSecret's rule and must never match a credential key.
func hostKey(k string) bool {
	lk := strings.ToLower(strings.TrimSpace(k))
	switch lk {
	case "database.hostname", "database.port", "database.url", "hostname", "host", "hosts", "port",
		"connection.url", "connection.uri":
		return true
	}
	for _, suffix := range []string{".hostname", ".host", ".hosts", ".port", ".url", ".uri", ".bootstrap.servers"} {
		if strings.HasSuffix(lk, suffix) {
			return true
		}
	}
	return false
}
