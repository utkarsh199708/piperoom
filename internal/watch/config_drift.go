package watch

// config_drift (X3b; docs/api.md "Config drift (X3)"): a connector's config
// changed outside an approved fix. The watcher keeps, per pipeline and
// connector, the last approved config as an actions.ConfigDigest (the hash,
// the redacted form and keyed per-key fingerprints, in memory only; never a
// raw value). The approved config is the first config read after the start
// (every leadership term starts from nothing: the fingerprints are only
// comparable inside one process), the first config read after an
// action.executed that touched the connector (ApproveConnectorChange), and the
// config POST .../accept-config read (AcceptConfig). Every poll digests the
// config again; actions.ChangedConfigKeys between the two is the signal, so
// key order never fires it and a changed secret shows only as
// events.SecretKeyChanged. A Connect read error changes neither the signal
// nor the baseline.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/events"
	"piperoom/internal/owners"
)

// configDriftSignal is the settings of config_drift until owners.yaml carries
// it (A3 adds "config_drift" to watch.signals): on, room, and a one-minute
// `for`, so the read of a tick that raced an approved action's
// action.executed (the config already changed, the hook not yet here) is
// rebased on the next tick before it can fire.
var configDriftSignal = owners.WatchSignal{Enabled: true, Action: owners.ActionRoom, For: time.Minute}

// AcceptConfigTimeout is how long AcceptConfig waits for Kafka Connect
// (docs/api.md, accept-config: 10 s).
const AcceptConfigTimeout = 10 * time.Second

// Errors of AcceptConfig (wrapped; compare with errors.Is).
var (
	// ErrNotWatched: the watcher does not look at the pipeline or the
	// connector (the pipeline opted out, its signals are all off, or the
	// scope drops the connector), so it keeps no baseline to move.
	ErrNotWatched = errors.New("watch: the watcher does not watch this connector")
	// ErrConnectUnavailable: Kafka Connect could not be read in time (or the
	// pipeline has no Connect client). The baseline is unchanged.
	ErrConnectUnavailable = errors.New("watch: kafka connect cannot be read")
)

// configDriftID is the state-machine id of one connector's config_drift
// ("config_drift:orders-src"): one evidence entry per drifting connector.
func configDriftID(connector string) string { return events.SignalConfigDrift + ":" + connector }

// configDriftConnector is the connector of a config_drift id, "" for any other
// signal.
func configDriftConnector(id string) string {
	c, ok := strings.CutPrefix(id, events.SignalConfigDrift+":")
	if !ok {
		return ""
	}
	return c
}

// configDriftIDs are the config_drift ids that hold state in ps, sorted, so
// titles and evidence list them in a stable order after signalIDs. Caller
// holds w.mu.
func configDriftIDs(ps *pipeState) []string {
	var out []string
	for id := range ps.sigs {
		if configDriftConnector(id) != "" {
			out = append(out, id)
		}
	}
	slices.Sort(out)
	return out
}

// driftBase is the approved config of one connector.
type driftBase struct {
	// approved is the last approved config; ok says it is set.
	approved actions.ConfigDigest
	ok       bool
	// rebaseAt: an action.executed touched the connector then. The first
	// config read that starts at or after it becomes the baseline; a read
	// that started earlier is not judged.
	rebaseAt time.Time
	// setAt is when AcceptConfig set approved. A tick read that started
	// before it is not judged (it may show the config before the accept).
	setAt time.Time
	// The stored config versions (X5c, config_versions.go), apart from the
	// approved config: seen is the last config written as a version (seenOK
	// says it is set), rebaseBy the "proposal:<id>" of the action that set
	// rebaseAt, and versionBy the By of the version still to be written.
	seen      actions.ConfigDigest
	seenOK    bool
	rebaseBy  string
	versionBy string
}

// driftBase returns the baseline of a connector, creating an empty one.
// Caller holds w.mu.
func (ps *pipeState) driftBase(connector string) *driftBase {
	if ps.cfgBase == nil {
		ps.cfgBase = map[string]*driftBase{}
	}
	b := ps.cfgBase[connector]
	if b == nil {
		b = &driftBase{}
		ps.cfgBase[connector] = b
	}
	return b
}

// evaluateConfigDrift steps each connector's config_drift. A connector that
// left the pipeline (owners reload, scope) gets clear readings until its
// state resolves, and its baseline is dropped. Caller holds w.mu.
func (w *Watcher) evaluateConfigDrift(now time.Time, t target, ps *pipeState, obs *observation, fired map[string][]string) {
	for _, c := range t.connectors {
		id := configDriftID(c)
		// Before the reading: it consumes rebaseAt, which names the version's By.
		w.planVersion(now, t.p.ID, c, ps, obs)
		r := configDriftReading(c, ps, obs)
		if ss := ps.sigs[id]; r.v == vHold && ss != nil && ss.phase != phaseOK {
			// A hold keeps what the room shows: the last breach's label and
			// evidence, not an empty reading.
			keep := ss.last
			keep.v = vHold
			r = keep
		}
		w.apply(now, t, ps, id, configDriftSignal, r, fired)
	}
	for _, id := range configDriftIDs(ps) {
		c := configDriftConnector(id)
		if slices.Contains(t.connectors, c) {
			continue
		}
		delete(ps.cfgBase, c)
		w.apply(now, t, ps, id, configDriftSignal, reading{v: vClear, value: "not watched", threshold: "approved config"}, fired)
		if ps.sigs[id].phase == phaseOK {
			delete(ps.sigs, id)
		}
	}
}

// configDriftReading judges one connector's config of this tick against its
// approved config, and moves the baseline when it is due. Caller holds w.mu.
func configDriftReading(c string, ps *pipeState, obs *observation) reading {
	co := obs.conns[c]
	switch {
	case connectorGone(co):
		// connector_down reports a missing connector; there is no config to
		// judge, and the baseline stays for when it comes back.
		return reading{v: vHold}
	case co == nil || !co.cfgTried:
		return reading{v: vUnknown, err: fmt.Errorf("watch: connector %s: config not read", c)}
	case co.cfgErr != nil:
		return reading{v: vUnknown, err: co.cfgErr}
	}
	cur := actions.DigestConfig(co.cfg)
	th := "no change since the approved config"
	b := ps.driftBase(c)
	switch {
	case !b.ok:
		// The first read since the start is the baseline.
		b.approved, b.ok = cur, true
		return reading{v: vClear, value: "baseline set", threshold: th}
	case !b.rebaseAt.IsZero() && obs.at.Before(b.rebaseAt):
		// Read before the approved action's event arrived: wait for a fresh read.
		return reading{v: vHold, value: "approved change pending", threshold: th}
	case !b.rebaseAt.IsZero():
		b.approved, b.rebaseAt = cur, time.Time{}
		return reading{v: vClear, value: "baseline moved by an approved action", threshold: th}
	case obs.at.Before(b.setAt):
		return reading{v: vHold, value: "accepted while reading", threshold: th}
	}
	changed := actions.ChangedConfigKeys(b.approved, cur)
	if len(changed) == 0 {
		return reading{v: vClear, value: "no change", threshold: th}
	}
	sig := events.ConfigDriftSignal{
		Signal: events.SignalConfigDrift, Severity: events.SignalSeverity(events.SignalConfigDrift),
		Connector: c, OldHash: b.approved.Hash, NewHash: cur.Hash,
		ChangedKeys: changed[:min(len(changed), events.MaxChangedKeys)],
		KeysOmitted: max(len(changed)-events.MaxChangedKeys, 0),
	}
	r := reading{
		v: vBreach, sev: sig.Severity, label: configDriftLabel(c, changed),
		// Names and counts only in the log line: never a value.
		value:     fmt.Sprintf("%d %s changed", len(changed), plural(len(changed), "key", "keys")),
		threshold: th,
	}
	raw, _ := json.Marshal(sig) // names, hashes and a count: cannot fail
	_ = json.Unmarshal(raw, &r.ev)
	return r
}

// maxDriftLabelKeys is how many changed key names the room title shows.
const maxDriftLabelKeys = 3

// configDriftLabel is the title fragment: the room "connector config changed
// outside an approved fix" with the first few key names.
func configDriftLabel(c string, changed []string) string {
	shown := changed[:min(len(changed), maxDriftLabelKeys)]
	s := fmt.Sprintf("connector %s config changed outside an approved fix (%s", c, strings.Join(shown, ", "))
	if more := len(changed) - len(shown); more > 0 {
		s += fmt.Sprintf(" +%d more", more)
	}
	return s + ")"
}

// ApproveConnectorChange tells the watcher that an action.executed of
// pipelineID touched connector (an approved Piperoom fix ran): the first
// config read that starts from now on becomes the connector's approved
// config, so the fix itself never fires config_drift. Wire it to the room
// service's action hook. It does no I/O.
func (w *Watcher) ApproveConnectorChange(pipelineID, connector string) {
	w.ApproveConnectorChangeBy(pipelineID, connector, "")
}

// ApproveConnectorChangeBy is ApproveConnectorChange for the action of
// proposal proposalID: the stored config version (X5c) that read shows, if
// the config changed, gets By "proposal:<proposalID>". An empty proposalID
// leaves By empty.
func (w *Watcher) ApproveConnectorChangeBy(pipelineID, connector, proposalID string) {
	if pipelineID == "" || connector == "" {
		return
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	ps := w.pipes[pipelineID]
	if ps == nil || ps.cfgBase[connector] == nil {
		return // no baseline yet: the next read sets one anyway
	}
	ps.cfgBase[connector].rebaseAt = w.clock.Now()
	ps.cfgBase[connector].rebaseBy = ""
	if proposalID != "" {
		ps.cfgBase[connector].rebaseBy = "proposal:" + proposalID
	}
	w.log.Info("watch: connector changed by an approved action; its config becomes the baseline at the next read",
		"pipeline", pipelineID, "connector", connector)
}

// AcceptConfig makes the connector's current config the approved baseline of
// config_drift (POST /api/pipelines/{pid}/connectors/{name}/accept-config).
// It reads the config from Kafka Connect within AcceptConfigTimeout and
// returns the old baseline's hash ("" when there was none) and the new one's
// (actions.ConfigHash of the redacted config). A read failure returns
// ErrConnectUnavailable and leaves the baseline alone; a pipeline or
// connector the watcher does not look at returns ErrNotWatched.
func (w *Watcher) AcceptConfig(ctx context.Context, pipelineID, connector string) (oldHash, newHash string, err error) {
	var tg *target
	for _, t := range w.targets(ctx) {
		if t.p.ID == pipelineID && slices.Contains(t.connectors, connector) {
			tg = &t
			break
		}
	}
	if tg == nil {
		return "", "", fmt.Errorf("watch: accept config of %s/%s: %w", pipelineID, connector, ErrNotWatched)
	}
	if tg.conn == nil {
		return "", "", fmt.Errorf("watch: accept config of %s/%s: %w: %w", pipelineID, connector, ErrConnectUnavailable, errNoConnect)
	}
	cfg, err := call(ctx, AcceptConfigTimeout, "connector config", func(c context.Context) (map[string]string, error) {
		return tg.conn.Config(c, connector)
	})
	if err != nil {
		return "", "", fmt.Errorf("watch: accept config of %s/%s: %w: %w", pipelineID, connector, ErrConnectUnavailable, err)
	}
	cur := actions.DigestConfig(cfg)
	w.mu.Lock()
	defer w.mu.Unlock()
	b := w.pipe(pipelineID).driftBase(connector)
	if b.ok {
		oldHash = b.approved.Hash
	}
	b.approved, b.ok, b.rebaseAt, b.setAt = cur, true, time.Time{}, w.clock.Now()
	return oldHash, cur.Hash, nil
}
