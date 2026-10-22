package watch

// Connector config versions (X5c; docs/api.md "Fleet view"): every connector
// config the watcher reads is digested (actions.DigestConfig, the same digest
// config_drift judges), and a config whose hash or keyed fingerprints differ
// from the last one written is kept in Piperoom's own
// connector_config_versions table (Deps.ConfigVersions) in its redacted form
// (ConfigDigest.Redacted; a raw value never leaves the watcher), with the full
// list of changed key names. The first read after the start has no earlier
// digest in memory: it is written with no changed keys, and the store drops
// it when it repeats the connector's newest stored version. A failed write is
// tried again on the next tick against the same earlier digest, so no change
// is lost while the store is down.

import (
	"context"
	"errors"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/clients"
)

// versionJob is one config version to write in this tick.
type versionJob struct {
	pipeline, connector string
	at                  time.Time
	digest              actions.ConfigDigest
	changed             []string
	by                  string
}

// planVersion queues a version of connector c when this tick read its config
// and the config differs from the last version written (or none was written
// since the start). A read that started before an approved action's
// action.executed arrived waits for the next read, so the version the action
// produced carries its By. Caller holds w.mu; it runs before
// configDriftReading, which consumes rebaseAt.
func (w *Watcher) planVersion(now time.Time, pipelineID, c string, ps *pipeState, obs *observation) {
	if w.versions == nil {
		return
	}
	co := obs.conns[c]
	if connectorGone(co) || co == nil || !co.cfgTried || co.cfgErr != nil {
		return
	}
	b := ps.driftBase(c)
	if !b.rebaseAt.IsZero() {
		if obs.at.Before(b.rebaseAt) {
			return
		}
		b.versionBy = b.rebaseBy
	}
	cur := actions.DigestConfig(co.cfg)
	changed := []string{}
	if b.seenOK {
		changed = actions.ChangedConfigKeys(b.seen, cur)
		if len(changed) == 0 {
			// The approved action changed nothing (a failed step): a later
			// change is not its doing.
			b.versionBy = ""
			return
		}
	}
	w.versionJobs = append(w.versionJobs, versionJob{
		pipeline: pipelineID, connector: c, at: now, digest: cur,
		changed: changed[:min(len(changed), clients.MaxConfigKeys)], by: b.versionBy,
	})
}

// writeVersions writes the versions planned in this tick, each within the
// check timeout. A success makes the config the connector's last version; a
// failure is logged once per error text and leaves it, so the next tick
// plans the same change again.
func (w *Watcher) writeVersions(ctx context.Context, jobs []versionJob) {
	for _, j := range jobs {
		if ctx.Err() != nil {
			return
		}
		_, err := call(ctx, w.cfg.CheckTimeout, "record config version", func(c context.Context) (bool, error) {
			return w.versions.RecordConfigVersion(c, clients.ConnectorConfigVersion{
				PipelineID: j.pipeline, Connector: j.connector, Hash: j.digest.Hash, At: j.at, By: j.by,
				ChangedKeys: j.changed, Config: j.digest.Redacted,
			})
		})
		w.mu.Lock()
		ps := w.pipe(j.pipeline)
		id := "config_version:" + j.connector
		if err != nil {
			w.noteError(j.pipeline, id, ps.sig(id), err)
			w.mu.Unlock()
			if errors.Is(err, context.DeadlineExceeded) {
				// A store that does not answer would hold the tick for a
				// timeout per connector: the rest is planned again next tick.
				return
			}
			continue
		}
		delete(ps.sigs, id)
		// The connector may have left the pipeline meanwhile: its memory is
		// gone, and nothing is set.
		if b := ps.cfgBase[j.connector]; b != nil {
			b.seen, b.seenOK = j.digest, true
			if b.versionBy == j.by {
				b.versionBy = ""
			}
		}
		w.mu.Unlock()
	}
}
