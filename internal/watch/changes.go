package watch

// The pipeline change feed (tasks/F8-what-changed-checklist.md, section 2).
// Every tick the watcher already holds each connector's status and config;
// here it diffs them against what it saw on the previous tick, adds the
// registry's subject versions and the source's DDL audit rows, and records the
// differences through a clients.ChangeWriter. Nothing here writes to a
// customer system, and nothing stored holds a credential or a record value:
// credential config keys read "***", traces are one redacted line.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/dlq"
	"piperoom/internal/redact"
	"piperoom/internal/tools"
)

// DDLReader is the part of clients.SourceReader the change feed uses (the
// audit table written by the event trigger).
type DDLReader interface {
	RecentDDL(ctx context.Context, since time.Time, limit int) ([]clients.DDLChange, error)
}

const (
	// changeTextCap is the cap of every one-line text of a change row.
	changeTextCap = 300
	// ddlLimit is how many audit rows one tick asks for.
	ddlLimit = 200
	// maskedValue replaces a credential config value.
	maskedValue = "***"
)

var errNoDDL = errors.New("watch: no DDL audit reader configured")

// cfgVal is one config key as the baseline keeps it. shown is what a row may
// store (masked); token is what is compared: a hash of the raw value, so a
// changed secret is seen without keeping it.
type cfgVal struct{ shown, token string }

// connBase is what the watcher saw of one connector on the last tick whose
// diff was recorded.
type connBase struct {
	states map[string]stateVal // object ("name" or "name/ID") -> state
	cfg    map[string]cfgVal
	hasCfg bool
}

type stateVal struct{ state, trace string }

// changeBase is the baseline of one pipeline. Only the tick goroutine touches
// it, so it needs no lock.
type changeBase struct {
	conns    map[string]*connBase
	versions map[string]int // subject -> latest version
}

// setChanges is the per-set read of one tick: registry versions and DDL rows.
type setChanges struct {
	versions  map[string]versionObs
	ddl       []clients.DDLChange
	ddlErr    error
	ddlWindow time.Time
}

type versionObs struct {
	latest int
	err    error
}

// credentialKeyParts are the lower-case fragments that mark a config key as
// holding a credential. It is wider than kafkatools.sensitiveKey on purpose:
// this text is stored for good. ".key" matches "ssl.key.password" and
// "api.key" but not "key.converter"; "jaas" covers sasl.jaas.config, whose value
// embeds a password.
var credentialKeyParts = []string{
	"password", "passwd", "pwd", "passphrase", "secret", "token", "credential",
	"apikey", "api_key", "api-key", "authorization", "jaas", ".key",
}

// sensitiveConfigKey reports whether a config key holds a credential.
func sensitiveConfigKey(k string) bool {
	l := strings.ToLower(k)
	for _, part := range credentialKeyParts {
		if strings.Contains(l, part) {
			return true
		}
	}
	return false
}

// makeCfg builds the baseline of one config. A credential key shows "***". Any
// other value can still carry a secret (connection.url with user:pass@ or
// ?password=), so what a row may show goes through redact.Text. Nothing keeps
// the raw value: the compared token is always a hash, so a changed secret is
// still seen as a change.
func makeCfg(cfg map[string]string) map[string]cfgVal {
	out := make(map[string]cfgVal, len(cfg))
	for k, v := range cfg {
		h := sha256.Sum256([]byte(v))
		shown := maskedValue
		if !sensitiveConfigKey(k) {
			shown, _ = redact.Text(v)
		}
		out[k] = cfgVal{shown: shown, token: hex.EncodeToString(h[:])}
	}
	return out
}

// fingerprint is the dedup key of a row: sha256 over the parts.
func fingerprint(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

// oneLine removes control characters, masks secrets (internal/redact) and PII
// (e-mail, card, SSN, phone: a DDL statement can hold them as literals) in
// text that came from a customer system, and only then cuts to changeTextCap
// runes, so the cut never leaves part of a value unmasked. The PII masks run
// first: internal/redact keeps an e-mail's domain.
func oneLine(s string) string {
	s, _ = redact.Text(dlq.MaskPII(clean(s, 4*changeTextCap)))
	return clean(s, changeTextCap)
}

// readChanges is the registry and DDL part of observeSet: one Versions call per
// subject of the set's pipelines and one RecentDDL call. Every call has its own
// deadline and error.
func (w *Watcher) readChanges(ctx context.Context, ts []target) *setChanges {
	sc := &setChanges{versions: map[string]versionObs{}}
	reg, ddl := ts[0].reg, ts[0].ddl // every target of a set has that set's clients
	var subjects []string
	for _, t := range ts {
		for _, s := range t.subjects {
			if !slices.Contains(subjects, s) {
				subjects = append(subjects, s)
			}
		}
	}
	if reg != nil {
		var mu sync.Mutex
		var wg sync.WaitGroup
		for _, s := range subjects {
			wg.Add(1)
			go func() {
				defer wg.Done()
				vs, err := call(ctx, w.cfg.CheckTimeout, "schema versions", func(c context.Context) ([]int, error) { return reg.Versions(c, s) })
				v := versionObs{err: err}
				if err == nil {
					v.latest = slices.Max(append([]int{0}, vs...))
				}
				mu.Lock()
				sc.versions[s] = v
				mu.Unlock()
			}()
		}
		wg.Wait()
	}
	if ddl == nil {
		sc.ddlErr = errNoDDL
		return sc
	}
	sc.ddlWindow = w.ddlSince(ts[0].set)
	sc.ddl, sc.ddlErr = call(ctx, w.cfg.CheckTimeout, "recent ddl", func(c context.Context) ([]clients.DDLChange, error) {
		return ddl.RecentDDL(c, sc.ddlWindow, ddlLimit)
	})
	return sc
}

// ddlSince is where the next DDL read of a client set starts: two intervals
// before the last fully recorded tick, so a row the audit trigger committed late
// is still seen; the fingerprint keeps it from being recorded twice.
func (w *Watcher) ddlSince(set string) time.Time {
	last, ok := w.ddlOK[set]
	if !ok {
		last = w.clock.Now()
	}
	return last.Add(-2 * w.cfg.Interval)
}

// recordChanges diffs one tick's observation and writes the differences. It
// runs after observe, on the tick goroutine, outside w.mu.
func (w *Watcher) recordChanges(ctx context.Context, now time.Time, ts []target, obs map[string]*observation) {
	if w.changes == nil {
		return
	}
	failed := false
	ddlOK := map[string]bool{}
	for _, t := range ts {
		o := obs[t.set]
		if o == nil {
			continue
		}
		base := w.chg[t.p.ID]
		if base == nil {
			base = &changeBase{conns: map[string]*connBase{}, versions: map[string]int{}}
			w.chg[t.p.ID] = base
		}
		for _, name := range t.connectors {
			co := o.conns[name]
			if co == nil || co.statusErr != nil {
				continue // unknown says nothing: the baseline stays
			}
			if !w.recordConnector(ctx, now, t, base, name, co) {
				failed = true
			}
		}
		if sc := o.chg; sc != nil {
			if !w.recordVersions(ctx, now, t, base, sc) {
				failed = true
			}
			if _, seen := ddlOK[t.set]; !seen {
				ddlOK[t.set] = true
			}
			if !w.recordDDL(ctx, t, sc) {
				ddlOK[t.set] = false
				failed = true
			}
		}
	}
	for set, ok := range ddlOK {
		if ok {
			w.ddlOK[set] = now
		}
	}
	w.noteWriteHealth(failed)
}

// noteWriteHealth ends a write-failure episode (write opens it and logs once).
func (w *Watcher) noteWriteHealth(failed bool) {
	if !failed && w.chgFailing {
		w.chgFailing = false
		w.log.Info("watch: change log writes work again")
	}
}

// write records one row. It returns false when the write failed; the caller
// then leaves its baseline alone, so the same diff is found again next tick.
func (w *Watcher) write(ctx context.Context, c clients.PipelineChange) bool {
	c.Summary = oneLine(c.Summary)
	_, err := call(ctx, w.cfg.CheckTimeout, "record change", func(cc context.Context) (bool, error) {
		return w.changes.RecordChange(cc, c)
	})
	if err != nil {
		if ctx.Err() == nil && !w.chgFailing {
			w.log.Warn("watch: cannot record a pipeline change; retrying next tick",
				"pipeline", c.PipelineID, "kind", c.Kind, "err", err)
		}
		w.chgFailing = true
		return false
	}
	return true
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		return nil // map[string]any of strings and ints cannot fail
	}
	return b
}

// recordConnector diffs one connector's states and config. It reports false
// when a write failed.
func (w *Watcher) recordConnector(ctx context.Context, now time.Time, t target, base *changeBase, name string, co *connObs) bool {
	ok := true
	cur := map[string]stateVal{name: {state: co.status.State}}
	for _, tk := range co.status.Tasks {
		cur[name+"/"+strconv.Itoa(tk.ID)] = stateVal{state: tk.State, trace: traceLine(tk.Trace)}
	}
	cb := base.conns[name]
	if cb == nil {
		cb = &connBase{states: cur}
		base.conns[name] = cb
	} else {
		objects := make([]string, 0, len(cur))
		for o := range cur {
			objects = append(objects, o)
		}
		sort.Strings(objects)
		for _, obj := range objects {
			now2 := cur[obj]
			old, seen := cb.states[obj]
			if !seen {
				cb.states[obj] = now2 // a new task: baseline only
				continue
			}
			if old.state == now2.state {
				if now2.trace != "" || old.trace == "" {
					cb.states[obj] = stateVal{state: old.state, trace: now2.trace}
				}
				continue
			}
			before := mustJSON(map[string]string{"state": old.state, "trace": oneLine(old.trace)})
			after := mustJSON(map[string]string{"state": now2.state, "trace": oneLine(now2.trace)})
			summary := fmt.Sprintf("%s: %s → %s", obj, old.state, now2.state)
			if now2.trace != "" {
				summary += " (" + oneLine(now2.trace) + ")"
			}
			if !w.write(ctx, clients.PipelineChange{
				PipelineID: t.p.ID, Kind: clients.ChangeConnectorState, Object: obj, At: now, Source: "watcher",
				Fingerprint: fingerprint(clients.ChangeConnectorState, obj, now.UTC().Format(time.RFC3339Nano), string(before), string(after)),
				Before:      before, After: after, Summary: summary,
			}) {
				ok = false
				continue
			}
			cb.states[obj] = now2
		}
		for obj := range cb.states {
			if _, still := cur[obj]; !still {
				delete(cb.states, obj) // a task that went away
			}
		}
	}
	if co.cfgErr != nil || co.cfg == nil {
		return ok
	}
	cfg := makeCfg(co.cfg)
	if !cb.hasCfg {
		cb.cfg, cb.hasCfg = cfg, true
		return ok
	}
	var keys []string
	for k := range cfg {
		if old, had := cb.cfg[k]; !had || old.token != cfg[k].token {
			keys = append(keys, k)
		}
	}
	for k := range cb.cfg {
		if _, still := cfg[k]; !still {
			keys = append(keys, k)
		}
	}
	if len(keys) == 0 {
		return ok
	}
	sort.Strings(keys)
	before, after := map[string]string{}, map[string]string{}
	var fp []string
	for _, k := range keys {
		o, hadOld := cb.cfg[k]
		n, hasNew := cfg[k]
		if hadOld {
			before[k] = o.shown
		}
		if hasNew {
			after[k] = n.shown
		}
		fp = append(fp, k, o.token, n.token)
	}
	b, a := mustJSON(before), mustJSON(after)
	summary := fmt.Sprintf("config of %s: %d %s changed (%s)", name, len(keys), plural(len(keys), "key", "keys"), strings.Join(keys, ", "))
	if w.write(ctx, clients.PipelineChange{
		PipelineID: t.p.ID, Kind: clients.ChangeConnectorConfig, Object: name, At: now, Source: "watcher",
		Fingerprint: fingerprint(append([]string{clients.ChangeConnectorConfig, name}, fp...)...),
		Before:      b, After: a, Summary: summary,
	}) {
		cb.cfg = cfg
	} else {
		ok = false
	}
	return ok
}

func plural(n int, one, many string) string {
	if n == 1 {
		return one
	}
	return many
}

// recordVersions diffs the latest version of each of the pipeline's subjects.
// A subject whose read failed says nothing (the registry may be down).
func (w *Watcher) recordVersions(ctx context.Context, now time.Time, t target, base *changeBase, sc *setChanges) bool {
	ok := true
	for _, s := range t.subjects {
		v, got := sc.versions[s]
		if !got || v.err != nil {
			continue
		}
		old, seen := base.versions[s]
		if !seen {
			base.versions[s] = v.latest
			continue
		}
		if old == v.latest {
			continue
		}
		verb := "registered"
		if v.latest < old {
			verb = "deleted"
		}
		before, after := mustJSON(map[string]int{"version": old}), mustJSON(map[string]int{"version": v.latest})
		if !w.write(ctx, clients.PipelineChange{
			PipelineID: t.p.ID, Kind: clients.ChangeSchemaVersion, Object: s, At: now, Source: "watcher",
			Fingerprint: fingerprint(clients.ChangeSchemaVersion, s, now.UTC().Format(time.RFC3339Nano), string(before), string(after)),
			Before:      before, After: after,
			Summary: fmt.Sprintf("schema %s: version %d %s (was %d)", s, v.latest, verb, old),
		}) {
			ok = false
			continue
		}
		base.versions[s] = v.latest
	}
	return ok
}

// ddlInScope reports whether an audit row is about a source table of the
// pipeline: the table itself or one of its columns (a column rename or comment
// is logged as schema.table.column). The identity is tried as written first
// (an owners entry in the quoted form public."Name"), then with the quotes
// removed (an owners entry holds the catalog name public.Name, while the audit
// trigger quotes a mixed-case one). Same rule as ddlInScope in
// internal/tools/changes, plus the quoted forms.
func ddlInScope(sc tools.Scope, identity string) bool {
	if sc.SourceTableInScope(identity) {
		return true
	}
	raw, plain, ok := splitIdentity(identity)
	if !ok || len(plain) < 2 || len(plain) > 3 {
		return false
	}
	// A third part is a column: the table is what precedes it.
	if len(plain) == 3 && sc.SourceTableInScope(strings.Join(raw[:2], ".")) {
		return true
	}
	return sc.SourceTableInScope(strings.Join(plain[:2], "."))
}

// splitIdentity splits an object_identity on the dots outside double quotes.
// raw keeps each part as written, plain has the quotes removed ("" inside
// quotes is one quote). It reports false for an unterminated quote or an empty
// part.
func splitIdentity(s string) (raw, plain []string, ok bool) {
	var cur, curRaw strings.Builder
	inQ := false
	rs := []rune(s)
	flush := func() {
		plain, raw = append(plain, cur.String()), append(raw, curRaw.String())
		cur.Reset()
		curRaw.Reset()
	}
	for i := 0; i < len(rs); i++ {
		r := rs[i]
		if r == '.' && !inQ {
			flush()
			continue
		}
		curRaw.WriteRune(r)
		switch {
		case r == '"' && inQ && i+1 < len(rs) && rs[i+1] == '"':
			cur.WriteRune('"')
			curRaw.WriteRune('"')
			i++
		case r == '"':
			inQ = !inQ
		default:
			cur.WriteRune(r)
		}
	}
	if inQ {
		return nil, nil, false
	}
	flush()
	for k := range plain {
		if plain[k] == "" {
			return nil, nil, false
		}
	}
	return raw, plain, true
}

// recordDDL writes the audit rows of the pipeline's source tables. The row
// carries its own time; the fingerprint is (at, object, query), so reading the
// same row on the next tick writes nothing.
func (w *Watcher) recordDDL(ctx context.Context, t target, sc *setChanges) bool {
	if errors.Is(sc.ddlErr, errNoDDL) {
		return true // no reader wired: nothing to warn about every tick
	}
	if sc.ddlErr != nil {
		if !w.ddlWarned[t.set] && ctx.Err() == nil {
			w.ddlWarned[t.set] = true
			w.log.Warn("watch: cannot read the DDL audit table; ddl changes are off until it works",
				"set", t.set, "err", sc.ddlErr)
		}
		return true // not a write failure: nothing to retry
	}
	delete(w.ddlWarned, t.set)
	scope, err := tools.ScopeFor(tools.WithPipeline(ctx, t.p), w.own)
	if err != nil {
		return true
	}
	ok := true
	rows := slices.Clone(sc.ddl)
	slices.SortFunc(rows, func(a, b clients.DDLChange) int { return a.At.Compare(b.At) })
	for _, r := range rows {
		if !ddlInScope(scope, r.ObjectIdentity) {
			continue
		}
		summary := strings.TrimSpace(r.CommandTag+" "+r.ObjectIdentity) + " | " + r.Query
		if !w.write(ctx, clients.PipelineChange{
			PipelineID: t.p.ID, Kind: clients.ChangeDDL, Object: r.ObjectIdentity, At: r.At, Actor: oneLine(r.Username), Source: "ddl_audit",
			Fingerprint: fingerprint(clients.ChangeDDL, r.At.UTC().Format(time.RFC3339Nano), r.ObjectIdentity, r.Query),
			Summary:     summary,
		}) {
			ok = false
		}
	}
	return ok
}
