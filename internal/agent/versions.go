package agent

import (
	"context"
	"encoding/json"
	"regexp"
	"slices"
	"strings"
	"sync"

	"piperoom/internal/tools/runbooks"
)

// Q1d: the runner reads the pipeline's running versions once per room and
// hands them to runbook_lookup, so a known issue is judged against the
// version the room actually runs instead of whatever the model guessed or
// left out. The read is connect_plugins (Q1c): the Kafka release the Connect
// worker reports on GET / and the version of each Debezium connector plugin.
// It goes through the room's own tool registry, scoped to the room's
// pipeline like any read, and only when the model first calls
// runbook_lookup, so a room that never looks up a runbook makes no extra
// read. A room without connect_plugins (a batch room, a client without the
// plugin list, the eval replay) passes no versions, as before.
//
// The source Postgres server_version is not read: no read tool or reader
// reports it yet, so the model passes it itself when a read showed it.

// connectPluginsTool is the read the running versions come from.
const connectPluginsTool = "connect_plugins"

// maxVersionReadAttempts bounds the connect_plugins reads of one room: a
// failed read (Connect down) is tried again on the next runbook_lookup, but
// not forever.
const maxVersionReadAttempts = 2

// debeziumComponents maps a Debezium connector plugin's package to the
// runbook_lookup component its version is judged under. The class is the
// plugin's Java class as Connect lists it.
var debeziumComponents = []struct{ prefix, component string }{
	{"io.debezium.connector.postgresql.", "debezium-postgres"},
	{"io.debezium.connector.mysql.", "debezium-mysql"},
	{"io.debezium.connector.mariadb.", "debezium-mariadb"},
	{"io.debezium.connector.sqlserver.", "debezium-sqlserver"},
	{"io.debezium.connector.mongodb.", "debezium-mongodb"},
	{"io.debezium.connector.oracle.", "debezium-oracle"},
	{"io.debezium.connector.jdbc.", "debezium-jdbc-sink"},
}

// runningVersionRe is the shape of a version the runner passes on: 1 to 4
// numbers and an optional release word (2.5.1.Final, 3.6.1, 7.5.0-ccs). It is
// stricter than runbook_lookup's own parser, so a passed version never makes
// the call fail, and it keeps Connect's untrusted text to digits and one
// word: nothing that could read as an instruction reaches the model.
var runningVersionRe = regexp.MustCompile(`^[0-9]{1,6}(\.[0-9]{1,6}){0,3}([.-][A-Za-z]{1,12}[0-9]{0,3})?$`)

// versionReads holds the room's running versions once read. It is safe for
// concurrent use: read tools run in parallel, and a second runbook_lookup
// waits for the first one's read instead of making its own.
type versionReads struct {
	mu       sync.Mutex
	done     bool
	attempts int
	vs       []runbooks.RunningVersion
}

func newVersionReads() *versionReads { return &versionReads{} }

// runningVersions returns the room's running versions, reading them with
// connect_plugins on the first call. It returns nil when the room has no
// connect_plugins tool or the read failed; a failed read is retried on a
// later call until maxVersionReadAttempts.
func (r *runner) runningVersions(ctx context.Context) []runbooks.RunningVersion {
	v := r.versions
	if v == nil || r.deps.Tools == nil {
		return nil
	}
	v.mu.Lock()
	defer v.mu.Unlock()
	if v.done {
		return v.vs
	}
	t, ok := r.deps.Tools.Get(connectPluginsTool)
	if !ok {
		v.done = true
		return nil
	}
	v.attempts++
	tctx, cancel := context.WithTimeout(r.toolScope(ctx), r.timing.toolTimeout)
	defer cancel()
	res, err := t.Run(tctx, json.RawMessage(`{}`))
	if err != nil {
		// The error text is Connect's and may hold anything: only the fact
		// of the failure is logged.
		r.log.Debug("agent: running versions not read", "tool", connectPluginsTool, "attempt", v.attempts)
		v.done = v.attempts >= maxVersionReadAttempts
		return nil
	}
	v.done, v.vs = true, versionsFromPlugins(res.Data)
	return v.vs
}

// versionsFromPlugins turns connect_plugins Data into runbook_lookup
// versions: "connect" is the Kafka release the worker reports (Connect ships
// with Kafka, so it is the Connect runtime's version; the brokers may run
// another, so it is not passed as "kafka"), and one debezium-<kind> entry
// per Debezium connector plugin. A component whose plugins report different
// versions is left out: judging one of them would be a guess. Data is
// round-tripped through JSON so the tool's struct and a fake's map read the
// same; anything that does not decode gives no versions. The order is fixed
// (connect, then the components in debeziumComponents order), so the same
// inventory always gives the same tool input.
func versionsFromPlugins(data any) []runbooks.RunningVersion {
	if data == nil {
		return nil
	}
	b, err := json.Marshal(data)
	if err != nil {
		return nil
	}
	var d struct {
		KafkaVersion string `json:"kafka_version"`
		Plugins      []struct {
			Class   string `json:"class"`
			Type    string `json:"type"`
			Version string `json:"version"`
		} `json:"plugins"`
	}
	if err := json.Unmarshal(b, &d); err != nil {
		return nil
	}
	var out []runbooks.RunningVersion
	if v := strings.TrimSpace(d.KafkaVersion); runningVersionRe.MatchString(v) {
		out = append(out, runbooks.RunningVersion{Component: "connect", Version: v})
	}
	found := map[string]string{}
	conflict := map[string]bool{}
	for _, p := range d.Plugins {
		switch strings.ToLower(strings.TrimSpace(p.Type)) {
		case "source", "sink", "":
		default:
			continue // converters and transforms version with their own artifact
		}
		comp := debeziumComponent(p.Class)
		v := strings.TrimSpace(p.Version)
		if comp == "" || !runningVersionRe.MatchString(v) {
			continue
		}
		if prev, ok := found[comp]; ok && prev != v {
			conflict[comp] = true
		}
		found[comp] = v
	}
	for _, c := range debeziumComponents {
		if v, ok := found[c.component]; ok && !conflict[c.component] {
			out = append(out, runbooks.RunningVersion{Component: c.component, Version: v})
		}
	}
	return out
}

// debeziumComponent returns the runbook_lookup component of a Debezium
// connector class, or "" for any other class.
func debeziumComponent(class string) string {
	class = strings.TrimSpace(class)
	for _, c := range debeziumComponents {
		if strings.HasPrefix(class, c.prefix) {
			return c.component
		}
	}
	return ""
}

// withRunningVersions returns a runbook_lookup input with the room's running
// versions added for every component the input does not already name: the
// model's own versions win. It returns the input unchanged, and nil added,
// when there is nothing to add or the input does not decode as an object.
func withRunningVersions(input json.RawMessage, running []runbooks.RunningVersion) (json.RawMessage, []runbooks.RunningVersion) {
	if len(running) == 0 {
		return input, nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(input, &obj); err != nil || obj == nil {
		return input, nil
	}
	var given []runbooks.RunningVersion
	if raw, ok := obj["versions"]; ok {
		if err := json.Unmarshal(raw, &given); err != nil {
			return input, nil
		}
	}
	var added []runbooks.RunningVersion
	for _, rv := range running {
		if !slices.ContainsFunc(given, func(g runbooks.RunningVersion) bool { return g.Component == rv.Component }) {
			added = append(added, rv)
		}
	}
	if len(added) == 0 {
		return input, nil
	}
	vs, err := json.Marshal(append(given, added...))
	if err != nil {
		return input, nil
	}
	obj["versions"] = vs
	out, err := json.Marshal(obj)
	if err != nil {
		return input, nil
	}
	return out, added
}

// runningVersionsLine is the line put before a runbook_lookup summary when
// the runner passed versions, so the model (and the room log) see what the
// known issues were judged against. It sits inside the tool's wrapper with
// the rest of the result: the versions are Connect's text, if checked.
func runningVersionsLine(added []runbooks.RunningVersion) string {
	parts := make([]string, len(added))
	for i, v := range added {
		parts[i] = v.Component + " " + v.Version
	}
	return "Running versions (read once from Connect with connect_plugins): " + strings.Join(parts, ", ") + "."
}

// upgradeRuleNote is fixed agent text appended after a runbook_lookup result
// that lists a known issue affecting a running version and fixed in a later
// release. The rule sits beside the evidence, as the X4c ranking and the
// pause rule do, so the CDC system prompt stays under its word ceiling and a
// room that never meets a fixed bug reads the prompt unchanged. It carries no
// tool data, so no version text can forge or break it.
const upgradeRuleNote = "[agent rule] A known issue above affects a running version. If you recommend an upgrade, name the running version and the fixed-in release, and cite this step."

// upgradeAdvised reports whether runbook_lookup Data lists a known issue the
// running version is affected by and a later release fixes.
func upgradeAdvised(tool string, data any) bool {
	if tool != runbooks.ToolName || data == nil {
		return false
	}
	b, err := json.Marshal(data)
	if err != nil {
		return false
	}
	var d struct {
		KnownIssues []struct {
			FixedIn string `json:"fixed_in"`
			Affects string `json:"affects"`
		} `json:"known_issues"`
	}
	if err := json.Unmarshal(b, &d); err != nil {
		return false
	}
	for _, k := range d.KnownIssues {
		if k.Affects == "yes" && strings.TrimSpace(k.FixedIn) != "" {
			return true
		}
	}
	return false
}
