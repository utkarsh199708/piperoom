package actions

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/dlq"
	"piperoom/internal/tools"
)

// The two MySQL connector keys connect_update_connector_config admits (Q8e,
// docs/design/mysql-source.md §8.3). Like the heartbeat keys their values are
// checked in code, not by a fixed list, and they apply to Debezium MySQL
// source connectors only:
//
//   - database.server.id fixes server-id-clash: the connector joins the
//     source as a replica with this id, so it must differ from the server's
//     own @@server_id and from every other connector of the pipeline;
//   - snapshot.mode=recovery (schema_only_recovery before Debezium 2.6, the
//     Q1 known-issues rename) rebuilds a lost schema history topic. Any other
//     documented mode is accepted only to move a connector back out of
//     recovery, which is the rollback of this change.
const (
	keyServerID     = "database.server.id"
	keySnapshotMode = "snapshot.mode"
)

// maxServerID is the largest MySQL server id (an unsigned 32-bit value).
const maxServerID = 4294967295

// serverIDRe admits a plain decimal without sign or leading zero; 0 is not a
// valid replica id.
var serverIDRe = regexp.MustCompile(`^[1-9][0-9]{0,9}$`)

// recoveryModes are the snapshot.mode values that rebuild the schema history.
var recoveryModes = map[string]bool{"recovery": true, "schema_only_recovery": true}

// restoreModes are the documented modes a connector in recovery may be set
// back to. always, custom, configuration_based and exported are absent: none
// is a mode this action ever moves a connector out of, so none is a rollback.
var restoreModes = map[string]bool{
	"initial": true, "when_needed": true, "no_data": true, "schema_only": true, "never": true, "initial_only": true,
}

// isMySQLKey reports whether key is one of the two MySQL keys.
func isMySQLKey(key string) bool { return key == keyServerID || key == keySnapshotMode }

// isCodeChecked reports whether key's value is checked in code (an empty
// value set in the allow-list) rather than by the list.
func isCodeChecked(key string) bool { return isHeartbeatKey(key) || isMySQLKey(key) }

// codeValueCheck checks the value of a code-checked key without I/O.
func codeValueCheck(key, value string) error {
	switch key {
	case keyServerID:
		if !serverIDRe.MatchString(value) {
			return mysqlKeyErr(key, "value %q is not a server id: a whole number 1..%d", shortText(value), uint64(maxServerID))
		}
		if n, err := strconv.ParseUint(value, 10, 64); err != nil || n > maxServerID {
			return mysqlKeyErr(key, "value %s is out of range 1..%d", value, uint64(maxServerID))
		}
		return nil
	case keySnapshotMode:
		if !recoveryModes[value] && !restoreModes[value] {
			return mysqlKeyErr(key, "value %q is not allowed: recovery (schema_only_recovery before Debezium 2.6) rebuilds a lost schema history topic; a connector already in recovery may go back to initial, when_needed, no_data, schema_only, never or initial_only", shortText(value))
		}
		return nil
	}
	return heartbeatValueCheck(key, value)
}

// mysqlKeyErr is a refusal of a MySQL key's value: tools.ErrBadInput.
func mysqlKeyErr(key, format string, args ...any) error {
	return fmt.Errorf("actions: %s: %w: %s", key, tools.ErrBadInput, fmt.Sprintf(format, args...))
}

// mysqlRoom reports whether the action runs where a MySQL binlog reader is
// wired: only there are the MySQL keys offered and described.
func (d Deps) mysqlRoom() bool { return d.Binlog != nil }

// mysqlDescribe is the Describe sentence of the MySQL keys and heartbeat form,
// empty outside a MySQL room.
func (d Deps) mysqlDescribe() string {
	if !d.mysqlRoom() {
		return ""
	}
	return fmt.Sprintf(" On a Debezium MySQL source connector only: database.server.id 1..%d, different from the source's @@server_id and every other connector of the pipeline (fixes a server-id clash); snapshot.mode recovery (schema_only_recovery before Debezium 2.6) to rebuild a lost schema history topic, or back from recovery to the mode it had; and the MySQL heartbeat form INSERT INTO db.t (id, ts) VALUES (1, NOW(6)) ON DUPLICATE KEY UPDATE ts = NOW(6).", uint64(maxServerID))
}

// codeKeySchemaText is the params-schema description of a code-checked key.
func (d Deps) codeKeySchemaText(key string) string {
	switch key {
	case keyServerID:
		return fmt.Sprintf("Debezium MySQL source connectors only: the replica id the connector joins the source with, 1..%d, different from the source's @@server_id and from every other connector of the pipeline (fixes a server-id clash)", uint64(maxServerID))
	case keySnapshotMode:
		return "Debezium MySQL source connectors only: recovery (schema_only_recovery before Debezium 2.6) rebuilds a lost schema history topic; a connector in recovery may be set back to initial, when_needed, no_data, schema_only, never or initial_only"
	}
	if key == keyHeartbeatQuery && d.mysqlRoom() {
		return heartbeatSchemaText(key) + "; on MySQL: db.table, upsert with ON DUPLICATE KEY UPDATE, NOW(6)"
	}
	return heartbeatSchemaText(key)
}

// checkMySQLKeyGuard refuses a MySQL key on a connector that is not a
// Debezium MySQL source.
func checkMySQLKeyGuard(connector, key string, cfg map[string]string) error {
	if !isMySQLConnector(cfg) || dlq.Parse(cfg).IsSink {
		return fmt.Errorf("actions: %s only applies to a Debezium MySQL source connector; %s is not one (connector.class %q)", key, connector, shortText(strings.TrimSpace(cfg["connector.class"])))
	}
	return nil
}

// checkValueGuards runs the config-dependent checks that need the new
// values: the heartbeat query's SQL dialect against the connector class, and
// the snapshot.mode rule that only a connector in recovery may leave it.
func (s configChangeSet) checkValueGuards(cfg map[string]string) error {
	if q, ok := s.changes[keyHeartbeatQuery]; ok {
		if err := checkHeartbeatDialect(s.connector, q, cfg); err != nil {
			return err
		}
	}
	if v, ok := s.changes[keySnapshotMode]; ok && !recoveryModes[v] {
		cur := strings.TrimSpace(cfg[keySnapshotMode])
		if !recoveryModes[cur] && cur != v {
			return mysqlKeyErr(keySnapshotMode, "%s is not in recovery (snapshot.mode is %s); this action sets snapshot.mode only to recovery, or back from it", s.connector, configText(cfg, keySnapshotMode))
		}
	}
	return nil
}

// checkSourceGuards is Preview's subset of checkGuards: the refusals of the
// source-side keys (heartbeat and MySQL), which Execute would make too.
func (s configChangeSet) checkSourceGuards(cfg map[string]string) error {
	for _, k := range s.keys() {
		switch {
		case isHeartbeatKey(k):
			if err := checkHeartbeatGuard(s.connector, k, cfg); err != nil {
				return err
			}
		case isMySQLKey(k):
			if err := checkMySQLKeyGuard(s.connector, k, cfg); err != nil {
				return err
			}
		}
	}
	return s.checkValueGuards(cfg)
}

// checkServerID refuses a database.server.id that equals the source's own
// @@server_id or another connector's of the pipeline: either makes the
// source drop one of the two binlog readers (server-id-clash). The source's
// id comes from the binlog reader; without one the change is refused, since
// a clash with the server cannot be ruled out. With require, the pipeline
// must be in ctx (Execute); without it (Validate, Preview) a missing
// pipeline skips the per-connector check, as ctxHeartbeatScope does.
func (d Deps) checkServerID(ctx context.Context, s configChangeSet, require bool) error {
	v, ok := s.changes[keyServerID]
	if !ok {
		return nil
	}
	if d.Binlog == nil {
		return fmt.Errorf("actions: %s: no binlog reader of the source is configured, so Piperoom cannot check the id against the source's @@server_id: %w", keyServerID, clients.ErrNotSupported)
	}
	st, err := d.Binlog.BinlogStatus(ctx)
	if err != nil {
		return fmt.Errorf("actions: %s: read the source's @@server_id: %w", keyServerID, err)
	}
	if strconv.FormatInt(st.ServerID, 10) == v {
		return mysqlKeyErr(keyServerID, "%s is the source server's own @@server_id; a connector must join with another id", v)
	}
	pl, ok := tools.PipelineFrom(ctx)
	if !ok {
		if require {
			return fmt.Errorf("actions: %s: %w: needs the room's pipeline in ctx to compare with its other connectors", keyServerID, tools.ErrNoScope)
		}
		return nil
	}
	for _, c := range pl.Connectors {
		if c == s.connector {
			continue
		}
		cfg, err := d.Connect.Config(ctx, c)
		if err != nil {
			return fmt.Errorf("actions: %s: read config of %s to compare server ids: %w", keyServerID, c, err)
		}
		if isMySQLConnector(cfg) && strings.TrimSpace(cfg[keyServerID]) == v {
			return mysqlKeyErr(keyServerID, "%s is already the server id of %s, another connector of pipeline %q", v, c, pl.ID)
		}
	}
	return nil
}

// recoveryRename is the Debezium version that renamed schema_only_recovery
// to recovery (the Q1 known-issues index, renamed.yaml).
const recoveryRenameMajor, recoveryRenameMinor = 2, 6

// checkRecoveryName refuses snapshot.mode=recovery on a Debezium MySQL plugin
// older than 2.6, which knows only schema_only_recovery, and reports when
// schema_only_recovery is the deprecated name on 2.6+. It reads the worker's
// plugin list when the client has one; an unknown version checks nothing.
func (d Deps) checkRecoveryName(ctx context.Context, s configChangeSet, cfg map[string]string) (note string, err error) {
	v, ok := s.changes[keySnapshotMode]
	if !ok || !recoveryModes[v] {
		return "", nil
	}
	pr, ok := d.Connect.(connect.PluginReader)
	if !ok {
		return "", nil
	}
	plugins, err := pr.Plugins(ctx)
	if err != nil {
		return "", nil // a version hint, not a gate: the dry run validates the value
	}
	class := strings.TrimSpace(cfg["connector.class"])
	for _, p := range plugins {
		if p.Class != class {
			continue
		}
		major, minor, ok := majorMinor(p.Version)
		if !ok {
			return "", nil
		}
		renamed := major > recoveryRenameMajor || (major == recoveryRenameMajor && minor >= recoveryRenameMinor)
		switch {
		case v == "recovery" && !renamed:
			return "", mysqlKeyErr(keySnapshotMode, "the connector runs Debezium %s, which knows this mode as schema_only_recovery (recovery is its 2.6+ name)", shortText(p.Version))
		case v == "schema_only_recovery" && renamed:
			return fmt.Sprintf("Debezium %s still accepts schema_only_recovery but deprecates it for recovery", shortText(p.Version)), nil
		}
		return "", nil
	}
	return "", nil
}

// majorMinor reads the leading "major.minor" of a plugin version.
func majorMinor(v string) (int, int, bool) {
	parts := strings.SplitN(strings.TrimSpace(v), ".", 3)
	if len(parts) < 2 {
		return 0, 0, false
	}
	a, err1 := strconv.Atoi(parts[0])
	b, err2 := strconv.Atoi(parts[1])
	if err1 != nil || err2 != nil || a < 0 || b < 0 {
		return 0, 0, false
	}
	return a, b, true
}

// mysqlKeyPreview is what the Preview of the MySQL keys adds: Debezium's
// warning on a recovery snapshot and what to verify after it.
func mysqlKeyPreview(todo map[string]string) []string {
	var out []string
	if v, ok := todo[keySnapshotMode]; ok && recoveryModes[v] {
		out = append(out,
			"a recovery snapshot rebuilds the schema history from the tables' CURRENT schema: it is safe only if no schema change happened since the stored position, which kafka_schema_history and the recreated topic cannot prove; events between that position and a later DDL would be read with the wrong schema",
			"verify after the restart: the connector is RUNNING (connect_status) and kafka_schema_history shows the history topic present with one partition and infinite retention; then set snapshot.mode back (the rollback)")
	}
	if v, ok := todo[keyServerID]; ok {
		out = append(out, fmt.Sprintf("the connector re-joins the source as replica id %s on its task restart; a server-id clash ends once no other reader uses the old id", v))
	}
	return out
}

// mysqlKeyUndo returns the value that undoes writing key, read from the
// connector's current config, or false when none can be written back: an
// unset snapshot.mode is Debezium's default initial, an unset
// database.server.id has no value to put back.
func mysqlKeyUndo(key string, cfg map[string]string) (string, bool) {
	v, set := cfg[key]
	switch key {
	case keySnapshotMode:
		if !set {
			return "initial", true
		}
		return v, recoveryModes[v] || restoreModes[v]
	case keyServerID:
		return v, set && codeValueCheck(key, v) == nil
	}
	return "", false
}
