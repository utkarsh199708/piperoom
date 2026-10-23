package evals

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"piperoom/internal/actions"
	"piperoom/internal/actions/sourceactions"
	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/wiring"
)

// The Q2 source admin actions on replay clients (Q2h).
//
// pg_set_replica_identity, source_create_heartbeat_table, source_grant_select
// and source_end_session are registered through wiring.RegisterSourceActions,
// production's registration, after the P3 actions (production's order), so a
// scenario can name them in expect.fix, expect.forbidden_actions and
// inputs[].action. Their Validate and Preview read the recorded stack through
// replaySource's Q2a reads (postgres.SourceSettingsReader plus
// BlockersOnTables), each answering what the matching tool recording
// answers: source_settings, source_table_replication, source_role_grants and
// source_blocking_sessions. Connector configs come from replayConfig, as for
// the P3 actions. The admin writer is replayWriter: every write is a no-op
// recorded in the run's writeLog.

var (
	_ postgres.SourceSettingsReader        = replaySource{}
	_ sourceactions.TableReplicationReader = replaySource{}
	_ sourceactions.RoleInfoReader         = replaySource{}
	_ sourceactions.BlockerReader          = replaySource{}
	_ sourceactions.ScopedBlockerReader    = replaySource{}
	_ postgres.SourceAdminWriter           = replayWriter{}
)

// registerSourceActions adds the Q2 source admin actions to reg through
// wiring.RegisterSourceActions over replay clients that read res and recs and
// record every write in wl.
func registerSourceActions(reg *actions.Registry, res Resources, recs []Recording, wl *writeLog) error {
	if err := wiring.RegisterSourceActions(reg, wiring.SourceActionDeps{
		Connect: replayConfig{res: res, recs: recs},
		Source:  replaySource{res: res, recs: recs},
		Admin:   replayWriter{log: wl},
	}); err != nil {
		return fmt.Errorf("evals: source action definitions: %w", err)
	}
	return nil
}

// matchedData returns the data of the recording of tool that the replayer
// would serve for a call with input call in the start state: the
// recording with the most input fields that all match call, one without a
// phase before a phased one (as firstData), the first in file order on a
// tie. An action's read then sees what the agent's tool call saw.
func matchedData(recs []Recording, tool string, call map[string]any) ([]byte, bool) {
	call = normalize(call).(map[string]any)
	for _, phased := range []bool{false, true} {
		best, bestScore := -1, -1
		for i, r := range recs {
			if r.Tool != tool || (r.Phase != "") != phased || !matches(r.Input, call) {
				continue
			}
			if recordingData(r) == nil {
				continue
			}
			if len(r.Input) > bestScore {
				best, bestScore = i, len(r.Input)
			}
		}
		if best < 0 {
			continue
		}
		b, err := json.Marshal(recordingData(recs[best]))
		if err != nil {
			return nil, false
		}
		return b, true
	}
	return nil, false
}

// Settings answers the source_settings recording.
func (s replaySource) Settings(context.Context) ([]postgres.DBSetting, error) {
	b, ok := matchedData(s.recs, "source_settings", map[string]any{})
	var out []postgres.DBSetting
	if !ok || json.Unmarshal(b, &out) != nil {
		return nil, fmt.Errorf("evals: source settings: %w (record source_settings with data)", errNotReplayed)
	}
	return out, nil
}

// replicaIdentityWords maps resources.tables' pg_class.relreplident letter
// to the word the Q2a reader returns.
var replicaIdentityWords = map[string]string{"": "default", "d": "default", "n": "nothing", "f": "full", "i": "index"}

// TableReplication answers each table from the source_table_replication
// recordings (start state first), else from resources.tables; a table in neither is not
// replayed (a recording covers the pipeline's tables only, so its absence
// does not prove the table is missing). A table resources.tables declares
// absent while it lists others is clients.ErrNotFound, as on the source.
func (s replaySource) TableReplication(_ context.Context, tables []string) ([]postgres.TableReplication, error) {
	recorded := map[string]postgres.TableReplication{}
	for _, phased := range []bool{false, true} {
		for _, r := range s.recs {
			if r.Tool != "source_table_replication" || (r.Phase != "") != phased {
				continue
			}
			b, err := json.Marshal(recordingData(r))
			if err != nil {
				continue
			}
			var rows []postgres.TableReplication
			if json.Unmarshal(b, &rows) != nil {
				continue
			}
			for _, t := range rows {
				if _, dup := recorded[t.Table]; !dup {
					recorded[t.Table] = t
				}
			}
		}
	}
	out := make([]postgres.TableReplication, 0, len(tables))
	for _, name := range tables {
		if t, ok := recorded[name]; ok {
			out = append(out, t)
			continue
		}
		if t, ok := s.res.Tables[name]; ok {
			out = append(out, postgres.TableReplication{Table: name, HasPrimaryKey: t.PrimaryKey, ReplicaIdentity: replicaIdentityWords[t.ReplicaIdentity]})
			continue
		}
		if len(s.res.Tables) > 0 {
			return nil, fmt.Errorf("evals: table %s: %w", name, clients.ErrNotFound)
		}
		return nil, fmt.Errorf("evals: replica identity of %s: %w (record source_table_replication or declare resources.tables)", name, errNotReplayed)
	}
	return out, nil
}

// roleGrantsRecord is the part of source_role_grants' Data the replay reads.
type roleGrantsRecord struct {
	ConnectorUser string            `json:"connector_user"`
	Role          postgres.RoleInfo `json:"role_info"`
}

// RoleInfo answers from the source_role_grants recording of role (the
// connector's database.user the tool read). Only a recorded role and its
// recorded tables are replayed: the tool never probes another role, so the
// scenario cannot say whether one exists.
func (s replaySource) RoleInfo(_ context.Context, role string, tables []string) (postgres.RoleInfo, error) {
	var info postgres.RoleInfo
	found := firstData(s.recs, "source_role_grants", func(_ Recording, b []byte) bool {
		var d roleGrantsRecord
		if json.Unmarshal(b, &d) != nil {
			return false
		}
		if d.ConnectorUser != role && d.Role.Role != role {
			return false
		}
		info = d.Role
		return true
	})
	if !found {
		return postgres.RoleInfo{}, fmt.Errorf("evals: role %q: %w (record source_role_grants for a connector whose database.user it is)", role, errNotReplayed)
	}
	out := postgres.RoleInfo{Role: role, Exists: info.Exists, CanLogin: info.CanLogin, Replication: info.Replication, Selectable: map[string]bool{}}
	for _, t := range tables {
		sel, ok := info.Selectable[t]
		if !ok && info.Exists {
			return postgres.RoleInfo{}, fmt.Errorf("evals: SELECT of role %q on %s: %w (source_role_grants recorded the pipeline's tables only)", role, t, errNotReplayed)
		}
		out.Selectable[t] = sel
	}
	return out, nil
}

// Blockers answers what source_blocking_sessions answers for the same
// older_than_minutes (the action's olderThan, in whole minutes).
func (s replaySource) Blockers(_ context.Context, olderThan time.Duration) ([]postgres.DBSession, error) {
	return s.blockers(olderThan)
}

// BlockersOnTables is Blockers: the recordings come from a room, so they
// are already scoped to the scenario's pipeline (in_scope as recorded).
func (s replaySource) BlockersOnTables(_ context.Context, olderThan time.Duration, _ []string) ([]postgres.DBSession, error) {
	return s.blockers(olderThan)
}

func (s replaySource) blockers(olderThan time.Duration) ([]postgres.DBSession, error) {
	call := map[string]any{"older_than_minutes": int(olderThan / time.Minute)}
	b, ok := matchedData(s.recs, "source_blocking_sessions", call)
	var out []postgres.DBSession
	if !ok || json.Unmarshal(b, &out) != nil {
		return nil, fmt.Errorf("evals: blocking sessions older than %s: %w (record source_blocking_sessions with data for that older_than_minutes)", olderThan, errNotReplayed)
	}
	return out, nil
}

// SetReplicaIdentity records pg_set_replica_identity's write.
func (w replayWriter) SetReplicaIdentity(_ context.Context, table, mode, index string) error {
	w.log.record("SetReplicaIdentity", map[string]any{"table": table, "mode": mode, "index": index})
	return nil
}

// GrantSelect records source_grant_select's write.
func (w replayWriter) GrantSelect(_ context.Context, role string, tables []string) error {
	w.log.record("GrantSelect", map[string]any{"role": role, "tables": append([]string(nil), tables...)})
	return nil
}

// CreateHeartbeatTable records source_create_heartbeat_table's write.
func (w replayWriter) CreateHeartbeatTable(_ context.Context, table string) error {
	w.log.record("CreateHeartbeatTable", map[string]any{"table": table})
	return nil
}

// EndSession records source_end_session's write.
func (w replayWriter) EndSession(_ context.Context, pid int64, startedAt time.Time) error {
	w.log.record("EndSession", map[string]any{"pid": pid, "started_at": startedAt.UTC().Format(time.RFC3339Nano)})
	return nil
}
