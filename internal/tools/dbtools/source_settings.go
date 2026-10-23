package dbtools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"

	"piperoom/internal/clients/postgres"
	"piperoom/internal/tools"
	"piperoom/internal/topology"
)

// Caps of the source_* reads. A pipeline with more source tables than
// maxSourceTables is refused before any I/O rather than silently half read.
const (
	maxSourceTables     = 500
	maxReplicationLines = 100
	maxBlockerSessions  = 100
	maxBlockerLines     = 20
)

// sourceSettingsClient is the read side of the Q2a source reader. The
// interface and its DTOs live in internal/clients/postgres until Q0 adopts
// them into clients.SourceSettingsReader (contract change request in the PR);
// the tool finds the implementation by type assertion, so a source client
// without it (MySQL today, a fake) is refused before any I/O.
type sourceSettingsClient interface {
	Settings(ctx context.Context) ([]postgres.DBSetting, error)
	TableReplication(ctx context.Context, tables []string) ([]postgres.TableReplication, error)
	RoleInfo(ctx context.Context, role string, tables []string) (postgres.RoleInfo, error)
	Blockers(ctx context.Context, olderThan time.Duration) ([]postgres.DBSession, error)
}

// scopedBlockersClient is the scoped variant of Blockers: in a room scoped to
// a pipeline, only sessions touching the pipeline's tables may show their
// identity, so a client that cannot filter is refused rather than read in
// full (the pg_long_transactions rule).
type scopedBlockersClient interface {
	BlockersOnTables(ctx context.Context, olderThan time.Duration, tables []string) ([]postgres.DBSession, error)
}

// settingsReader returns the source client's settings reader, or an error
// naming the missing capability.
func (d Deps) settingsReader(tool string) (sourceSettingsClient, error) {
	c, ok := d.Src.(sourceSettingsClient)
	if !ok {
		return nil, fmt.Errorf("dbtools: %s: the source client cannot read database settings", tool)
	}
	return c, nil
}

// oneLineShort folds whitespace and control characters to single spaces and
// cuts the result to at most n runes. Identity fields that reach a summary
// (usename, application_name, a connector's database.user) are free text
// chosen outside piperoom: a name holding a newline must not be able to add
// lines of its own to the summary the model reads.
func oneLineShort(s string, n int) string {
	s = strings.Join(strings.FieldsFunc(s, func(r rune) bool {
		return unicode.IsSpace(r) || unicode.IsControl(r)
	}), " ")
	return cutShort(s, n)
}

// optionalSetting reports whether a setting is expected only on some servers:
// rds.logical_replication exists on RDS/Aurora alone, so its absence is normal
// and is not reported.
func optionalSetting(name string) bool { return name == "rds.logical_replication" }

// settingsSummary renders the settings in the fixed order, adding a line for a
// required setting the server does not report and appending any setting the
// client returned beyond the fixed list (so a newer client is not hidden).
func settingsSummary(settings []postgres.DBSetting) string {
	byName := make(map[string]postgres.DBSetting, len(settings))
	for _, s := range settings {
		byName[s.Name] = s
	}
	var sb strings.Builder
	seen := make(map[string]bool, len(settings))
	for _, name := range postgres.SettingNames() {
		s, ok := byName[name]
		if !ok {
			if !optionalSetting(name) {
				fmt.Fprintf(&sb, "%s: not reported by pg_settings\n", name)
			}
			continue
		}
		seen[name] = true
		writeSetting(&sb, s)
	}
	for _, s := range settings {
		if !seen[s.Name] {
			seen[s.Name] = true
			writeSetting(&sb, s)
		}
	}
	if sb.Len() == 0 {
		sb.WriteString("(the server reports none of the CDC settings)\n")
	}
	return sb.String()
}

// writeSetting folds name, value and unit to one line each: the real client
// reads them from pg_settings, but a fake or future client must not be able
// to add summary lines of its own.
func writeSetting(sb *strings.Builder, s postgres.DBSetting) {
	fmt.Fprintf(sb, "%s=%s", oneLineShort(s.Name, 64), oneLineShort(s.Value, 128))
	if s.Unit != "" {
		sb.WriteString(" " + oneLineShort(s.Unit, 16))
	}
	if s.PendingRestart {
		sb.WriteString(" (pending_restart: the configured value is not the running one)")
	}
	sb.WriteString("\n")
}

// sourceSettingsTool reports the wal_level/replication settings a CDC source
// needs, with pending_restart.
func sourceSettingsTool(d Deps) tools.Tool {
	return &tool{
		name:   "source_settings",
		desc:   "Read the CDC-relevant settings of the source Postgres: wal_level, max_replication_slots, max_wal_senders, max_slot_wal_keep_size and wal_sender_timeout, and rds.logical_replication on RDS/Aurora, each with pending_restart (a changed value that needs a restart). Read it before proposing a configuration change.",
		schema: objSchema(map[string]any{}),
		run: func(ctx context.Context, _ json.RawMessage) (tools.Result, error) {
			if _, err := d.scope(ctx); err != nil {
				return tools.Result{}, err
			}
			c, err := d.settingsReader("source_settings")
			if err != nil {
				return tools.Result{}, err
			}
			ctx, cancel := toolCtx(ctx)
			defer cancel()

			settings, err := c.Settings(ctx)
			if err != nil {
				return tools.Result{}, fmt.Errorf("dbtools: source_settings: %w", err)
			}
			summary, truncated := tools.Truncate(settingsSummary(settings))
			return tools.Result{Summary: summary, Data: settings, Truncated: truncated}, nil
		},
	}
}

// sourceTableReplicationTool reports replica identity and primary key of each
// pipeline source table.
func sourceTableReplicationTool(d Deps) tools.Tool {
	return &tool{
		name: "source_table_replication",
		desc: "For each source table of the pipeline, show its replica identity (default, full, index or nothing) and whether it has a primary key. Debezium's Postgres connector needs a primary key or REPLICA IDENTITY FULL to emit update and delete changes; a table without either shows here.",
		schema: objSchema(map[string]any{
			"pipeline_id": strProp("Pipeline id from owners.yaml."),
		}, "pipeline_id"),
		run: func(ctx context.Context, in json.RawMessage) (tools.Result, error) {
			var input struct {
				PipelineID string `json:"pipeline_id"`
			}
			if err := decode(in, &input); err != nil {
				return tools.Result{}, err
			}
			if input.PipelineID == "" {
				return tools.Result{}, fmt.Errorf("%w: pipeline_id is required", tools.ErrBadInput)
			}
			sc, err := d.scope(ctx)
			if err != nil {
				return tools.Result{}, err
			}
			p, err := d.resolve(sc, input.PipelineID)
			if err != nil {
				return tools.Result{}, err
			}
			tables, err := pipelineTables(p.ID, p.SourceTables)
			if err != nil {
				return tools.Result{}, err
			}
			c, err := d.settingsReader("source_table_replication")
			if err != nil {
				return tools.Result{}, err
			}
			if len(tables) == 0 {
				return tools.Result{Summary: fmt.Sprintf("(pipeline %s has no source tables)\n", p.ID), Data: []postgres.TableReplication{}}, nil
			}

			ctx, cancel := toolCtx(ctx)
			defer cancel()
			repl, err := c.TableReplication(ctx, tables)
			if err != nil {
				return tools.Result{}, fmt.Errorf("dbtools: source_table_replication: %w", err)
			}

			var sb strings.Builder
			var noPK, nothing int
			for _, t := range repl {
				if !t.HasPrimaryKey {
					noPK++
				}
				if t.ReplicaIdentity == "nothing" {
					nothing++
				}
			}
			fmt.Fprintf(&sb, "tables=%d without_primary_key=%d replica_identity_nothing=%d\n", len(repl), noPK, nothing)
			shown := min(len(repl), maxReplicationLines)
			for _, t := range repl[:shown] {
				fmt.Fprintf(&sb, "%s: primary_key=%s replica_identity=%s\n", t.Table, yesNo(t.HasPrimaryKey), orDash(t.ReplicaIdentity))
			}
			more := len(repl) - shown
			if more > 0 {
				fmt.Fprintf(&sb, "(%d more %s, in Data)\n", more, tools.Plural(more, "table", "tables"))
			}
			summary, truncated := tools.Truncate(sb.String())
			return tools.Result{Summary: summary, Data: repl, Truncated: truncated || more > 0}, nil
		},
	}
}

// pipelineTables validates the pipeline's source tables and refuses a pipeline
// with more tables than one read may cover.
func pipelineTables(pipelineID string, tables []string) ([]string, error) {
	if len(tables) > maxSourceTables {
		return nil, fmt.Errorf("%w: pipeline %s has %d source tables, at most %d per read", tools.ErrBadInput, pipelineID, len(tables), maxSourceTables)
	}
	for _, t := range tables {
		if !validTable(t) {
			return nil, fmt.Errorf("%w: pipeline %s lists source table %q, which is not \"schema.table\"", tools.ErrBadInput, pipelineID, cutShort(t, 80))
		}
	}
	return tables, nil
}

// roleGrantsData is the structured Data of source_role_grants: the connector
// whose database.user was read and where that role came from, plus the role's
// rights.
type roleGrantsData struct {
	Connector           string            `json:"connector"`
	ConnectorUser       string            `json:"connector_user"`
	ConnectorUserSource string            `json:"connector_user_source"`
	Role                postgres.RoleInfo `json:"role_info"`
}

// sourceRoleGrantsTool reports the connector role's login, REPLICATION and
// per-table SELECT rights.
func sourceRoleGrantsTool(d Deps) tools.Tool {
	return &tool{
		name: "source_role_grants",
		desc: "For the pipeline's Debezium connector role (database.user from its Connect config), show whether the role exists, can log in, has the REPLICATION privilege and can SELECT each source table. Use for connector authentication, replication and permission failures.",
		schema: objSchema(map[string]any{
			"pipeline_id": strProp("Pipeline id from owners.yaml."),
		}, "pipeline_id"),
		run: func(ctx context.Context, in json.RawMessage) (tools.Result, error) {
			var input struct {
				PipelineID string `json:"pipeline_id"`
			}
			if err := decode(in, &input); err != nil {
				return tools.Result{}, err
			}
			if input.PipelineID == "" {
				return tools.Result{}, fmt.Errorf("%w: pipeline_id is required", tools.ErrBadInput)
			}
			sc, err := d.scope(ctx)
			if err != nil {
				return tools.Result{}, err
			}
			p, err := d.resolve(sc, input.PipelineID)
			if err != nil {
				return tools.Result{}, err
			}
			tables, err := pipelineTables(p.ID, p.SourceTables)
			if err != nil {
				return tools.Result{}, err
			}
			ctx, cancel := toolCtx(ctx)
			defer cancel()

			// The role is the connector's database.user, never model input: no
			// probe of arbitrary roles.
			conn, err := resolveConnector(ctx, "source_role_grants", p, d.Connect)
			if err != nil {
				return tools.Result{}, err
			}
			c, err := d.settingsReader("source_role_grants")
			if err != nil {
				return tools.Result{}, err
			}
			info, err := c.RoleInfo(ctx, conn.role, tables)
			if err != nil {
				return tools.Result{}, fmt.Errorf("dbtools: source_role_grants: %w", err)
			}

			var sb strings.Builder
			// The role is the connector config's database.user: external text,
			// folded to one line before it joins the summary.
			fmt.Fprintf(&sb, "connector %s role %s: ", conn.name, oneLineShort(conn.role, 64))
			if !info.Exists {
				sb.WriteString("the role does not exist (connector config database.user)\n")
			} else {
				fmt.Fprintf(&sb, "exists=yes login=%s replication=%s\n", yesNo(info.CanLogin), yesNo(info.Replication))
			}
			var granted int
			for _, t := range tables {
				if info.Selectable[t] {
					granted++
				}
			}
			fmt.Fprintf(&sb, "selectable tables: %d of %d\n", granted, len(tables))
			shown := min(len(tables), maxReplicationLines)
			for _, t := range tables[:shown] {
				fmt.Fprintf(&sb, "%s: selectable=%s\n", t, yesNo(info.Selectable[t]))
			}
			more := len(tables) - shown
			if more > 0 {
				fmt.Fprintf(&sb, "(%d more %s, in Data)\n", more, tools.Plural(more, "table", "tables"))
			}
			summary, truncated := tools.Truncate(sb.String())
			return tools.Result{
				Summary: summary,
				Data: roleGrantsData{
					Connector:           conn.name,
					ConnectorUser:       conn.role,
					ConnectorUserSource: "connector " + conn.name + " config database.user",
					Role:                info,
				},
				Truncated: truncated || more > 0,
			}, nil
		},
	}
}

// sourceBlockingSessionsTool lists the sessions that can hold back logical
// replication, with the query redacted and cut.
func sourceBlockingSessionsTool(d Deps) tools.Tool {
	return &tool{
		name: "source_blocking_sessions",
		desc: "List the source Postgres sessions that hold back logical replication: an open transaction older than older_than_minutes, or a session holding the server's oldest xid horizon (a logical slot cannot advance past it, and CREATE_REPLICATION_SLOT waits for it). Each row has pid, start times, user, application, state, whether it blocks a slot, and its query redacted and cut to 200 bytes as untrusted text; a session outside the pipeline's tables is listed with identity hidden. Prepared transactions and sessions the connecting role may not see (pg_read_all_stats) are not listed. Use it when a slot keeps WAL or a slot creation hangs; ending a session is an action, not this tool.",
		schema: objSchema(map[string]any{
			"older_than_minutes": intProp("Minimum transaction age in minutes, 1-43200. Default 5."),
		}),
		run: func(ctx context.Context, in json.RawMessage) (tools.Result, error) {
			var input struct {
				OlderThanMinutes *int `json:"older_than_minutes"`
			}
			if err := decode(in, &input); err != nil {
				return tools.Result{}, err
			}
			minutes, err := olderThanMinutes(input.OlderThanMinutes)
			if err != nil {
				return tools.Result{}, err
			}
			sc, err := d.scope(ctx)
			if err != nil {
				return tools.Result{}, err
			}
			c, err := d.settingsReader("source_blocking_sessions")
			if err != nil {
				return tools.Result{}, err
			}

			ctx, cancel := toolCtx(ctx)
			defer cancel()
			var sessions []postgres.DBSession
			if sc.Unscoped() {
				sessions, err = c.Blockers(ctx, time.Duration(minutes)*time.Minute)
			} else {
				// Scoped: identity (user, application, query) is shown only for
				// sessions touching the pipeline's tables; every other session
				// belongs to another pipeline, user or database and appears
				// anonymised. A client that cannot filter is refused, never
				// read in full (the pg_long_transactions rule).
				l, ok := d.Src.(scopedBlockersClient)
				if !ok {
					return tools.Result{}, errors.New("dbtools: source_blocking_sessions: the source client cannot scope sessions to the pipeline's tables, so none are shown")
				}
				tables, terr := pipelineTables(sc.Pipeline().ID, sc.Pipeline().SourceTables)
				if terr != nil {
					return tools.Result{}, terr
				}
				sessions, err = l.BlockersOnTables(ctx, time.Duration(minutes)*time.Minute, tables)
			}
			if err != nil {
				return tools.Result{}, fmt.Errorf("dbtools: source_blocking_sessions: %w", err)
			}

			// The client redacts on the way out; do it again so a fake or a
			// future client cannot put an unredacted query in the room.
			rows := make([]postgres.DBSession, 0, len(sessions))
			for _, s := range sessions {
				if !s.InScope {
					// The client already blanks these in SQL; enforce it here
					// too so a fake or future client cannot hand another
					// pipeline's identity to the room.
					s.User, s.App, s.Query = "", "", ""
				}
				s.Query = postgres.RedactSessionQuery(s.Query)
				// usename and application_name are chosen by whoever connects
				// to the source: fold control characters so a crafted name
				// cannot add lines to the summary.
				s.User = oneLineShort(s.User, 64)
				s.App = oneLineShort(s.App, 64)
				if !backendStates[s.State] {
					// State is a fixed Postgres vocabulary; anything else is
					// free text from a client that is not the real one.
					s.State = "?"
				}
				rows = append(rows, s)
			}

			total := len(rows)
			data := rows[:min(total, maxBlockerSessions)]
			shown := min(len(data), maxBlockerLines)

			var sb strings.Builder
			now := time.Now()
			for _, s := range data[:shown] {
				age := now.Sub(s.XactStart)
				if s.XactStart.IsZero() {
					age = now.Sub(s.StartedAt)
				}
				if !s.InScope {
					fmt.Fprintf(&sb, "pid=%d state=%s age=%s blocks_slot=%s (outside the pipeline's tables; identity hidden)\n",
						s.PID, orDash(s.State), topology.HumanAge(age), yesNo(s.BlocksSlot))
					continue
				}
				fmt.Fprintf(&sb, "pid=%d user=%s app=%s state=%s age=%s blocks_slot=%s query=%q\n",
					s.PID, orDash(s.User), orDash(s.App), orDash(s.State), topology.HumanAge(age), yesNo(s.BlocksSlot), s.Query)
			}
			if total == 0 {
				sb.WriteString("(no blocking sessions visible to this role; other sessions' transactions need pg_read_all_stats, and prepared transactions are not listed)\n")
			}
			if more := len(data) - shown; more > 0 {
				fmt.Fprintf(&sb, "(%d more %s, in Data)\n", more, tools.Plural(more, "session", "sessions"))
			}
			if more := total - len(data); more > 0 {
				fmt.Fprintf(&sb, "(%d more sessions not shown)\n", more)
			}
			summary, cut := tools.Truncate(sb.String())
			return tools.Result{Summary: summary, Data: data, Truncated: cut || total > len(data)}, nil
		},
	}
}
