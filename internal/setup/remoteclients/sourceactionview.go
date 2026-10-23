package remoteclients

import (
	"context"
	"time"

	"piperoom/internal/actions/sourceactions"
	"piperoom/internal/clients"
)

// ActionView is the source client as package sourceactions' local Q2a DTOs
// see it. That package declares its own copies of TableReplication, RoleInfo
// and DBSession instead of aliasing the postgres types, and Go has no return
// type covariance, so one type cannot satisfy both postgres.SourceSettingsReader
// and the action's TableReplicationReader/RoleInfoReader/BlockerReader. The
// view embeds *Source (so clients.SourceDB is promoted) and answers the same
// ops with the action's types; Source still serves the Q2a tools and dbtools,
// which read the postgres types.
//
// This is the local adapter AGENTS.md §2 asks for, not a workaround hidden in
// a caller: Pipeline wiring passes Source.ActionView() to
// sourceactions.ReplicaIdentityDeps.Source and GrantSelectDeps.Source, and the
// PR carries a contract change request to make sourceactions' DTOs aliases of
// the postgres ones, after which the view is no longer needed.
type ActionView struct{ *Source }

var (
	_ clients.SourceDB                     = (*ActionView)(nil)
	_ sourceactions.TableReplicationReader = (*ActionView)(nil)
	_ sourceactions.RoleInfoReader         = (*ActionView)(nil)
	_ sourceactions.BlockerReader          = (*ActionView)(nil)
)

// ActionView returns the sourceactions view of this client.
func (s *Source) ActionView() *ActionView { return &ActionView{Source: s} }

// TableReplication reads each table's replica identity with the action's DTO.
func (v *ActionView) TableReplication(ctx context.Context, tables []string) ([]sourceactions.TableReplication, error) {
	rows, err := v.Source.TableReplication(ctx, tables)
	if err != nil {
		return nil, err
	}
	out := make([]sourceactions.TableReplication, len(rows))
	for i, r := range rows {
		out[i] = sourceactions.TableReplication{
			Table:           r.Table,
			HasPrimaryKey:   r.HasPrimaryKey,
			ReplicaIdentity: r.ReplicaIdentity,
		}
	}
	return out, nil
}

// RoleInfo reads a role's rights and per-table SELECT privilege with the
// action's DTO.
func (v *ActionView) RoleInfo(ctx context.Context, role string, tables []string) (sourceactions.RoleInfo, error) {
	info, err := v.Source.RoleInfo(ctx, role, tables)
	if err != nil {
		return sourceactions.RoleInfo{}, err
	}
	return sourceactions.RoleInfo{
		Role:        info.Role,
		Exists:      info.Exists,
		CanLogin:    info.CanLogin,
		Replication: info.Replication,
		Selectable:  info.Selectable,
	}, nil
}

// Blockers lists the sessions that can hold back logical replication with the
// action's DTO.
func (v *ActionView) Blockers(ctx context.Context, olderThan time.Duration) ([]sourceactions.DBSession, error) {
	rows, err := v.Source.Blockers(ctx, olderThan)
	if err != nil {
		return nil, err
	}
	out := make([]sourceactions.DBSession, len(rows))
	for i, r := range rows {
		out[i] = sourceactions.DBSession{
			PID:        r.PID,
			StartedAt:  r.StartedAt,
			XactStart:  r.XactStart,
			User:       r.User,
			App:        r.App,
			State:      r.State,
			Query:      r.Query,
			BlocksSlot: r.BlocksSlot,
		}
	}
	return out, nil
}
