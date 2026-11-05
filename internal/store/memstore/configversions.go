package memstore

import (
	"context"
	"fmt"
	"maps"
	"time"

	"piperoom/internal/clients"
	"piperoom/internal/store"
)

var _ store.ConfigVersionStore = (*Store)(nil)

// connKey names one connector of the in-memory connector_config_versions
// table (migration 00020). The table sits under Store.mu.
type connKey struct{ pipeline, connector string }

// cfgVersion is one stored version; id orders versions with the same at.
type cfgVersion struct {
	id int64
	v  clients.ConnectorConfigVersion
}

// configVersionTable holds every connector's versions, oldest first by (at, id).
type configVersionTable struct {
	byConn map[connKey][]cfgVersion
	nextID int64
}

// newer reports whether a sorts after b by (at, id).
func newer(a, b cfgVersion) bool {
	if !a.v.At.Equal(b.v.At) {
		return a.v.At.After(b.v.At)
	}
	return a.id > b.id
}

// RecordConfigVersion implements clients.ConfigVersionWriter with the rules
// of store.PG: no row when the connector's newest version has the same hash
// and v has no ChangedKeys; at most store.MaxConfigVersionsPerConnector kept.
func (s *Store) RecordConfigVersion(ctx context.Context, v clients.ConnectorConfigVersion) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, fmt.Errorf("memstore: record config version: %w", err)
	}
	v, err := store.NormalizeConfigVersion(v)
	if err != nil {
		return false, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	t := &s.cfgVersions
	if t.byConn == nil {
		t.byConn = map[connKey][]cfgVersion{}
	}
	k := connKey{v.PipelineID, v.Connector}
	vs := t.byConn[k]
	if n := len(vs); n > 0 && len(v.ChangedKeys) == 0 && vs[n-1].v.Hash == v.Hash {
		return false, nil
	}
	t.nextID++
	row := cfgVersion{id: t.nextID, v: v}
	i := len(vs)
	for i > 0 && newer(vs[i-1], row) {
		i--
	}
	vs = append(vs, cfgVersion{})
	copy(vs[i+1:], vs[i:])
	vs[i] = row
	if over := len(vs) - store.MaxConfigVersionsPerConnector; over > 0 {
		vs = append([]cfgVersion{}, vs[over:]...)
	}
	t.byConn[k] = vs
	return true, nil
}

// ConfigVersions implements clients.ConfigVersionReader (newest first, at
// most limit, Config empty).
func (s *Store) ConfigVersions(ctx context.Context, pipelineID, connector string, limit int) ([]clients.ConnectorConfigVersion, error) {
	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("memstore: list config versions: %w", err)
	}
	if err := store.CheckConfigVersionLimit(limit); err != nil {
		return nil, err
	}
	if err := store.CheckText(pipelineID, connector); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	vs := s.cfgVersions.byConn[connKey{pipelineID, connector}]
	out := []clients.ConnectorConfigVersion{}
	for i := len(vs) - 1; i >= 0 && len(out) < limit; i-- {
		v := vs[i].v
		v.Config = nil
		v.ChangedKeys = append([]string{}, v.ChangedKeys...)
		out = append(out, v)
	}
	return out, nil
}

// ConfigVersion implements clients.ConfigVersionReader (the newest version
// with that hash; none is clients.ErrNotFound).
func (s *Store) ConfigVersion(ctx context.Context, pipelineID, connector, hash string) (clients.ConnectorConfigVersion, error) {
	if err := ctx.Err(); err != nil {
		return clients.ConnectorConfigVersion{}, fmt.Errorf("memstore: get config version: %w", err)
	}
	if err := store.CheckText(pipelineID, connector, hash); err != nil {
		return clients.ConnectorConfigVersion{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	vs := s.cfgVersions.byConn[connKey{pipelineID, connector}]
	for i := len(vs) - 1; i >= 0; i-- {
		if v := vs[i].v; v.Hash == hash {
			v.Config = maps.Clone(v.Config)
			v.ChangedKeys = append([]string{}, v.ChangedKeys...)
			return v, nil
		}
	}
	return clients.ConnectorConfigVersion{}, fmt.Errorf("memstore: config version of %s/%s: %w", pipelineID, connector, clients.ErrNotFound)
}

// PurgeConfigVersionsBefore implements store.ConfigVersionStore: every
// version older than cutoff goes except each connector's newest.
func (s *Store) PurgeConfigVersionsBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	if err := ctx.Err(); err != nil {
		return 0, fmt.Errorf("memstore: purge config versions: %w", err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	var n int64
	for k, vs := range s.cfgVersions.byConn {
		keep := vs[:0:0]
		for i, v := range vs {
			if i < len(vs)-1 && v.v.At.Before(cutoff) {
				n++
				continue
			}
			keep = append(keep, v)
		}
		s.cfgVersions.byConn[k] = keep
	}
	return n, nil
}
