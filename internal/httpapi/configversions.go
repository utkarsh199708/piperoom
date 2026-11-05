package httpapi

import (
	"context"
	"errors"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"

	"piperoom/internal/clients"
)

// Bounds of GET /api/pipelines/{pid}/connectors/{name}/config-versions (X5c).
const (
	configVersionsDefaultLimit = 50
	// configVersionsReadTimeout is the store read deadline of docs/api.md.
	configVersionsReadTimeout = 10 * time.Second
	// maxConnectorNameLen bounds {name}; a longer one is no connector.
	maxConnectorNameLen = 255
)

// WithConfigVersions serves the config-versions routes from r (X5c; the room
// store's connector_config_versions). Without it the list answers [] for a
// connector owners.yaml names and the diff answers 404 "version not found".
func WithConfigVersions(r clients.ConfigVersionReader) Option {
	return func(s *Server) { s.configVersions = r }
}

// configVersionRow is one ConfigVersion of docs/api.md: key names only.
type configVersionRow struct {
	Hash        string    `json:"hash"`
	At          time.Time `json:"at"`
	By          string    `json:"by"`
	ChangedKeys []string  `json:"changed_keys"`
}

// configDiffEnd is the from or to side of a ConfigDiff.
type configDiffEnd struct {
	Hash string    `json:"hash"`
	At   time.Time `json:"at"`
}

// configDiffKey is one key of a ConfigDiff. Old and New are pointers so an
// empty value still shows; Old is absent for "added", New for "removed".
type configDiffKey struct {
	Key    string  `json:"key"`
	Change string  `json:"change"`
	Old    *string `json:"old,omitempty"`
	New    *string `json:"new,omitempty"`
}

// configDiff is the ConfigDiff of docs/api.md.
type configDiff struct {
	Connector string          `json:"connector"`
	From      configDiffEnd   `json:"from"`
	To        configDiffEnd   `json:"to"`
	Keys      []configDiffKey `json:"keys"`
}

// handleConfigVersions serves GET
// /api/pipelines/{pid}/connectors/{name}/config-versions[?limit=|?diff=a..b]
// (docs/api.md, Fleet view). Visibility first: an unknown pipeline or one
// outside the caller's tenants is 404 "pipeline not found", then a name that
// is neither in owners.yaml's connectors: nor recorded for the pipeline is
// 404 "connector not found". The list (key names only) is for any logged-in
// user; the diff shows redacted values, so it needs the /api/usage rule
// (403 not_allowed) before its own 400 and 404. Only Piperoom's own table is
// read.
func (s *Server) handleConfigVersions(w http.ResponseWriter, r *http.Request) {
	pid, name := chi.URLParam(r, "pid"), chi.URLParam(r, "name")
	u := userFrom(r.Context())
	if !s.svc.UsagePipelineVisible(u, pid) {
		writeError(w, http.StatusNotFound, "not_found", "pipeline not found")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), configVersionsReadTimeout)
	defer cancel()
	known, err := s.connectorKnown(ctx, pid, name)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	if !known {
		writeError(w, http.StatusNotFound, "not_found", "connector not found")
		return
	}
	q := r.URL.Query()
	if q.Has("diff") {
		if !s.svc.CanReadPipelineUsage(u, pid) {
			writeError(w, http.StatusForbidden, "not_allowed", "a config diff shows values: it needs an approver or backup of one of the pipeline's layers, or the auditor role")
			return
		}
		s.serveConfigDiff(ctx, w, pid, name, q.Get("diff"))
		return
	}
	limit := configVersionsDefaultLimit
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > clients.MaxConfigVersionList {
			s.writeErr(w, validation("limit must be a whole number from 1 to "+strconv.Itoa(clients.MaxConfigVersionList)))
			return
		}
		limit = n
	}
	out := []configVersionRow{}
	if s.configVersions == nil {
		writeJSON(w, http.StatusOK, out)
		return
	}
	vs, err := s.configVersions.ConfigVersions(ctx, pid, name, limit)
	if err != nil {
		s.writeErr(w, err)
		return
	}
	for _, v := range vs {
		keys := v.ChangedKeys
		if keys == nil {
			keys = []string{}
		}
		out = append(out, configVersionRow{Hash: v.Hash, At: v.At, By: v.By, ChangedKeys: keys})
	}
	writeJSON(w, http.StatusOK, out)
}

// connectorKnown reports whether name is a connector of the pipeline:
// owners.yaml lists it, or the store holds a version of it. A name that
// cannot be a stored connector (empty, too long, not UTF-8, control
// characters) is not one, and the store is not asked.
func (s *Server) connectorKnown(ctx context.Context, pid, name string) (bool, error) {
	if name == "" || len(name) > maxConnectorNameLen || !utf8.ValidString(name) ||
		strings.ContainsFunc(name, unicode.IsControl) {
		return false, nil
	}
	if s.svc.PipelineListsConnector(pid, name) {
		return true, nil
	}
	if s.configVersions == nil {
		return false, nil
	}
	vs, err := s.configVersions.ConfigVersions(ctx, pid, name, 1)
	if err != nil {
		return false, err
	}
	return len(vs) > 0, nil
}

// isHash reports whether h is a full 64-hex config hash (lowercase, as
// actions.ConfigHash writes it).
func isHash(h string) bool {
	if len(h) != 64 {
		return false
	}
	for i := 0; i < len(h); i++ {
		if c := h[i]; !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

// serveConfigDiff answers ?diff=a..b: 400 for a malformed value, 404
// "version not found" for a hash that is no version of the connector, else
// the key-by-key difference of the stored (redacted) configs from a to b.
func (s *Server) serveConfigDiff(ctx context.Context, w http.ResponseWriter, pid, name, raw string) {
	a, b, ok := strings.Cut(strings.ToLower(raw), "..")
	if !ok || !isHash(a) || !isHash(b) {
		s.writeErr(w, validation("diff must be two 64-hex config hashes joined by '..'"))
		return
	}
	if s.configVersions == nil {
		writeError(w, http.StatusNotFound, "not_found", "version not found")
		return
	}
	from, err := s.configVersions.ConfigVersion(ctx, pid, name, a)
	if err == nil {
		var to clients.ConnectorConfigVersion
		if to, err = s.configVersions.ConfigVersion(ctx, pid, name, b); err == nil {
			writeJSON(w, http.StatusOK, configDiff{
				Connector: name,
				From:      configDiffEnd{Hash: from.Hash, At: from.At},
				To:        configDiffEnd{Hash: to.Hash, At: to.At},
				Keys:      diffConfigs(from.Config, to.Config),
			})
			return
		}
	}
	if errors.Is(err, clients.ErrNotFound) {
		writeError(w, http.StatusNotFound, "not_found", "version not found")
		return
	}
	s.writeErr(w, err)
}

// diffConfigs lists the keys added, removed or changed from old to cur,
// sorted by key, at most clients.MaxConfigKeys. The values are the stored,
// redacted ones.
func diffConfigs(old, cur map[string]string) []configDiffKey {
	keys := slices.Sorted(maps.Keys(old))
	for k := range cur {
		if _, ok := old[k]; !ok {
			keys = append(keys, k)
		}
	}
	slices.Sort(keys)
	out := []configDiffKey{}
	for _, k := range keys {
		if len(out) == clients.MaxConfigKeys {
			break
		}
		ov, inOld := old[k]
		nv, inCur := cur[k]
		switch {
		case !inOld:
			out = append(out, configDiffKey{Key: k, Change: "added", New: &nv})
		case !inCur:
			out = append(out, configDiffKey{Key: k, Change: "removed", Old: &ov})
		case ov != nv:
			out = append(out, configDiffKey{Key: k, Change: "changed", Old: &ov, New: &nv})
		}
	}
	return out
}
