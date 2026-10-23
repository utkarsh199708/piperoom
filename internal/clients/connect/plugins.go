package connect

import (
	"context"
	"net/http"
)

var _ PluginReader = (*Client)(nil)

// Plugin is one entry of GET /connector-plugins: the plugin's class, its type
// and, from Kafka Connect 3.2 on, the version it ships with.
//
// It is the Q0 clients.ConnectPlugin shape from tasks/Q-board.md §1, declared
// here until that contract lands; the PR carries a contract change request
// with the exact clients.ConnectPlugin to move it.
type Plugin struct {
	Class string `json:"class"`
	// Type is one of source, sink, converter, header_converter,
	// transformation or predicate. A Connect older than 3.2 lists only source
	// and sink plugins and may leave it empty.
	Type string `json:"type"`
	// Version is empty on a Connect older than 3.2, which does not report
	// plugin versions.
	Version string `json:"version"`
}

// PluginReader lists the Connect worker's plugins. It is optional: a client
// without it does not get the connect_plugins tool, so the model cannot call
// it in vain. It is separate from ConnectInfoReader so that interface keeps
// its Q5a semantics (ServerInfo only) and every existing fake still compiles.
type PluginReader interface {
	// Plugins lists every plugin the worker knows: connectors, converters,
	// transforms and predicates on Kafka Connect 3.2+, connectors only on an
	// older worker (see the connect_plugins tool, which marks that case).
	Plugins(ctx context.Context) ([]Plugin, error)
}

// Plugins reads GET /connector-plugins?connectorsOnly=false. The query asks
// Kafka Connect 3.2+ for converters, transforms and predicates too; an older
// worker ignores the unknown parameter and answers its connector plugins
// only, without versions. The response is a top-level JSON array of
// {class, type, version} (apache/kafka ConnectorPluginsResource.listConnectorPlugins).
// It is read-only: it lists what is installed and changes nothing.
func (c *Client) Plugins(ctx context.Context) ([]Plugin, error) {
	var out []Plugin
	// The query is not a path segment, so netsec.CheckPath cuts it before
	// validating the path; the provenance call keeps it, and it holds no
	// customer data.
	if err := c.do(ctx, http.MethodGet, "/connector-plugins?connectorsOnly=false", &out); err != nil {
		return nil, err
	}
	return out, nil
}

// LegacyPlugins reports whether a plugin list came from a Connect older than
// 3.2: such a worker lists its connector plugins only and reports no plugin
// versions, so every returned entry has an empty Version. An empty list says
// nothing about the worker's age.
func LegacyPlugins(plugins []Plugin) bool {
	if len(plugins) == 0 {
		return false
	}
	for _, p := range plugins {
		if p.Version != "" {
			return false
		}
	}
	return true
}
