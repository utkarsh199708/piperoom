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
	// transformation or predicate. A Connect older than 3.2 (KIP-769) lists
	// only source and sink plugins and may leave it empty.
	Type string `json:"type"`
	// Version is what the plugin reports, the literal "null" or "" when it
	// reports none. Workers older than 3.2 report connector plugin versions
	// too, so a missing version says nothing about the worker's age.
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
// only. The response is a top-level JSON array of
// {class, type, version} (apache/kafka ConnectorPluginsResource.listConnectorPlugins).
// It is read-only: it lists what is installed and changes nothing.
func (c *Client) Plugins(ctx context.Context) ([]Plugin, error) {
	var out []Plugin
	// The query is not a path segment, so netsec.CheckPath cuts it before
	// validating the path; the provenance call keeps it, and it holds no
	// customer data.
	if err := c.do(ctx, http.MethodGet, "/connector-plugins?connectorsOnly=false", &out); err != nil {
		// safeErr folds, redacts and caps a non-2xx body before it can reach
		// an error string, a log or a prompt (the same rule as ServerInfo).
		return nil, safeErr(err)
	}
	return out, nil
}

// LegacyPlugins reports whether a plugin list came from a Connect older than
// 3.2 (KIP-769): such a worker ignores connectorsOnly=false and answers its
// connector plugins only, so the list holds no converter, header_converter,
// transformation or predicate entry — a 3.2+ worker always lists at least
// its built-in converters. Versions say nothing here: workers older than 3.2
// report connector plugin versions too, and a plugin on any worker may
// report none. An empty list says nothing about the worker's age.
func LegacyPlugins(plugins []Plugin) bool {
	if len(plugins) == 0 {
		return false
	}
	for _, p := range plugins {
		switch p.Type {
		case "converter", "header_converter", "transformation", "predicate":
			return false
		}
	}
	return true
}
