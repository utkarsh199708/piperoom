// Package adminconfig builds the read-only admin view of what the server runs
// with (docs/api.md, "Admin: settings and connection tests"; design
// docs/design/onboarding.md phase 1) and runs live tests of its connections.
//
// The view never holds a secret value: DSNs, user names and passwords are
// shown as set or unset with where they come from, URLs and brokers without
// user info, query or fragment. Phase 1 changes no configuration: the files
// and the environment stay the source of truth.
package adminconfig

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"piperoom/internal/config"
	"piperoom/internal/connprobe"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/wiring"
)

// Scopes of a connection.
const (
	ScopeProcessWide = "process-wide"
	ScopePipeline    = "pipeline"
)

// SourceFile is the only config source of phase 1.
const SourceFile = "file"

// testTimeout bounds one connection test as a whole, below the HTTP request
// budget (60 s) so the handler answers with the result rather than a 503.
const testTimeout = 55 * time.Second

// ErrUnknownConnection is returned by Test for an id that is not in the view;
// the HTTP layer answers 404 not_found.
var ErrUnknownConnection = errors.New("adminconfig: unknown connection id")

// AdminConfig is GET /api/admin/config.
type AdminConfig struct {
	Source      string         `json:"source"`
	OwnersFile  string         `json:"owners_file"`
	UsersFile   string         `json:"users_file"`
	Connections []Connection   `json:"connections"`
	Pipelines   []PipelineView `json:"pipelines"`
	People      []Person       `json:"people"`
}

// Connection is one client set's settings of one kind.
type Connection struct {
	ID         string         `json:"id"`
	Scope      string         `json:"scope"`
	PipelineID string         `json:"pipeline_id"`
	Kind       connprobe.Kind `json:"kind"`
	// UsedBy lists the pipelines the connection serves, in owners order.
	UsedBy   []string  `json:"used_by"`
	Settings []Setting `json:"settings"`
}

// Setting is one row of a connection. For a secret row Display is always ""
// and Set says whether a value is present; Set is omitted on other rows.
type Setting struct {
	Key     string `json:"key"`
	Display string `json:"display"`
	Secret  bool   `json:"secret"`
	Set     *bool  `json:"set,omitempty"`
	// From is "env NAME", "file /path" or "value" for a pipeline reference,
	// "env NAME" for a process-wide setting.
	From string `json:"from"`
}

// PipelineView mirrors one owners.yaml pipeline.
type PipelineView struct {
	ID     string `json:"id"`
	Title  string `json:"title"`
	Tenant string `json:"tenant"`
	// Connections is "own" (a connections: block) or "process-wide".
	Connections    string     `json:"connections"`
	Connectors     []string   `json:"connectors"`
	ConsumerGroups []string   `json:"consumer_groups"`
	SourceTables   []string   `json:"source_tables"`
	SinkTables     []string   `json:"sink_tables"`
	Owners         []OwnerRow `json:"owners"`
	Shadow         bool       `json:"shadow"`
}

// OwnerRow is the approvers of one layer of a pipeline, as written
// (user:<id>, group:<name>).
type OwnerRow struct {
	Layer      string   `json:"layer"`
	Principals []string `json:"principals"`
}

// Person mirrors one users.yaml entry.
type Person struct {
	ID     string   `json:"id"`
	Name   string   `json:"name"`
	Layers []string `json:"layers"`
	Admin  bool     `json:"admin"`
}

// Provider builds the view and runs the tests. It is safe for concurrent use:
// nothing in it changes after New.
type Provider struct {
	cfg        config.Config
	own        *owners.Owners
	users      []room.User
	ownersFile string
	usersFile  string
	export     []byte
	// exportErr is why the files could not be read at start; Export returns it.
	exportErr error

	// resolve returns a pipeline's resolved settings (wiring.PipelineConfig).
	resolve func(owners.Pipeline) (config.Config, error)
	// lookupEnv and stat answer "is a secret reference set?" without reading
	// a secret file.
	lookupEnv func(string) (string, bool)
	stat      func(string) (fs.FileInfo, error)
	run       func(context.Context, connprobe.Target, *connprobe.Guard) connprobe.Result
}

// New builds a Provider over the loaded config, owners and users, and reads
// the owners and users files at their paths for Export, so the export is the
// files as read at start. users may be nil (no people).
//
// The settings pages are an optional admin feature, so a file that cannot be
// read never stops the server: the view still works and only Export reports
// the failure. --quickstart, for one, writes its users file to a temporary
// path while USERS_FILE keeps naming a file that may not exist.
func New(cfg config.Config, own *owners.Owners, users *room.Users, ownersPath, usersPath string) (*Provider, error) {
	var export []byte
	var exportErr error
	ob, err := os.ReadFile(ownersPath)
	if err != nil {
		exportErr = fmt.Errorf("adminconfig: export: read owners file: %w", err)
	}
	ub, err := os.ReadFile(usersPath)
	if err != nil && exportErr == nil {
		exportErr = fmt.Errorf("adminconfig: export: read users file: %w", err)
	}
	if exportErr == nil {
		export = make([]byte, 0, len(ob)+len(ub)+5)
		export = append(export, ob...)
		export = append(export, "\n---\n"...)
		export = append(export, ub...)
	}
	if own == nil {
		own = &owners.Owners{}
	}
	var us []room.User
	if users != nil {
		us = users.All()
	}
	return &Provider{
		cfg: cfg, own: own, users: us,
		ownersFile: filepath.Base(ownersPath), usersFile: filepath.Base(usersPath),
		export:    export,
		exportErr: exportErr,
		resolve:   wiring.PipelineConfig,
		lookupEnv: os.LookupEnv,
		stat:      os.Stat,
		run:       connprobe.Run,
	}, nil
}

// Export returns the owners file, a "---" line and the users file, as read
// at start. They hold no secrets by design: connection settings in owners.yaml
// are env:/file: references, and users.yaml holds no credential.
func (p *Provider) Export() ([]byte, error) {
	if p.exportErr != nil {
		return nil, p.exportErr
	}
	return append([]byte(nil), p.export...), nil
}

// Config builds the view.
func (p *Provider) Config() AdminConfig {
	out := AdminConfig{
		Source:      SourceFile,
		OwnersFile:  p.ownersFile,
		UsersFile:   p.usersFile,
		Connections: []Connection{},
		Pipelines:   []PipelineView{},
		People:      []Person{},
	}
	for _, c := range p.connections() {
		out.Connections = append(out.Connections, c.view)
	}
	for _, pl := range p.own.Pipelines {
		out.Pipelines = append(out.Pipelines, p.pipelineView(pl))
	}
	for _, u := range p.users {
		layers := u.Layers
		if layers == nil {
			layers = []string{}
		}
		out.People = append(out.People, Person{ID: u.ID, Name: u.Name, Layers: layers, Admin: u.Admin})
	}
	return out
}

// conn is a connection of the view with what a test needs.
type conn struct {
	view      Connection
	pipeline  *owners.Pipeline // nil for process-wide
	pipelines []owners.Pipeline
}

// connections lists the process-wide connections, then each pipeline's own,
// in owners order, kinds in connprobe.Kinds order.
func (p *Provider) connections() []conn {
	var shared []owners.Pipeline
	usedBy := []string{}
	for _, pl := range p.own.Pipelines {
		if !pl.HasConnections() {
			shared = append(shared, pl)
			usedBy = append(usedBy, pl.ID)
		}
	}
	var out []conn
	for _, k := range connprobe.Kinds {
		if strings.TrimSpace(*keys[requiredKey[k]].field(&p.cfg)) == "" {
			continue
		}
		out = append(out, conn{
			view: Connection{
				ID: ScopeProcessWide + "." + string(k), Scope: ScopeProcessWide, Kind: k,
				UsedBy: append([]string{}, usedBy...), Settings: p.processSettings(k),
			},
			pipelines: shared,
		})
	}
	for i := range p.own.Pipelines {
		pl := &p.own.Pipelines[i]
		if !pl.HasConnections() {
			continue
		}
		for _, k := range connprobe.Kinds {
			if _, ok := pl.Connections[requiredKey[k]]; !ok {
				continue
			}
			out = append(out, conn{
				view: Connection{
					ID: ScopePipeline + "." + pl.ID + "." + string(k), Scope: ScopePipeline, PipelineID: pl.ID, Kind: k,
					UsedBy: []string{pl.ID}, Settings: p.pipelineSettings(*pl, k),
				},
				pipeline:  pl,
				pipelines: []owners.Pipeline{*pl},
			})
		}
	}
	return out
}

// processSettings are the rows of a process-wide connection: every key of the
// kind that is set or required.
func (p *Provider) processSettings(k connprobe.Kind) []Setting {
	out := []Setting{}
	for _, key := range keysOf(k) {
		info := keys[key]
		v := *info.field(&p.cfg)
		if strings.TrimSpace(v) == "" && key != requiredKey[k] {
			continue
		}
		s := Setting{Key: key, From: "env " + info.env}
		if isSecretKey(key) {
			set := strings.TrimSpace(v) != ""
			s.Secret, s.Set = true, &set
		} else {
			s.Display, s.Secret = displayValue(key, v)
		}
		out = append(out, s)
	}
	return out
}

// pipelineSettings are the rows of a pipeline connection: every key of the
// kind its connections: block sets (the required ones always are).
func (p *Provider) pipelineSettings(pl owners.Pipeline, k connprobe.Kind) []Setting {
	out := []Setting{}
	for _, key := range keysOf(k) {
		r, ok := pl.Connections[key]
		if !ok {
			continue
		}
		s := Setting{Key: key, From: refFrom(r)}
		if isSecretKey(key) {
			// Presence only: a secret is never read for display.
			set := p.present(r)
			s.Secret, s.Set = true, &set
			out = append(out, s)
			continue
		}
		var v string
		switch {
		case r.Value != "":
			v = r.Value
		case r.File != "" && owners.IsPathKey(key):
			v = r.File // the reference is the path itself
		default:
			c, err := p.resolve(onlyKeys(pl, key))
			if err != nil {
				// Unresolvable: nothing to show; the test names the reference.
				out = append(out, s)
				continue
			}
			v = *keys[key].field(&c)
		}
		s.Display, s.Secret = displayValue(key, v)
		out = append(out, s)
	}
	return out
}

// present reports whether a secret reference names a value: a non-empty
// variable, or a non-empty regular file. The file is not read.
func (p *Provider) present(r owners.ConnRef) bool {
	switch {
	case r.Env != "":
		v, ok := p.lookupEnv(r.Env)
		return ok && strings.TrimSpace(v) != ""
	case r.File != "":
		fi, err := p.stat(r.File)
		return err == nil && fi.Mode().IsRegular() && fi.Size() > 0
	}
	return r.Value != ""
}

// onlyKeys returns a copy of pl whose connections: block holds only keys, so
// resolving it reads nothing else.
func onlyKeys(pl owners.Pipeline, want ...string) owners.Pipeline {
	c := owners.Connections{}
	for _, k := range want {
		if r, ok := pl.Connections[k]; ok {
			c[k] = r
		}
	}
	pl.Connections = c
	return pl
}

// pipelineView mirrors a pipeline. Owners are the approvers that apply to the
// pipeline per layer: its owners: entry, else the top-level layers: one.
func (p *Provider) pipelineView(pl owners.Pipeline) PipelineView {
	v := PipelineView{
		ID: pl.ID, Title: pl.Title, Tenant: pl.TenantName(), Connections: ScopeProcessWide,
		Connectors: nonNil(pl.Connectors), ConsumerGroups: nonNil(pl.ConsumerGroups),
		SourceTables: nonNil(pl.SourceTables), SinkTables: nonNil(pl.SinkTables),
		Owners: []OwnerRow{}, Shadow: pl.Shadow(),
	}
	if pl.HasConnections() {
		v.Connections = "own"
	}
	for _, layer := range events.Layers {
		ps := p.own.Layers[layer]
		if r, ok := pl.Owners[layer]; ok {
			ps = r.Approvers
		}
		if len(ps) == 0 {
			continue
		}
		row := OwnerRow{Layer: layer, Principals: make([]string, 0, len(ps))}
		for _, pr := range ps {
			row.Principals = append(row.Principals, pr.String())
		}
		v.Owners = append(v.Owners, row)
	}
	return v
}

func nonNil(s []string) []string {
	if s == nil {
		return []string{}
	}
	return append([]string{}, s...)
}

// Test probes the connection id. An unknown id is ErrUnknownConnection. A
// pipeline reference that cannot be resolved is a result with one failed
// "config" check naming the reference, never a value. A failed check is
// ok: false, never an error.
func (p *Provider) Test(ctx context.Context, id string) (connprobe.Result, error) {
	var c *conn
	all := p.connections()
	for i := range all {
		if all[i].view.ID == id {
			c = &all[i]
			break
		}
	}
	if c == nil {
		return connprobe.Result{}, ErrUnknownConnection
	}
	ctx, cancel := context.WithTimeout(ctx, testTimeout)
	defer cancel()
	k := c.view.Kind
	var cfg config.Config
	if c.pipeline == nil {
		// Only the kind's settings: a probe of one system never holds the
		// credentials of another.
		for _, key := range keysOf(k) {
			*keys[key].field(&cfg) = *keys[key].field(&p.cfg)
		}
	} else {
		start := time.Now()
		resolved, err := p.resolve(onlyKeys(*c.pipeline, keysOf(k)...))
		if err != nil {
			return p.configFailure(*c.pipeline, k, start), nil
		}
		cfg = resolved
	}
	guard := &connprobe.Guard{AllowLoopback: true, AllowPrivate: true}
	return p.run(ctx, connprobe.Target{Kind: k, Config: cfg, Pipelines: c.pipelines}, guard), nil
}

// configFailure is the result of a pipeline connection whose references do
// not resolve. It names the first failing key and its reference (resolving
// the keys one by one, as wiring reports only text); the resolver's error is
// not passed on, so no text from a file or variable can reach the answer.
func (p *Provider) configFailure(pl owners.Pipeline, k connprobe.Kind, start time.Time) connprobe.Result {
	check := connprobe.Check{
		ID: "config", Status: connprobe.StatusFail,
		Problem: fmt.Sprintf("pipeline %s: a connection setting of kind %s cannot be resolved", pl.ID, k),
		Fix:     "check the connections: references of this pipeline in owners.yaml and the variables and files they name",
	}
	ks := keysOf(k)
	sort.Strings(ks)
	for _, key := range ks {
		r, ok := pl.Connections[key]
		if !ok {
			continue
		}
		if _, err := p.resolve(onlyKeys(pl, key)); err == nil {
			continue
		}
		check.Problem = fmt.Sprintf("pipeline %s: connections.%s from %s cannot be resolved", pl.ID, key, refFrom(r))
		switch {
		case r.Env != "":
			check.Fix = fmt.Sprintf("set the environment variable %s (not empty) on the server", r.Env)
		case r.File != "":
			check.Fix = fmt.Sprintf("make the file %s readable by the server and not empty", r.File)
		default:
			check.Fix = fmt.Sprintf("give connections.%s a value the client accepts", key)
		}
		break
	}
	check.MS = time.Since(start).Milliseconds()
	return connprobe.Result{Kind: k, OK: false, TookMS: check.MS, Checks: []connprobe.Check{check}}
}
