package kafkatools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/clients/connect"
	"piperoom/internal/owners"
	"piperoom/internal/tools"
)

// fakePluginsConnect is a read-only Connect fake that also implements the
// optional plugin reader (Q1c) and the version reader.
type fakePluginsConnect struct {
	*fakeConnect
	pluginsFn func(context.Context) ([]connect.Plugin, error)
	infoFn    func(context.Context) (connect.ServerInfo, error)
}

func (f *fakePluginsConnect) Plugins(ctx context.Context) ([]connect.Plugin, error) {
	return f.pluginsFn(ctx)
}

func (f *fakePluginsConnect) ServerInfo(ctx context.Context) (connect.ServerInfo, error) {
	if f.infoFn == nil {
		return connect.ServerInfo{Version: "3.6.1", Commit: "abc123"}, nil
	}
	return f.infoFn(ctx)
}

// pluginOnlyConnect implements PluginReader without ConnectInfoReader: the
// tool must still list plugins and say the Kafka version is unknown.
type pluginOnlyConnect struct{ *fakeConnect }

func (f *pluginOnlyConnect) Plugins(context.Context) ([]connect.Plugin, error) {
	return []connect.Plugin{{Class: "io.debezium.connector.postgresql.PostgresConnector", Type: "source", Version: "3.0.0.Final"}}, nil
}

func newPluginsFake(t *testing.T, plugins []connect.Plugin) *fakePluginsConnect {
	t.Helper()
	f := &fakePluginsConnect{
		fakeConnect: &fakeConnect{},
		pluginsFn:   func(context.Context) ([]connect.Plugin, error) { return plugins, nil },
	}
	guardWrites(t, f.fakeConnect)
	return f
}

func pluginsRegistry(t *testing.T, d Deps) tools.Tool {
	t.Helper()
	reg := tools.NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	tool, ok := reg.Get("connect_plugins")
	if !ok {
		t.Fatal("connect_plugins not registered")
	}
	return tool
}

func allPluginKinds() []connect.Plugin {
	return []connect.Plugin{
		{Class: "org.apache.kafka.connect.file.FileStreamSinkConnector", Type: "sink", Version: "3.6.1"},
		{Class: "io.confluent.connect.avro.AvroConverter", Type: "converter", Version: "7.6.1"},
		{Class: "io.debezium.connector.postgresql.PostgresConnector", Type: "source", Version: "3.0.0.Final"},
		{Class: "io.debezium.transforms.Filter", Type: "predicate", Version: "3.0.0.Final"},
		{Class: "io.debezium.transforms.ExtractNewRecordState", Type: "transformation", Version: "3.0.0.Final"},
		{Class: "org.apache.kafka.connect.storage.StringConverter", Type: "header_converter", Version: "3.6.1"},
	}
}

func TestConnectPluginsListsEveryKindWithVersions(t *testing.T) {
	tool := pluginsRegistry(t, Deps{Connect: newPluginsFake(t, allPluginKinds())})
	r := runTool(t, tool, `{}`)
	for _, want := range []string{
		"Kafka 3.6.1", "6 plugins",
		"1 source", "1 sink", "1 converter", "1 transformation", "1 predicate", "1 header_converter",
		"source io.debezium.connector.postgresql.PostgresConnector 3.0.0.Final",
		"converter io.confluent.connect.avro.AvroConverter 7.6.1",
		"transformation io.debezium.transforms.ExtractNewRecordState 3.0.0.Final",
		"predicate io.debezium.transforms.Filter 3.0.0.Final",
	} {
		if !strings.Contains(r.Summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, r.Summary)
		}
	}
	data, ok := r.Data.(connectPluginsData)
	if !ok {
		t.Fatalf("data %T, want connectPluginsData", r.Data)
	}
	if data.Legacy || data.PluginCount != 6 || data.KafkaVersion != "3.6.1" || data.ConnectCommit != "abc123" {
		t.Fatalf("data %+v", data)
	}
	if data.ByType["source"] != 1 || data.ByType["predicate"] != 1 || len(data.Plugins) != 6 {
		t.Fatalf("by_type/plugins in data: %+v", data)
	}
	if r.Truncated {
		t.Fatal("uncapped result marked truncated")
	}
	// The count line is exact and in pluginTypeOrder.
	if !strings.Contains(r.Summary, "1 source, 1 sink, 1 converter, 1 transformation, 1 predicate, 1 header_converter") {
		t.Fatalf("count line missing or out of order:\n%s", r.Summary)
	}
	// Every plugin line starts with "- " (so a hostile class can never render
	// as one of the tool's own marker lines), and the full type-order chain
	// holds: source, sink, converter, transformation, predicate,
	// header_converter.
	var pluginLines []string
	for _, line := range strings.Split(r.Summary, "\n") {
		if strings.HasPrefix(line, "- ") {
			pluginLines = append(pluginLines, line)
		}
	}
	if len(pluginLines) != 6 {
		t.Fatalf("got %d \"- \" plugin lines, want 6:\n%s", len(pluginLines), r.Summary)
	}
	for i, wantPrefix := range []string{"- source ", "- sink ", "- converter ", "- transformation ", "- predicate ", "- header_converter "} {
		if !strings.HasPrefix(pluginLines[i], wantPrefix) {
			t.Fatalf("plugin line %d is %q, want prefix %q", i, pluginLines[i], wantPrefix)
		}
	}
}

// A pre-3.2 Connect lists connectors only; it may well report their versions
// (pre-3.2 workers did), so the tool keys legacy on the absence of every
// non-connector type, never on missing versions.
func TestConnectPluginsMarksLegacyWorker(t *testing.T) {
	legacy := []connect.Plugin{
		{Class: "io.debezium.connector.postgresql.PostgresConnector", Type: "source"},
		{Class: "org.apache.kafka.connect.file.FileStreamSinkConnector", Type: "sink", Version: "3.1.2"},
	}
	f := newPluginsFake(t, legacy)
	f.infoFn = func(context.Context) (connect.ServerInfo, error) {
		return connect.ServerInfo{Version: "3.1.2"}, nil
	}
	tool := pluginsRegistry(t, Deps{Connect: f})
	r := runTool(t, tool, `{}`)
	if !strings.Contains(r.Summary, "pre-3.2 Connect") || !strings.Contains(r.Summary, "connector plugins only") {
		t.Fatalf("legacy worker not marked: %q", r.Summary)
	}
	if strings.Contains(r.Summary, "connector io.debezium") {
		t.Fatalf("legacy summary claims converters: %q", r.Summary)
	}
	if strings.Contains(r.Summary, "PostgresConnector 3") {
		t.Fatalf("legacy summary invents plugin versions: %q", r.Summary)
	}
	data := r.Data.(connectPluginsData)
	if !data.Legacy || data.KafkaVersion != "3.1.2" {
		t.Fatalf("data %+v", data)
	}
}

func TestConnectPluginsRegisteredOnlyWithReader(t *testing.T) {
	fc := &fakeConnect{}
	guardWrites(t, fc)
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Connect: fc}); err != nil {
		t.Fatal(err)
	}
	if _, ok := reg.Get("connect_plugins"); ok {
		t.Fatal("tool registered for a Connect without the optional plugin reader")
	}
}

func TestConnectPluginsPropagatesErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"target error", errors.New("connect: GET /connector-plugins: status 500: boom")},
		{"not found", fmt.Errorf("connect: GET /connector-plugins: %w", clients.ErrNotFound)},
		{"deadline", context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &fakePluginsConnect{
				fakeConnect: &fakeConnect{},
				pluginsFn:   func(context.Context) ([]connect.Plugin, error) { return nil, tc.err },
			}
			guardWrites(t, f.fakeConnect)
			tool := pluginsRegistry(t, Deps{Connect: f})
			_, err := tool.Run(tools.WithUnscoped(context.Background()), json.RawMessage(`{}`))
			if !errors.Is(err, tc.err) {
				t.Fatalf("err %v, want %v", err, tc.err)
			}
		})
	}
}

// Without a room pipeline the tool refuses and reaches no client, like every
// other read tool; a room pipeline or the explicit opt-out lets it read.
func TestConnectPluginsScope(t *testing.T) {
	calls := 0
	f := &fakePluginsConnect{
		fakeConnect: &fakeConnect{},
		pluginsFn: func(context.Context) ([]connect.Plugin, error) {
			calls++
			return allPluginKinds(), nil
		},
	}
	guardWrites(t, f.fakeConnect)
	tool := pluginsRegistry(t, Deps{Connect: f})
	if _, err := tool.Run(context.Background(), json.RawMessage(`{}`)); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("no-room err %v, want ErrNoScope", err)
	}
	if calls != 0 {
		t.Fatalf("client reached without a room: %d calls", calls)
	}
	ctx := tools.WithPipeline(context.Background(), owners.Pipeline{ID: "orders", Connectors: []string{"pg-orders-connector"}})
	if _, err := tool.Run(ctx, json.RawMessage(`{}`)); err != nil {
		t.Fatalf("room call: %v", err)
	}
	if calls != 1 {
		t.Fatalf("room call read %d times, want 1", calls)
	}
}

// A worker that lists plugins but fails GET / still answers; the summary says
// the version is unknown instead of failing whole.
func TestConnectPluginsVersionUnknown(t *testing.T) {
	f := newPluginsFake(t, allPluginKinds())
	f.infoFn = func(context.Context) (connect.ServerInfo, error) {
		return connect.ServerInfo{}, errors.New("connect: GET /: status 500")
	}
	tool := pluginsRegistry(t, Deps{Connect: f})
	r := runTool(t, tool, `{}`)
	// The client can read GET /, so the summary blames the worker, not the
	// client.
	if !strings.Contains(r.Summary, "Kafka version unknown (the worker's GET / did not answer)") {
		t.Fatalf("summary %q", r.Summary)
	}
	if !strings.Contains(r.Summary, "io.confluent.connect.avro.AvroConverter") {
		t.Fatalf("plugins missing: %q", r.Summary)
	}
}

// A GET / that succeeds but reports no version (or a hostile version that
// safeVersion rejects) is "not reported by the worker", not "unknown": the
// read worked, the worker just said nothing usable.
func TestConnectPluginsVersionNotReported(t *testing.T) {
	for _, tc := range []struct {
		name string
		info connect.ServerInfo
	}{
		{"empty answer", connect.ServerInfo{}},
		{"hostile version", connect.ServerInfo{Version: "3.6.1\x1b[31m\nX", Commit: "abc\u202e"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newPluginsFake(t, allPluginKinds())
			f.infoFn = func(context.Context) (connect.ServerInfo, error) { return tc.info, nil }
			tool := pluginsRegistry(t, Deps{Connect: f})
			r := runTool(t, tool, `{}`)
			if !strings.Contains(r.Summary, "Kafka version not reported by the worker: 6 plugins") {
				t.Fatalf("summary %q", r.Summary)
			}
			if strings.ContainsAny(r.Summary, "\x1b\u202e") {
				t.Fatalf("summary carries hostile version bytes: %q", r.Summary)
			}
			data := r.Data.(connectPluginsData)
			if data.KafkaVersion != "" || data.ConnectCommit != "" {
				t.Fatalf("hostile/empty info kept in data: %+v", data)
			}
		})
	}
}

// An unknown plugin type sorts after the known kinds and renders verbatim; an
// empty type renders as "(unknown)" so the line always carries a type token.
func TestConnectPluginsUnknownAndEmptyTypes(t *testing.T) {
	plugins := []connect.Plugin{
		{Class: "io.example.NoType", Version: "1.0"},
		{Class: "io.example.Restarter", Type: "connector_restarter", Version: "2.0"},
		{Class: "io.debezium.connector.postgresql.PostgresConnector", Type: "source", Version: "3.0.0.Final"},
	}
	tool := pluginsRegistry(t, Deps{Connect: newPluginsFake(t, plugins)})
	r := runTool(t, tool, `{}`)
	for _, want := range []string{
		"- source io.debezium.connector.postgresql.PostgresConnector 3.0.0.Final",
		"- (unknown) io.example.NoType 1.0",
		"- connector_restarter io.example.Restarter 2.0",
	} {
		if !strings.Contains(r.Summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, r.Summary)
		}
	}
	// Known type first, then the unknown types ("" before "connector_restarter").
	iSource := strings.Index(r.Summary, "- source ")
	iEmpty := strings.Index(r.Summary, "- (unknown) ")
	iRestarter := strings.Index(r.Summary, "- connector_restarter ")
	if !(iSource < iEmpty && iEmpty < iRestarter) {
		t.Fatalf("unknown types not ordered after known kinds:\n%s", r.Summary)
	}
	// The count line names the unknown type after the known kinds; the empty
	// type is counted but never rendered as a nameless ", 1 " fragment.
	if !strings.Contains(r.Summary, "1 source, 1 connector_restarter") {
		t.Fatalf("count line missing unknown type:\n%s", r.Summary)
	}
}

// Types are untrusted Connect text: an answer minting one type per row keeps
// ByType and the count line at maxPluginTypes distinct keys, with the
// overflow summed under "other" and every plugin still counted.
func TestConnectPluginsCapsHostileTypeKeys(t *testing.T) {
	const total = maxPluginTypes + 20
	plugins := make([]connect.Plugin, total)
	for i := range plugins {
		plugins[i] = connect.Plugin{
			Class: fmt.Sprintf("io.example.Plugin%03d", i),
			Type:  fmt.Sprintf("minted_type_%03d", i),
		}
	}
	plugins[0].Type = "source"
	tool := pluginsRegistry(t, Deps{Connect: newPluginsFake(t, plugins)})
	r := runTool(t, tool, `{}`)
	data := r.Data.(connectPluginsData)
	if len(data.ByType) > maxPluginTypes {
		t.Fatalf("ByType holds %d keys, want <= %d", len(data.ByType), maxPluginTypes)
	}
	if data.ByType["source"] != 1 {
		t.Fatalf("known type lost under the cap: %+v", data.ByType)
	}
	if data.ByType["other"] == 0 {
		t.Fatalf("overflow types not summed under other: %+v", data.ByType)
	}
	sum := 0
	for _, n := range data.ByType {
		sum += n
	}
	if sum != total {
		t.Fatalf("ByType sums to %d, want %d", sum, total)
	}
}

// One row over the summary-line cap: the extra row is cut from the summary
// (Truncated), but Data still holds every row (Omitted 0, data cap untouched).
func TestConnectPluginsSummaryCapOnly(t *testing.T) {
	const total = maxPluginSummaryLines + 1
	plugins := make([]connect.Plugin, total)
	for i := range plugins {
		plugins[i] = connect.Plugin{Class: fmt.Sprintf("io.example.Plugin%03d", i), Type: "source", Version: "1.0"}
	}
	tool := pluginsRegistry(t, Deps{Connect: newPluginsFake(t, plugins)})
	r := runTool(t, tool, `{}`)
	if !r.Truncated {
		t.Fatal("summary-capped result not marked truncated")
	}
	if got := strings.Count(r.Summary, "\n- "); got != maxPluginSummaryLines {
		t.Fatalf("summary shows %d plugin lines, want %d", got, maxPluginSummaryLines)
	}
	if !strings.Contains(r.Summary, "1 more plugins not shown (cap)") {
		t.Fatalf("cut summary not marked:\n%s", r.Summary)
	}
	data := r.Data.(connectPluginsData)
	if data.Omitted != 0 || len(data.Plugins) != total || data.PluginCount != total {
		t.Fatalf("data cut below maxPluginRows: omitted %d, rows %d, count %d", data.Omitted, len(data.Plugins), data.PluginCount)
	}
}

// A client without the optional version read still lists plugins and says the
// version is unknown.
func TestConnectPluginsWithoutInfoReader(t *testing.T) {
	tool := pluginsRegistry(t, Deps{Connect: &pluginOnlyConnect{fakeConnect: &fakeConnect{}}})
	r := runTool(t, tool, `{}`)
	if !strings.Contains(r.Summary, "Kafka version unknown (this client does not read GET /)") || !strings.Contains(r.Summary, "PostgresConnector 3.0.0.Final") {
		t.Fatalf("summary %q", r.Summary)
	}
}

// Plugin classes and versions are untrusted Connect text: a line break, an
// ANSI escape or a bidi override must not forge or reorder a summary line,
// and a huge or numerous inventory is capped and the result says so.
func TestConnectPluginsSanitizesAndCaps(t *testing.T) {
	const total = maxPluginRows + 100
	plugins := make([]connect.Plugin, total)
	for i := range plugins {
		plugins[i] = connect.Plugin{
			Class:   fmt.Sprintf("io.example.Plugin%03d\nforged line %d", i, i),
			Type:    "source",
			Version: "1.0.0\x1b[31m\u202e",
		}
	}
	plugins[0].Class = strings.Repeat("x", 400) + "\nsecond line"
	tool := pluginsRegistry(t, Deps{Connect: newPluginsFake(t, plugins)})
	r := runTool(t, tool, `{}`)

	if len(r.Summary) > tools.MaxSummaryBytes {
		t.Fatalf("summary %d bytes", len(r.Summary))
	}
	for _, banned := range []string{"\x1b", "\u202e", "\nforged", "second line", strings.Repeat("x", 300)} {
		if strings.Contains(r.Summary, banned) {
			t.Fatalf("summary carries %q:\n%s", banned, r.Summary)
		}
	}
	if !r.Truncated || !strings.Contains(r.Summary, "more plugins not shown (cap)") {
		t.Fatalf("capped result not marked: truncated=%v\n%s", r.Truncated, r.Summary)
	}
	data := r.Data.(connectPluginsData)
	if len(data.Plugins) != maxPluginRows || data.Omitted != total-maxPluginRows || data.PluginCount != total {
		t.Fatalf("data holds %d plugins (omitted %d, count %d), want %d rows over a total of %d",
			len(data.Plugins), data.Omitted, data.PluginCount, maxPluginRows, total)
	}
	for _, p := range data.Plugins {
		if strings.ContainsAny(p.Class, "\x1b\n\u202e") || strings.ContainsAny(p.Version, "\x1b\n\u202e") {
			t.Fatalf("control characters kept in data: %+v", p)
		}
		// Data rows carry the same per-string cap as the summary: a huge class
		// cannot blow up the result through Data instead.
		if len(p.Class) > maxPluginTextBytes+len("\u2026") || len(p.Version) > maxPluginTextBytes+len("\u2026") {
			t.Fatalf("uncapped string in data row: %d/%d bytes", len(p.Class), len(p.Version))
		}
	}
}

// End to end over the real client and httptest: the tool reads both GET / and
// GET /connector-plugins?connectorsOnly=false and reports versions.
func TestConnectPluginsRealClient(t *testing.T) {
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		paths = append(paths, r.URL.Path+"?"+r.URL.RawQuery)
		switch r.URL.Path {
		case "/":
			w.Write([]byte(`{"version":"3.6.1","commit":"abc123","kafka_cluster_id":"c-1"}`))
		case "/connector-plugins":
			w.Write([]byte(`[{"class":"io.debezium.connector.postgresql.PostgresConnector","type":"source","version":"3.0.0.Final"},{"class":"io.confluent.connect.avro.AvroConverter","type":"converter","version":"7.6.1"}]`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	tool := pluginsRegistry(t, Deps{Connect: connect.New(srv.URL, srv.Client())})
	r := runTool(t, tool, `{}`)
	for _, want := range []string{"Kafka 3.6.1", "2 plugins", "source io.debezium.connector.postgresql.PostgresConnector 3.0.0.Final", "converter io.confluent.connect.avro.AvroConverter 7.6.1"} {
		if !strings.Contains(r.Summary, want) {
			t.Fatalf("summary missing %q:\n%s", want, r.Summary)
		}
	}
	if len(paths) != 2 || paths[0] != "/connector-plugins?connectorsOnly=false" || paths[1] != "/?" {
		t.Fatalf("requests %v", paths)
	}
}

// A 404 from the plugin endpoint is the tool's error; it does not degrade to an
// empty list.
func TestConnectPluginsRealClient404(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			w.Write([]byte(`{"version":"3.6.1"}`))
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()
	tool := pluginsRegistry(t, Deps{Connect: connect.New(srv.URL, srv.Client())})
	_, err := tool.Run(tools.WithUnscoped(context.Background()), json.RawMessage(`{}`))
	if !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("err %v, want ErrNotFound", err)
	}
}
