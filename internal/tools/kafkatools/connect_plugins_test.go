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
	// The summary groups source before sink before the converter kinds.
	iSource := strings.Index(r.Summary, "source io.debezium")
	iSink := strings.Index(r.Summary, "sink org.apache")
	iConv := strings.Index(r.Summary, "converter io.confluent")
	if !(iSource < iSink && iSink < iConv) {
		t.Fatalf("plugins not grouped in type order:\n%s", r.Summary)
	}
}

// A pre-3.2 Connect lists connectors only and reports no versions. The tool
// marks that instead of guessing versions.
func TestConnectPluginsMarksLegacyWorker(t *testing.T) {
	legacy := []connect.Plugin{
		{Class: "io.debezium.connector.postgresql.PostgresConnector", Type: "source"},
		{Class: "org.apache.kafka.connect.file.FileStreamSinkConnector", Type: "sink"},
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
	if !strings.Contains(r.Summary, "Kafka version unknown") {
		t.Fatalf("summary %q", r.Summary)
	}
	if !strings.Contains(r.Summary, "io.confluent.connect.avro.AvroConverter") {
		t.Fatalf("plugins missing: %q", r.Summary)
	}
}

// A client without the optional version read still lists plugins and says the
// version is unknown.
func TestConnectPluginsWithoutInfoReader(t *testing.T) {
	tool := pluginsRegistry(t, Deps{Connect: &pluginOnlyConnect{fakeConnect: &fakeConnect{}}})
	r := runTool(t, tool, `{}`)
	if !strings.Contains(r.Summary, "Kafka version unknown") || !strings.Contains(r.Summary, "PostgresConnector 3.0.0.Final") {
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
