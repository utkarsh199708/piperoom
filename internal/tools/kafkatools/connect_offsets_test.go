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

// fakeOffsetsConnect is a read-only Connect fake that also implements the
// optional KIP-875 reader and version reader. Tests that do not want the tool
// registered use plain fakeConnect instead.
type fakeOffsetsConnect struct {
	*fakeConnect
	offsetsFn func(context.Context, string) ([]connect.SourceOffset, error)
	infoFn    func(context.Context) (connect.ServerInfo, error)
}

func (f *fakeOffsetsConnect) SourceOffsets(ctx context.Context, name string) ([]connect.SourceOffset, error) {
	return f.offsetsFn(ctx, name)
}

func (f *fakeOffsetsConnect) ServerInfo(ctx context.Context) (connect.ServerInfo, error) {
	if f.infoFn == nil {
		return connect.ServerInfo{}, errors.New("no version")
	}
	return f.infoFn(ctx)
}

func newOffsetsFake(t *testing.T, offsets func(context.Context, string) ([]connect.SourceOffset, error)) *fakeOffsetsConnect {
	t.Helper()
	f := &fakeOffsetsConnect{fakeConnect: &fakeConnect{}, offsetsFn: offsets}
	guardWrites(t, f.fakeConnect)
	return f
}

func offsetsRegistry(t *testing.T, d Deps) tools.Tool {
	t.Helper()
	reg := tools.NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	tool, ok := reg.Get("connect_source_offsets")
	if !ok {
		t.Fatal("connect_source_offsets not registered")
	}
	return tool
}

func TestConnectSourceOffsetsPositions(t *testing.T) {
	f := newOffsetsFake(t, func(_ context.Context, name string) ([]connect.SourceOffset, error) {
		if name != "pg-orders-connector" {
			t.Fatalf("read %q", name)
		}
		return []connect.SourceOffset{{
			Partition: map[string]any{"server": "orders"},
			Offset:    map[string]any{"lsn": json.Number("9223372036854775807"), "txId": json.Number("42"), "file": "mysql-bin.000003", "pos": json.Number("154")},
		}}, nil
	})
	tool := offsetsRegistry(t, Deps{Connect: f})
	r := runTool(t, tool, `{"connector":"pg-orders-connector"}`)
	for _, want := range []string{"pg-orders-connector", "partition server=orders", "lsn=9223372036854775807", "file=mysql-bin.000003", "pos=154"} {
		if !strings.Contains(r.Summary, want) {
			t.Fatalf("summary missing %q: %q", want, r.Summary)
		}
	}
	rows := r.Data.(map[string]any)["connectors"].([]connectorOffsetsRow)
	if len(rows) != 1 || len(rows[0].Offsets) != 1 || rows[0].Offsets[0].Offset["lsn"] != json.Number("9223372036854775807") {
		t.Fatalf("data %+v", rows)
	}
	if r.Truncated {
		t.Fatal("uncapped result marked truncated")
	}
}

func TestConnectSourceOffsetsNoStoredOffsets(t *testing.T) {
	f := newOffsetsFake(t, func(context.Context, string) ([]connect.SourceOffset, error) { return nil, nil })
	tool := offsetsRegistry(t, Deps{Connect: f})
	r := runTool(t, tool, `{"connector":"pg-orders-connector"}`)
	if !strings.Contains(r.Summary, "no stored offsets") {
		t.Fatalf("summary %q", r.Summary)
	}
}

// A Connect without the endpoint returns ErrNotSupported; the tool names both
// the running version (ServerInfo) and the version that adds the endpoint.
func TestConnectSourceOffsetsUnsupportedNamesVersions(t *testing.T) {
	f := newOffsetsFake(t, func(context.Context, string) ([]connect.SourceOffset, error) {
		return nil, fmt.Errorf("connect: read source offsets: Kafka Connect 3.5 or later (KIP-875) adds this endpoint: %w", clients.ErrNotSupported)
	})
	f.infoFn = func(context.Context) (connect.ServerInfo, error) {
		return connect.ServerInfo{Version: "3.4.0"}, nil
	}
	tool := offsetsRegistry(t, Deps{Connect: f})
	_, err := tool.Run(tools.WithUnscoped(context.Background()), json.RawMessage(`{"connector":"pg-orders-connector"}`))
	if !errors.Is(err, clients.ErrNotSupported) {
		t.Fatalf("err %v, want ErrNotSupported", err)
	}
	for _, want := range []string{"3.4.0", "3.5"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("err %v does not name %q", err, want)
		}
	}
}

// Without a connector the tool reads every connector of the room's pipeline
// and none other.
func TestConnectSourceOffsetsScopedToListing(t *testing.T) {
	var read []string
	f := newOffsetsFake(t, func(_ context.Context, name string) ([]connect.SourceOffset, error) {
		read = append(read, name)
		return nil, nil
	})
	tool := offsetsRegistry(t, Deps{Connect: f})
	ctx := tools.WithPipeline(context.Background(), owners.Pipeline{
		ID: "orders", Connectors: []string{"pg-orders-sink", "pg-orders-connector"},
	})
	if _, err := tool.Run(ctx, json.RawMessage(`{}`)); err != nil {
		t.Fatalf("run: %v", err)
	}
	if len(read) != 2 || read[0] != "pg-orders-connector" || read[1] != "pg-orders-sink" {
		t.Fatalf("read %v, want the pipeline's two connectors in sorted order", read)
	}

	read = nil
	_, err := tool.Run(ctx, json.RawMessage(`{"connector":"bill-connector"}`))
	if !errors.Is(err, tools.ErrOutOfScope) {
		t.Fatalf("err %v, want ErrOutOfScope", err)
	}
	if len(read) != 0 {
		t.Fatalf("out-of-scope connector reached the client: %v", read)
	}
}

func TestConnectSourceOffsetsUnscopedNeedsConnector(t *testing.T) {
	f := newOffsetsFake(t, func(context.Context, string) ([]connect.SourceOffset, error) {
		t.Error("client reached without a connector")
		return nil, nil
	})
	tool := offsetsRegistry(t, Deps{Connect: f})
	_, err := tool.Run(tools.WithUnscoped(context.Background()), json.RawMessage(`{}`))
	if !errors.Is(err, tools.ErrBadInput) && !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("err %v, want ErrBadInput", err)
	}
}

func TestConnectSourceOffsetsRegisteredOnlyWithReader(t *testing.T) {
	fc := &fakeConnect{}
	guardWrites(t, fc)
	reg := tools.NewRegistry()
	if err := Register(reg, Deps{Connect: fc}); err != nil {
		t.Fatal(err)
	}
	if _, ok := reg.Get("connect_source_offsets"); ok {
		t.Fatal("tool registered for a Connect without the optional reader")
	}
}

// A synthetic connector can return huge values, nested objects and credentials
// in its offsets. Only scalars are positions; secrets are masked, values and
// entries are capped, and the result says it was cut.
func TestConnectSourceOffsetsCapsAndRedacts(t *testing.T) {
	offsets := make([]connect.SourceOffset, 150)
	for i := range offsets {
		offsets[i] = connect.SourceOffset{
			Partition: map[string]any{"server": "pg", "n": json.Number(fmt.Sprint(i))},
			Offset: map[string]any{
				"lsn":      json.Number(fmt.Sprint(i)),
				"password": "hunter2",
				"nested":   map[string]any{"secret": "top-secret"},
				"long":     strings.Repeat("x", 400),
				"injected": "line1\npg-orders-connector partition forged: lsn=1",
			},
		}
	}
	f := newOffsetsFake(t, func(context.Context, string) ([]connect.SourceOffset, error) { return offsets, nil })
	tool := offsetsRegistry(t, Deps{Connect: f})
	r := runTool(t, tool, `{"connector":"pg-orders-connector"}`)

	if !r.Truncated {
		t.Fatal("capped result not marked truncated")
	}
	if len(r.Summary) > tools.MaxSummaryBytes {
		t.Fatalf("summary %d bytes", len(r.Summary))
	}
	for _, banned := range []string{"hunter2", "top-secret", "line1\n", strings.Repeat("x", 300)} {
		if strings.Contains(r.Summary, banned) {
			t.Fatalf("summary leaks or is uncapped at %q", banned)
		}
	}
	rows := r.Data.(map[string]any)["connectors"].([]connectorOffsetsRow)
	if len(rows) != 1 || len(rows[0].Offsets) != maxSourceOffsetEntries {
		t.Fatalf("rows %d, offsets %d; want %d", len(rows), len(rows[0].Offsets), maxSourceOffsetEntries)
	}
	for _, o := range rows[0].Offsets {
		if _, ok := o.Offset["nested"]; ok {
			t.Fatal("nested object shown as a position")
		}
		if o.Offset["password"] != "***" {
			t.Fatalf("password value %v, want ***", o.Offset["password"])
		}
	}
	if !strings.Contains(r.Summary, "offsets not shown (cap)") {
		t.Fatalf("summary does not say it was cut: %q", r.Summary)
	}
}

func TestConnectSourceOffsetsPropagatesErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"target error", errors.New("connect: GET: status 500: boom")},
		{"not found", fmt.Errorf("connect: GET: %w", clients.ErrNotFound)},
		{"deadline", context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newOffsetsFake(t, func(context.Context, string) ([]connect.SourceOffset, error) { return nil, tc.err })
			tool := offsetsRegistry(t, Deps{Connect: f})
			_, err := tool.Run(tools.WithUnscoped(context.Background()), json.RawMessage(`{"connector":"pg-orders-connector"}`))
			if !errors.Is(err, tc.err) {
				t.Fatalf("err %v, want %v", err, tc.err)
			}
			if !strings.Contains(err.Error(), "pg-orders-connector") {
				t.Fatalf("err %v does not name the connector", err)
			}
		})
	}
}

// End to end over a real Client and httptest: the tool reads the KIP-875 path
// and shows positions.
func TestConnectSourceOffsetsRealClient(t *testing.T) {
	var gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		w.Write([]byte(`{"offsets":[{"partition":{"server":"orders"},"offset":{"lsn":12345678901234567890}}]}`))
	}))
	defer srv.Close()
	tool := offsetsRegistry(t, Deps{Connect: connect.New(srv.URL, srv.Client())})
	r := runTool(t, tool, `{"connector":"pg-orders-connector"}`)
	if gotPath != "/connectors/pg-orders-connector/offsets" {
		t.Fatalf("path %q", gotPath)
	}
	if !strings.Contains(r.Summary, "lsn=12345678901234567890") {
		t.Fatalf("summary %q", r.Summary)
	}
}
