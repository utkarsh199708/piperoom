package remoteclients

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/clients/postgres"
	"piperoom/internal/events"
	"piperoom/internal/setup/ops"
	pgdriver "piperoom/internal/setup/sources/postgres"
)

// The U1-9 Postgres driver's checks run over the runner in a setup room: the
// remote Source is the driver's whole Reader.
var _ pgdriver.Reader = (*Source)(nil)

func TestPublicationTablesRoundTrip(t *testing.T) {
	// The op name this package sends is the one the runner's catalogue
	// registers.
	if OpSourcePublicationTables != ops.OpSourcePublicationTables {
		t.Fatalf("remoteclients sends %q, the catalogue registers %q", OpSourcePublicationTables, ops.OpSourcePublicationTables)
	}
	f := newFakeRunner()
	src := New(f, "r_setup").Source()
	got, err := src.PublicationTables(roomCtx(t), "dbz_publication", "debezium", []string{"public.orders"})
	if err != nil {
		t.Fatalf("PublicationTables: %v", err)
	}
	want := postgres.PublicationTable{Schema: "public", Table: "orders", InPublication: true, Selectable: true}
	if len(got) != 1 || got[0] != want {
		t.Fatalf("PublicationTables = %+v, want [%+v]", got, want)
	}

	// No tables is the empty list on the wire, never null.
	f = newFakeRunner()
	if _, err := New(f, "r_setup").Source().PublicationTables(roomCtx(t), "dbz_publication", "", nil); err != nil {
		t.Fatalf("PublicationTables without tables: %v", err)
	}
	if calls := f.callsFor(OpSourcePublicationTables); len(calls) != 1 ||
		string(calls[0].req.Params) != `{"publication":"dbz_publication","role":"","tables":[]}` {
		t.Fatalf("wire params = %+v", calls)
	}
}

func TestPublicationTablesErrorPaths(t *testing.T) {
	t.Run("empty publication is refused before sending", func(t *testing.T) {
		f := newFakeRunner()
		_, err := New(f, "r_setup").Source().PublicationTables(roomCtx(t), "", "debezium", nil)
		if !errors.Is(err, ErrBadArgs) {
			t.Fatalf("err = %v, want ErrBadArgs", err)
		}
		if n := len(f.allCalls()); n != 0 {
			t.Fatalf("%d ops sent", n)
		}
	})
	t.Run("not_found is clients.ErrNotFound", func(t *testing.T) {
		f := newFakeRunner()
		f.failures[OpSourcePublicationTables] = fakeFailure{summary: "publication dbz does not exist", code: ErrorCodeNotFound}
		_, err := New(f, "r_setup").Source().PublicationTables(roomCtx(t), "dbz", "debezium", nil)
		if !errors.Is(err, clients.ErrNotFound) {
			t.Fatalf("err = %v, want clients.ErrNotFound", err)
		}
	})
	t.Run("a failure without a code is not a sentinel", func(t *testing.T) {
		f := newFakeRunner()
		f.failures[OpSourcePublicationTables] = fakeFailure{summary: "connection refused"}
		_, err := New(f, "r_setup").Source().PublicationTables(roomCtx(t), "dbz", "debezium", nil)
		if err == nil || errors.Is(err, clients.ErrNotFound) {
			t.Fatalf("err = %v", err)
		}
	})
	t.Run("a result without tables is a bad result", func(t *testing.T) {
		f := newFakeRunner()
		f.overrides[OpSourcePublicationTables] = json.RawMessage(`{"members":[]}`)
		_, err := New(f, "r_setup").Source().PublicationTables(roomCtx(t), "dbz", "debezium", nil)
		if !errors.Is(err, ErrBadResult) {
			t.Fatalf("err = %v, want ErrBadResult", err)
		}
	})
	t.Run("transport error", func(t *testing.T) {
		f := newFakeRunner()
		f.err = errors.New("socket closed")
		if _, err := New(f, "r_setup").Source().PublicationTables(roomCtx(t), "dbz", "debezium", nil); err == nil {
			t.Fatal("err = nil for a broken transport")
		}
	})
	t.Run("no runner", func(t *testing.T) {
		if _, err := New(nil, "").Source().PublicationTables(roomCtx(t), "dbz", "debezium", nil); !errors.Is(err, ErrNoRunner) {
			t.Fatalf("err = %v, want ErrNoRunner", err)
		}
	})
}

// pubSource is the runner's source database in the end-to-end test.
type pubSource struct {
	out []postgres.PublicationTable
	err error
}

func (p pubSource) PublicationTables(context.Context, string, string, []string) ([]postgres.PublicationTable, error) {
	return p.out, p.err
}

// TestPublicationTablesThroughTheRealCatalogue drives the remote Source
// through the real runner and the real ops catalogue: the op the client sends
// is one the catalogue validates and runs, its Data decodes as the client
// expects, a missing publication comes back as clients.ErrNotFound, and a
// publication name the identifier rule refuses never reaches the source.
func TestPublicationTablesThroughTheRealCatalogue(t *testing.T) {
	newCat := func(t *testing.T, src ops.SourceReader) *ops.Registry {
		t.Helper()
		cat, err := ops.New(ops.Config{
			ProjectDir: t.TempDir(),
			Targets:    []events.SetupTarget{{Kind: "postgres", Address: "db.acme:5432"}},
			Source:     src,
		})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cat.Close() })
		return cat
	}

	want := []postgres.PublicationTable{{Schema: "public", Table: "orders", InPublication: true, Selectable: false}}
	exec := startRunner(t, newCat(t, pubSource{out: want}), false)
	got, err := New(exec, "r_setup").Source().PublicationTables(roomCtx(t), "dbz_publication", "debezium", []string{"public.orders"})
	if err != nil {
		t.Fatalf("PublicationTables: %v", err)
	}
	if len(got) != 1 || got[0] != want[0] {
		t.Fatalf("PublicationTables = %+v, want %+v", got, want)
	}

	exec = startRunner(t, newCat(t, pubSource{err: fmt.Errorf("postgres: publication %q: %w", "dbz", clients.ErrNotFound)}), false)
	if _, err := New(exec, "r_setup").Source().PublicationTables(roomCtx(t), "dbz", "debezium", nil); !errors.Is(err, clients.ErrNotFound) {
		t.Fatalf("missing publication: err = %v, want clients.ErrNotFound", err)
	}

	called := &countingSource{}
	exec = startRunner(t, newCat(t, called), false)
	if _, err := New(exec, "r_setup").Source().PublicationTables(roomCtx(t), "dbz; DROP TABLE orders", "debezium", nil); err == nil {
		t.Fatal("a non-identifier publication was accepted")
	}
	if called.n != 0 {
		t.Fatalf("the source was read %d times for a refused name", called.n)
	}
}

type countingSource struct{ n int }

func (c *countingSource) PublicationTables(context.Context, string, string, []string) ([]postgres.PublicationTable, error) {
	c.n++
	return nil, nil
}
