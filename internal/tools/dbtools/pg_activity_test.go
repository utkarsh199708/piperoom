package dbtools

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"piperoom/internal/clients/postgres"
	"piperoom/internal/tools"
)

// activitySource is the test source with the table statistics reader.
type activitySource struct {
	*fakeSource
	rows   []postgres.TableCounters
	err    error
	called [][]string
}

func (s *activitySource) TableCounters(_ context.Context, tables []string) ([]postgres.TableCounters, error) {
	s.called = append(s.called, append([]string(nil), tables...))
	return s.rows, s.err
}

func activityDeps() (Deps, *activitySource) {
	d := testDeps()
	vac := time.Date(2026, 10, 15, 9, 0, 0, 0, time.UTC)
	src := &activitySource{fakeSource: d.Src.(*fakeSource), rows: []postgres.TableCounters{
		{Table: "public.orders", Inserts: 120, Updates: 7, Deletes: 1, LiveTuples: 119, LastAutovacuum: &vac},
	}}
	d.Src = src
	return d, src
}

func activityTool(t *testing.T, d Deps) tools.Tool {
	t.Helper()
	reg := tools.NewRegistry()
	if err := Register(reg, d); err != nil {
		t.Fatal(err)
	}
	if err := RegisterTableActivity(reg, d); err != nil {
		t.Fatal(err)
	}
	tl, ok := reg.Get("pg_table_activity")
	if !ok {
		t.Fatal("pg_table_activity not registered")
	}
	return tl
}

func TestTableActivityScopedToPipelineTables(t *testing.T) {
	d, src := activityDeps()
	res, err := activityTool(t, d).Run(roomCtx(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(src.called) != 1 || strings.Join(src.called[0], ",") != "public.orders" {
		t.Fatalf("client got %v, want only the pipeline's source tables", src.called)
	}
	for _, want := range []string{"public.orders", "ins=120", "upd=7", "del=1", "live=119", "last_autovacuum=2026-10-15", "last_autoanalyze=never"} {
		if !strings.Contains(res.Summary, want) {
			t.Errorf("summary lacks %q: %q", want, res.Summary)
		}
	}
	b, _ := json.Marshal(res.Data)
	var data struct {
		Tables []struct {
			Table   string `json:"table"`
			Found   bool   `json:"found"`
			Inserts int64  `json:"n_tup_ins"`
		} `json:"tables"`
	}
	if err := json.Unmarshal(b, &data); err != nil || len(data.Tables) != 1 || !data.Tables[0].Found || data.Tables[0].Inserts != 120 {
		t.Fatalf("data = %s (%v)", b, err)
	}
}

func TestTableActivityMissingTableIsReported(t *testing.T) {
	d, src := activityDeps()
	src.rows = nil
	res, err := activityTool(t, d).Run(roomCtx(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(res.Summary, "public.orders: not in pg_stat_user_tables") {
		t.Fatalf("summary = %q", res.Summary)
	}
	b, _ := json.Marshal(res.Data)
	if !strings.Contains(string(b), `"found":false`) {
		t.Fatalf("data must mark the table not found: %s", b)
	}
}

func TestTableActivityDropsRowsOutsideThePipeline(t *testing.T) {
	d, src := activityDeps()
	src.rows = append(src.rows, postgres.TableCounters{Table: "public.payments", Inserts: 9})
	res, err := activityTool(t, d).Run(roomCtx(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	b, _ := json.Marshal(res.Data)
	if strings.Contains(res.Summary, "payments") || strings.Contains(string(b), "payments") {
		t.Fatalf("another pipeline's table leaked: %q %s", res.Summary, b)
	}
}

func TestTableActivityRefusals(t *testing.T) {
	d, src := activityDeps()
	tl := activityTool(t, d)
	if _, err := tl.Run(context.Background(), json.RawMessage(`{}`)); !errors.Is(err, tools.ErrNoScope) {
		t.Errorf("no room: err = %v, want ErrNoScope", err)
	}
	if _, err := tl.Run(roomCtx(), json.RawMessage(`{"pipeline_id":"payments"}`)); !errors.Is(err, tools.ErrOutOfScope) {
		t.Errorf("other pipeline: err = %v, want ErrOutOfScope", err)
	}
	if len(src.called) != 0 {
		t.Fatalf("a refused call reached the client: %v", src.called)
	}
	src.err = errors.New("statement timeout")
	if _, err := tl.Run(roomCtx(), json.RawMessage(`{}`)); err == nil || !strings.Contains(err.Error(), "pg_table_activity") {
		t.Errorf("client error: err = %v", err)
	}
}

// A source that cannot read table statistics gets no tool, so the agent never
// sees one that always fails (the verifier then keeps its slot rule).
func TestTableActivityNotRegisteredWithoutReader(t *testing.T) {
	d := testDeps()
	reg := tools.NewRegistry()
	if err := RegisterTableActivity(reg, d); err != nil {
		t.Fatal(err)
	}
	if _, ok := reg.Get("pg_table_activity"); ok {
		t.Fatal("registered without a reader")
	}
}
