package migrations

import (
	"context"
	"errors"
	"io/fs"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/pressly/goose/v3"
)

// fakeProvider holds applied versions; Down pops the newest unless downErr or
// wrong is set.
type fakeProvider struct {
	all       []int64 // every source version, ascending
	applied   map[int64]bool
	statusErr error
	downErr   error
	wrong     int64 // Down reports this version instead
	downs     int
}

func (f *fakeProvider) Status(context.Context) ([]*goose.MigrationStatus, error) {
	if f.statusErr != nil {
		return nil, f.statusErr
	}
	var out []*goose.MigrationStatus
	for _, v := range f.all {
		st := goose.StatePending
		if f.applied[v] {
			st = goose.StateApplied
		}
		out = append(out, &goose.MigrationStatus{Source: &goose.Source{Version: v, Path: name(v)}, State: st})
	}
	return out, nil
}

func (f *fakeProvider) Down(context.Context) (*goose.MigrationResult, error) {
	f.downs++
	if f.downErr != nil {
		return nil, f.downErr
	}
	var top int64 = -1
	for v := range f.applied {
		if f.applied[v] && v > top {
			top = v
		}
	}
	if f.wrong != 0 {
		top = f.wrong
	}
	delete(f.applied, top)
	return &goose.MigrationResult{Source: &goose.Source{Version: top, Path: name(top)}, Direction: "down"}, nil
}

func name(v int64) string {
	return map[int64]string{1: "00001_init.sql", 2: "00002_cols.sql", 3: "00003_idx.sql"}[v]
}

var testFS = fstest.MapFS{
	"00001_init.sql": {Data: []byte("-- +goose Up\nCREATE TABLE room_events (x int);\n-- +goose Down\nDROP TABLE IF EXISTS room_events;\n")},
	"00002_cols.sql": {Data: []byte("-- +goose Up\nCREATE INDEX room_events_x_idx ON room_events (x);\n-- +goose Down\n-- keeps room_events rows\nDROP INDEX IF EXISTS room_events_x_idx;\n")},
	"00003_idx.sql":  {Data: []byte("-- +goose Up\nCREATE TABLE t (x int);\n-- +goose Down\nDROP TABLE t;\n")},
}

func newFake() *fakeProvider {
	return &fakeProvider{all: []int64{1, 2, 3}, applied: map[int64]bool{1: true, 2: true, 3: true}}
}

func TestPlanDown(t *testing.T) {
	tests := []struct {
		n          int
		wantTarget int64
		wantSteps  []int64
		wantDrops  bool
	}{
		{n: 1, wantTarget: 2, wantSteps: []int64{3}},
		{n: 2, wantTarget: 1, wantSteps: []int64{3, 2}},
		{n: 3, wantTarget: 0, wantSteps: []int64{3, 2, 1}, wantDrops: true},
	}
	for _, tt := range tests {
		p := newFake()
		plan, err := planDown(context.Background(), p, testFS, tt.n)
		if err != nil {
			t.Fatalf("n=%d: %v", tt.n, err)
		}
		var got []int64
		for _, s := range plan.Steps {
			got = append(got, s.Version)
		}
		if plan.Current != 3 || plan.Target != tt.wantTarget || !slices.Equal(got, tt.wantSteps) || plan.DropsEvents() != tt.wantDrops {
			t.Errorf("n=%d: plan = %+v, want target %d steps %v drops %v", tt.n, plan, tt.wantTarget, tt.wantSteps, tt.wantDrops)
		}
		if p.downs != 0 {
			t.Errorf("n=%d: planning ran Down %d times", tt.n, p.downs)
		}
	}
}

func TestPlanDownRefusesBadCount(t *testing.T) {
	for _, n := range []int{-1, 0, 4} {
		_, err := planDown(context.Background(), newFake(), testFS, n)
		if !errors.Is(err, ErrDownCount) {
			t.Errorf("n=%d: err = %v, want ErrDownCount", n, err)
		}
	}
}

func TestPlanDownStatusError(t *testing.T) {
	p := newFake()
	p.statusErr = errors.New("db down")
	if _, err := planDown(context.Background(), p, testFS, 1); err == nil || !strings.Contains(err.Error(), "db down") {
		t.Fatalf("err = %v, want the status error", err)
	}
}

// A migration applied in the database but missing from the files cannot be
// classified, so the plan is refused rather than guessed.
func TestPlanDownUnknownFile(t *testing.T) {
	_, err := planDown(context.Background(), newFake(), fstest.MapFS{}, 1)
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("err = %v, want not-exist", err)
	}
}

func TestApplyDown(t *testing.T) {
	p := newFake()
	plan, err := planDown(context.Background(), p, testFS, 2)
	if err != nil {
		t.Fatal(err)
	}
	done, err := applyDown(context.Background(), p, testFS, plan)
	if err != nil || !slices.Equal(done, []int64{3, 2}) {
		t.Fatalf("done = %v, err = %v", done, err)
	}
	if !p.applied[1] || p.applied[2] || p.applied[3] {
		t.Fatalf("applied after = %v", p.applied)
	}
}

func TestApplyDownRefusesChangedDatabase(t *testing.T) {
	p := newFake()
	plan, err := planDown(context.Background(), p, testFS, 1)
	if err != nil {
		t.Fatal(err)
	}
	delete(p.applied, 3) // someone else rolled back in between
	done, err := applyDown(context.Background(), p, testFS, plan)
	if !errors.Is(err, ErrPlanChanged) || len(done) != 0 || p.downs != 0 {
		t.Fatalf("done = %v, downs = %d, err = %v; want ErrPlanChanged and no Down", done, p.downs, err)
	}
}

func TestApplyDownStopsOnWrongVersion(t *testing.T) {
	p := newFake()
	plan, err := planDown(context.Background(), p, testFS, 2)
	if err != nil {
		t.Fatal(err)
	}
	p.wrong = 1
	done, err := applyDown(context.Background(), p, testFS, plan)
	if !errors.Is(err, ErrPlanChanged) || p.downs != 1 || !slices.Equal(done, []int64{1}) {
		t.Fatalf("done = %v, downs = %d, err = %v", done, p.downs, err)
	}
}

func TestApplyDownStopsOnError(t *testing.T) {
	p := newFake()
	plan, err := planDown(context.Background(), p, testFS, 2)
	if err != nil {
		t.Fatal(err)
	}
	p.downErr = errors.New("boom")
	done, err := applyDown(context.Background(), p, testFS, plan)
	if err == nil || p.downs != 1 || len(done) != 0 {
		t.Fatalf("done = %v, downs = %d, err = %v", done, p.downs, err)
	}
}

// The shipped migrations: only 00001 loses room_events data. A new migration
// whose Down drops or empties the table must show up here.
func TestShippedDownClassification(t *testing.T) {
	files, err := fs.Glob(FS, "*.sql")
	if err != nil || len(files) == 0 {
		t.Fatalf("glob: %v %v", files, err)
	}
	for _, f := range files {
		drops, err := downDropsEvents(FS, f)
		if err != nil {
			t.Fatal(err)
		}
		if want := f == "00001_init.sql"; drops != want {
			t.Errorf("%s: DropsEvents = %v, want %v", f, drops, want)
		}
	}
}

func TestEventLossPatterns(t *testing.T) {
	tests := []struct {
		sql  string
		want bool
	}{
		{"DROP TABLE IF EXISTS room_events;", true},
		{"drop table rooms, room_events cascade;", true},
		{"ALTER TABLE room_events DROP COLUMN actor;", true},
		{"TRUNCATE room_events;", true},
		{"DELETE FROM room_events WHERE true;", true},
		{"DROP INDEX IF EXISTS room_events_room_seq_idx;", false},
		{"CREATE INDEX room_events_room_seq_idx ON room_events (room_id, seq);", false},
		{"ALTER TABLE room_events ADD COLUMN x int;", false},
		{"DROP TABLE shadow_events;", false},
		{"-- DROP TABLE room_events;\nSELECT 1;", false},
	}
	for _, tt := range tests {
		fsys := fstest.MapFS{"m.sql": {Data: []byte("-- +goose Up\nSELECT 1;\n-- +goose Down\n" + tt.sql + "\n")}}
		got, err := downDropsEvents(fsys, "m.sql")
		if err != nil || got != tt.want {
			t.Errorf("%q: got %v (err %v), want %v", tt.sql, got, err, tt.want)
		}
	}
}
