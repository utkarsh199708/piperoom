package store

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
)

var (
	pgconnErr55P03 = pgconn.PgError{Code: "55P03"} // lock_not_available
	pgconnErr57014 = pgconn.PgError{Code: "57014"} // query_canceled
)

// fakeRow is one uncounted row in fakeBackfill.
type fakeRow struct {
	lastSeq int64
	bytes   int64  // payload bytes per seq
	counted *int64 // nil until written
	// moveOnCount appends this many events (moving last_seq) during each of
	// the next countOne sums, as a concurrent Append would.
	moveOnCount []int64
	busy        bool // every attempt hits lock_timeout
}

// fakeBackfill is backfillDB over an in-memory table, applying the same
// guards as the SQL: list only NULL rows by id, write only when byte_size is
// NULL and last_seq still matches.
type fakeBackfill struct {
	tables  map[string]map[string]*fakeRow // keyed by backfillTarget.update
	lists   int
	counts  map[string]int
	failOn  string
	listErr error
}

func newFakeBackfill() *fakeBackfill {
	f := &fakeBackfill{tables: map[string]map[string]*fakeRow{}, counts: map[string]int{}}
	for _, t := range backfillTargets {
		f.tables[t.update] = map[string]*fakeRow{}
	}
	return f
}

func (f *fakeBackfill) rooms() map[string]*fakeRow   { return f.tables[backfillTargets[0].update] }
func (f *fakeBackfill) shadows() map[string]*fakeRow { return f.tables[backfillTargets[1].update] }

func (f *fakeBackfill) list(_ context.Context, t backfillTarget, after string, limit int) ([]backfillRow, error) {
	f.lists++
	if f.listErr != nil {
		return nil, f.listErr
	}
	var ids []string
	for id, r := range f.tables[t.update] {
		if r.counted == nil && id > after {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	if len(ids) > limit {
		ids = ids[:limit]
	}
	out := make([]backfillRow, 0, len(ids))
	for _, id := range ids {
		out = append(out, backfillRow{id: id, lastSeq: f.tables[t.update][id].lastSeq})
	}
	return out, nil
}

func (f *fakeBackfill) reseq(_ context.Context, t backfillTarget, id string) (int64, bool, error) {
	r, ok := f.tables[t.update][id]
	if !ok || r.counted != nil {
		return 0, false, nil
	}
	return r.lastSeq, true, nil
}

func (f *fakeBackfill) countOne(_ context.Context, t backfillTarget, br backfillRow) (countOutcome, error) {
	f.counts[br.id]++
	if br.id == f.failOn {
		return 0, errors.New("connection reset")
	}
	r, ok := f.tables[t.update][br.id]
	if !ok {
		return countMissed, nil
	}
	if r.busy {
		return countBusy, nil
	}
	sum := br.lastSeq * r.bytes // summed up to the listed seq, no lock
	if len(r.moveOnCount) > 0 {
		r.lastSeq += r.moveOnCount[0]
		r.moveOnCount = r.moveOnCount[1:]
	}
	if r.counted != nil || r.lastSeq != br.lastSeq {
		return countMissed, nil
	}
	r.counted = &sum
	return countWritten, nil
}

func TestRunBackfillCountsEveryRowInBatches(t *testing.T) {
	f := newFakeBackfill()
	for _, id := range []string{"r1", "r2", "r3", "r4", "r5"} {
		f.rooms()[id] = &fakeRow{lastSeq: 4, bytes: 10}
	}
	f.shadows()["r1"] = &fakeRow{lastSeq: 2, bytes: 7}
	res, err := runBackfill(context.Background(), f, 2)
	if err != nil {
		t.Fatal(err)
	}
	if res.Rooms != 5 || res.ShadowRuns != 1 || res.Left != 0 {
		t.Fatalf("res = %+v, want 5 rooms, 1 shadow run, none left", res)
	}
	for id, r := range f.rooms() {
		if r.counted == nil || *r.counted != 40 {
			t.Fatalf("%s: byte_size %v, want 40", id, r.counted)
		}
	}
	if got := *f.shadows()["r1"].counted; got != 14 {
		t.Fatalf("shadow byte_size %d, want 14", got)
	}
}

// An append that lands between the sum and the guarded UPDATE moves last_seq:
// the stale sum is not written, the room is summed again at its new last_seq.
func TestRunBackfillRecountsWhenAppendMovesLastSeq(t *testing.T) {
	f := newFakeBackfill()
	f.rooms()["busy"] = &fakeRow{lastSeq: 3, bytes: 10, moveOnCount: []int64{1, 2}}
	res, err := runBackfill(context.Background(), f, 10)
	if err != nil {
		t.Fatal(err)
	}
	r := f.rooms()["busy"]
	if res.Rooms != 1 || res.Left != 0 || r.counted == nil || *r.counted != 60 || r.lastSeq != 6 {
		t.Fatalf("res %+v, byte_size %v at last_seq %d; want 1 room counted to 60 at seq 6", res, r.counted, r.lastSeq)
	}
	if f.counts["busy"] != 3 {
		t.Fatalf("summed %d times, want 3", f.counts["busy"])
	}
}

// A room that keeps moving is left NULL after backfillRetries, not looped on,
// and the rest of the batch is still counted.
func TestRunBackfillLeavesRoomStillMoving(t *testing.T) {
	f := newFakeBackfill()
	f.rooms()["a"] = &fakeRow{lastSeq: 1, bytes: 1, moveOnCount: []int64{1, 1, 1, 1, 1, 1}}
	f.rooms()["b"] = &fakeRow{lastSeq: 1, bytes: 1}
	res, err := runBackfill(context.Background(), f, 10)
	if err != nil {
		t.Fatal(err)
	}
	if res.Rooms != 1 || res.Left != 1 || f.rooms()["a"].counted != nil {
		t.Fatalf("res %+v, a counted %v; want b counted, a left", res, f.rooms()["a"].counted)
	}
	if f.counts["a"] != backfillRetries {
		t.Fatalf("a summed %d times, want %d", f.counts["a"], backfillRetries)
	}
}

// A room whose row lock is not free in time (lock_timeout) is left for the
// next start; keyset paging never lists it again in this call, so the loop
// ends.
func TestRunBackfillLeavesLockedRoomAndEnds(t *testing.T) {
	f := newFakeBackfill()
	f.rooms()["a"] = &fakeRow{lastSeq: 1, bytes: 1, busy: true}
	f.rooms()["b"] = &fakeRow{lastSeq: 2, bytes: 1}
	res, err := runBackfill(context.Background(), f, 1)
	if err != nil {
		t.Fatal(err)
	}
	if res.Rooms != 1 || res.Left != 1 || f.counts["a"] != 1 {
		t.Fatalf("res %+v, a tried %d times; want b counted, a left after one try", res, f.counts["a"])
	}
	// rooms: [a] [b] [] ; shadow_runs: []
	if f.lists != 4 {
		t.Fatalf("listed %d times, want 4", f.lists)
	}
}

func TestRunBackfillErrors(t *testing.T) {
	t.Run("list fails", func(t *testing.T) {
		f := newFakeBackfill()
		f.listErr = errors.New("db down")
		if _, err := runBackfill(context.Background(), f, 1); !errors.Is(err, f.listErr) {
			t.Fatalf("err = %v, want wrapped list error", err)
		}
	})
	t.Run("count fails", func(t *testing.T) {
		f := newFakeBackfill()
		f.rooms()["a"] = &fakeRow{lastSeq: 1, bytes: 1}
		f.failOn = "a"
		if _, err := runBackfill(context.Background(), f, 1); err == nil {
			t.Fatal("err = nil, want the count error")
		}
	})
	t.Run("ctx cancelled", func(t *testing.T) {
		f := newFakeBackfill()
		f.rooms()["a"] = &fakeRow{lastSeq: 1, bytes: 1}
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := runBackfill(ctx, f, 1); !errors.Is(err, context.Canceled) {
			t.Fatalf("err = %v, want context.Canceled", err)
		}
		if f.rooms()["a"].counted != nil {
			t.Fatal("counted after cancel")
		}
	})
}

func TestBusyOr(t *testing.T) {
	alive := context.Background()
	dead, cancel := context.WithCancel(context.Background())
	cancel()
	cases := []struct {
		name    string
		ctx     context.Context
		err     error
		want    countOutcome
		wantErr bool
	}{
		{"lock timeout", alive, &pgconnErr55P03, countBusy, false},
		{"statement timeout", alive, &pgconnErr57014, countBusy, false},
		{"room deadline", alive, context.DeadlineExceeded, countBusy, false},
		{"other", alive, errors.New("syntax"), 0, true},
		{"caller gone", dead, &pgconnErr55P03, 0, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := busyOr(c.ctx, c.err)
			if got != c.want || (err != nil) != c.wantErr {
				t.Fatalf("busyOr = %v, %v; want %v, err %v", got, err, c.want, c.wantErr)
			}
		})
	}
}
