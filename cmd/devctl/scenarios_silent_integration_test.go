//go:build integration

package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"piperoom/internal/testenv"
)

// The silent-corruption scenarios against the real dev stack (X1g TC2, TC3,
// TC5): after each break a direct check shows the corruption and `devctl
// health` is green; after the fix the check is clean; break twice and fix once
// ends healthy. They change the shared pipeline, so each test holds the stack
// lock, and they skip when the stack is not reachable (testenv.Require).

func silentStack(t *testing.T) (*stack, context.Context) {
	t.Helper()
	testenv.Require(t)
	s, err := newStack(slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Minute)
	t.Cleanup(cancel)
	acquireStackLock(t)
	return s, ctx
}

func requireHealthy(t *testing.T, ctx context.Context, s *stack, when string) {
	t.Helper()
	if err := settleHealth(ctx, func(w io.Writer) error { return s.health(ctx, w) }, 5*time.Second, 90*time.Second, io.Discard); err != nil {
		t.Fatalf("devctl health %s: %v", when, err)
	}
}

func mustCount(t *testing.T, ctx context.Context, s *stack, db, q string) int64 {
	t.Helper()
	n, err := s.countOf(ctx, db, q)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestIntegrationSinkFilteredRows(t *testing.T) {
	s, ctx := silentStack(t)
	t.Cleanup(func() { _ = fixSinkFilteredRows(context.Background(), s) })
	for range 2 { // TC5: twice
		if err := breakSinkFilteredRows(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	if missing, err := s.backfillMissing(ctx); err != nil || missing < 2*backfillOrders {
		t.Fatalf("after break: %d backfill orders missing (err %v), want at least %d", missing, err, 2*backfillOrders)
	}
	if src, wh := mustCount(t, ctx, s, "orders", sqlCountSourceOrders), mustCount(t, ctx, s, "warehouse", sqlCountWarehouse); src-wh < 2*backfillOrders {
		t.Errorf("count drift %d, want at least %d", src-wh, 2*backfillOrders)
	}
	requireHealthy(t, ctx, s, "after the break (the fault is silent)")
	if err := fixSinkFilteredRows(ctx, s); err != nil {
		t.Fatal(err)
	}
	if missing, _ := s.backfillMissing(ctx); missing != 0 {
		t.Errorf("after fix: %d backfill orders missing", missing)
	}
	requireHealthy(t, ctx, s, "after the fix")
}

func TestIntegrationSinkDroppedColumn(t *testing.T) {
	s, ctx := silentStack(t)
	t.Cleanup(func() { _ = fixSinkDroppedColumn(context.Background(), s) })
	for range 2 {
		if err := breakSinkDroppedColumn(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	if n := mustCount(t, ctx, s, "warehouse", sqlSinkNullTS); n < 2*droppedOrders {
		t.Errorf("after break: %d NULL updated_at, want at least %d", n, 2*droppedOrders)
	}
	requireHealthy(t, ctx, s, "after the break (the fault is silent)")
	if err := fixSinkDroppedColumn(ctx, s); err != nil {
		t.Fatal(err)
	}
	if n := mustCount(t, ctx, s, "warehouse", sqlSinkNullTS); n != 0 {
		t.Errorf("after fix: %d NULL updated_at", n)
	}
	requireHealthy(t, ctx, s, "after the fix")
}

// The duplicate check reads the topic: the same (key, value) twice. The kafka
// client's DupScan (X1c TC8) reads the same records by key and source.lsn.
func TestIntegrationConsumerDuplicates(t *testing.T) {
	s, ctx := silentStack(t)
	t.Cleanup(func() { _ = fixConsumerDuplicates(context.Background(), s) })
	for range 2 {
		if err := breakConsumerDuplicates(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := s.duplicateEvents(ctx); err != nil || n < 2*dupCopies {
		t.Fatalf("after break: %d duplicate events (err %v), want at least %d", n, err, 2*dupCopies)
	}
	requireHealthy(t, ctx, s, "after the break (the fault is silent)")
	if err := fixConsumerDuplicates(ctx, s); err != nil {
		t.Fatal(err)
	}
	if n, err := s.duplicateEvents(ctx); err != nil || n != 0 {
		t.Errorf("after fix: %d duplicate events (err %v)", n, err)
	}
	requireHealthy(t, ctx, s, "after the fix")
}

func TestIntegrationToastPlaceholder(t *testing.T) {
	s, ctx := silentStack(t)
	t.Cleanup(func() { _ = fixToastPlaceholder(context.Background(), s) })
	for range 2 {
		if err := breakToastPlaceholder(ctx, s); err != nil {
			t.Fatal(err)
		}
	}
	if n := mustCount(t, ctx, s, "warehouse", sqlSinkPlaceholders); n < 2*toastOrders {
		t.Errorf("after break: %d placeholder rows, want at least %d", n, 2*toastOrders)
	}
	requireHealthy(t, ctx, s, "after the break (the fault is silent)")
	if err := fixToastPlaceholder(ctx, s); err != nil {
		t.Fatal(err)
	}
	if n := mustCount(t, ctx, s, "warehouse", sqlSinkPlaceholders); n != 0 {
		t.Errorf("after fix: %d placeholder rows", n)
	}
	requireHealthy(t, ctx, s, "after the fix")
}
