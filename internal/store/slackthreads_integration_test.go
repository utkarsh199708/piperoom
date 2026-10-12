//go:build integration

package store_test

import (
	"context"
	"testing"
	"time"

	"piperoom/internal/slackbridge"
	"piperoom/internal/testenv"
)

func TestSlackThreadsPutGetUpsert(t *testing.T) {
	s, _ := requireStore(t)
	roomID := createTestRoom(t, s)
	th := s.SlackThreads()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if _, ok, err := th.Get(ctx, roomID); err != nil || ok {
		t.Fatalf("get before put: ok=%v err=%v, want ok=false", ok, err)
	}
	first := slackbridge.Thread{Channel: "C1", TS: "1700000000.000100", LastSeq: 1}
	if err := th.Put(ctx, roomID, first); err != nil {
		t.Fatalf("put: %v", err)
	}
	got, ok, err := th.Get(ctx, roomID)
	if err != nil || !ok || got != first {
		t.Fatalf("get after put = %+v ok=%v err=%v, want %+v", got, ok, err, first)
	}
	next := slackbridge.Thread{Channel: "C1", TS: first.TS, LastSeq: 7}
	if err := th.Put(ctx, roomID, next); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	if got, ok, err = th.Get(ctx, roomID); err != nil || !ok || got != next {
		t.Fatalf("get after upsert = %+v ok=%v err=%v, want %+v", got, ok, err, next)
	}
}

func TestSlackThreadsMissingRoom(t *testing.T) {
	s, _ := requireStore(t)
	th := s.SlackThreads()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	missing := testenv.Name("noroom")
	if _, ok, err := th.Get(ctx, missing); err != nil || ok {
		t.Fatalf("get missing room: ok=%v err=%v, want ok=false, nil", ok, err)
	}
	// The foreign key keeps orphan threads out.
	if err := th.Put(ctx, missing, slackbridge.Thread{Channel: "C1", TS: "1.1"}); err == nil {
		t.Fatal("put for a room that does not exist: want error")
	}
}
