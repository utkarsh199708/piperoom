package httpapi_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"piperoom/internal/agent"
	"piperoom/internal/events"
	"piperoom/internal/httpapi"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
)

// While a hand-off request is in flight, every POST to the route answers
// 409 handoff_in_progress first: from a non-driver, with a bad body, and
// concurrently from the driver. Exactly one request creates a hand-off.
func TestHandoffInProgressHTTP(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	st := memstore.New()
	svc := room.New(room.Deps{
		Store: st, Users: room.DefaultUsers(), Run: fakeRun,
		WriteHandoff: func(ctx context.Context, _ []events.Event, _ agent.Options) (events.HandoffSummary, error) {
			entered <- struct{}{}
			<-release
			return events.HandoffSummary{WhatBroke: "x"}, nil
		},
	})
	api := httpapi.New(svc, st, nil)
	srv := httptest.NewServer(api)
	t.Cleanup(func() { srv.Close(); api.Close() })
	snap, err := svc.CreateRoom(context.Background(), mustUserFrom(t, svc, "arjun"), "t", "sev2", "orders")
	if err != nil {
		t.Fatal(err)
	}
	url := srv.URL + "/api/rooms/" + snap.ID + "/handoffs"

	first := make(chan int, 1)
	go func() {
		resp := doJSON(t, http.MethodPost, url, "arjun", `{"to":"priya"}`)
		resp.Body.Close()
		first <- resp.StatusCode
	}()
	<-entered

	for _, c := range []struct{ user, body string }{
		{"arjun", `{"to":"priya"}`},
		{"kabir", `{"to":"priya"}`}, // not the driver
		{"arjun", `not json`},       // bad body
		{"arjun", `{"to":"arjun"}`}, // to self
	} {
		resp := doJSON(t, http.MethodPost, url, c.user, c.body)
		wantStatus(t, resp, http.StatusConflict)
		if b := readErr(t, resp); b.Error.Code != "handoff_in_progress" {
			t.Fatalf("%s %s: code %q", c.user, c.body, b.Error.Code)
		}
		resp.Body.Close()
	}

	// Concurrent requests from the driver while in flight: all 409.
	var wg sync.WaitGroup
	codes := make(chan int, 8)
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp := doJSON(t, http.MethodPost, url, "arjun", `{"to":"meera"}`)
			resp.Body.Close()
			codes <- resp.StatusCode
		}()
	}
	wg.Wait()
	close(codes)
	for c := range codes {
		if c != http.StatusConflict {
			t.Fatalf("concurrent request got %d", c)
		}
	}

	close(release)
	if c := <-first; c != http.StatusOK {
		t.Fatalf("the first request got %d", c)
	}
}
