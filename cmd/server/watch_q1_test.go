package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"piperoom/internal/agent"
	"piperoom/internal/alertin"
	"piperoom/internal/config"
	"piperoom/internal/notify"
	"piperoom/internal/room"
	"piperoom/internal/store/memstore"
	"piperoom/internal/watch"
)

func (f *fakeRoomSvc) OpenRoomInfo(p string) (room.WatchRoomInfo, bool) {
	f.pipeline = p
	return f.info, f.infoOK
}

func (f *fakeRoomSvc) PostRoomEvidence(_ context.Context, roomID, text string, ev json.RawMessage) error {
	f.gotPost, f.gotEvidence, f.gotIncident = [2]string{roomID, text}, string(ev), false
	return f.postErr
}

func (f *fakeRoomSvc) PostIncidentJSON(_ context.Context, roomID, text string, ev json.RawMessage) error {
	f.gotPost, f.gotEvidence, f.gotIncident = [2]string{roomID, text}, string(ev), true
	return f.postErr
}

func TestAdapterOpenRoomInfo(t *testing.T) {
	at := time.Date(2026, 10, 15, 4, 0, 0, 0, time.UTC)
	svc := &fakeRoomSvc{infoOK: true, info: room.WatchRoomInfo{RoomID: "r_1", AlertSource: "watcher", OpenedAt: at, LastEventAt: at.Add(time.Hour)}}
	got, ok := watchRooms{svc}.OpenRoomInfo("orders")
	want := watch.RoomInfo{RoomID: "r_1", AlertSource: "watcher", OpenedAt: at, LastEventAt: at.Add(time.Hour)}
	if !ok || got != want || svc.pipeline != "orders" {
		t.Fatalf("got %+v %v (pipeline %q), want %+v", got, ok, svc.pipeline, want)
	}
	if _, ok := (watchRooms{&fakeRoomSvc{}}).OpenRoomInfo("orders"); ok {
		t.Fatal("no open room: want ok false")
	}
}

func TestAdapterEvidenceAndIncidentPosts(t *testing.T) {
	closed := &room.Error{Status: 409, Code: "room_closed", Msg: "room already closed"}
	missing := &room.Error{Status: 404, Code: "not_found", Msg: "not found"}
	for _, incident := range []bool{false, true} {
		post := func(r watchRooms) error {
			if incident {
				return r.PostIncidentJSON(context.Background(), "r_1", "note", json.RawMessage(`{"source":"watcher"}`))
			}
			return r.PostEvidenceJSON(context.Background(), "r_1", "note", json.RawMessage(`{"source":"watcher"}`))
		}
		svc := &fakeRoomSvc{}
		if err := post(watchRooms{svc}); err != nil {
			t.Fatalf("incident=%v: %v", incident, err)
		}
		if svc.gotPost != [2]string{"r_1", "note"} || svc.gotEvidence != `{"source":"watcher"}` || svc.gotIncident != incident {
			t.Fatalf("incident=%v: service got %v %q incident=%v", incident, svc.gotPost, svc.gotEvidence, svc.gotIncident)
		}
		if err := post(watchRooms{&fakeRoomSvc{postErr: closed}}); !errors.Is(err, watch.ErrRoomClosed) || errors.Is(err, watch.ErrNoRoom) {
			t.Errorf("incident=%v closed: err %v", incident, err)
		}
		if err := post(watchRooms{&fakeRoomSvc{postErr: missing}}); !errors.Is(err, watch.ErrRoomClosed) || !errors.Is(err, watch.ErrNoRoom) {
			t.Errorf("incident=%v missing: err %v", incident, err)
		}
	}
}

// The adapter over the real service: a watcher room reports its origin, and a
// new incident on a room restored after a restart starts its agent.
func TestAdapterRealServiceIncidentStartsAgent(t *testing.T) {
	st := memstore.New()
	starts := make(chan struct{}, 4)
	first := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: fakeRun})
	snap, _, err := first.OpenAlertRoom(context.Background(), alertin.Alert{
		Source: alertin.Watcher, Status: alertin.StatusFiring, Severity: "sev2", PipelineID: "orders",
		Title: "orders: connector down", DedupKey: "watcher:orders", Raw: json.RawMessage(`{"source":"watcher"}`),
	})
	if err != nil {
		t.Fatal(err)
	}
	first.Close()

	svc := room.New(room.Deps{Store: st, Users: room.DefaultUsers(), Run: countingRun(starts)})
	t.Cleanup(svc.Close)
	if err := svc.Load(context.Background()); err != nil {
		t.Fatal(err)
	}
	a := watchRooms{svc}
	info, ok := a.OpenRoomInfo("orders")
	if !ok || info.RoomID != snap.ID || info.AlertSource != "watcher" || info.OpenedAt.IsZero() {
		t.Fatalf("info = %+v %v", info, ok)
	}
	if err := a.PostIncidentJSON(context.Background(), snap.ID, "watcher: new signal", json.RawMessage(`{"source":"watcher","signals":[{"signal":"consumer_lag"}]}`)); err != nil {
		t.Fatal(err)
	}
	select {
	case <-starts:
	case <-time.After(5 * time.Second):
		t.Fatal("a new incident did not start the agent of a restored room")
	}
}

// countingRun is an agent run that reports its start and waits for the end.
func countingRun(starts chan<- struct{}) func(context.Context, agent.Alert, agent.Deps, agent.Options) error {
	return func(ctx context.Context, _ agent.Alert, _ agent.Deps, _ agent.Options) error {
		starts <- struct{}{}
		<-ctx.Done()
		return nil
	}
}

func TestPagingSetup(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, nil))
	cfg := config.Config{NotifyWebhookURL: "https://hooks.example.com/T0K3N", NotifyWebhookSecret: "sekrit", PageUndrivenAfter: "7m", PublicURL: "https://piperoom.example.com"}
	p, err := pagingSetup(cfg, log)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := p.Notifier.(*notify.Webhook); !ok || p.UndrivenAfter != 7*time.Minute {
		t.Fatalf("paging = %+v", p)
	}
	if out := buf.String(); strings.Contains(out, "T0K3N") || strings.Contains(out, "sekrit") {
		t.Fatalf("setup logged the URL or the secret: %s", out)
	}

	// No webhook, default and "0" waits.
	if p, err := pagingSetup(config.Config{}, discardLog()); err != nil || p.Notifier != nil || p.UndrivenAfter != 5*time.Minute {
		t.Fatalf("defaults: %+v %v", p, err)
	}
	if p, err := pagingSetup(config.Config{PageUndrivenAfter: "0"}, discardLog()); err != nil || p.UndrivenAfter != 0 {
		t.Fatalf("off: %+v %v", p, err)
	}

	for _, tc := range []struct {
		cfg  config.Config
		want string
	}{
		{config.Config{NotifyWebhookURL: "http://hooks.example.com/x"}, "PIPEROOM_NOTIFY_WEBHOOK_URL"},
		{config.Config{NotifyWebhookURL: "https://user:pw@hooks.example.com/x"}, "PIPEROOM_NOTIFY_WEBHOOK_URL"},
		{config.Config{NotifyWebhookURL: "not a url"}, "PIPEROOM_NOTIFY_WEBHOOK_URL"},
		{config.Config{PageUndrivenAfter: "soon"}, "PIPEROOM_PAGE_UNDRIVEN_AFTER"},
		{config.Config{PageUndrivenAfter: "-5m"}, "PIPEROOM_PAGE_UNDRIVEN_AFTER"},
	} {
		if _, err := pagingSetup(tc.cfg, discardLog()); err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("%+v: err %v, want one naming %s", tc.cfg, err, tc.want)
		}
	}

	buf.Reset()
	if _, err := pagingSetup(config.Config{NotifyWebhookSecret: "sekrit"}, log); err != nil {
		t.Fatal(err)
	}
	if out := buf.String(); !strings.Contains(out, "PIPEROOM_NOTIFY_WEBHOOK_SECRET is set") || strings.Contains(out, "sekrit") {
		t.Fatalf("secret without URL: log %s", out)
	}
}
