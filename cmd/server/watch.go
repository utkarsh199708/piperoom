package main

// Wiring of the built-in watcher (internal/watch) into the server: config, the
// adapter from room.Service to watch.RoomSink, the room-closed hook, and the
// start/stop of the watcher goroutine. Design: docs/design/watcher.md, section 8.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"piperoom/internal/alertin"
	"piperoom/internal/config"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/watch"
)

// Codes of *room.Error that the adapter maps (docs/api.md error table). The room
// package keeps them unexported; the adapter tests run the real service, so a
// change there fails a test here.
const (
	roomErrClosed   = "room_closed"
	roomErrNotFound = "not_found"
)

// watchStopTimeout bounds how long shutdown waits for the watcher to return.
// Every call the watcher makes is bound to its context, so this only guards a
// client that ignores cancellation: the server must still be able to stop.
const watchStopTimeout = 15 * time.Second

// watchConfig reads the PIPEROOM_WATCH_* settings. An invalid value is an error
// that names the variable, so a typo stops the start instead of silently
// switching the watcher to a default. --demo has no real stack to watch, so it
// forces the mode to off (after the values were checked).
func watchConfig(cfg config.Config, demo bool, log *slog.Logger) (config.Watch, error) {
	wc, err := config.ParseWatch(cfg)
	if err != nil {
		return config.Watch{}, fmt.Errorf("server: %w", err)
	}
	if demo && wc.Mode != config.WatchOff {
		log.Info("server: watcher is off in --demo", "configured_mode", string(wc.Mode))
		wc.Mode = config.WatchOff
	}
	return wc, nil
}

// watchRoomService is the part of *room.Service the adapter uses.
type watchRoomService interface {
	OpenAlertRoom(ctx context.Context, a alertin.Alert) (room.RoomSnapshot, bool, error)
	OpenRoomForPipeline(pipelineID string) (roomID string, ok bool)
	LastRoomForKey(pipelineID, dedupKey string) (roomID string, closedAt *time.Time, ok bool)
	PostRoomNote(ctx context.Context, roomID, text string) error
}

// watchRooms adapts the room service to watch.RoomSink. It only translates
// values and errors; the watcher decides what to do about them.
type watchRooms struct{ svc watchRoomService }

var _ watch.RoomSink = watchRooms{}

// OpenAlertRoom opens or joins the room for a. A resolved alert that matches no
// open room becomes watch.ErrNoRoom.
func (r watchRooms) OpenAlertRoom(ctx context.Context, a alertin.Alert) (string, bool, error) {
	snap, created, err := r.svc.OpenAlertRoom(ctx, a)
	if err != nil {
		if errors.Is(err, room.ErrResolvedAlertNoRoom) {
			return "", false, fmt.Errorf("%w: %w", watch.ErrNoRoom, err)
		}
		return "", false, err
	}
	return snap.ID, created, nil
}

// OpenRoomForPipeline passes straight through.
func (r watchRooms) OpenRoomForPipeline(pipelineID string) (string, bool) {
	return r.svc.OpenRoomForPipeline(pipelineID)
}

// LastRoomForKey passes straight through.
func (r watchRooms) LastRoomForKey(pipelineID, dedupKey string) (string, *time.Time, bool) {
	return r.svc.LastRoomForKey(pipelineID, dedupKey)
}

// PostEvidence posts a system note. A closed room becomes watch.ErrRoomClosed.
// An unknown room (purged by retention, say) becomes watch.ErrNoRoom and also
// watch.ErrRoomClosed: the watcher only stops retrying on ErrRoomClosed, and a
// room that is gone is as finished as a closed one.
func (r watchRooms) PostEvidence(ctx context.Context, roomID, text string) error {
	err := r.svc.PostRoomNote(ctx, roomID, text)
	if err == nil {
		return nil
	}
	var re *room.Error
	if errors.As(err, &re) {
		switch re.Code {
		case roomErrClosed:
			return fmt.Errorf("%w: %w", watch.ErrRoomClosed, err)
		case roomErrNotFound:
			return fmt.Errorf("%w: %w: %w", watch.ErrNoRoom, watch.ErrRoomClosed, err)
		}
	}
	return err
}

// roomClosedWatcher is the part of *watch.Watcher the hook calls.
type roomClosedWatcher interface {
	OnRoomClosed(roomID, pipelineID, dedupKey string)
}

// roomForgetter is the part of *agent.Endpoint the hook calls: Forget drops the
// room's token count so the map does not grow for the life of the process.
type roomForgetter interface {
	Forget(roomID string)
}

// roomClosedHook is room.Deps.OnRoomClosed. The service is built before the
// watcher (the watcher needs the service as its RoomSink), so the watcher is set
// afterwards; until then, and when the mode is off, the hook only forgets. It
// does no I/O: it takes a read lock, updates two in-memory maps and returns.
type roomClosedHook struct {
	mu      sync.RWMutex
	watcher roomClosedWatcher
	forget  roomForgetter
}

// setWatcher installs the watcher.
func (h *roomClosedHook) setWatcher(w roomClosedWatcher) {
	h.mu.Lock()
	h.watcher = w
	h.mu.Unlock()
}

// setForget installs the token-budget forgetter. Demo mode has none.
func (h *roomClosedHook) setForget(f roomForgetter) {
	h.mu.Lock()
	h.forget = f
	h.mu.Unlock()
}

// onClosed matches room.Deps.OnRoomClosed.
func (h *roomClosedHook) onClosed(_ context.Context, roomID, pipelineID, dedupKey string) {
	h.mu.RLock()
	w, f := h.watcher, h.forget
	h.mu.RUnlock()
	if w != nil {
		w.OnRoomClosed(roomID, pipelineID, dedupKey)
	}
	if f != nil {
		f.Forget(roomID)
	}
}

// watchedPipelines counts the pipelines the watcher looks at: enabled, with at
// least one v1 signal on (the same rule as watch.Watcher.targets).
func watchedPipelines(o *owners.Owners) int {
	if o == nil {
		return 0
	}
	n := 0
	for _, p := range o.Pipelines {
		c := p.Watch()
		if c.Enabled && (c.ConnectorDown.Enabled || c.SlotInactive.Enabled || c.SlotWAL.Enabled) {
			n++
		}
	}
	return n
}

// watchRunner is the part of *watch.Watcher the server starts.
type watchRunner interface {
	Run(ctx context.Context) error
}

// startWatch runs w in its own goroutine and returns a stop func that cancels it
// and waits for Run to return (at most timeout, then it logs and gives up). The
// stop func is safe to call more than once and from several goroutines.
func startWatch(ctx context.Context, w watchRunner, timeout time.Duration, log *slog.Logger) (stop func()) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := w.Run(ctx); err != nil {
			log.Error("server: watcher stopped with an error", "err", err)
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(timeout):
				log.Warn("server: watcher did not stop in time", "timeout", timeout.String())
			}
		})
	}
}

// startWatcher builds the watcher from d, hands it to the room-closed hook and
// starts it. It must be called after the room service has loaded. Mode off
// builds nothing and returns a no-op stop func. The returned stop func cancels
// the watcher and waits for it, so shutdown calls it before closing the service.
func startWatcher(ctx context.Context, d watch.Deps, hook *roomClosedHook, log *slog.Logger) (stop func(), err error) {
	if d.Config.Mode == config.WatchOff {
		log.Info("server: watcher off")
		return func() {}, nil
	}
	d.Log = log
	w, err := watch.New(d)
	if err != nil {
		return nil, fmt.Errorf("server: build the watcher: %w", err)
	}
	hook.setWatcher(w)
	log.Info("server: watcher starting",
		"mode", string(d.Config.Mode),
		"interval", d.Config.Interval.String(),
		"pipelines", watchedPipelines(d.Owners))
	return startWatch(ctx, w, watchStopTimeout, log), nil
}

// httpShutdowner is the part of *http.Server that shutdown uses.
type httpShutdowner interface {
	Shutdown(ctx context.Context) error
}

// serviceCloser is the part of the HTTP handler that closes WebSocket
// connections and the room service.
type serviceCloser interface {
	Close()
}

// shutdownServer is the stop sequence after a signal or a listen error: the
// watcher first (so nothing posts into the service while it closes), then the
// HTTP server, then the handler, which closes the room service.
func shutdownServer(stopWatch func(), srv httpShutdowner, h serviceCloser) {
	stopWatch()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	// Shutdown waits for non-hijacked requests; WS connections are hijacked, so
	// close them (and the room service) explicitly.
	_ = srv.Shutdown(shutdownCtx)
	h.Close()
}
