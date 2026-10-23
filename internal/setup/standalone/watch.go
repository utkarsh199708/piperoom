package standalone

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"time"
)

// heartbeatTicks is how many polls pass between the info-level "watching"
// lines; the debug line on every poll is the per-tick proof for --verbose
// logs, the heartbeat (every ten seconds at the default interval) keeps a
// plain log showing the watcher is alive without one line a second.
const heartbeatTicks = 10

// fileState is what --watch compares between ticks: the content digest, the
// modification time and the size, so a rewrite is seen even when the digest
// was read at the same moment the write landed.
type fileState struct {
	mod  time.Time
	size int64
	sum  string
}

// statFile reads path's state. A missing file is a zero state, not an error:
// the owners file a first start bootstraps or the setup room writes may not
// exist yet.
func statFile(path string) (fileState, error) {
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return fileState{}, nil
		}
		return fileState{}, fmt.Errorf("setup: watch %s: %w", path, err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return fileState{}, fmt.Errorf("setup: watch %s: %w", path, err)
	}
	sum := sha256.Sum256(data)
	return fileState{mod: info.ModTime(), size: info.Size(), sum: hex.EncodeToString(sum[:])}, nil
}

// watchRestart runs run until ctx ends and restarts it whenever path is
// written to a state check accepts. The watch outlives the file's bad moments:
// a write that fails check (a half-finished save, a typo) is logged and waited
// out while the running server keeps serving its loaded file, and a restarted
// lifetime that fails is logged and the watch keeps polling, so the next write
// brings the server back. Only a first lifetime that fails ends the watch with
// its error (the person just started it and must see the refusal), and only a
// clean stop nobody asked for ends it with nil. Every poll logs a tick
// (debug), and every heartbeatTicks polls an info line, so "the watcher ticks"
// is visible in any log.
func watchRestart(ctx context.Context, path string, interval time.Duration, log *slog.Logger, run func(context.Context) error, check func() error) error {
	if log == nil {
		log = slog.Default()
	}
	last, err := statFile(path)
	if err != nil {
		return err
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	log.Info("setup: watching the owners file; a write restarts the standalone server", "path", path, "interval", interval.String())
	ticks := 1
	// tick polls once and reports whether the file changed to a state check
	// accepts, so a restart is due.
	tick := func() bool {
		state, err := statFile(path)
		if err != nil {
			log.Warn("setup: watch tick could not read the owners file", "path", path, "err", err)
			return false
		}
		if state == last {
			log.Debug("setup: watch tick", "n", ticks, "path", path)
			if ticks%heartbeatTicks == 0 {
				log.Info("setup: watching the owners file", "ticks", ticks, "path", path)
			}
			ticks++
			return false
		}
		last = state
		if check != nil {
			if err := check(); err != nil {
				log.Warn("setup: the owners file was written but does not load; the server keeps its loaded file until the next write", "path", path, "err", err)
				return false
			}
		}
		log.Info("setup: owners file written; restarting the standalone server", "path", path, "tick", ticks)
		return true
	}
lifetimes:
	for lifetime := 1; ; lifetime++ {
		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- run(runCtx) }()
		restart := false
		running := true
		var runErr error
		for running && !restart {
			select {
			case <-ctx.Done():
				cancel()
				<-done
				return nil
			case runErr = <-done:
				running = false
			case <-ticker.C:
				restart = tick()
			}
		}
		cancel()
		if restart {
			if err := <-done; err != nil {
				log.Debug("setup: the previous server lifetime ended for the restart", "err", err)
			}
			continue
		}
		// The server stopped on its own.
		if runErr == nil {
			// A clean stop nobody asked for leaves nothing to watch.
			return nil
		}
		if lifetime == 1 {
			// A first start that fails is a start failure, not a watch event.
			return runErr
		}
		log.Error("setup: the restarted server failed; --watch keeps polling and restarts on the next owners write", "path", path, "err", runErr)
		for {
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
				if tick() {
					continue lifetimes
				}
			}
		}
	}
}
