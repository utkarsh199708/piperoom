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

// fileState is what --watch compares between ticks: the content digest, so a
// rewrite with the same bytes restarts nothing, and the modification time and
// size, so a write that leaves the digest readable at the same moment is still
// seen next tick.
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
// written. A server that stops on its own is a failure and ends the watch; a
// server stopped for a restart is not. Every poll logs a tick (debug), and
// every heartbeatTicks polls an info line, so "the watcher ticks" is visible
// in any log.
func watchRestart(ctx context.Context, path string, interval time.Duration, log *slog.Logger, run func(context.Context) error) error {
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
	for ticks := 1; ; {
		runCtx, cancel := context.WithCancel(ctx)
		done := make(chan error, 1)
		go func() { done <- run(runCtx) }()
		restart := false
		for !restart {
			select {
			case <-ctx.Done():
				cancel()
				<-done
				return nil
			case err := <-done:
				cancel()
				if err != nil {
					return err
				}
				// A clean stop nobody asked for leaves nothing to watch.
				return nil
			case <-ticker.C:
				state, err := statFile(path)
				if err != nil {
					log.Warn("setup: watch tick could not read the owners file", "path", path, "err", err)
					continue
				}
				if state != last {
					last = state
					restart = true
					log.Info("setup: owners file written; restarting the standalone server", "path", path, "tick", ticks)
					continue
				}
				log.Debug("setup: watch tick", "n", ticks, "path", path)
				if ticks%heartbeatTicks == 0 {
					log.Info("setup: watching the owners file", "ticks", ticks, "path", path)
				}
				ticks++
			}
		}
		cancel()
		if err := <-done; err != nil {
			log.Debug("setup: the previous server lifetime ended for the restart", "err", err)
		}
	}
}
