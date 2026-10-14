package main

import (
	"fmt"
	"log/slog"
	"os"

	"piperoom/internal/config"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/store"
)

// limitsSetup reads the room limits and the retention policy
// (docs/design/retention.md). Every bad value stops the start: a typo must
// never silently keep (or delete) incident data. own has already been loaded,
// which validated each pipeline's retention_days.
func limitsSetup(cfg config.Config, own *owners.Owners, log *slog.Logger) (config.Limits, room.RetentionPolicy, error) {
	retentionDays, err := store.ParseRetentionDays(cfg.RetentionDays)
	if err != nil {
		return config.Limits{}, room.RetentionPolicy{}, fmt.Errorf("server: PIPEROOM_RETENTION_DAYS: %w", err)
	}
	limits, err := config.ParseLimits(cfg)
	if err != nil {
		return config.Limits{}, room.RetentionPolicy{}, fmt.Errorf("server: %w", err)
	}
	if err := checkArchiveDir(limits.ArchiveDir); err != nil {
		return config.Limits{}, room.RetentionPolicy{}, err
	}
	policy := room.NewRetentionPolicy(retentionDays, own)
	log.Info("server: room limits",
		"max_open_days", limits.MaxOpenDays,
		"room_max_events", limits.RoomMaxEvents,
		"room_max_bytes", limits.RoomMaxBytes,
		"event_max_data_bytes", limits.EventMaxDataBytes,
		"archive", limits.ArchiveDir != "")
	return limits, policy, nil
}

// checkArchiveDir refuses a PIPEROOM_ARCHIVE_DIR that is not an existing
// directory this process can write: otherwise every retention run and every
// delete would fail later, one room at a time. "" (no archive) passes.
func checkArchiveDir(dir string) error {
	if dir == "" {
		return nil
	}
	fi, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("server: PIPEROOM_ARCHIVE_DIR %s: %w", dir, err)
	}
	if !fi.IsDir() {
		return fmt.Errorf("server: PIPEROOM_ARCHIVE_DIR %s: not a directory", dir)
	}
	f, err := os.CreateTemp(dir, ".piperoom-write-check-*")
	if err != nil {
		return fmt.Errorf("server: PIPEROOM_ARCHIVE_DIR %s: not writable: %w", dir, err)
	}
	name := f.Name()
	cerr := f.Close()
	rerr := os.Remove(name)
	if cerr != nil || rerr != nil {
		return fmt.Errorf("server: PIPEROOM_ARCHIVE_DIR %s: write check: %w", dir, firstErr(cerr, rerr))
	}
	return nil
}

func firstErr(errs ...error) error {
	for _, err := range errs {
		if err != nil {
			return err
		}
	}
	return nil
}

// storeLimits gives the store the per-room log caps. Both stores implement
// store.Capped; the check keeps a test double without caps working.
func storeLimits(st store.Store, l config.Limits) {
	if c, ok := st.(store.Capped); ok {
		c.SetLimits(store.Limits{MaxEvents: l.RoomMaxEvents, MaxBytes: l.RoomMaxBytes})
	}
}
