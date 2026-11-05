package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"

	"piperoom/internal/clients"
	"piperoom/internal/clients/pgconv"
)

// ErrSlotLost is returned (wrapped) by DropReplicationSlotChecked when the
// slot's wal_status is "lost" and the caller did not allow it: Postgres has
// already removed WAL the slot still needed. Nothing is dropped.
var ErrSlotLost = errors.New("postgres: replication slot has wal_status lost")

// SlotSeen is the state of a replication slot as the drop transaction read it
// right before the drop: what the action reports as "the state it saw".
type SlotSeen struct {
	Name             string `json:"name"`
	Plugin           string `json:"plugin"`
	Active           bool   `json:"active"`
	WALStatus        string `json:"wal_status"`         // reserved | extended | unreserved | lost
	RetainedWALBytes int64  `json:"retained_wal_bytes"` // WAL head - restart_lsn
}

// slotStateSQL re-reads the LOGICAL slot $1 for DropReplicationSlotChecked:
// the columns of slotCheckSQL plus what the action reports. walHead is the
// read tools' WAL position; the writer is on the primary, where it is
// pg_current_wal_lsn().
const slotStateSQL = `
		SELECT active, COALESCE(database = current_database(), false),
		       COALESCE(plugin, ''), COALESCE(wal_status, ''),
		       COALESCE(pg_wal_lsn_diff(` + walHead + `, restart_lsn)::bigint, 0)
		FROM pg_replication_slots
		WHERE slot_name = $1 AND slot_type = 'logical'`

// DropReplicationSlotChecked is DropReplicationSlot with the wal_status rule
// of pg_drop_replication_slot (P3b): in the same transaction as the drop it
// re-reads the slot and refuses, before any write, a missing or physical slot
// or one of another database (clients.ErrNotFound), an active one
// (clients.ErrSlotActive) and, unless allowLost, one whose wal_status is
// "lost" (ErrSlotLost). A consumer that attaches between the re-read and the
// drop is refused by Postgres itself (pg_drop_replication_slot does not wait
// for an active slot), also as clients.ErrSlotActive. It returns the state it
// read, with the error too once the slot was read. Same scope rule as
// DropReplicationSlot.
func (w *Writer) DropReplicationSlotChecked(ctx context.Context, slot string, allowLost bool) (SlotSeen, error) {
	if w == nil || w.db == nil {
		return SlotSeen{}, ErrNoWriter
	}
	if !slotNameRe.MatchString(slot) {
		return SlotSeen{}, fmt.Errorf("postgres: drop replication slot: invalid slot name %q (lower-case letters, digits and _, at most 63)", cutParam(slot))
	}
	if err := checkWriteScope(ctx, "", nil); err != nil {
		return SlotSeen{}, err
	}
	ctx, cancel := pgconv.WithDeadline(ctx, w.timeout)
	defer cancel()

	seen := SlotSeen{Name: slot}
	dropped := false
	err := w.inTx(ctx, func(tx writeTx) error {
		var here bool
		err := w.queryRow(ctx, tx, slotStateSQL, []any{slot}, &seen.Active, &here, &seen.Plugin, &seen.WALStatus, &seen.RetainedWALBytes)
		if errors.Is(err, pgx.ErrNoRows) {
			return fmt.Errorf("no logical slot of this name (physical slots are never dropped): %w", clients.ErrNotFound)
		}
		if err != nil {
			return fmt.Errorf("read slot: %w", err)
		}
		if !here {
			return fmt.Errorf("the slot belongs to another database: %w", clients.ErrNotFound)
		}
		if seen.Active {
			return clients.ErrSlotActive
		}
		if seen.WALStatus == "lost" && !allowLost {
			return ErrSlotLost
		}
		if err := w.dropSlot(ctx, tx, slot); err != nil {
			return err
		}
		dropped = true
		return nil
	})
	if err != nil && dropped {
		return seen, fmt.Errorf("postgres: drop replication slot %q: the slot is dropped (a drop does not roll back), but: %w", slot, err)
	}
	if err != nil {
		return seen, fmt.Errorf("postgres: drop replication slot %q: %w", slot, err)
	}
	return seen, nil
}

// dropSlot runs pg_drop_replication_slot($1) in tx and maps the two races
// with the re-read before it to their sentinels.
func (w *Writer) dropSlot(ctx context.Context, tx writeTx, slot string) error {
	if err := w.exec(ctx, tx, dropSlotSQL, slot); err != nil {
		switch pgCode(err) {
		case "55006": // object_in_use: a consumer attached since the check
			return fmt.Errorf("drop: %w: %w", clients.ErrSlotActive, err)
		case "42704": // undefined_object: dropped by someone else since the check
			return fmt.Errorf("drop: %w: %w", clients.ErrNotFound, err)
		}
		return fmt.Errorf("drop: %w", err)
	}
	return nil
}
