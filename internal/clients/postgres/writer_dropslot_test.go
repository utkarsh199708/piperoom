package postgres

import (
	"errors"
	"reflect"
	"strings"
	"testing"

	"piperoom/internal/clients"
	"piperoom/internal/tools"
)

// slotStateRow answers slotStateSQL.
func slotStateRow(active, here bool, walStatus string) func(dest ...any) error {
	return func(dest ...any) error {
		*dest[0].(*bool) = active
		*dest[1].(*bool) = here
		*dest[2].(*string) = "pgoutput"
		*dest[3].(*string) = walStatus
		*dest[4].(*int64) = 4096
		return nil
	}
}

func TestDropReplicationSlotCheckedSQL(t *testing.T) {
	tx := &fakeTx{scan: slotStateRow(false, true, "extended")}
	w, _ := testWriter(tx)
	seen, err := w.DropReplicationSlotChecked(unscopedCtx(), "debezium_orders", false)
	if err != nil {
		t.Fatal(err)
	}
	checkTx(t, tx, true)
	if got, want := tx.sqls(), []string{slotStateSQL, "SELECT pg_drop_replication_slot($1)"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("statements:\n got %q\nwant %q", got, want)
	}
	for _, s := range tx.stmts[1:] {
		if !reflect.DeepEqual(s.args, []any{"debezium_orders"}) {
			t.Fatalf("%q args = %v, want the slot as $1", s.sql, s.args)
		}
	}
	want := SlotSeen{Name: "debezium_orders", Plugin: "pgoutput", WALStatus: "extended", RetainedWALBytes: 4096}
	if seen != want {
		t.Fatalf("seen = %+v, want %+v", seen, want)
	}
	if !strings.Contains(slotStateSQL, "slot_type = 'logical'") || !strings.Contains(slotStateSQL, "current_database()") {
		t.Fatal("the re-read must keep to logical slots of this database")
	}
}

func TestDropReplicationSlotCheckedRefusals(t *testing.T) {
	cases := []struct {
		name      string
		scan      func(dest ...any) error
		allowLost bool
		execErr   func(string) error
		want      error
		dropped   bool
	}{
		{name: "missing slot", want: clients.ErrNotFound},
		{name: "another database", scan: slotStateRow(false, false, "reserved"), want: clients.ErrNotFound},
		{name: "active", scan: slotStateRow(true, true, "reserved"), want: clients.ErrSlotActive},
		{name: "active and lost", scan: slotStateRow(true, true, "lost"), allowLost: true, want: clients.ErrSlotActive},
		{name: "lost not allowed", scan: slotStateRow(false, true, "lost"), want: ErrSlotLost},
		{name: "active since the check", scan: slotStateRow(false, true, "reserved"), execErr: failOn("SELECT pg_drop", pgErr("55006")), want: clients.ErrSlotActive, dropped: true},
		{name: "dropped since the check", scan: slotStateRow(false, true, "reserved"), execErr: failOn("SELECT pg_drop", pgErr("42704")), want: clients.ErrNotFound, dropped: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tx := &fakeTx{scan: tc.scan, execErr: tc.execErr}
			w, _ := testWriter(tx)
			_, err := w.DropReplicationSlotChecked(unscopedCtx(), "debezium_orders", tc.allowLost)
			if !errors.Is(err, tc.want) {
				t.Fatalf("error %v does not wrap %v", err, tc.want)
			}
			sent := false
			for _, s := range tx.sqls() {
				sent = sent || strings.HasPrefix(s, "SELECT pg_drop")
			}
			if sent != tc.dropped {
				t.Fatalf("drop sent = %v, want %v", sent, tc.dropped)
			}
			checkTx(t, tx, false)
		})
	}
}

func TestDropReplicationSlotCheckedLostAllowed(t *testing.T) {
	tx := &fakeTx{scan: slotStateRow(false, true, "lost")}
	w, _ := testWriter(tx)
	seen, err := w.DropReplicationSlotChecked(unscopedCtx(), "debezium_orders", true)
	if err != nil || seen.WALStatus != "lost" {
		t.Fatalf("lost with allowLost: %+v %v", seen, err)
	}
}

func TestDropReplicationSlotCheckedRefusesBeforeIO(t *testing.T) {
	for _, name := range []string{"", "Debezium", "x'); DROP TABLE t; --", strings.Repeat("s", 64)} {
		w, b := testWriter(&fakeTx{scan: slotStateRow(false, true, "reserved")})
		if _, err := w.DropReplicationSlotChecked(unscopedCtx(), name, false); err == nil {
			t.Fatalf("slot %q accepted", name)
		}
		if b.begins != 0 {
			t.Fatalf("slot %q reached the database", name)
		}
	}
	w, b := testWriter(&fakeTx{scan: slotStateRow(false, true, "reserved")})
	if _, err := w.DropReplicationSlotChecked(t.Context(), "debezium_orders", false); !errors.Is(err, tools.ErrNoScope) {
		t.Fatalf("no scope: %v", err)
	}
	if b.begins != 0 {
		t.Fatal("no-scope call reached the database")
	}
	var nilW *Writer
	if _, err := nilW.DropReplicationSlotChecked(unscopedCtx(), "s", false); !errors.Is(err, ErrNoWriter) {
		t.Fatalf("nil writer: %v", err)
	}
}

func TestDropReplicationSlotCheckedCommitFailsAfterDrop(t *testing.T) {
	tx := &fakeTx{scan: slotStateRow(false, true, "reserved"), commitErr: errors.New("connection lost")}
	w, _ := testWriter(tx)
	_, err := w.DropReplicationSlotChecked(unscopedCtx(), "debezium_orders", false)
	if err == nil || !strings.Contains(err.Error(), "the slot is dropped") {
		t.Fatalf("a failed commit after the drop must say the slot is gone: %v", err)
	}
}
