package plan

import (
	"testing"
)

// TestGuardRowRefuses is the test per row of the guard table: every row from
// GuardRows() forbids its (op, class) with the strictest facts, and names a
// reason.
func TestGuardRowRefuses(t *testing.T) {
	rows := GuardRows()
	if len(rows) == 0 {
		t.Fatal("the guard table is empty")
	}
	seen := make(map[string]bool, len(rows))
	for _, row := range rows {
		name := row.Op + "|" + row.Class
		if seen[name] {
			t.Errorf("duplicate row %s", name)
		}
		seen[name] = true
		if row.Op == "" {
			t.Errorf("row %+v has no op", row)
		}
		if !ValidClass(row.Class) {
			t.Errorf("row %s has class %q", row.Op, row.Class)
		}
		if row.Reason == "" {
			t.Errorf("row %s has no reason", row.Op)
		}
		got, forbidden := Forbidden(GuardRequest{Op: row.Op, Class: row.Class})
		if !forbidden {
			t.Errorf("Forbidden(%s on %s) = false, want true", row.Op, row.Class)
			continue
		}
		if got.Op != row.Op || got.Class != row.Class || got.Reason != row.Reason {
			t.Errorf("Forbidden(%s on %s) = %+v, want %+v", row.Op, row.Class, got, row)
		}
	}
}

// TestGuardRows are the exceptions each row names (plan §3.6): an object this
// room created, an ALTER SYSTEM parameter on the allow-list, a compose_down
// that keeps its volumes. Rows with no exception stay forbidden whatever the
// facts.
func TestGuardExceptions(t *testing.T) {
	tests := []struct {
		name      string
		op        string
		class     string
		facts     StepFacts
		forbidden bool
		reason    string
	}{
		{name: "drop table is refused even when this room created it", op: OpSourceDropTable, class: ClassProduction, facts: StepFacts{CreatedByRoom: true}, forbidden: true, reason: ReasonDropTable},
		{name: "truncate table is refused even when this room created it", op: OpSourceTruncateTable, class: ClassProduction, facts: StepFacts{CreatedByRoom: true}, forbidden: true, reason: ReasonTruncateTable},
		{name: "drop slot is refused when this room did not create it", op: OpSourceDropSlot, class: ClassProduction, forbidden: true, reason: ReasonDropSlot},
		{name: "drop slot is allowed when this room created it", op: OpSourceDropSlot, class: ClassProduction, facts: StepFacts{CreatedByRoom: true}, forbidden: false},
		{name: "drop publication is refused when this room did not create it", op: OpSourceDropPublication, class: ClassProduction, forbidden: true, reason: ReasonDropPublication},
		{name: "drop publication is allowed when this room created it", op: OpSourceDropPublication, class: ClassProduction, facts: StepFacts{CreatedByRoom: true}, forbidden: false},
		{name: "alter system is refused without a parameter", op: OpSourceAlterSystem, class: ClassProduction, forbidden: true, reason: ReasonAlterSystem},
		{name: "alter system outside the allow-list is refused", op: OpSourceAlterSystem, class: ClassProduction, facts: StepFacts{Param: "shared_buffers"}, forbidden: true, reason: ReasonAlterSystem},
		{name: "alter system on wal_level is allowed", op: OpSourceAlterSystem, class: ClassProduction, facts: StepFacts{Param: "wal_level"}, forbidden: false},
		{name: "delete connector is refused when this room did not create it", op: OpConnectDeleteConnector, class: ClassProduction, forbidden: true, reason: ReasonDeleteConnector},
		{name: "delete connector is allowed when this room created it", op: OpConnectDeleteConnector, class: ClassProduction, facts: StepFacts{CreatedByRoom: true}, forbidden: false},
		{name: "delete topic is refused even when this room created it", op: OpKafkaDeleteTopic, class: ClassProduction, facts: StepFacts{CreatedByRoom: true}, forbidden: true, reason: ReasonDeleteTopic},
		{name: "compose down with volumes is refused", op: OpComposeDown, class: ClassProduction, forbidden: true, reason: ReasonComposeDownVolumes},
		{name: "compose down keeping volumes is allowed", op: OpComposeDown, class: ClassProduction, facts: StepFacts{KeepsVolumes: true}, forbidden: false},
		{name: "delete namespace is refused even when this room created it", op: OpK8sDeleteNamespace, class: ClassProduction, facts: StepFacts{CreatedByRoom: true}, forbidden: true, reason: ReasonDeleteNamespace},
		{name: "delete cloud connector is refused when this room did not create it", op: OpMSKDeleteConnector, class: ClassProduction, forbidden: true, reason: ReasonDeleteCloud},
		{name: "delete cloud connector is allowed when this room created it", op: OpMSKDeleteConnector, class: ClassProduction, facts: StepFacts{CreatedByRoom: true}, forbidden: false},
		{name: "file write on production is refused", op: OpFileWrite, class: ClassProduction, facts: StepFacts{CreatedByRoom: true}, forbidden: true, reason: ReasonLocalFileOnProduction},
		{name: "file delete on production is refused", op: OpFileDelete, class: ClassProduction, facts: StepFacts{KeepsVolumes: true}, forbidden: true, reason: ReasonLocalFileOnProduction},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row, forbidden := Forbidden(GuardRequest{Op: tt.op, Class: tt.class, Facts: tt.facts})
			if forbidden != tt.forbidden {
				t.Fatalf("Forbidden(%s on %s, %+v) = %v, want %v", tt.op, tt.class, tt.facts, forbidden, tt.forbidden)
			}
			if forbidden && row.Reason != tt.reason {
				t.Errorf("reason = %q, want %q", row.Reason, tt.reason)
			}
		})
	}
}

// TestGuardOnlyProduction: the table's rows are production-only; local and
// shared targets, and a class that is not a class, are never refused by it.
func TestGuardOnlyProduction(t *testing.T) {
	for _, row := range GuardRows() {
		for _, class := range []string{ClassLocal, ClassShared, "", "prod"} {
			if _, forbidden := Forbidden(GuardRequest{Op: row.Op, Class: class}); forbidden {
				t.Errorf("Forbidden(%s on %q) = true", row.Op, class)
			}
		}
	}
	for _, class := range []string{ClassLocal, ClassShared, ClassProduction} {
		for _, op := range []string{"", "no_such_op", "file_read", "env_detect"} {
			if _, forbidden := Forbidden(GuardRequest{Op: op, Class: class}); forbidden {
				t.Errorf("Forbidden(%q on %s) = true", op, class)
			}
		}
	}
}

func TestGuardRowsIsACopy(t *testing.T) {
	rows := GuardRows()
	rows[0].Op = "changed"
	rows[0].Reason = "changed"
	if again := GuardRows(); again[0].Op == "changed" || again[0].Reason == "changed" {
		t.Error("GuardRows shares its backing array with the table")
	}
}

func TestAlterSystemAllowed(t *testing.T) {
	params := AlterSystemParams()
	want := []string{"wal_level", "max_replication_slots", "max_wal_senders", "max_slot_wal_keep_size", "wal_sender_timeout"}
	if len(params) != len(want) {
		t.Fatalf("AlterSystemParams() = %v, want %v", params, want)
	}
	for i := range want {
		if params[i] != want[i] {
			t.Fatalf("AlterSystemParams()[%d] = %q, want %q", i, params[i], want[i])
		}
		if !AlterSystemAllowed(want[i]) {
			t.Errorf("AlterSystemAllowed(%q) = false", want[i])
		}
	}
	for _, param := range []string{"", "shared_buffers", "WAL_LEVEL", " wal_level", "wal_level ", "max_connections"} {
		if AlterSystemAllowed(param) {
			t.Errorf("AlterSystemAllowed(%q) = true", param)
		}
	}
	params[0] = "shared_buffers"
	if AlterSystemAllowed("shared_buffers") {
		t.Error("AlterSystemParams shares its backing array with the allow-list")
	}
}
