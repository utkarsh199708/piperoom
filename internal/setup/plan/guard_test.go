package plan

import (
	"encoding/json"
	"errors"
	"testing"

	"piperoom/internal/setup/protocol"
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

// TestGuardExceptions are the exceptions each row names (plan §3.6): an ALTER
// SYSTEM parameter on the allow-list, a compose_down that keeps its volumes.
// Rows with no exception stay forbidden whatever the facts — in particular,
// CreatedByRoom waives nothing, because the room's receipts are
// endpoint-granular and cannot say this room created the slot, publication or
// connector the op acts on.
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
		{name: "drop slot is refused unconditionally", op: OpSourceDropSlot, class: ClassProduction, forbidden: true, reason: ReasonDropSlot},
		{name: "drop slot is refused even when this room reached the endpoint", op: OpSourceDropSlot, class: ClassProduction, facts: StepFacts{CreatedByRoom: true}, forbidden: true, reason: ReasonDropSlot},
		{name: "the registered drop-slot action is refused on production", op: OpPGDropReplicationSlot, class: ClassProduction, forbidden: true, reason: ReasonDropSlot},
		{name: "the registered drop-slot action is refused even when this room reached the endpoint", op: OpPGDropReplicationSlot, class: ClassProduction, facts: StepFacts{CreatedByRoom: true}, forbidden: true, reason: ReasonDropSlot},
		{name: "drop publication is refused unconditionally", op: OpSourceDropPublication, class: ClassProduction, forbidden: true, reason: ReasonDropPublication},
		{name: "drop publication is refused even when this room reached the endpoint", op: OpSourceDropPublication, class: ClassProduction, facts: StepFacts{CreatedByRoom: true}, forbidden: true, reason: ReasonDropPublication},
		{name: "alter system is refused without a parameter", op: OpSourceAlterSystem, class: ClassProduction, forbidden: true, reason: ReasonAlterSystem},
		{name: "alter system outside the allow-list is refused", op: OpSourceAlterSystem, class: ClassProduction, facts: StepFacts{Param: "shared_buffers"}, forbidden: true, reason: ReasonAlterSystem},
		{name: "alter system on wal_level is allowed", op: OpSourceAlterSystem, class: ClassProduction, facts: StepFacts{Param: "wal_level"}, forbidden: false},
		{name: "delete connector is refused unconditionally", op: OpConnectDeleteConnector, class: ClassProduction, forbidden: true, reason: ReasonDeleteConnector},
		{name: "delete connector is refused even when this room reached the endpoint", op: OpConnectDeleteConnector, class: ClassProduction, facts: StepFacts{CreatedByRoom: true}, forbidden: true, reason: ReasonDeleteConnector},
		{name: "delete topic is refused even when this room created it", op: OpKafkaDeleteTopic, class: ClassProduction, facts: StepFacts{CreatedByRoom: true}, forbidden: true, reason: ReasonDeleteTopic},
		{name: "compose down with volumes is refused", op: OpComposeDown, class: ClassProduction, forbidden: true, reason: ReasonComposeDownVolumes},
		{name: "compose down keeping volumes is allowed", op: OpComposeDown, class: ClassProduction, facts: StepFacts{KeepsVolumes: true}, forbidden: false},
		{name: "delete namespace is refused even when this room created it", op: OpK8sDeleteNamespace, class: ClassProduction, facts: StepFacts{CreatedByRoom: true}, forbidden: true, reason: ReasonDeleteNamespace},
		{name: "delete cloud connector is refused unconditionally", op: OpMSKDeleteConnector, class: ClassProduction, forbidden: true, reason: ReasonDeleteCloud},
		{name: "k8s_delete is refused on production", op: OpK8sDelete, class: ClassProduction, forbidden: true, reason: ReasonDeleteK8sObject},
		{name: "k8s_delete is refused even when this room reached the endpoint", op: OpK8sDelete, class: ClassProduction, facts: StepFacts{CreatedByRoom: true, Kind: "KafkaConnector"}, forbidden: true, reason: ReasonDeleteK8sObject},
		{name: "k8s_unapply of the KafkaConnect is refused", op: OpK8sUnapply, class: ClassProduction, facts: StepFacts{Kind: "KafkaConnect"}, forbidden: true, reason: ReasonUnapplyProduction},
		{name: "k8s_unapply of the Secret is refused", op: OpK8sUnapply, class: ClassProduction, facts: StepFacts{Kind: "Secret"}, forbidden: true, reason: ReasonUnapplyProduction},
		{name: "k8s_unapply of an unknown kind is refused", op: OpK8sUnapply, class: ClassProduction, facts: StepFacts{Kind: "Kafka"}, forbidden: true, reason: ReasonUnapplyProduction},
		{name: "k8s_unapply without a kind is refused", op: OpK8sUnapply, class: ClassProduction, forbidden: true, reason: ReasonUnapplyProduction},
		{name: "k8s_unapply of a KafkaUser is allowed", op: OpK8sUnapply, class: ClassProduction, facts: StepFacts{Kind: "KafkaUser"}},
		{name: "k8s_unapply of the logging ConfigMap is allowed", op: OpK8sUnapply, class: ClassProduction, facts: StepFacts{Kind: "ConfigMap"}},
		{name: "delete cloud connector is refused even when this room reached the endpoint", op: OpMSKDeleteConnector, class: ClassProduction, facts: StepFacts{CreatedByRoom: true}, forbidden: true, reason: ReasonDeleteCloud},
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
// shared targets are never refused by it, and an op outside the table is
// allowed on every valid class.
func TestGuardOnlyProduction(t *testing.T) {
	for _, row := range GuardRows() {
		for _, class := range []string{ClassLocal, ClassShared} {
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

// TestGuardFailsClosedOnUnknownClass: a class that is not a class is
// forbidden, whatever the op and whatever the facts. The guard is the last
// boundary before a destructive step, so an unvalidated class must read as a
// refusal, never as "matches no row".
func TestGuardFailsClosedOnUnknownClass(t *testing.T) {
	for _, class := range []string{"", "prod", "Production", "LOCAL"} {
		for _, op := range []string{OpSourceDropTable, "file_read", "no_such_op", ""} {
			row, forbidden := Forbidden(GuardRequest{Op: op, Class: class, Facts: StepFacts{CreatedByRoom: true, KeepsVolumes: true, Param: "wal_level"}})
			if !forbidden {
				t.Errorf("Forbidden(%q on %q) = false, want true", op, class)
				continue
			}
			if row.Reason != ReasonUnknownClass {
				t.Errorf("Forbidden(%q on %q) reason = %q, want %q", op, class, row.Reason, ReasonUnknownClass)
			}
		}
	}
}

// TestGuardTableComplete pins every destructive op this package names to a
// production row: removing a row is a deliberate table change, never an
// accident this test lets through. With the strictest facts, each op is
// refused on production.
func TestGuardTableComplete(t *testing.T) {
	ops := []string{
		OpSourceDropTable,
		OpSourceTruncateTable,
		OpSourceDropSlot,
		OpPGDropReplicationSlot,
		OpSourceDropPublication,
		OpSourceAlterSystem,
		OpConnectDeleteConnector,
		OpKafkaDeleteTopic,
		OpComposeDown,
		OpK8sDeleteNamespace,
		OpMSKDeleteConnector,
		OpK8sDelete,
		OpK8sUnapply,
		OpFileWrite,
		OpFileDelete,
	}
	rows := GuardRows()
	if len(rows) != len(ops) {
		t.Errorf("the guard table has %d rows, want one per destructive op (%d)", len(rows), len(ops))
	}
	for _, op := range ops {
		if _, forbidden := Forbidden(GuardRequest{Op: op, Class: ClassProduction}); !forbidden {
			t.Errorf("Forbidden(%s on production) = false: the op has lost its row", op)
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

// TestDecodeAlterSystemStep: the guard's read of the parameter name is the
// executor's decode, so a key spelled in another case is the name both see,
// and two keys that fold together (the probe) are refused, never judged on
// one and run as the other.
func TestDecodeAlterSystemStep(t *testing.T) {
	p, err := DecodeAlterSystemStep(json.RawMessage(`{"target":"db:5432","NAME":"archive_command","value":"x"}`))
	if err != nil || p.Name != "archive_command" {
		t.Fatalf("decode = %+v, %v; want the folded key's value, as the executor reads it", p, err)
	}
	for _, raw := range []string{
		`{"name":"wal_level","NAME":"archive_command","value":"x"}`,
		`{"name":"wal_level","name":"archive_command","value":"x"}`,
	} {
		if _, err := DecodeAlterSystemStep(json.RawMessage(raw)); !errors.Is(err, protocol.ErrParamKeys) {
			t.Fatalf("decode(%s) = %v, want ErrParamKeys", raw, err)
		}
	}
	if ClientFloor(OpSourceAlterSystem) != TierT3 || ClientFloor(OpSourceDropSlot) != "" {
		t.Fatal("ClientFloor: alter system is sent at T3, other ops at their routed tier")
	}
}
