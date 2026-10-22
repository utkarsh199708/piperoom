package plan

import (
	"errors"
	"slices"
)

// ErrGuard is returned (wrapped, with the row's reason) when the guard table
// forbids a step on its class. The room checks it at Create, again before
// Execute and on every rollback step; the runner checks the same row from the
// OpRequest's Class (design §3.5).
var ErrGuard = errors.New("plan: the guard table forbids this step")

// Canonical op names of the guard table (plan §3.6). Driver rows implement
// these ops under exactly these names; the names live here so the server's
// guard, the rollback path and the runner agree on one spelling.
const (
	OpSourceDropTable        = "source_drop_table"
	OpSourceTruncateTable    = "source_truncate_table"
	OpSourceDropSlot         = "source_drop_slot"
	OpSourceDropPublication  = "source_drop_publication"
	OpSourceAlterSystem      = "source_alter_system"
	OpConnectDeleteConnector = "connect_delete_connector"
	OpKafkaDeleteTopic       = "kafka_delete_topic"
	OpComposeDown            = "compose_down"
	OpK8sDeleteNamespace     = "k8s_delete_namespace"
	OpMSKDeleteConnector     = "msk_delete_connector"
	OpFileWrite              = "file_write"
	OpFileDelete             = "file_delete"
)

// Reasons of a guard row: fixed codes a refusal and the UI carry, never text
// from a model or an endpoint.
const (
	ReasonDropTable             = "drop_table"
	ReasonTruncateTable         = "truncate_table"
	ReasonDropSlot              = "drop_slot"
	ReasonDropPublication       = "drop_publication"
	ReasonAlterSystem           = "alter_system"
	ReasonDeleteConnector       = "delete_connector"
	ReasonDeleteTopic           = "delete_topic"
	ReasonComposeDownVolumes    = "compose_down_volumes"
	ReasonDeleteNamespace       = "delete_namespace"
	ReasonDeleteCloud           = "delete_cloud_resource"
	ReasonLocalFileOnProduction = "local_file_on_production"
)

// StepFacts are the per-step facts a guard row's exception reads. The server
// derives them from the step's params and this room's receipts, never from
// model text. The zero value is the strictest reading — nothing this room
// created, no ALTER SYSTEM parameter on the allow-list, volumes not kept — so
// a caller that does not answer a question gets the refusal, not the
// exception.
type StepFacts struct {
	// CreatedByRoom: the table, slot, publication or connector the op acts on
	// was created by this room (plan.CreatedByRoom over the room log).
	CreatedByRoom bool
	// Param is the ALTER SYSTEM parameter name.
	Param string
	// KeepsVolumes: a compose_down that does not remove volumes (no "-v").
	KeepsVolumes bool
}

// GuardRequest is one question to the guard table.
type GuardRequest struct {
	Op    string
	Class string
	Facts StepFacts
}

// GuardRow is one row of the guard table: op must not run on class, unless
// Allowed says this step is the exception the row names. There is one row per
// forbidden (op, class) pair (plan §3.6).
type GuardRow struct {
	Op     string
	Class  string
	Reason string
	// Allowed is the row's exception, nil when the row has none. It reads
	// only StepFacts.
	Allowed func(StepFacts) bool
}

// guardRows is the server's guard table (design §3.5, plan §3.6), in
// declaration order. Production only: every row is a destructive operation
// that is refused on a target the person or the patterns marked production.
// The plain local writes (file_write, file_delete) are here too, so the server
// and the runner's own table (internal/setup/ops) refuse the same pairs.
//
// msk_delete_connector is "unless this room created it", following its row
// (U13b2: T2, T3 on production) rather than reading plan §3.6's "deleting a
// cloud resource" as an unconditional refusal; every other cloud delete the
// wave adds must get its own row here.
var guardRows = []GuardRow{
	{Op: OpSourceDropTable, Class: ClassProduction, Reason: ReasonDropTable},
	{Op: OpSourceTruncateTable, Class: ClassProduction, Reason: ReasonTruncateTable},
	{Op: OpSourceDropSlot, Class: ClassProduction, Reason: ReasonDropSlot,
		Allowed: func(f StepFacts) bool { return f.CreatedByRoom }},
	{Op: OpSourceDropPublication, Class: ClassProduction, Reason: ReasonDropPublication,
		Allowed: func(f StepFacts) bool { return f.CreatedByRoom }},
	{Op: OpSourceAlterSystem, Class: ClassProduction, Reason: ReasonAlterSystem,
		Allowed: func(f StepFacts) bool { return AlterSystemAllowed(f.Param) }},
	{Op: OpConnectDeleteConnector, Class: ClassProduction, Reason: ReasonDeleteConnector,
		Allowed: func(f StepFacts) bool { return f.CreatedByRoom }},
	{Op: OpKafkaDeleteTopic, Class: ClassProduction, Reason: ReasonDeleteTopic},
	{Op: OpComposeDown, Class: ClassProduction, Reason: ReasonComposeDownVolumes,
		Allowed: func(f StepFacts) bool { return f.KeepsVolumes }},
	{Op: OpK8sDeleteNamespace, Class: ClassProduction, Reason: ReasonDeleteNamespace},
	{Op: OpMSKDeleteConnector, Class: ClassProduction, Reason: ReasonDeleteCloud,
		Allowed: func(f StepFacts) bool { return f.CreatedByRoom }},
	{Op: OpFileWrite, Class: ClassProduction, Reason: ReasonLocalFileOnProduction},
	{Op: OpFileDelete, Class: ClassProduction, Reason: ReasonLocalFileOnProduction},
}

// GuardRows returns the guard table, one row per forbidden (op, class), in
// declaration order. The slice is a copy: callers cannot edit the table.
func GuardRows() []GuardRow {
	return slices.Clone(guardRows)
}

// Forbidden evaluates the guard table for req and returns the row that
// forbids the op on the class. A class that is not a class and an op that is
// not in the table match no row: callers validate the class first (Resolve
// does), so an unanswered guard question never reads as forbidden, and the
// zero StepFacts reads as the strictest answer for every row that has an
// exception.
func Forbidden(req GuardRequest) (GuardRow, bool) {
	for _, row := range guardRows {
		if row.Op != req.Op || row.Class != req.Class {
			continue
		}
		if row.Allowed != nil && row.Allowed(req.Facts) {
			continue
		}
		return row, true
	}
	return GuardRow{}, false
}

// alterSystemParams is the ALTER SYSTEM allow-list of plan §3.6: the settings
// a setup may change, and even then only inside a recorded maintenance window
// with pending_restart shown.
var alterSystemParams = []string{
	"wal_level",
	"max_replication_slots",
	"max_wal_senders",
	"max_slot_wal_keep_size",
	"wal_sender_timeout",
}

// AlterSystemParams returns the allow-list, as a copy.
func AlterSystemParams() []string {
	return slices.Clone(alterSystemParams)
}

// AlterSystemAllowed reports whether param is on the ALTER SYSTEM allow-list.
// Exact and case-sensitive: Postgres folds an unquoted name to lower case, so
// a spelling that is not exactly one of the five is not the parameter a
// maintenance window approved.
func AlterSystemAllowed(param string) bool {
	return slices.Contains(alterSystemParams, param)
}
