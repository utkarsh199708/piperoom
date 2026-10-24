package plan

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"piperoom/internal/setup/protocol"
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
	OpSourceDropTable     = "source_drop_table"
	OpSourceTruncateTable = "source_truncate_table"
	OpSourceDropSlot      = "source_drop_slot"
	// OpPGDropReplicationSlot is the registered name of the incident drop-slot
	// action (pgactions.DropSlotName). A setup room can propose a registered
	// action under its own name, so the guard needs a row for that spelling
	// too: without it a production drop-slot step matched no row.
	OpPGDropReplicationSlot  = "pg_drop_replication_slot"
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
	// ReasonUnknownClass is the reason Forbidden returns for a class that is
	// not a class: the guard boundary fails closed, never open.
	ReasonUnknownClass = "unknown_class"
)

// StepFacts are the per-step facts a guard row's exception reads. The server
// derives them from the step's params and this room's receipts, never from
// model text. The zero value is the strictest reading — nothing this room
// created, no ALTER SYSTEM parameter on the allow-list, volumes not kept — so
// a caller that does not answer a question gets the refusal, not the
// exception.
type StepFacts struct {
	// CreatedByRoom: a receipt in this room's log names the step's endpoint
	// (plan.CreatedByRoom over the room log). The receipt is endpoint-granular,
	// not object-granular — it does not say this room created the slot,
	// publication or connector the op acts on — so no guard row reads it: a
	// destructive op on production stays refused whatever this field says,
	// until receipts carry object identity.
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
// Dropping a slot or a publication, and deleting a Connect or MSK connector,
// are refused on production unconditionally: the room's receipts name the
// endpoint a step reached, not the object it created, so "created by this
// room" cannot distinguish a slot this room made from one that was always
// there. The rows get their exception back when receipts carry object
// identity; every other cloud delete the wave adds must get its own row here.
var guardRows = []GuardRow{
	{Op: OpSourceDropTable, Class: ClassProduction, Reason: ReasonDropTable},
	{Op: OpSourceTruncateTable, Class: ClassProduction, Reason: ReasonTruncateTable},
	{Op: OpSourceDropSlot, Class: ClassProduction, Reason: ReasonDropSlot},
	{Op: OpPGDropReplicationSlot, Class: ClassProduction, Reason: ReasonDropSlot},
	{Op: OpSourceDropPublication, Class: ClassProduction, Reason: ReasonDropPublication},
	{Op: OpSourceAlterSystem, Class: ClassProduction, Reason: ReasonAlterSystem,
		Allowed: func(f StepFacts) bool { return AlterSystemAllowed(f.Param) }},
	{Op: OpConnectDeleteConnector, Class: ClassProduction, Reason: ReasonDeleteConnector},
	{Op: OpKafkaDeleteTopic, Class: ClassProduction, Reason: ReasonDeleteTopic},
	{Op: OpComposeDown, Class: ClassProduction, Reason: ReasonComposeDownVolumes,
		Allowed: func(f StepFacts) bool { return f.KeepsVolumes }},
	{Op: OpK8sDeleteNamespace, Class: ClassProduction, Reason: ReasonDeleteNamespace},
	{Op: OpMSKDeleteConnector, Class: ClassProduction, Reason: ReasonDeleteCloud},
	{Op: OpFileWrite, Class: ClassProduction, Reason: ReasonLocalFileOnProduction},
	{Op: OpFileDelete, Class: ClassProduction, Reason: ReasonLocalFileOnProduction},
}

// GuardRows returns the guard table, one row per forbidden (op, class), in
// declaration order. The slice is a copy: callers cannot edit the table.
func GuardRows() []GuardRow {
	return slices.Clone(guardRows)
}

// Forbidden evaluates the guard table for req and returns the row that
// forbids the op on the class. A class that is not a class is forbidden
// outright, with ReasonUnknownClass: the boundary fails closed, so a caller
// that skipped class validation gets a refusal, never a pass. An op that is
// not in the table matches no row, and the zero StepFacts reads as the
// strictest answer for every row that has an exception.
func Forbidden(req GuardRequest) (GuardRow, bool) {
	if !ValidClass(req.Class) {
		return GuardRow{Op: req.Op, Class: req.Class, Reason: ReasonUnknownClass}, true
	}
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

// AlterSystemStep is the params of a source_alter_system step exactly as its
// executor decodes them (the setup Source client binds its write to these
// fields). The guard reads the parameter name through DecodeAlterSystemStep
// too, so the name the allow-list judged is the name that runs.
type AlterSystemStep struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// DecodeAlterSystemStep decodes a source_alter_system step's params the way
// the executor does (encoding/json, other fields ignored), after refusing keys
// that differ only in letter case (protocol.CheckParamKeys), which the decoder
// would fold into Name with the last one winning.
func DecodeAlterSystemStep(raw json.RawMessage) (AlterSystemStep, error) {
	var p AlterSystemStep
	if err := protocol.CheckParamKeys(raw); err != nil {
		return p, err
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return p, fmt.Errorf("plan: source_alter_system params: %w", err)
	}
	return p, nil
}

// clientFloors are the ops whose setup client sends them above T2 whatever
// the step's routed tier (remoteclients raises a request to its method's
// floor). The room bounds such a step by that tier, not the routed one.
var clientFloors = map[string]string{
	OpSourceAlterSystem: TierT3,
}

// ClientFloor returns the tier the setup clients send op at least at, and ""
// for an op they send at the step's routed tier.
func ClientFloor(op string) string { return clientFloors[op] }
