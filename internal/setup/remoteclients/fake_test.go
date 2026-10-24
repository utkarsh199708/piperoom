package remoteclients

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"piperoom/internal/clients/connect"
	"piperoom/internal/events"
	"piperoom/internal/owners"
	"piperoom/internal/room"
	"piperoom/internal/setup/protocol"
	"piperoom/internal/tools"
)

// fakeRunner is the paired runner in tests: it records every OpRequest and
// answers each op with the valid JSON shape the method docs pin. An override
// replaces one op's Data; a failure makes one op answer OK=false with an
// error_code (the runner convention this package maps back to sentinels).
type fakeRunner struct {
	mu        sync.Mutex
	calls     []recordedCall
	overrides map[string]json.RawMessage
	failures  map[string]fakeFailure
	err       error // returned for every call
	noRunner  bool  // answers protocol.ErrNoRunner
}

type recordedCall struct {
	roomID string
	req    protocol.OpRequest
}

type fakeFailure struct {
	summary string
	code    string
}

func newFakeRunner() *fakeRunner {
	return &fakeRunner{overrides: map[string]json.RawMessage{}, failures: map[string]fakeFailure{}}
}

func (f *fakeRunner) Execute(ctx context.Context, roomID string, req protocol.OpRequest) (protocol.OpResult, error) {
	f.mu.Lock()
	f.calls = append(f.calls, recordedCall{roomID: roomID, req: req})
	f.mu.Unlock()

	if f.noRunner {
		return protocol.OpResult{}, protocol.ErrNoRunner
	}
	if f.err != nil {
		return protocol.OpResult{}, f.err
	}
	if err := ctx.Err(); err != nil {
		return protocol.OpResult{}, err
	}
	if fail, ok := f.failures[req.Op]; ok {
		data := json.RawMessage(`{}`)
		if fail.code != "" {
			data, _ = json.Marshal(map[string]string{"error_code": fail.code})
		}
		return protocol.OpResult{ID: req.ID, OK: false, Summary: fail.summary, Data: data}, nil
	}
	data, ok := f.overrides[req.Op]
	if !ok {
		data, ok = defaultAnswers[req.Op]
	}
	if !ok {
		return protocol.OpResult{}, &unknownFakeOp{op: req.Op}
	}
	return protocol.OpResult{ID: req.ID, OK: true, Summary: "ok", Data: append(json.RawMessage(nil), data...)}, nil
}

type unknownFakeOp struct{ op string }

func (e *unknownFakeOp) Error() string { return "fake runner has no answer for op " + e.op }

// callsFor returns the recorded calls of one op, under the lock.
func (f *fakeRunner) callsFor(op string) []recordedCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []recordedCall
	for _, c := range f.calls {
		if c.req.Op == op {
			out = append(out, c)
		}
	}
	return out
}

// allCalls returns every recorded call.
func (f *fakeRunner) allCalls() []recordedCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recordedCall(nil), f.calls...)
}

// defaultAnswers is one valid result per op, in the shapes the method doc
// comments state. TestOpsTable asserts every op in Ops() has one.
var defaultAnswers = map[string]json.RawMessage{
	OpConnectListConnectors: json.RawMessage(`{"connectors":["pg-orders-connector"]}`),
	OpConnectStatus: json.RawMessage(`{"status":{"name":"pg-orders-connector","type":"source","state":"RUNNING",` +
		`"worker_id":"connect:8083","tasks":[{"id":0,"state":"RUNNING","worker_id":"connect:8083"}]}}`),
	OpConnectConfig: json.RawMessage(`{"config":{"connector.class":"io.debezium.connector.postgresql.PostgresConnector",` +
		`"database.hostname":"postgres","database.user":"debezium","topic.prefix":"pg"}}`),
	OpConnectValidateConfig: json.RawMessage(`{"validate":{"error_count":0,"errors":{}}}`),
	OpConnectPluginConfig: json.RawMessage(`{"definitions":[` +
		`{"name":"connector.class","type":"STRING","required":true},` +
		`{"name":"database.hostname","type":"STRING","required":true},` +
		`{"name":"database.password","type":"PASSWORD","required":false},` +
		`{"name":"database.user","type":"STRING","required":true},` +
		`{"name":"topic.prefix","type":"STRING","required":true},` +
		`{"name":"plugin.name","type":"STRING","required":false}]}`),
	OpConnectPlugins: json.RawMessage(`{"plugins":[{"class":"io.debezium.connector.postgresql.PostgresConnector",` +
		`"type":"source","version":"3.1.0.Final"}]}`),
	OpConnectSourceOffsets: json.RawMessage(`{"offsets":[{"partition":{"server":"postgres"},"offset":{"lsn":42}}]}`),
	OpConnectServerInfo:    json.RawMessage(`{"server_info":{"version":"3.9.0","commit":"abc123","kafka_cluster_id":"ck1"}}`),

	OpConnectRestartTask:        json.RawMessage(`{}`),
	OpConnectRestartConnector:   json.RawMessage(`{}`),
	OpConnectCreateConnector:    json.RawMessage(`{}`),
	OpConnectDeleteConnector:    json.RawMessage(`{}`),
	OpConnectStopConnector:      json.RawMessage(`{}`),
	OpConnectResetSourceOffsets: json.RawMessage(`{}`),

	OpSourceSettings: json.RawMessage(`{"settings":[{"name":"wal_level","value":"logical"},` +
		`{"name":"max_replication_slots","value":"10"}]}`),
	OpSourceTableReplication: json.RawMessage(`{"tables":[{"table":"public.orders","has_primary_key":false,` +
		`"replica_identity":"default"}]}`),
	OpSourceRoleInfo: json.RawMessage(`{"role":{"role":"debezium","exists":true,"can_login":true,` +
		`"replication":true,"selectable":{"public.orders":false}}}`),
	OpSourceBlockers:         json.RawMessage(`{"sessions":[]}`),
	OpSourceReplicationSlots: json.RawMessage(`{"slots":[]}`),
	OpSourceRecentDDL:        json.RawMessage(`{"changes":[]}`),
	OpSourceLongTransactions: json.RawMessage(`{"transactions":[]}`),
	OpSourceRow:              json.RawMessage(`{"row":{"id":1,"status":"new"},"found":true}`),
	OpSourcePublicationTables: json.RawMessage(`{"tables":[{"schema":"public","table":"orders",` +
		`"in_publication":true,"selectable":true}]}`),

	OpSourceSetReplicaIdentity:   json.RawMessage(`{}`),
	OpSourceGrantSelect:          json.RawMessage(`{}`),
	OpSourceCreateHeartbeatTable: json.RawMessage(`{}`),
	OpSourceEndSession:           json.RawMessage(`{}`),
	OpSourceCreatePublication:    json.RawMessage(`{}`),
	OpSourceCreateCanaryTable:    json.RawMessage(`{}`),
	OpSourceInsertCanary:         json.RawMessage(`{}`),
	OpSourceCreateReadRole:       json.RawMessage(`{"secret_name":"orders_reader"}`),
	OpSourceAlterSystem:          json.RawMessage(`{"pending_restart":true}`),

	OpKafkaTopics:         json.RawMessage(`{"topics":["pg.public.orders"]}`),
	OpKafkaTopicOffsets:   json.RawMessage(`{"offsets":[{"partition":0,"start":0,"end":10,"last_timestamp":"2026-01-01T00:00:00Z"}]}`),
	OpKafkaConsumerGroups: json.RawMessage(`{"groups":["orders-consumer"]}`),
	OpKafkaGroupLag:       json.RawMessage(`{"lag":{"group":"orders-consumer","state":"Stable","members":1,"total_lag":0}}`),
	OpKafkaFindByKey: json.RawMessage(`{"records":[{"topic":"pg.public.orders","partition":0,"offset":3,` +
		`"timestamp":"2026-01-01T00:00:00Z","key":"7"}]}`),
	OpKafkaFindByKeyScan: json.RawMessage(`{"scan":{"records":[{"topic":"pg.public.orders","partition":0,"offset":3,` +
		`"timestamp":"2026-01-01T00:00:00Z","key":"7"}],"truncated_partitions":[1]}}`),

	OpRegistrySubjects:      json.RawMessage(`{"subjects":["pg.public.orders-value"]}`),
	OpRegistryVersions:      json.RawMessage(`{"versions":[1,2]}`),
	OpRegistrySchema:        json.RawMessage(`{"schema":{"subject":"pg.public.orders-value","version":1,"id":7,"schema":"{\"type\":\"record\"}"}}`),
	OpRegistryCompatibility: json.RawMessage(`{"level":"BACKWARD","subject_level":true}`),
}

// testPipeline is the provisional pipeline of a setup room
// (owners.Provisional): one source connector, one table, the source and
// Connect hosts.
func testPipeline(t *testing.T) owners.Pipeline {
	t.Helper()
	p, err := owners.Provisional(events.SetupRequest{
		PipelineID:  "orders",
		Title:       "Orders CDC",
		TopicPrefix: "pg",
		Tables:      []string{"public.orders"},
		Connectors:  []string{"pg-orders-connector"},
		Targets: []events.SetupTarget{
			{Kind: "postgres", Address: "postgres:5432"},
			{Kind: "connect", Address: "connect:8083"},
		},
	})
	if err != nil {
		t.Fatalf("owners.Provisional: %v", err)
	}
	return p
}

// roomCtx is a tool call in the room: the pipeline scope and the room id, no
// proposal step (a read).
func roomCtx(t *testing.T) context.Context {
	t.Helper()
	ctx := tools.WithPipeline(context.Background(), testPipeline(t))
	return room.WithRoomID(ctx, "r_setup")
}

// stepCtx is an action call of proposal p_1, step 0: the pipeline scope, the
// room id, the proposal step the room routed, and the frozen proposal the
// runner recomputes params_hash from. It carries no frozen step action or
// params, so a write under it is refused (ErrStepMismatch); writeCtx is the
// complete step.
func stepCtx(t *testing.T, tier, class string) context.Context {
	t.Helper()
	ctx := roomCtx(t)
	ctx = room.WithSetupStep(ctx, room.SetupStep{
		ProposalID: "p_1",
		StepIndex:  0,
		ParamsHash: "b1946ac92492d2347c6235b4d2611184",
		Tier:       tier,
		Class:      class,
	})
	return WithProposal(ctx, json.RawMessage(`{"room_id":"r_setup","proposal_id":"p_1","title":"fix","steps":[]}`))
}

// stepCtxLocal is stepCtx for the common case: a T2 step on a local target.
func stepCtxLocal(t *testing.T) context.Context {
	t.Helper()
	return stepCtx(t, protocol.TierT2, protocol.ClassLocal)
}

// writeCtx is stepCtx with the frozen step itself: the action the step runs
// and its params as approved, which a write sends unchanged.
func writeCtx(t *testing.T, tier, class, action, params string) context.Context {
	t.Helper()
	ctx := roomCtx(t)
	ctx = room.WithSetupStep(ctx, room.SetupStep{
		ProposalID: "p_1",
		StepIndex:  0,
		ParamsHash: "b1946ac92492d2347c6235b4d2611184",
		Tier:       tier,
		Class:      class,
		Action:     action,
		Params:     json.RawMessage(params),
	})
	return WithProposal(ctx, json.RawMessage(`{"room_id":"r_setup","proposal_id":"p_1","title":"fix","steps":[]}`))
}

// writeCase is one write method and the proposal step it is the write of:
// the step's action and its whole params as the action's own schema has them
// (fields the method does not take included), a call whose arguments are the
// step's, and step params that each differ from the call in one field the
// method checks.
type writeCase struct {
	method    string
	action    string
	tier      string
	params    string
	run       func(context.Context, *Session) error
	mutations []string
}

// ctx is the case's own step, T2 (or the case's tier) on a local target.
func (w writeCase) ctx(t *testing.T) context.Context {
	t.Helper()
	return writeCtx(t, w.tier, protocol.ClassLocal, w.action, w.params)
}

// writeCaseFor returns the case of one method.
func writeCaseFor(t *testing.T, method string) writeCase {
	t.Helper()
	for _, w := range writeCases() {
		if w.method == method {
			return w
		}
	}
	t.Fatalf("no write case for %s", method)
	return writeCase{}
}

var canaryNonce = "00112233445566778899aabbccddeeff"

// writeCases is every write method of the package, with its step.
func writeCases() []writeCase {
	started := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	return []writeCase{
		{
			method: "Connect.RestartTask", action: OpConnectRestartTask, tier: protocol.TierT2,
			params: `{"connector":"pg-orders-connector","task":0}`,
			run: func(ctx context.Context, s *Session) error {
				return s.Connect().RestartTask(ctx, "pg-orders-connector", 0)
			},
			mutations: []string{
				`{"connector":"pg-other-connector","task":0}`,
				`{"connector":"pg-orders-connector","task":1}`,
			},
		},
		{
			method: "Connect.RestartConnector", action: OpConnectRestartConnector, tier: protocol.TierT2,
			params: `{"connector":"pg-orders-connector"}`,
			run: func(ctx context.Context, s *Session) error {
				return s.Connect().RestartConnector(ctx, "pg-orders-connector")
			},
			mutations: []string{`{"connector":"pg-other-connector"}`},
		},
		{
			method: "Connect.CreateConnector", action: OpConnectCreateConnector, tier: protocol.TierT2,
			params: `{"connector":"pg-orders-connector","config":{"connector.class":"c","topic.prefix":"pg"},"replace":true}`,
			run: func(ctx context.Context, s *Session) error {
				return s.Connect().CreateConnector(ctx, "pg-orders-connector", map[string]string{"connector.class": "c", "topic.prefix": "pg"})
			},
			mutations: []string{
				`{"connector":"pg-other-connector","config":{"connector.class":"c","topic.prefix":"pg"},"replace":true}`,
				`{"connector":"pg-orders-connector","config":{"connector.class":"c","topic.prefix":"px"},"replace":true}`,
				`{"connector":"pg-orders-connector","config":{"connector.class":"c","topic.prefix":"pg","extra":"x"},"replace":true}`,
			},
		},
		{
			method: "Connect.DeleteConnector", action: OpConnectDeleteConnector, tier: protocol.TierT2,
			params: `{"connector":"pg-orders-connector"}`,
			run: func(ctx context.Context, s *Session) error {
				return s.Connect().DeleteConnector(ctx, "pg-orders-connector")
			},
			mutations: []string{`{"connector":"pg-other-connector"}`},
		},
		{
			method: "Connect.StopConnector", action: OpConnectStopConnector, tier: protocol.TierT2,
			params: `{"connector":"pg-orders-connector"}`,
			run: func(ctx context.Context, s *Session) error {
				return s.Connect().StopConnector(ctx, "pg-orders-connector")
			},
			mutations: []string{`{"connector":"pg-other-connector"}`},
		},
		{
			method: "Connect.ResetSourceOffsets", action: OpConnectResetSourceOffsets, tier: protocol.TierT2,
			params: `{"connector":"pg-orders-connector","stop_first":true}`,
			run: func(ctx context.Context, s *Session) error {
				return s.Connect().ResetSourceOffsets(ctx, "pg-orders-connector")
			},
			mutations: []string{
				`{"connector":"pg-other-connector","stop_first":true}`,
				// A step that moves to a position is not a clear-everything reset.
				`{"connector":"pg-orders-connector","stop_first":true,"position":{"partition":{"server":"postgres"},"offset":{"lsn":42}}}`,
			},
		},
		{
			// The reset step with a position: the alter write sends the
			// step's action, and an LSN at the hash's exact-integer limit (2^53-1)
			// compares digit for digit.
			method: "Connect.AlterSourceOffsets", action: OpConnectResetSourceOffsets, tier: protocol.TierT2,
			params: `{"connector":"pg-orders-connector","stop_first":true,"position":{"partition":{"server":"postgres"},"offset":{"lsn":9007199254740991}}}`,
			run: func(ctx context.Context, s *Session) error {
				return s.Connect().AlterSourceOffsets(ctx, "pg-orders-connector", []connect.SourceOffset{{
					Partition: map[string]any{"server": "postgres"},
					Offset:    map[string]any{"lsn": json.Number("9007199254740991")},
				}})
			},
			mutations: []string{
				`{"connector":"pg-other-connector","stop_first":true,"position":{"partition":{"server":"postgres"},"offset":{"lsn":9007199254740991}}}`,
				`{"connector":"pg-orders-connector","stop_first":true,"position":{"partition":{"server":"postgres"},"offset":{"lsn":9007199254740990}}}`,
				`{"connector":"pg-orders-connector","stop_first":true,"position":{"partition":{"server":"other"},"offset":{"lsn":9007199254740991}}}`,
				`{"connector":"pg-orders-connector","stop_first":true}`,
			},
		},
		{
			method: "Source.SetReplicaIdentity", action: OpSourceSetReplicaIdentity, tier: protocol.TierT2,
			params: `{"table":"public.orders","mode":"full"}`,
			run: func(ctx context.Context, s *Session) error {
				return s.Source().SetReplicaIdentity(ctx, "public.orders", "full", "")
			},
			mutations: []string{
				`{"table":"public.other","mode":"full"}`,
				`{"table":"public.orders","mode":"default"}`,
				`{"table":"public.orders","mode":"full","index":"orders_pk"}`,
			},
		},
		{
			method: "Source.GrantSelect", action: OpSourceGrantSelect, tier: protocol.TierT2,
			params: `{"connector":"pg-orders-connector","role":"debezium","tables":["public.orders"]}`,
			run: func(ctx context.Context, s *Session) error {
				return s.Source().GrantSelect(ctx, "debezium", []string{"public.orders"})
			},
			mutations: []string{
				`{"connector":"pg-orders-connector","role":"postgres","tables":["public.orders"]}`,
				`{"connector":"pg-orders-connector","role":"debezium","tables":["public.orders","public.payroll"]}`,
			},
		},
		{
			method: "Source.CreateHeartbeatTable", action: OpSourceCreateHeartbeatTable, tier: protocol.TierT2,
			params: `{"connector":"pg-orders-connector","table":"public.piperoom_heartbeat"}`,
			run: func(ctx context.Context, s *Session) error {
				return s.Source().CreateHeartbeatTable(ctx, "public.piperoom_heartbeat")
			},
			mutations: []string{`{"connector":"pg-orders-connector","table":"public.other"}`},
		},
		{
			method: "Source.EndSession", action: OpSourceEndSession, tier: protocol.TierT2,
			params: `{"connector":"pg-orders-connector","pid":42,"started_at":"2026-01-01T00:00:00Z","older_than_minutes":5}`,
			run: func(ctx context.Context, s *Session) error {
				return s.Source().EndSession(ctx, 42, started)
			},
			mutations: []string{
				`{"connector":"pg-orders-connector","pid":43,"started_at":"2026-01-01T00:00:00Z","older_than_minutes":5}`,
				// Another session reusing the pid is another session.
				`{"connector":"pg-orders-connector","pid":42,"started_at":"2026-01-01T00:00:01Z","older_than_minutes":5}`,
			},
		},
		{
			method: "Source.CreatePublication", action: OpSourceCreatePublication, tier: protocol.TierT2,
			params: `{"target":"postgres:5432","publication":"pg_orders_pub","tables":["public.orders"]}`,
			run: func(ctx context.Context, s *Session) error {
				return s.Source().CreatePublication(ctx, "pg_orders_pub", []string{"public.orders"})
			},
			mutations: []string{
				`{"target":"postgres:5432","publication":"pg_other_pub","tables":["public.orders"]}`,
				`{"target":"postgres:5432","publication":"pg_orders_pub","tables":["public.payroll"]}`,
			},
		},
		{
			method: "Source.CreateCanaryTable", action: OpSourceCreateCanaryTable, tier: protocol.TierT2,
			params: `{"target":"postgres:5432","table":"public.piperoom_canary"}`,
			run: func(ctx context.Context, s *Session) error {
				return s.Source().CreateCanaryTable(ctx, "public.piperoom_canary")
			},
			mutations: []string{`{"target":"postgres:5432","table":"public.other"}`},
		},
		{
			method: "Source.InsertCanary", action: OpSourceInsertCanary, tier: protocol.TierT2,
			params: `{"target":"postgres:5432","table":"public.piperoom_canary","nonce":"` + canaryNonce + `"}`,
			run: func(ctx context.Context, s *Session) error {
				return s.Source().InsertCanary(ctx, "public.piperoom_canary", canaryNonce)
			},
			mutations: []string{
				`{"target":"postgres:5432","table":"public.other","nonce":"` + canaryNonce + `"}`,
				`{"target":"postgres:5432","table":"public.piperoom_canary","nonce":"ffeeddccbbaa99887766554433221100"}`,
			},
		},
		{
			method: "Source.CreateReadRole", action: OpSourceCreateReadRole, tier: protocol.TierT2,
			params: `{"target":"postgres:5432","role":"piperoom_reader","tables":["public.orders"]}`,
			run: func(ctx context.Context, s *Session) error {
				name, err := s.Source().CreateReadRole(ctx, "piperoom_reader", []string{"public.orders"})
				if err == nil && name != "orders_reader" {
					return errors.New("secret name " + name + ", want orders_reader")
				}
				return err
			},
			mutations: []string{
				`{"target":"postgres:5432","role":"postgres","tables":["public.orders"]}`,
				`{"target":"postgres:5432","role":"piperoom_reader","tables":["public.payroll"]}`,
			},
		},
		{
			method: "Source.AlterSystem", action: OpSourceAlterSystem, tier: protocol.TierT3,
			params: `{"target":"postgres:5432","name":"wal_level","value":"logical"}`,
			run: func(ctx context.Context, s *Session) error {
				pending, err := s.Source().AlterSystem(ctx, "wal_level", "logical")
				if err == nil && !pending {
					return errors.New("the pending_restart answer was not read")
				}
				return err
			},
			mutations: []string{
				`{"target":"postgres:5432","name":"max_wal_senders","value":"logical"}`,
				`{"target":"postgres:5432","name":"wal_level","value":"minimal"}`,
				// Keys that fold together, the call's value last so the
				// decoder alone would match it: the room's guard read the
				// other one (protocol.CheckParamKeys).
				`{"target":"postgres:5432","NAME":"archive_command","name":"wal_level","value":"logical"}`,
				`{"target":"postgres:5432","name":"archive_command","name":"wal_level","value":"logical"}`,
			},
		},
	}
}

// withStep adds a proposal step to ctx, with the given fields (the tests use
// the empty ones to prove the refusal).
func withStep(t *testing.T, ctx context.Context, tier, class string, idx int, hash, proposalID string) context.Context {
	t.Helper()
	return room.WithSetupStep(ctx, room.SetupStep{
		ProposalID: proposalID,
		StepIndex:  idx,
		ParamsHash: hash,
		Tier:       tier,
		Class:      class,
	})
}
