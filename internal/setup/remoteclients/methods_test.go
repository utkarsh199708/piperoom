package remoteclients

import (
	"context"
	"errors"
	"testing"
	"time"

	"piperoom/internal/setup/protocol"
)

// TestOneOpPerMethod calls every client method once against the fake runner
// and asserts it sent exactly one request, with the op and tier its Ops() spec
// names (the table and the code cannot disagree), the proposal fields exactly
// on the writes, and the wire params golden-checked: a read's params byte for
// byte as below, a write's the frozen step's params byte for byte. A method
// that fans out into several ops, forwards a different name, drops or renames
// a param, or forgets the proposal fields fails here.
func TestOneOpPerMethod(t *testing.T) {
	read := roomCtx
	at := func(epoch string) time.Time {
		ts, err := time.Parse(time.RFC3339, epoch)
		if err != nil {
			t.Fatalf("parse %s: %v", epoch, err)
		}
		return ts
	}
	type call struct {
		method string
		op     string
		tier   string
		ctx    func(*testing.T) context.Context
		run    func(context.Context, *Session) error
	}
	// wire is the exact params JSON each method sends: readWire for a read,
	// the frozen step's params for a write.
	wire := map[string]string{}
	for m, w := range readWire {
		wire[m] = w
	}
	calls := []call{
		// Connect.
		{"Connect.ListConnectors", OpConnectListConnectors, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, err := s.Connect().ListConnectors(ctx)
			return err
		}},
		{"Connect.Status", OpConnectStatus, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, err := s.Connect().Status(ctx, "pg-orders-connector")
			return err
		}},
		{"Connect.Config", OpConnectConfig, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, err := s.Connect().Config(ctx, "pg-orders-connector")
			return err
		}},
		{"Connect.ValidateConfig", OpConnectValidateConfig, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, err := s.Connect().ValidateConfig(ctx, "io.debezium.connector.postgresql.PostgresConnector", map[string]string{"connector.class": "c"})
			return err
		}},
		{"Connect.PluginConfigDef", OpConnectPluginConfig, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, err := s.Connect().PluginConfigDef(ctx, "io.debezium.connector.postgresql.PostgresConnector")
			return err
		}},
		{"Connect.Plugins", OpConnectPlugins, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, err := s.Connect().Plugins(ctx)
			return err
		}},
		{"Connect.SourceOffsets", OpConnectSourceOffsets, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, err := s.Connect().SourceOffsets(ctx, "pg-orders-connector")
			return err
		}},
		{"Connect.ServerInfo", OpConnectServerInfo, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, err := s.Connect().ServerInfo(ctx)
			return err
		}},
		// Source.
		{"Source.Settings", OpSourceSettings, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, err := s.Source().Settings(ctx)
			return err
		}},
		{"Source.TableReplication", OpSourceTableReplication, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, err := s.Source().TableReplication(ctx, []string{"public.orders"})
			return err
		}},
		{"Source.RoleInfo", OpSourceRoleInfo, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, err := s.Source().RoleInfo(ctx, "debezium", []string{"public.orders"})
			return err
		}},
		{"Source.Blockers", OpSourceBlockers, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, err := s.Source().Blockers(ctx, time.Minute)
			return err
		}},
		{"Source.BlockersOnTables", OpSourceBlockers, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, err := s.Source().BlockersOnTables(ctx, time.Minute, []string{"public.orders"})
			return err
		}},
		{"Source.ReplicationSlots", OpSourceReplicationSlots, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, err := s.Source().ReplicationSlots(ctx)
			return err
		}},
		{"Source.RecentDDL", OpSourceRecentDDL, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, err := s.Source().RecentDDL(ctx, at("2026-01-01T00:00:00Z"), 10)
			return err
		}},
		{"Source.LongTransactions", OpSourceLongTransactions, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, err := s.Source().LongTransactions(ctx, time.Minute)
			return err
		}},
		{"Source.Row", OpSourceRow, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, found, err := s.Source().Row(ctx, "public.orders", "id", "1")
			if err == nil && !found {
				t.Fatal("the fake row was reported missing")
			}
			return err
		}},
		{"Source.PublicationTables", OpSourcePublicationTables, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, err := s.Source().PublicationTables(ctx, "dbz_publication", "debezium", []string{"public.orders"})
			return err
		}},
		// SQL Server (U4e).
		{"SQLServer.ServerInfo", OpSourceServerInfo, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, err := s.SQLServer().ServerInfo(ctx)
			return err
		}},
		{"SQLServer.AgentState", OpSourceAgentState, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, err := s.SQLServer().AgentState(ctx)
			return err
		}},
		{"SQLServer.DatabaseCDC", OpSourceDatabaseCDC, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, err := s.SQLServer().DatabaseCDC(ctx, "orders")
			return err
		}},
		{"SQLServer.TableCDC", OpSourceTableCDC, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, err := s.SQLServer().TableCDC(ctx, []string{"dbo.orders"})
			return err
		}},
		{"SQLServer.RoleInfo", OpSourceRoleInfo, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, err := s.SQLServer().RoleInfo(ctx, "debezium", []string{"dbo.orders"})
			return err
		}},
		// Kafka.
		{"Kafka.Topics", OpKafkaTopics, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, err := s.Kafka().Topics(ctx)
			return err
		}},
		{"Kafka.TopicOffsets", OpKafkaTopicOffsets, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, err := s.Kafka().TopicOffsets(ctx, "pg.public.orders")
			return err
		}},
		{"Kafka.ConsumerGroups", OpKafkaConsumerGroups, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, err := s.Kafka().ConsumerGroups(ctx)
			return err
		}},
		{"Kafka.GroupLag", OpKafkaGroupLag, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, err := s.Kafka().GroupLag(ctx, "orders-consumer")
			return err
		}},
		{"Kafka.FindByKey", OpKafkaFindByKey, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, err := s.Kafka().FindByKey(ctx, "pg.public.orders", "7", at("2026-01-01T00:00:00Z"), 100)
			return err
		}},
		{"Kafka.FindByKeyScan", OpKafkaFindByKeyScan, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, err := s.Kafka().FindByKeyScan(ctx, "pg.public.orders", "7", at("2026-01-01T00:00:00Z"), 100)
			return err
		}},

		// Registry.
		{"Registry.Subjects", OpRegistrySubjects, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, err := s.Registry().Subjects(ctx)
			return err
		}},
		{"Registry.Versions", OpRegistryVersions, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, err := s.Registry().Versions(ctx, "pg.public.orders-value")
			return err
		}},
		{"Registry.Schema", OpRegistrySchema, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			_, err := s.Registry().Schema(ctx, "pg.public.orders-value", -1)
			return err
		}},
		{"Registry.Compatibility", OpRegistryCompatibility, protocol.TierT0, read, func(ctx context.Context, s *Session) error {
			level, subjectLevel, err := s.Registry().Compatibility(ctx, "pg.public.orders-value")
			if err == nil && (level != "BACKWARD" || !subjectLevel) {
				t.Fatalf("compatibility = %q %v, want BACKWARD true", level, subjectLevel)
			}
			return err
		}},
	}
	for _, w := range writeCases() {
		calls = append(calls, call{method: w.method, op: w.action, tier: w.tier, ctx: w.ctx, run: w.run})
		wire[w.method] = w.params
	}

	// The calls and the table are the same set of methods, each once.
	specs := map[string]OpSpec{}
	for _, spec := range Ops() {
		specs[spec.Method] = spec
	}
	seen := map[string]bool{}
	for _, tc := range calls {
		if seen[tc.method] {
			t.Fatalf("%s is under test twice", tc.method)
		}
		seen[tc.method] = true
		spec, ok := specs[tc.method]
		if !ok {
			t.Fatalf("%s is not in Ops()", tc.method)
		}
		if spec.Op != tc.op || spec.Tier != tc.tier {
			t.Fatalf("%s: Ops() says %s at %s, the test expects %s at %s", tc.method, spec.Op, spec.Tier, tc.op, tc.tier)
		}
		if wire[tc.method] == "" {
			t.Fatalf("%s has no golden wire params", tc.method)
		}
	}
	if len(calls) != len(Ops()) {
		t.Fatalf("%d methods under test, %d ops in the table", len(calls), len(Ops()))
	}
	for _, tc := range calls {
		t.Run(tc.method, func(t *testing.T) {
			f := newFakeRunner()
			if err := tc.run(tc.ctx(t), New(f, "r_setup", allowT3Steps())); err != nil {
				t.Fatalf("%s: %v", tc.method, err)
			}
			got := f.allCalls()
			if len(got) != 1 {
				t.Fatalf("%s sent %d ops, want 1", tc.method, len(got))
			}
			req := got[0].req
			spec := specs[tc.method]
			if req.Op != spec.Op {
				t.Fatalf("%s sent op %q, Ops() says %q", tc.method, req.Op, spec.Op)
			}
			if req.Tier != spec.Tier {
				t.Fatalf("%s sent tier %q, Ops() says %q", tc.method, req.Tier, spec.Tier)
			}
			if string(req.Params) != wire[tc.method] {
				t.Fatalf("%s sent params\n  %s\nwant\n  %s", tc.method, req.Params, wire[tc.method])
			}
			if tc.tier == protocol.TierT0 {
				if req.ProposalID != "" || req.StepIndex != nil || req.ParamsHash != "" {
					t.Fatalf("%s is a read but carried proposal fields: %+v", tc.method, req)
				}
				return
			}
			if req.ProposalID == "" || req.StepIndex == nil || req.ParamsHash == "" {
				t.Fatalf("%s is a write but is missing proposal fields: %+v", tc.method, req)
			}
			if req.Class == "" {
				t.Fatalf("%s is a write but names no class", tc.method)
			}
		})
	}
	// The same call with no runner: every method, read or write, answers
	// ErrNoRunner before it sends anything.
	for _, tc := range calls {
		t.Run(tc.method+" without a runner", func(t *testing.T) {
			if err := tc.run(tc.ctx(t), New(nil, "")); !errors.Is(err, ErrNoRunner) {
				t.Fatalf("%s without a runner = %v, want ErrNoRunner", tc.method, err)
			}
		})
	}
}

// readWire is the exact params JSON of every read, as TestOneOpPerMethod calls
// it. A field dropped, renamed or added on the wire fails the comparison.
var readWire = map[string]string{
	"Connect.ListConnectors":   `{}`,
	"Connect.Status":           `{"connector":"pg-orders-connector"}`,
	"Connect.Config":           `{"connector":"pg-orders-connector"}`,
	"Connect.ValidateConfig":   `{"class":"io.debezium.connector.postgresql.PostgresConnector","config":{"connector.class":"c"}}`,
	"Connect.PluginConfigDef":  `{"class":"io.debezium.connector.postgresql.PostgresConnector"}`,
	"Connect.Plugins":          `{}`,
	"Connect.SourceOffsets":    `{"connector":"pg-orders-connector"}`,
	"Connect.ServerInfo":       `{}`,
	"Source.Settings":          `{}`,
	"Source.TableReplication":  `{"tables":["public.orders"]}`,
	"Source.RoleInfo":          `{"role":"debezium","tables":["public.orders"]}`,
	"Source.Blockers":          `{"older_than_ms":60000}`,
	"Source.BlockersOnTables":  `{"older_than_ms":60000,"scoped":true,"tables":["public.orders"]}`,
	"Source.ReplicationSlots":  `{}`,
	"Source.RecentDDL":         `{"since":"2026-01-01T00:00:00Z","limit":10}`,
	"Source.LongTransactions":  `{"older_than_ms":60000}`,
	"Source.Row":               `{"table":"public.orders","pk_column":"id","pk":"1"}`,
	"Source.PublicationTables": `{"publication":"dbz_publication","role":"debezium","tables":["public.orders"]}`,
	"SQLServer.ServerInfo":     `{}`,
	"SQLServer.AgentState":     `{}`,
	"SQLServer.DatabaseCDC":    `{"database":"orders"}`,
	"SQLServer.TableCDC":       `{"tables":["dbo.orders"]}`,
	"SQLServer.RoleInfo":       `{"role":"debezium","tables":["dbo.orders"]}`,
	"Kafka.Topics":             `{}`,
	"Kafka.TopicOffsets":       `{"topic":"pg.public.orders"}`,
	"Kafka.ConsumerGroups":     `{}`,
	"Kafka.GroupLag":           `{"group":"orders-consumer"}`,
	"Kafka.FindByKey":          `{"topic":"pg.public.orders","key":"7","since":"2026-01-01T00:00:00Z","max_scan":100}`,
	"Kafka.FindByKeyScan":      `{"topic":"pg.public.orders","key":"7","since":"2026-01-01T00:00:00Z","max_scan":100}`,
	"Registry.Subjects":        `{}`,
	"Registry.Versions":        `{"subject":"pg.public.orders-value"}`,
	"Registry.Schema":          `{"subject":"pg.public.orders-value","version":-1}`,
	"Registry.Compatibility":   `{"subject":"pg.public.orders-value"}`,
}

// TestWriteArgsBoundToStep: every write checks each argument it takes against
// the frozen step's params, and a step that differs in any one of them (or runs
// another action) is refused with ErrStepMismatch before anything is sent. A
// field dropped from a method's check (EndSession without started_at, a reset
// without its position) lets a mutation through and fails here.
func TestWriteArgsBoundToStep(t *testing.T) {
	for _, w := range writeCases() {
		t.Run(w.method, func(t *testing.T) {
			if len(w.mutations) == 0 {
				t.Fatal("no mutation checks this write's arguments")
			}
			for _, m := range w.mutations {
				f := newFakeRunner()
				err := w.run(writeCtx(t, w.tier, protocol.ClassLocal, w.action, m), New(f, "r_setup"))
				if !errors.Is(err, ErrStepMismatch) {
					t.Fatalf("step %s: err = %v, want ErrStepMismatch", m, err)
				}
				if n := len(f.allCalls()); n != 0 {
					t.Fatalf("step %s: %d ops sent for a mismatched step", m, n)
				}
			}
			// The step runs another action: never this write, whatever its
			// params.
			f := newFakeRunner()
			err := w.run(writeCtx(t, w.tier, protocol.ClassLocal, "connect_tune_throughput", w.params), New(f, "r_setup"))
			if !errors.Is(err, ErrStepMismatch) {
				t.Fatalf("another action's step: err = %v, want ErrStepMismatch", err)
			}
			// A step with no params at all.
			err = w.run(writeCtx(t, w.tier, protocol.ClassLocal, w.action, ""), New(f, "r_setup"))
			if !errors.Is(err, ErrStepMismatch) {
				t.Fatalf("a step without params: err = %v, want ErrStepMismatch", err)
			}
			if n := len(f.allCalls()); n != 0 {
				t.Fatalf("%d ops sent for a mismatched step", n)
			}
		})
	}
}
