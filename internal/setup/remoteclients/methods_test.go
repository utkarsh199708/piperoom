package remoteclients

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"piperoom/internal/clients/connect"
	"piperoom/internal/setup/protocol"
)

// TestOneOpPerMethod calls every client method once against the fake runner
// and asserts it sent exactly one request, named as the spec says, at the
// method's tier, with the proposal fields exactly on the writes. A method that
// fans out into several ops, forwards a different name, or forgets the proposal
// fields fails here.
func TestOneOpPerMethod(t *testing.T) {
	read := roomCtx
	write := stepCtxLocal
	at := func(epoch string) time.Time {
		ts, err := time.Parse(time.RFC3339, epoch)
		if err != nil {
			t.Fatalf("parse %s: %v", epoch, err)
		}
		return ts
	}
	// every(connect.SourceOffset) is a valid alter entry: one partition.
	offsets := []connect.SourceOffset{{Partition: map[string]any{"server": "postgres"}, Offset: map[string]any{"lsn": 1}}}

	calls := []struct {
		method string
		op     string
		tier   string
		ctx    func(*testing.T) context.Context
		run    func(context.Context, *Session) error
	}{
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
		{"Connect.RestartTask", OpConnectRestartTask, protocol.TierT2, write, func(ctx context.Context, s *Session) error {
			return s.Connect().RestartTask(ctx, "pg-orders-connector", 0)
		}},
		{"Connect.RestartConnector", OpConnectRestartConnector, protocol.TierT2, write, func(ctx context.Context, s *Session) error {
			return s.Connect().RestartConnector(ctx, "pg-orders-connector")
		}},
		{"Connect.CreateConnector", OpConnectCreateConnector, protocol.TierT2, write, func(ctx context.Context, s *Session) error {
			return s.Connect().CreateConnector(ctx, "pg-orders-connector", map[string]string{"connector.class": "c"})
		}},
		{"Connect.DeleteConnector", OpConnectDeleteConnector, protocol.TierT2, write, func(ctx context.Context, s *Session) error {
			return s.Connect().DeleteConnector(ctx, "pg-orders-connector")
		}},
		{"Connect.StopConnector", OpConnectStopConnector, protocol.TierT2, write, func(ctx context.Context, s *Session) error {
			return s.Connect().StopConnector(ctx, "pg-orders-connector")
		}},
		{"Connect.ResetSourceOffsets", OpConnectResetSourceOffsets, protocol.TierT2, write, func(ctx context.Context, s *Session) error {
			return s.Connect().ResetSourceOffsets(ctx, "pg-orders-connector")
		}},
		{"Connect.AlterSourceOffsets", OpConnectAlterSourceOffsets, protocol.TierT2, write, func(ctx context.Context, s *Session) error {
			return s.Connect().AlterSourceOffsets(ctx, "pg-orders-connector", offsets)
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
		{"Source.SetReplicaIdentity", OpSourceSetReplicaIdentity, protocol.TierT2, write, func(ctx context.Context, s *Session) error {
			return s.Source().SetReplicaIdentity(ctx, "public.orders", "full", "")
		}},
		{"Source.GrantSelect", OpSourceGrantSelect, protocol.TierT2, write, func(ctx context.Context, s *Session) error {
			return s.Source().GrantSelect(ctx, "debezium", []string{"public.orders"})
		}},
		{"Source.CreateHeartbeatTable", OpSourceCreateHeartbeatTable, protocol.TierT2, write, func(ctx context.Context, s *Session) error {
			return s.Source().CreateHeartbeatTable(ctx, "public.piperoom_heartbeat")
		}},
		{"Source.EndSession", OpSourceEndSession, protocol.TierT2, write, func(ctx context.Context, s *Session) error {
			return s.Source().EndSession(ctx, 42, at("2026-01-01T00:00:00Z"))
		}},
		{"Source.CreatePublication", OpSourceCreatePublication, protocol.TierT2, write, func(ctx context.Context, s *Session) error {
			return s.Source().CreatePublication(ctx, "pg_orders_pub", []string{"public.orders"})
		}},
		{"Source.CreateCanaryTable", OpSourceCreateCanaryTable, protocol.TierT2, write, func(ctx context.Context, s *Session) error {
			return s.Source().CreateCanaryTable(ctx, "public.piperoom_canary")
		}},
		{"Source.InsertCanary", OpSourceInsertCanary, protocol.TierT2, write, func(ctx context.Context, s *Session) error {
			return s.Source().InsertCanary(ctx, "public.piperoom_canary", "00112233445566778899aabbccddeeff")
		}},
		{"Source.CreateReadRole", OpSourceCreateReadRole, protocol.TierT2, write, func(ctx context.Context, s *Session) error {
			name, err := s.Source().CreateReadRole(ctx, "piperoom_reader", []string{"public.orders"})
			if err == nil && name != "orders_reader" {
				t.Fatalf("secret name = %q, want orders_reader", name)
			}
			return err
		}},
		{"Source.AlterSystem", OpSourceAlterSystem, protocol.TierT3, write, func(ctx context.Context, s *Session) error {
			pending, err := s.Source().AlterSystem(ctx, "wal_level", "logical")
			if err == nil && !pending {
				t.Fatal("the fake's pending_restart was not read")
			}
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
	if len(calls) != len(Ops()) {
		t.Fatalf("%d methods under test, %d ops in the table", len(calls), len(Ops()))
	}
	for _, tc := range calls {
		t.Run(tc.method, func(t *testing.T) {
			f := newFakeRunner()
			if err := tc.run(tc.ctx(t), New(f, "r_setup")); err != nil {
				t.Fatalf("%s: %v", tc.method, err)
			}
			got := f.allCalls()
			if len(got) != 1 {
				t.Fatalf("%s sent %d ops, want 1", tc.method, len(got))
			}
			req := got[0].req
			if req.Op != tc.op {
				t.Fatalf("%s sent op %q, want %q", tc.method, req.Op, tc.op)
			}
			if req.Tier != tc.tier {
				t.Fatalf("%s sent tier %q, want %q", tc.method, req.Tier, tc.tier)
			}
			var params map[string]any
			if err := json.Unmarshal(req.Params, &params); err != nil {
				t.Fatalf("%s: params %s are not a JSON object: %v", tc.method, req.Params, err)
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
