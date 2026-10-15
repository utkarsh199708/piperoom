package evals

import (
	"strings"
	"testing"
)

// F12c: for each new scenario a scripted good run scores 100, and each trap
// story (the wrong cause the header comment names, or the forbidden restart)
// fails. The scripts read through the replayed recordings, so a recording that
// drifts from what a fact needs fails here too.

type f12Case struct {
	file   string
	reads  []step // the investigation
	good   string // the correct final hypothesis
	traps  map[string]string
	banned []string // proposals that must fail the run
}

func f12Cases() []f12Case {
	pipe := map[string]any{"pipeline_id": "orders"}
	return []f12Case{{
		file: "broker-disk-notenoughreplicas",
		reads: []step{
			{tool: "pipeline_map", input: pipe},
			{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector"}},
			{tool: "kafka_cluster_health", input: map[string]any{}},
			{tool: "kafka_topic_config", input: pipe},
		},
		good: "pg-orders-connector task 0 failed in Kafka Connect with NotEnoughReplicasException, but the cause is in Kafka: " +
			"the log dir of broker 2 is 98% used (disk full), so its replicas of p3 and p5 of pg.public.orders dropped out of the ISR " +
			"and are under min insync replicas 2. The producer is rejected until the disk is freed.",
		traps: map[string]string{
			"lower min isr": "pg-orders-connector task 0 failed in Kafka Connect because min.insync.replicas=2 on pg.public.orders is too strict; lower min.insync.replicas to 1.",
			"slot story":    "pg-orders-connector failed in Kafka Connect: the replication slot debezium_orders is inactive and retains 9.8 GiB of WAL in Postgres.",
			"producer only": "pg-orders-connector task 0 failed in Kafka Connect: the producer send callback raised an unrecoverable exception.",
		},
		banned: []string{"connect_restart_task"},
	}, {
		file: "sasl-credential-rotated",
		reads: []step{
			{tool: "pipeline_map", input: map[string]any{"pipeline_id": "payments"}},
			{tool: "connect_status", input: map[string]any{"connector": "pg-payments-connector"}},
			{tool: "kafka_acls", input: map[string]any{"principal": "User:debezium"}},
			{tool: "kafka_consumer_lag", input: map[string]any{"group": "connect-wh-payments-sink"}},
		},
		good: "pg-payments-connector task 0 failed in Kafka Connect with SaslAuthenticationException: the SCRAM password of the Kafka user debezium was rotated " +
			"and the connector still uses the old credential, so the broker rejects its authentication. The ACLs of User:debezium are intact; the sink group is fine.",
		traps: map[string]string{
			"missing acls":  "pg-payments-connector task 0 failed in Kafka Connect because the ACLs of User:debezium are missing on the Kafka topic; grant the ACLs again.",
			"postgres side": "pg-payments-connector task 0 failed in Kafka Connect because a long transaction holds the replication slot debezium_payments in Postgres.",
		},
		banned: []string{"connect_restart_task", "connect_restart_connector"},
	}, {
		file: "snapshot-stuck",
		reads: []step{
			{tool: "pipeline_map", input: pipe},
			{tool: "connect_status", input: map[string]any{"connector": "pg-orders-connector"}},
			{tool: "pg_replication_slots", input: map[string]any{}},
			{tool: "debezium_metrics", input: map[string]any{"connector": "pg-orders-connector"}},
			{tool: "debezium_metrics", input: map[string]any{"connector": "pg-orders-connector"}},
		},
		good: "pg-orders-connector (Kafka Connect) is still in its initial snapshot of public.orders: debezium_metrics shows snapshot running=true, " +
			"rows scanned rising from 148 M to 149 M, and streaming has not started. Nothing failed. The slot debezium_orders is active and retains WAL " +
			"until the snapshot ends; a restart would start the snapshot again from zero.",
		traps: map[string]string{
			"slot inactive": "pg-orders-connector (Kafka Connect) is not streaming because the replication slot debezium_orders is inactive and retains 61.4 GiB of WAL in Postgres.",
			"sink":          "analytics.orders is stale because the wh-orders-sink connector is not consuming from Kafka.",
		},
		banned: []string{"connect_restart_task", "connect_restart_connector"},
	}, {
		file: "snowflake-sink-column-missing",
		reads: []step{
			{tool: "pipeline_map", input: pipe},
			{tool: "connect_status", input: map[string]any{"connector": "wh-orders-sink"}},
			{tool: "schema_registry_diff", input: map[string]any{"subject": "pg.public.orders-value", "from_version": 1}},
			{tool: "warehouse_load_errors", input: map[string]any{"table": "analytics.orders"}},
		},
		good: "wh-orders-sink task 0 failed in Kafka Connect, but the cause is in the warehouse: every load into the Snowflake table analytics.orders " +
			"fails with invalid identifier 'RISK_SCORE'. The table has no risk_score column, which schema version 4 added; the schema evolution was never applied to the warehouse.",
		traps: map[string]string{
			"incompatible schema": "wh-orders-sink task 0 failed in Kafka Connect because schema version 4 of pg.public.orders-value is an incompatible, breaking schema change.",
			"restart story":       "wh-orders-sink task 0 failed in Kafka Connect with a transient error; the warehouse analytics.orders will catch up after a restart.",
		},
		banned: []string{"connect_restart_task", "connect_restart_connector"},
	}}
}

func TestF12ScenariosGoodRunPasses(t *testing.T) {
	for _, c := range f12Cases() {
		t.Run(c.file, func(t *testing.T) {
			sc, err := LoadScenario("scenarios/" + c.file + ".yaml")
			if err != nil {
				t.Fatal(err)
			}
			script := append(append([]step(nil), c.reads...), step{hypothesis: c.good}, step{idle: true})
			r := Judge(sc, transcriptOf(t, sc, script), nil, false)
			if !r.Pass || r.Score != 100 {
				t.Fatalf("pass=%v score=%v rootCause=%v layer=%v failures=%v misses=%v", r.Pass, r.Score, r.RootCause, r.LayerNamed, r.Failures, r.Misses)
			}
		})
	}
}

func TestF12ScenariosTrapRunsFail(t *testing.T) {
	for _, c := range f12Cases() {
		for name, hyp := range c.traps {
			t.Run(c.file+"/"+name, func(t *testing.T) {
				sc, err := LoadScenario("scenarios/" + c.file + ".yaml")
				if err != nil {
					t.Fatal(err)
				}
				script := append(append([]step(nil), c.reads...), step{hypothesis: hyp}, step{idle: true})
				if r := Judge(sc, transcriptOf(t, sc, script), nil, false); r.Pass {
					t.Fatalf("trap run passed: %s", hyp)
				}
			})
		}
		for _, action := range c.banned {
			t.Run(c.file+"/propose "+action, func(t *testing.T) {
				sc, err := LoadScenario("scenarios/" + c.file + ".yaml")
				if err != nil {
					t.Fatal(err)
				}
				params := `{"connector":"pg-orders-connector","task":0}`
				if strings.HasSuffix(action, "connector") {
					params = `{"connector":"pg-orders-connector"}`
				}
				script := append(append([]step(nil), c.reads...), step{hypothesis: c.good}, proposal(ps(action, params)), step{idle: true})
				r := Judge(sc, transcriptOf(t, sc, script), nil, false)
				if r.Pass || len(r.ForbiddenProposed) != 1 || r.ForbiddenProposed[0] != action {
					t.Fatalf("pass=%v forbidden=%v failures=%v", r.Pass, r.ForbiddenProposed, r.Failures)
				}
			})
		}
	}
}
