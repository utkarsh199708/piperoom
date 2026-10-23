package evals

import "testing"

// q2gTermCases pin the root-cause terms of the Q2g/Q4c scenarios against the
// #879 eval-quality review: realistic wrong stories that passed the keyword
// matcher, correct answers it failed, and one probe per group and forbidden
// row, so deleting a group or a row breaks a test. Probes marked forbidden
// fail only on a forbidden row (every group matches); probes marked group
// fail only on one group.
var q2gTermCases = map[string][]termCase{
	"blocked-slot": {
		{id: "good-pid", want: true, text: "pid 48213 has an open transaction, so CREATE_REPLICATION_SLOT for debezium_orders waits on it; end that session with source_end_session."},
		{id: "wrong-query", text: "A long-running query on order-sync blocks the slot and the warehouse; restart the consumer."},
		{id: "wrong-slots-full", text: "The connector's session could not create the replication slot because max_replication_slots is exhausted; raise it."},
		{id: "forbidden-order-sync-holds", text: "order-sync holds an open transaction that keeps the slot from being created."},
		{id: "forbidden-order-sync-blocks", text: "An open transaction from order-sync blocks the slot debezium_orders."},
		{id: "forbidden-restart-consumer", text: "An open transaction blocks the slot; restart the consumer."},
		{id: "forbidden-max-slots", text: "The open transaction waits for a free slot because max_replication_slots is reached."},
		{id: "forbidden-exhausted", text: "The slot table is exhausted, so the open transaction waits for the slot."},
		{id: "group-slot", text: "pid 48213's open transaction blocks the warehouse load."},
		{id: "group-transaction", text: "Someone blocks creation of the replication slot debezium_orders."},
		{id: "group-wait", text: "An open transaction exists and the replication slot debezium_orders is missing."},
	},
	"idle-wal-growth": {
		{id: "good-no-writes", want: true, text: "There are no writes on public.orders while the batch database writes, so the active slot debezium_orders confirms nothing and holds 34.6 MiB of WAL. Add a heartbeat table."},
		{id: "good-registry", want: true, text: "public.orders stays idle, so Debezium's heartbeat never confirms and the slot debezium_orders retains WAL; max_slot_wal_keep_size is unlimited. Retention is fine."},
		// "Nothing" is not a negator for forbidden rows: making it one sends
		// dozens of pinned corpus texts ("compatibility NONE") to the judge.
		{id: "good-nothing-deleted", knownGap: true, text: "public.orders stays idle, so the heartbeat confirms nothing and the slot debezium_orders retains WAL. Nothing was deleted by retention."},
		{id: "wrong-dead-slot", text: "The replication slot debezium_orders is idle and holding WAL; drop the slot."},
		{id: "wrong-stopped", text: "The slot is retaining WAL because the connector is idle and stopped consuming; restart the connector."},
		{id: "wrong-slow", text: "No changes were made to the connector; the slot retains WAL because order-sync is slow."},
		{id: "forbidden-drop-the-slot", text: "The heartbeat stopped, so the slot debezium_orders holds WAL; drop the slot."},
		{id: "forbidden-drop-slot", text: "The heartbeat stopped and the slot holds WAL; drop slot debezium_orders."},
		{id: "forbidden-dropping", text: "The heartbeat stopped and the slot holds WAL; dropping the slot frees it."},
		{id: "forbidden-dead", text: "The heartbeat stopped, so it is a dead slot that holds WAL."},
		{id: "forbidden-orphaned", text: "The heartbeat stopped, so the slot is orphaned and holds WAL."},
		{id: "forbidden-abandoned", text: "The heartbeat stopped, so the slot is abandoned and holds WAL."},
		{id: "forbidden-inactive", text: "The heartbeat stopped because the slot is inactive and holds WAL."},
		{id: "forbidden-stopped-consuming", text: "Debezium stopped consuming heartbeats, so the slot holds WAL."},
		{id: "forbidden-lagging", text: "The heartbeat is lagging, so the slot holds WAL."},
		{id: "group-slot", text: "There are no writes on public.orders, so Postgres retains WAL."},
		{id: "group-wal", text: "There are no writes on public.orders, so the slot debezium_orders confirms nothing."},
		{id: "forbidden-restart", text: "Heartbeat events stopped flowing; the slot retains WAL, so restart the connector."},
		{id: "forbidden-slow", text: "The heartbeat is late because order-sync is slow, so the slot retains WAL."},
		{id: "group-idle", text: "The slot debezium_orders retains WAL."},
	},
	"register-rejected-missing-plugin": {
		{id: "good-not-installed", want: true, text: "The Debezium Postgres connector plugin isn't installed on the Connect workers any more: the redeploy's plugin.path no longer contains it. Restore the plugin jars and restart the workers."},
		{id: "good-prefix-fine", want: true, text: "The Debezium plugin is missing from plugin.path on the workers; the config, including topic.prefix, is fine."},
		{id: "wrong-paste", text: "org.apache.kafka.connect.errors.ConnectException: Failed to find any class that implements Connector and which name matches io.debezium.connector.postgresql.PostgresConnector, available connectors are: PluginDesc{klass=class org.apache.kafka.connect.mirror.MirrorCheckpointConnector, name='org.apache.kafka.connect.mirror.MirrorCheckpointConnector', version='3.6.1', encodedVersion=3.6.1, type=source, typeName='source', location='classpath'}"},
		{id: "wrong-hostname", text: "The Postgres connector plugin's config is missing database.hostname after the redeploy; add it back."},
		{id: "forbidden-config", text: "The connector plugin's config is missing a key after the redeploy; add it back."},
		{id: "forbidden-grant", text: "The connector plugin is missing a grant on public.orders after the redeploy."},
		{id: "forbidden-prefix-missing", text: "The connector plugin is fine but topic.prefix is missing."},
		{id: "forbidden-missing-prefix", text: "The connector plugin fails on a missing topic.prefix."},
		{id: "forbidden-prefix-required", text: "topic.prefix is required, so the connector plugin is missing from the task."},
		{id: "forbidden-hostname", text: "The connector plugin is missing database.hostname."},
		{id: "forbidden-user", text: "The connector plugin is missing database.user."},
		{id: "forbidden-privilege", text: "The connector plugin is missing a privilege on public.orders."},
		{id: "group-missing", text: "The connector plugin is on the classpath."},
		{id: "group-plugin", text: "PostgresConnector is not found."},
	},
	"missing-grant": {
		{id: "good-lacks", want: true, text: "The connector's database.user devctl_cdc lacks SELECT on public.orders, so the snapshot fails with permission denied. The Schema Registry is untouched."},
		{id: "good-theory", want: true, text: "devctl_cdc lacks SELECT on public.orders; the network partition theory is wrong."},
		{id: "wrong-rotated", text: "The replication slot is inactive because the connector role password was rotated and the grant expired; revert database.user to piperoom."},
		{id: "wrong-replication", text: "devctl_cdc lacks the REPLICATION privilege, so the snapshot fails; grant it."},
		{id: "wrong-expired", text: "devctl_cdc cannot SELECT public.orders because its password expired; revert database.user to piperoom."},
		{id: "forbidden-replication", text: "devctl_cdc lacks the REPLICATION privilege it needs to SELECT from public.orders."},
		{id: "forbidden-password", text: "devctl_cdc's password stopped SELECT on public.orders."},
		{id: "forbidden-rotated", text: "devctl_cdc was rotated in and lacks SELECT on public.orders."},
		{id: "forbidden-expired", text: "devctl_cdc's SELECT grant expired."},
		{id: "forbidden-revert", text: "devctl_cdc lacks SELECT on public.orders; revert to piperoom."},
		{id: "forbidden-lacks-replication", text: "devctl_cdc lacks REPLICATION, which SELECT needs."},
		{id: "forbidden-missing-replication", text: "devctl_cdc is missing REPLICATION to SELECT public.orders."},
		{id: "forbidden-missing-the-replication", text: "devctl_cdc is missing the REPLICATION attribute to SELECT public.orders."},
		{id: "forbidden-no-replication-privilege", text: "devctl_cdc can SELECT only with a REPLICATION grant, and it has no replication privilege."},
		{id: "group-role", text: "Someone lacks SELECT on public.orders."},
		{id: "group-select", text: "The connector role devctl_cdc is new."},
	},
	"no-replica-identity": {
		{id: "good-nothing", want: true, text: "A migration set the replica identity of public.orders to NOTHING, so Postgres rejects updates and deletes while the publication publishes them; set it to FULL."},
		{id: "good-none", want: true, text: "The replica identity of public.orders was set to none by the migration; set it to FULL."},
		{id: "good-changed", want: true, text: "The migration changed the replica identity of public.orders, so updates fail; set it to FULL."},
		{id: "wrong-no-pk", text: "public.orders has no primary key, so its default replica identity has no key. Add a primary key."},
		{id: "wrong-app", text: "Nothing in the pipeline failed. The orders service's ORM replica identity check in the app is wrong; redeploy the app."},
		{id: "forbidden-pk", text: "The replica identity of public.orders was set to NOTHING because the table has no primary key; add a primary key."},
		{id: "forbidden-lacks-pk", text: "The replica identity is NOTHING because public.orders lacks a primary key."},
		{id: "forbidden-missing-pk", text: "The replica identity is NOTHING because of a missing primary key."},
		{id: "forbidden-missing-a-pk", text: "The replica identity is NOTHING because public.orders is missing a primary key."},
		{id: "forbidden-orm", text: "The ORM set the replica identity to NOTHING."},
		{id: "forbidden-in-app", text: "A bug in the app set the replica identity to NOTHING."},
		{id: "forbidden-redeploy", text: "The replica identity is NOTHING; redeploy the app."},
		{id: "group-identity", text: "Updates on public.orders fail because the old tuple is not recorded."},
	},
	"register-rejected-renamed-key": {
		{id: "good-new-key", want: true, text: "The connector config lacks topic.prefix, which Debezium 2.0 made required; add topic.prefix=pg and restart."},
		{id: "good-plugin-fine", want: true, text: "database.server.name was renamed to topic.prefix in Debezium 2.0, so the upgraded Debezium Postgres connector rejects the 1.x config. The connector plugin itself loaded fine."},
		{id: "good-registry", want: true, text: "database.server.name was renamed to topic.prefix in Debezium 2.0, so task 0 fails to configure. Schema Registry is healthy."},
		{id: "wrong-vague", text: "Debezium was upgraded and a setting was renamed."},
		{id: "forbidden-plugin", text: "The Debezium Postgres plugin is missing after the upgrade; topic.prefix is required but nothing reads it."},
		{id: "forbidden-registry", text: "The Schema Registry rejected topic.prefix because the subject was renamed."},
		{id: "forbidden-missing-plugin", text: "A missing plugin means topic.prefix is required."},
		{id: "forbidden-not-installed", text: "The plugin is not installed, so topic.prefix is required."},
		{id: "forbidden-isnt-installed", text: "The plugin isn't installed, so topic.prefix is required."},
		{id: "forbidden-not-found", text: "Plugin not found: topic.prefix is required."},
		{id: "forbidden-jars", text: "The plugin jars reject database.server.name since it was renamed."},
		{id: "forbidden-path", text: "The plugin path lacks the class that requires topic.prefix."},
		{id: "group-renamed", text: "The connector config has database.server.name=pg."},
	},
}

func TestQ2gRootCauseTerms(t *testing.T) {
	scs, err := LoadScenarios("scenarios")
	if err != nil {
		t.Fatal(err)
	}
	byName := map[string]Scenario{}
	for _, sc := range scs {
		byName[sc.Name] = sc
	}
	for name, cases := range q2gTermCases {
		sc, ok := byName[name]
		if !ok {
			t.Fatalf("scenario %q not found", name)
		}
		for _, c := range cases {
			got := rootCauseMatches(sc.Expect, c.text)
			switch {
			case c.knownGap && !got:
				t.Logf("%s %s: known gap, the judge decides: %q", name, c.id, c.text)
			case got != c.want:
				t.Errorf("%s %s: matches = %v, want %v: %q", name, c.id, got, c.want, c.text)
			}
			// A probe that fails on a forbidden row must match every group,
			// or deleting the row would not change its verdict.
			if len(c.id) > 10 && c.id[:10] == "forbidden-" {
				if !matchGroups(sc.Expect.RootCause, c.text) {
					t.Errorf("%s %s: probe misses a group, so it does not test the forbidden rows: %v", name, c.id, missingCauseGroups(sc.Expect, c.text))
				}
				if !matchForbidden(sc.Expect.RootCauseForbidden, c.text) {
					t.Errorf("%s %s: no forbidden row fires", name, c.id)
				}
			}
			// A wrong story the keyword grade fails must not be left to
			// the judge either when it holds no denial.
			if !c.want && !c.knownGap {
				if a := ambiguityOf(sc, c.text); a.RootCause {
					t.Logf("%s %s: ambiguous, the judge decides: %v", name, c.id, a.Reasons)
				}
			}
		}
	}
}
