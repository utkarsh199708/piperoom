package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// The Q2 scenarios (tasks/Q-board.md row Q2f, dev/scenarios.md §20-§23): the
// source-database settings behind the Stack Overflow Debezium problems the Q2
// actions end. Each break shows in one of the Q2a tools (source_settings,
// source_table_replication, source_role_grants, source_blocking_sessions) and
// opens a room through the watcher or an alert. Everything a break creates is
// named devctl_* / devctl-*, and each fix removes it.

// q2OwnersFile is the owners file idle-wal-growth needs: dev/owners.yaml plus
// a small slot_wal threshold (the baseline fires at 1 GiB, which a dev disk
// never reaches). The heartbeat table is not in its source_tables: the Q2c
// heartbeat action refuses a heartbeat table listed there, and devctl fix adds
// the table to the publication itself.
const q2OwnersFile = "dev/owners.q2.yaml"

const (
	// missing-grant: the least-privilege role the connector moves to.
	cdcRole = "devctl_cdc"
	// blocked-slot: application_name of the session left open, and the
	// analyst's role it runs as. The role is not the connector's
	// database.user (piperoom): source_end_session never ends a session of
	// that user, so a blocker running as piperoom could not be ended.
	blockerApp  = "devctl-blocker"
	blockerRole = "devctl_analyst"
	// idle-wal-growth: the busy database next to the captured one, and the
	// heartbeat table of the fix.
	batchDB        = "devctl_batch"
	heartbeatTable = "public.devctl_heartbeat"
	heartbeatTopic = "pg.public.devctl_heartbeat"
	// Debezium's heartbeat records go to topic.heartbeat.prefix + "." +
	// topic.prefix (default prefix __debezium-heartbeat).
	heartbeatRecordsTopic = "__debezium-heartbeat.pg"
	// idleWALFire and idleWALClear are slot_wal.fire and .clear in
	// dev/owners.q2.yaml (TestOwnersQ2MatchesBaseline keeps them in step).
	idleWALFire  int64 = 32 << 20
	idleWALClear int64 = 16 << 20
	// idleWALBatches bounds the filler inserts of the break.
	idleWALBatches = 4
)

// SQL the Q2 scenarios run, named so the unit tests' fake Postgres answers
// exactly these statements.
const (
	// no-replica-identity
	sqlReplicaIdentity = "SELECT relreplident FROM pg_class WHERE oid = 'public.orders'::regclass"
	sqlReplicaNothing  = "ALTER TABLE public.orders REPLICA IDENTITY NOTHING"
	// An application update that changes nothing: with no replica identity it
	// fails on a table whose publication publishes updates.
	sqlAppUpdate = "UPDATE orders SET updated_at = updated_at WHERE id = (SELECT min(id) FROM orders)"

	// missing-grant
	sqlCDCRoleExists  = "SELECT count(*) FROM pg_roles WHERE rolname = 'devctl_cdc'"
	sqlCreateCDCRole  = "CREATE ROLE devctl_cdc LOGIN REPLICATION PASSWORD 'piperoom-dev'"
	sqlCDCConnect     = "GRANT CONNECT ON DATABASE orders TO devctl_cdc"
	sqlCDCUsage       = "GRANT USAGE ON SCHEMA public TO devctl_cdc"
	sqlCDCCanSelect   = "SELECT has_table_privilege('devctl_cdc', 'public.orders', 'SELECT')"
	sqlGrantCDCSelect = "GRANT SELECT ON public.orders TO devctl_cdc"
	// DROP OWNED also revokes the role's privileges in this database and on
	// the database itself; the role owns no object (autocreate is disabled).
	sqlDropCDCRole = "DO $$ BEGIN IF EXISTS (SELECT FROM pg_roles WHERE rolname = 'devctl_cdc') THEN EXECUTE 'DROP OWNED BY devctl_cdc'; EXECUTE 'DROP ROLE devctl_cdc'; END IF; END $$"

	// blocked-slot
	sqlBlockerOpen = "SELECT count(*) FROM pg_stat_activity WHERE application_name = 'devctl-blocker' AND backend_xid IS NOT NULL"
	// The analyst's role: LOGIN only (PUBLIC has CONNECT on orders; the
	// session reads no table). Created if missing, so the break is safe to
	// run on a stack a half-run fix left it on.
	sqlCreateBlockerRole = "DO $$ BEGIN IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'devctl_analyst') THEN CREATE ROLE devctl_analyst LOGIN; END IF; END $$"
	// The role owns nothing; DROP ROLE does not wait for its terminated sessions.
	sqlDropBlockerRole = "DROP ROLE IF EXISTS devctl_analyst"
	// CREATE_REPLICATION_SLOT waits in XactLockTableWait for every transaction
	// with an xid that was running when it started: a Lock wait on transactionid.
	sqlSlotCreateWaits = "SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'walsender' AND wait_event_type = 'Lock' AND wait_event = 'transactionid'"
	sqlEndBlocker      = "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE application_name = 'devctl-blocker'"

	// idle-wal-growth
	sqlBatchDBExists = "SELECT count(*) FROM pg_database WHERE datname = 'devctl_batch'"
	sqlCreateBatchDB = "CREATE DATABASE devctl_batch"
	sqlCreateFiller  = "CREATE TABLE IF NOT EXISTS devctl_wal_filler (id bigserial PRIMARY KEY, pad text NOT NULL)"
	// About 50 MB of rows under the TOAST threshold, so none is compressed.
	sqlFillWAL         = "INSERT INTO devctl_wal_filler (pad) SELECT repeat(md5(g::text), 32) FROM generate_series(1, 48000) g"
	sqlDropBatchDB     = "DROP DATABASE IF EXISTS devctl_batch WITH (FORCE)"
	sqlCreateHeartbeat = "CREATE TABLE IF NOT EXISTS public.devctl_heartbeat (id integer PRIMARY KEY, ts timestamptz NOT NULL DEFAULT now())"
	sqlHeartbeatExists = "SELECT to_regclass('public.devctl_heartbeat') IS NOT NULL"
	sqlHeartbeatInPub  = "SELECT count(*) FROM pg_publication_tables WHERE pubname = 'dbz_publication' AND schemaname = 'public' AND tablename = 'devctl_heartbeat'"
	sqlPubAddHeartbeat = "ALTER PUBLICATION dbz_publication ADD TABLE public.devctl_heartbeat"
	sqlDropHeartbeat   = "DROP TABLE IF EXISTS public.devctl_heartbeat"
	// heartbeatActionQuery is the connector's heartbeat.action.query in the fix.
	heartbeatActionQuery = "INSERT INTO public.devctl_heartbeat (id, ts) VALUES (1, now()) ON CONFLICT (id) DO UPDATE SET ts = excluded.ts"
)

// Alerts of the Q2 breaks the watcher cannot see. Each says what was observed,
// never the cause.
const (
	noReplicaIdentityAlert = `{
  "title": "orders: updates to public.orders failing in the orders service",
  "severity": "sev2",
  "pipeline_id": "orders",
  "dedup_key": "devctl-no-replica-identity",
  "source": "app",
  "description": "Every UPDATE on public.orders fails with: cannot update table \"orders\" because it does not have a replica identity and publishes updates. Inserts still succeed."
}`
	blockedSlotAlert = `{
  "title": "orders: analytics.orders stopped receiving new orders",
  "severity": "sev2",
  "pipeline_id": "orders",
  "dedup_key": "devctl-blocked-slot",
  "source": "freshness",
  "description": "No new order has reached analytics.orders since pg-orders-connector was restarted with a recreated replication slot after a host move. New orders are in public.orders."
}`
)

// missingGrantConnector is the connector config of missing-grant: the
// connector moved to the least-privilege role devctl_cdc (LOGIN, REPLICATION;
// the SELECT grant was never applied). The DBA owns the publication
// (autocreate disabled, else the role would also need to own the table, which
// is connector-auth), and snapshot.mode=always makes every start read the
// table, which is where a missing SELECT fails.
var missingGrantConnector = map[string]string{
	"database.user":               cdcRole,
	"database.password":           "piperoom-dev",
	"publication.autocreate.mode": "disabled",
	"snapshot.mode":               "always",
}

// heartbeatConnector is the connector config the idle-wal-growth fix applies
// (what source_create_heartbeat_table's follow-up config step sets): the
// heartbeat table is captured, so each heartbeat is a change Debezium receives
// and confirms, and the slot advances although the orders table is idle.
var heartbeatConnector = map[string]string{
	"table.include.list":     "public.orders," + heartbeatTable,
	"heartbeat.interval.ms":  "5000",
	"heartbeat.action.query": heartbeatActionQuery,
}

// ------------------------------------------------------------ no-replica-identity

// no-replica-identity: a migration copied from a table that is not replicated
// set REPLICA IDENTITY NOTHING on public.orders. dbz_publication publishes
// updates, so Postgres refuses every UPDATE and DELETE on the table; inserts
// still flow to the warehouse. source_table_replication shows
// replica_identity=nothing.
func breakNoReplicaIdentity(ctx context.Context, s *stack) error {
	ident, err := s.psql(ctx, "orders", sqlReplicaIdentity)
	if err != nil {
		return err
	}
	if ident == "n" {
		return errors.New("devctl: public.orders already has REPLICA IDENTITY NOTHING — run `devctl fix no-replica-identity` (or reset) first")
	}
	if _, err := s.psql(ctx, "orders", sqlReplicaNothing); err != nil {
		return err
	}
	// Proof: the application's update fails, and an insert still streams.
	_, err = s.psql(ctx, "orders", sqlAppUpdate)
	if err == nil {
		return errors.New("devctl: an UPDATE on public.orders succeeded with REPLICA IDENTITY NOTHING: the break did not hold (is orders still in dbz_publication?)")
	}
	if !strings.Contains(err.Error(), "does not have a replica identity") {
		return fmt.Errorf("devctl: UPDATE on public.orders failed for another reason: %w", err)
	}
	if err := s.waitMarkerOrder(ctx); err != nil {
		return err
	}
	s.log.Info("broken: public.orders has REPLICA IDENTITY NOTHING — every UPDATE and DELETE fails on the source, inserts still stream",
		"replica_identity_before", ident)
	return nil
}

// The fix is what pg_set_replica_identity (mode full) does: FULL is the dev
// baseline (dev/postgres-init/01-orders.sql). An update proves it. Idempotent.
func fixNoReplicaIdentity(ctx context.Context, s *stack) error {
	ident, err := s.psql(ctx, "orders", sqlReplicaIdentity)
	if err != nil {
		return err
	}
	if ident == "f" {
		s.log.Info("skipped: public.orders already has REPLICA IDENTITY FULL")
	} else if _, err := s.psql(ctx, "orders", sqlReplicaFull); err != nil {
		return err
	}
	if _, err := s.psql(ctx, "orders", sqlAppUpdate); err != nil {
		return fmt.Errorf("devctl: UPDATE on public.orders still fails after the fix: %w", err)
	}
	s.log.Info("fixed: public.orders REPLICA IDENTITY FULL, updates succeed", "replica_identity_before", ident)
	return nil
}

// ------------------------------------------------------------------ missing-grant

// missing-grant: the connector runs as devctl_cdc, which may log in and
// replicate but cannot SELECT public.orders. The snapshot every start takes
// fails with "permission denied for table orders", task 0 FAILS and the slot
// goes inactive. source_role_grants (the role is the connector config's
// database.user) shows select=false on public.orders.
func breakMissingGrant(ctx context.Context, s *stack) error {
	if n, err := s.psql(ctx, "orders", sqlCDCRoleExists); err != nil {
		return err
	} else if n != "0" {
		return fmt.Errorf("devctl: role %s already exists — run `devctl fix missing-grant` (or reset) first", cdcRole)
	}
	if _, err := s.psql(ctx, "orders", sqlCreateCDCRole, sqlCDCConnect, sqlCDCUsage); err != nil {
		return err
	}
	if err := s.registerConnectorWith(ctx, missingGrantConnector); err != nil {
		return err
	}
	if err := s.restartTask(ctx, 0); err != nil {
		return err
	}
	if err := s.waitConnector(ctx, "FAILED", 90*time.Second); err != nil {
		return fmt.Errorf("devctl: task 0 did not fail without SELECT on public.orders: %w", err)
	}
	trace := ""
	if st, err := s.connectorStatus(ctx); err == nil && len(st.Tasks) > 0 {
		trace = firstLine(st.Tasks[0].Trace)
	}
	s.log.Info("broken: connector runs as "+cdcRole+" without SELECT on public.orders — task 0 FAILED on the snapshot, slot inactive",
		"trace", trace)
	return nil
}

// The fix is what source_grant_select does (SELECT on the pipeline's tables
// for the connector's database.user, nothing else), then a task restart that
// proves the snapshot now runs. Then the dev baseline: the connector back on
// piperoom from dev/connectors, and the role dropped. Idempotent.
func fixMissingGrant(ctx context.Context, s *stack) error {
	n, err := s.psql(ctx, "orders", sqlCDCRoleExists)
	if err != nil {
		return err
	}
	if n != "0" {
		can, err := s.psql(ctx, "orders", sqlCDCCanSelect)
		if err != nil {
			return err
		}
		if can == "t" {
			s.log.Info("skipped: " + cdcRole + " can already SELECT public.orders")
		} else {
			if _, err := s.psql(ctx, "orders", sqlGrantCDCSelect); err != nil {
				return err
			}
			s.log.Info("granted SELECT on public.orders to " + cdcRole)
		}
		if err := s.restartTask(ctx, 0); err != nil {
			return err
		}
		if err := s.waitConnector(ctx, "RUNNING", 90*time.Second); err != nil {
			return fmt.Errorf("devctl: task 0 did not recover after the grant: %w", err)
		}
	} else {
		s.log.Info("skipped: role " + cdcRole + " does not exist, nothing to grant")
	}
	if err := s.registerConnector(ctx); err != nil {
		return err
	}
	if err := s.restartTask(ctx, 0); err != nil {
		return err
	}
	if err := s.waitConnector(ctx, "RUNNING", 90*time.Second); err != nil {
		return err
	}
	if err := s.waitSlotActive(ctx); err != nil {
		return err
	}
	// Only now: the connector's sessions as devctl_cdc are gone.
	if _, err := s.psql(ctx, "orders", sqlDropCDCRole); err != nil {
		return err
	}
	if err := s.waitMarkerOrder(ctx); err != nil {
		return err
	}
	s.log.Info("fixed: SELECT granted and proven, connector back on piperoom (dev/connectors), role " + cdcRole + " dropped, task 0 RUNNING, slot active")
	return nil
}

// ------------------------------------------------------------------- blocked-slot

// blocked-slot: the connector is moved to a new host and set up again — its
// slot dropped, its stored offsets cleared — while an analyst's session holds
// a transaction open. Debezium's CREATE_REPLICATION_SLOT waits for every
// transaction that was running when it started, so the task never gets past
// start: nothing streams and nothing fails. source_blocking_sessions shows the
// session (blocks_slot=true: it holds the oldest xmin). The session runs as
// devctl_analyst, not as the connector's piperoom, so source_end_session may
// end it; in a pipeline room its identity is hidden (it locks no pipeline
// table), only its pid, times, state and blocks_slot show.
func breakBlockedSlot(ctx context.Context, s *stack) error {
	if slot, ok, err := s.slotRow(ctx); err != nil {
		return err
	} else if !ok || !slot.active {
		return fmt.Errorf("devctl: slot %s is missing or inactive — run `devctl fix blocked-slot` (or reset) first", slotName)
	}
	if n, err := s.psql(ctx, "orders", sqlBlockerOpen); err != nil {
		return err
	} else if n != "0" {
		return fmt.Errorf("devctl: a %s session is already open — run `devctl fix blocked-slot` first", blockerApp)
	}
	if _, err := s.psql(ctx, "orders", sqlCreateBlockerRole); err != nil {
		return err
	}
	// docker exec -d keeps the session alive after devctl exits. The xid
	// (pg_current_xact_id assigns one) is what slot creation waits for.
	if err := s.detached(ctx, "docker", "exec", "-d", "-e", "PGAPPNAME="+blockerApp, pgContainer,
		"psql", "-U", blockerRole, "-d", "orders",
		"-c", "BEGIN",
		"-c", "SELECT pg_current_xact_id()",
		"-c", "SELECT pg_sleep(1800)",
		"-c", "COMMIT"); err != nil {
		return err
	}
	if err := s.waitFor(ctx, 15*time.Second, func() (bool, error) {
		n, err := s.psql(ctx, "orders", sqlBlockerOpen)
		return err == nil && n != "0", nil
	}); err != nil {
		return fmt.Errorf("devctl: the %s transaction did not start: %w", blockerApp, err)
	}
	// The re-setup: stop (Connect deletes offsets only on a stopped connector,
	// and a stopped one holds no WAL sender), drop the slot, clear the offsets,
	// resume. Debezium then creates the slot, and the creation waits.
	if err := s.stopConnectorUntilSlotInactive(ctx); err != nil {
		return err
	}
	if _, err := s.psql(ctx, "orders", sqlDropSlot); err != nil {
		return err
	}
	if err := s.clearConnectorOffsets(ctx); err != nil {
		return err
	}
	if _, err := s.do(ctx, http.MethodPut, s.cfg.ConnectURL+"/connectors/"+connectorName+"/resume", nil, http.StatusAccepted, http.StatusOK, http.StatusNoContent); err != nil {
		return err
	}
	if err := s.waitFor(ctx, 60*time.Second, func() (bool, error) {
		n, err := s.psql(ctx, "orders", sqlSlotCreateWaits)
		return err == nil && n != "0", nil
	}); err != nil {
		return fmt.Errorf("devctl: no walsender waits on a transaction (did Debezium create the slot anyway?): %w", err)
	}
	s.log.Info("broken: slot " + slotName + " re-created by Debezium waits for the open " + blockerApp + " transaction (30 min) — nothing streams, nothing fails")
	return nil
}

// The fix is what source_end_session does: end the blocking session. The
// slot creation then finishes, Debezium snapshots (no offsets) and streams.
// The fix waits for an active slot, a marker row in the warehouse and lag 0;
// a connector left STOPPED by a half-run break is resumed. Idempotent.
func fixBlockedSlot(ctx context.Context, s *stack) error {
	out, err := s.psql(ctx, "orders", sqlEndBlocker)
	if err != nil {
		return err
	}
	s.log.Info("ended "+blockerApp+" sessions", "count", strings.TrimSpace(out))
	if _, err := s.psql(ctx, "orders", sqlDropBlockerRole); err != nil {
		return err
	}
	st, err := s.connectorStatus(ctx)
	if err != nil {
		return err
	}
	if st.Connector.State != "RUNNING" {
		if _, err := s.do(ctx, http.MethodPut, s.cfg.ConnectURL+"/connectors/"+connectorName+"/resume", nil, http.StatusAccepted, http.StatusOK, http.StatusNoContent); err != nil {
			return err
		}
	}
	if err := s.waitConnector(ctx, "RUNNING", 90*time.Second); err != nil {
		return err
	}
	if err := s.waitSlotActive(ctx); err != nil {
		return err
	}
	if err := s.waitMarkerOrder(ctx); err != nil {
		return fmt.Errorf("devctl: connector is not streaming after the blocker ended: %w", err)
	}
	if err := s.waitLagZero(ctx, ordersGroup, 90*time.Second); err != nil {
		return err
	}
	s.log.Info("fixed: blocking session ended, role " + blockerRole + " dropped, slot " + slotName + " created and active, connector RUNNING, order-sync lag 0")
	return nil
}

// --------------------------------------------------------------- idle-wal-growth

// idle-wal-growth: the captured table is idle while another database on the
// same server (devctl_batch, a nightly batch) writes a lot. Logical decoding is
// per database, so Debezium receives nothing, confirms nothing, and the active
// slot retains every WAL segment the batch writes. slot_wal fires on
// dev/owners.q2.yaml (32 MiB); source_settings shows no max_slot_wal_keep_size
// cap (-1). Do not run `devctl load` meanwhile: an order change would let
// Debezium confirm and release the WAL.
func breakIdleWALGrowth(ctx context.Context, s *stack) error {
	slot, ok, err := s.slotRow(ctx)
	if err != nil {
		return err
	}
	if !ok || !slot.active {
		return fmt.Errorf("devctl: slot %s is missing or inactive — run `devctl fix idle-wal-growth` (or reset) first", slotName)
	}
	if n, err := s.psql(ctx, "orders", sqlBatchDBExists); err != nil {
		return err
	} else if n == "0" {
		if _, err := s.psql(ctx, "orders", sqlCreateBatchDB); err != nil {
			return err
		}
	}
	if _, err := s.psql(ctx, batchDB, sqlCreateFiller); err != nil {
		return err
	}
	retained := slot.retained
	for i := 0; i < idleWALBatches && retained < idleWALFire; i++ {
		if _, err := s.psql(ctx, batchDB, sqlFillWAL); err != nil {
			return err
		}
		if retained, err = s.retainedWALBytes(ctx); err != nil {
			return err
		}
	}
	slot, ok, err = s.slotRow(ctx)
	if err != nil {
		return err
	}
	if !ok || !slot.active || slot.retained < idleWALFire {
		return fmt.Errorf("devctl: slot %s retains %d bytes (active %v), want an active slot at >= %d: the break did not hold", slotName, slot.retained, slot.active, idleWALFire)
	}
	s.log.Info("broken: slot "+slotName+" is active but confirms nothing while "+batchDB+" writes — retained WAL past slot_wal fire",
		"retained_bytes", slot.retained, "fire_bytes", idleWALFire)
	return nil
}

// The fix is what source_create_heartbeat_table and its follow-up steps do:
// create the heartbeat table, capture it (with autocreate filtered Debezium
// adds it to dbz_publication; added here when it did not), and set
// heartbeat.interval.ms and heartbeat.action.query. Each heartbeat is then a
// change the connector confirms, and retained WAL falls below slot_wal clear
// (after an offset commit: Connect flushes offsets every 60 s). Then the dev
// baseline: the connector config from dev/connectors, the heartbeat table,
// its topics and subjects and devctl_batch removed. Idempotent.
func fixIdleWALGrowth(ctx context.Context, s *stack) error {
	slot, ok, err := s.slotRow(ctx)
	if err != nil {
		return err
	}
	if ok && slot.retained >= idleWALClear {
		if err := s.heartbeatUntilReleased(ctx); err != nil {
			return err
		}
	} else {
		s.log.Info("skipped: heartbeat, slot "+slotName+" retains less than slot_wal clear", "retained_bytes", slot.retained)
	}
	if err := s.registerConnector(ctx); err != nil {
		return err
	}
	if err := s.restartTask(ctx, 0); err != nil {
		return err
	}
	if err := s.waitConnector(ctx, "RUNNING", 90*time.Second); err != nil {
		return err
	}
	// After the baseline config: the heartbeat query no longer writes to it.
	if _, err := s.psql(ctx, "orders", sqlDropHeartbeat, sqlDropBatchDB); err != nil {
		return err
	}
	if err := s.deleteTopics(ctx, heartbeatTopic, heartbeatRecordsTopic); err != nil {
		return err
	}
	for _, sub := range []string{heartbeatTopic + "-key", heartbeatTopic + "-value"} {
		if err := s.deleteSubject(ctx, sub); err != nil {
			return err
		}
	}
	s.log.Info("fixed: retained WAL released by a heartbeat, connector config back to dev/connectors, devctl_heartbeat, " + batchDB + " and their topics removed")
	return nil
}

func (s *stack) heartbeatUntilReleased(ctx context.Context) error {
	if _, err := s.psql(ctx, "orders", sqlCreateHeartbeat); err != nil {
		return err
	}
	if err := s.registerConnectorWith(ctx, heartbeatConnector); err != nil {
		return err
	}
	if err := s.restartTask(ctx, 0); err != nil {
		return err
	}
	if err := s.waitConnector(ctx, "RUNNING", 90*time.Second); err != nil {
		return fmt.Errorf("devctl: connector not RUNNING with the heartbeat: %w", err)
	}
	if n, err := s.psql(ctx, "orders", sqlHeartbeatInPub); err != nil {
		return err
	} else if n == "0" {
		if _, err := s.psql(ctx, "orders", sqlPubAddHeartbeat); err != nil {
			return err
		}
		s.log.Info("added " + heartbeatTable + " to " + publicationName)
	}
	var retained int64
	if err := s.waitFor(ctx, 3*time.Minute, func() (bool, error) {
		row, ok, err := s.slotRow(ctx)
		if err != nil || !ok {
			return false, nil
		}
		retained = row.retained
		return row.active && row.retained < idleWALClear, nil
	}); err != nil {
		return fmt.Errorf("devctl: slot %s still retains %d bytes with the heartbeat on: %w", slotName, retained, err)
	}
	s.log.Info("heartbeat released the WAL", "retained_bytes", retained, "clear_bytes", idleWALClear)
	return nil
}

// -------------------------------------------------------------------- helpers

// waitSlotActive waits until slotName exists and has a WAL sender.
func (s *stack) waitSlotActive(ctx context.Context) error {
	if err := s.waitFor(ctx, 60*time.Second, func() (bool, error) {
		active, err := s.slotActive(ctx)
		return err == nil && active, nil
	}); err != nil {
		return fmt.Errorf("devctl: slot %s did not become active: %w", slotName, err)
	}
	return nil
}

// firstLine is the first line of a Connect task trace, for a log field.
func firstLine(trace string) string {
	line, _, _ := strings.Cut(strings.TrimSpace(trace), "\n")
	return strings.TrimSpace(line)
}
