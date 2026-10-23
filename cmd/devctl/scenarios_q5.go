package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// stale-source-offsets (tasks/Q-board.md row Q5e, dev/scenarios.md §24): the
// orders connector is repointed at a fresh database (devctl_orders_v2, the
// migration target) under the same name and topic.prefix. Kafka Connect keys
// a source connector's offsets by connector name and source partition
// ({"server": "pg"}), so the connector starts with the old database's WAL
// position: Debezium skips the snapshot of the new database (its offsets say
// the snapshot is done) and asks the new slot for a position from before the
// slot existed. connect_source_offsets (GET /connectors/{name}/offsets) shows
// the stored LSN that belongs to the old database.

const (
	freshDB   = "devctl_orders_v2"
	freshSlot = "devctl_orders_v2"
)

const (
	sqlFreshDBExists = "SELECT count(*) FROM pg_database WHERE datname = 'devctl_orders_v2'"
	sqlCreateFreshDB = "CREATE DATABASE devctl_orders_v2"
	// The orders table of the new database, as dev/postgres-init/01-orders.sql
	// creates it, with ten restored rows the connector never snapshots.
	sqlFreshOrders      = "CREATE TABLE orders (id BIGSERIAL PRIMARY KEY, customer_id BIGINT NOT NULL, amount INTEGER NOT NULL, status TEXT NOT NULL DEFAULT 'placed', updated_at TIMESTAMPTZ NOT NULL DEFAULT now())"
	sqlFreshReplicaFull = "ALTER TABLE orders REPLICA IDENTITY FULL"
	sqlFreshRows        = "INSERT INTO orders (customer_id, amount) SELECT g, 100 + g FROM generate_series(1, 10) g"
	// The new database's slot is dropped before the database: Postgres refuses
	// to drop a database that a logical slot belongs to.
	sqlDropFreshSlot = "SELECT count(pg_drop_replication_slot(slot_name)) FROM pg_replication_slots WHERE slot_name = 'devctl_orders_v2' AND NOT active"
	sqlDropFreshDB   = "DROP DATABASE IF EXISTS devctl_orders_v2 WITH (FORCE)"
)

// staleOffsetsConnector repoints the connector: the new database and its own
// slot (a logical slot belongs to one database). The name and topic.prefix
// stay, which is what keeps the old offsets.
var staleOffsetsConnector = map[string]string{
	"database.dbname": freshDB,
	"slot.name":       freshSlot,
}

// storedOffsets is the body of GET /connectors/{name}/offsets (KIP-875).
type storedOffsets struct {
	Offsets []struct {
		Partition map[string]any `json:"partition"`
		Offset    map[string]any `json:"offset"`
	} `json:"offsets"`
}

// lsn is the first offset's "lsn" (Debezium Postgres), "" when there is none.
func (o storedOffsets) lsn() string {
	for _, e := range o.Offsets {
		if v, ok := e.Offset["lsn"]; ok {
			return fmt.Sprint(v)
		}
	}
	return ""
}

// connectorOffsets reads the connector's stored source offsets (Kafka Connect
// 3.5+). Numbers stay json.Number: an LSN does not fit a float64 exactly.
func (s *stack) connectorOffsets(ctx context.Context) (storedOffsets, error) {
	b, err := s.do(ctx, http.MethodGet, s.cfg.ConnectURL+"/connectors/"+connectorName+"/offsets", nil, http.StatusOK)
	if err != nil {
		return storedOffsets{}, err
	}
	var o storedOffsets
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.UseNumber()
	if err := dec.Decode(&o); err != nil {
		return storedOffsets{}, fmt.Errorf("devctl: decode connector offsets: %w", err)
	}
	return o, nil
}

func breakStaleSourceOffsets(ctx context.Context, s *stack) error {
	if n, err := s.psql(ctx, "orders", sqlFreshDBExists); err != nil {
		return err
	} else if n != "0" {
		return fmt.Errorf("devctl: database %s already exists — run `devctl fix stale-source-offsets` (or reset) first", freshDB)
	}
	before, err := s.connectorOffsets(ctx)
	if err != nil {
		return err
	}
	if len(before.Offsets) == 0 {
		return fmt.Errorf("devctl: %s has no stored offsets, so nothing can go stale — run `devctl reset` first", connectorName)
	}
	if _, err := s.psql(ctx, "orders", sqlCreateFreshDB); err != nil {
		return err
	}
	if _, err := s.psql(ctx, freshDB, sqlFreshOrders, sqlFreshReplicaFull, sqlFreshRows); err != nil {
		return err
	}
	if err := s.registerConnectorWith(ctx, staleOffsetsConnector); err != nil {
		return err
	}
	if err := s.restartTask(ctx, 0); err != nil {
		return err
	}
	if err := s.waitConnector(ctx, "FAILED", 90*time.Second); err != nil {
		return fmt.Errorf("devctl: task 0 did not fail on the stale offsets (this Debezium version may stream from the new slot instead; the 10 rows of %s then never reach Kafka): %w", freshDB, err)
	}
	after, err := s.connectorOffsets(ctx)
	if err != nil {
		return err
	}
	trace := ""
	if st, err := s.connectorStatus(ctx); err == nil && len(st.Tasks) > 0 {
		trace = firstLine(st.Tasks[0].Trace)
	}
	s.log.Info("broken: "+connectorName+" repointed at "+freshDB+" kept the old database's offsets — snapshot skipped, task 0 FAILED",
		"stored_lsn_before", before.lsn(), "stored_lsn_now", after.lsn(), "trace", trace)
	return nil
}

// The fix is what connect_stop_connector and connect_reset_source_offsets (to
// empty) do: stop, then delete the stored offsets. In an incident the
// connector then resumes on the new database and snapshots it; on the dev
// stack the fix puts the baseline back instead: the connector config from
// dev/connectors (orders, slot debezium_orders), resumed with no offsets, so
// Debezium snapshots orders (the load is an upsert by id) and streams from its
// slot. The new database and its slot are dropped. Idempotent: a healthy stack
// without the new database is left alone.
func fixStaleSourceOffsets(ctx context.Context, s *stack) error {
	n, err := s.psql(ctx, "orders", sqlFreshDBExists)
	if err != nil {
		return err
	}
	st, err := s.connectorStatus(ctx)
	if err != nil {
		return err
	}
	healthy := st.Connector.State == "RUNNING" && len(st.Tasks) > 0 && st.Tasks[0].State == "RUNNING"
	if n == "0" && healthy {
		s.log.Info("skipped: no " + freshDB + " and the connector is RUNNING, nothing is stale")
		return nil
	}
	if err := s.stopConnectorUntilSlotInactive(ctx); err != nil {
		return err
	}
	stale, err := s.connectorOffsets(ctx)
	if err != nil {
		return err
	}
	if err := s.clearConnectorOffsets(ctx); err != nil {
		return err
	}
	s.log.Info("stopped the connector and cleared its stored offsets", "stored_lsn", stale.lsn())
	if err := s.registerConnector(ctx); err != nil {
		return err
	}
	if n != "0" {
		if _, err := s.psql(ctx, "orders", sqlDropFreshSlot, sqlDropFreshDB); err != nil {
			return err
		}
		s.log.Info("dropped " + freshDB + " and its slot")
	}
	if _, err := s.do(ctx, http.MethodPut, s.cfg.ConnectURL+"/connectors/"+connectorName+"/resume", nil, http.StatusAccepted, http.StatusOK, http.StatusNoContent); err != nil {
		return err
	}
	if err := s.waitConnector(ctx, "RUNNING", 90*time.Second); err != nil {
		return err
	}
	if err := s.waitSlotActive(ctx); err != nil {
		return err
	}
	if err := s.waitMarkerOrder(ctx); err != nil {
		return fmt.Errorf("devctl: connector is not streaming after the offset reset: %w", err)
	}
	if err := s.waitLagZero(ctx, ordersGroup, 90*time.Second); err != nil {
		return err
	}
	s.log.Info("fixed: offsets cleared, connector back on orders (dev/connectors) after a fresh snapshot, " + freshDB + " removed, order-sync lag 0")
	return nil
}
