package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type scenario struct {
	breakIt func(ctx context.Context, s *stack) error
	fix     func(ctx context.Context, s *stack) error
}

var scenarios = map[string]scenario{
	"schema-change":    {breakSchemaChange, fixSchemaChange},
	"connector-down":   {breakConnectorDown, fixConnectorDown},
	"consumer-stuck":   {breakConsumerStuck, fixConsumerStuck},
	"long-transaction": {breakLongTransaction, fixLongTransaction},
}

// schema-change: a migration changes orders.amount from integer to
// numeric(12,2). Debezium's new Avro schema (bytes/decimal instead of int) is
// not BACKWARD compatible, Schema Registry rejects it, and the task fails.
func breakSchemaChange(ctx context.Context, s *stack) error {
	typ, err := s.psql(ctx, "orders", "SELECT data_type FROM information_schema.columns WHERE table_name = 'orders' AND column_name = 'amount'")
	if err != nil {
		return err
	}
	if typ != "integer" {
		return fmt.Errorf("devctl: orders.amount is %q, not integer — run `devctl reset` first", typ)
	}
	if _, err := s.psql(ctx, "orders",
		"ALTER TABLE orders ALTER COLUMN amount TYPE numeric(12,2)",
		"INSERT INTO orders (customer_id, amount) VALUES (42, 1249.50), (43, 99.99)"); err != nil {
		return err
	}
	if err := s.waitConnector(ctx, "FAILED", 60*time.Second); err != nil {
		return fmt.Errorf("devctl: task 0 did not fail: %w", err)
	}
	s.log.Info("broken: connector task 0 FAILED after the amount type change")
	return nil
}

// The fix: allow the new schema version, restart the task, then restore the
// compatibility rule so the next breaking change is caught again.
func fixSchemaChange(ctx context.Context, s *stack) error {
	if err := s.setCompatibility(ctx, valueSubject, "NONE"); err != nil {
		return err
	}
	// Count the registered versions before restarting, so we can tell when the
	// task has registered the new one.
	before, err := s.schemaVersions(ctx, valueSubject)
	if err != nil {
		return err
	}
	if err := s.restartTask(ctx, 0); err != nil {
		return err
	}
	if err := s.waitConnector(ctx, "RUNNING", 60*time.Second); err != nil {
		return err
	}
	// Wait until the new schema version is registered before restoring BACKWARD;
	// otherwise the task fails again on the next record.
	if err := s.waitFor(ctx, 60*time.Second, func() (bool, error) {
		vs, err := s.schemaVersions(ctx, valueSubject)
		if err != nil {
			return false, nil
		}
		return len(vs) > len(before), nil
	}); err != nil {
		return fmt.Errorf("devctl: new schema version was not registered: %w", err)
	}
	if err := s.setCompatibility(ctx, valueSubject, "BACKWARD"); err != nil {
		return err
	}
	s.log.Info("fixed: new schema registered, task 0 RUNNING, compatibility back to BACKWARD")
	return nil
}

// connector-down: the connector is paused, so the replication slot goes
// inactive and Postgres keeps WAL for it.
func breakConnectorDown(ctx context.Context, s *stack) error {
	_, err := s.do(ctx, "PUT", s.cfg.ConnectURL+"/connectors/"+connectorName+"/pause", nil, 202, 200, 204)
	if err != nil {
		return err
	}
	s.log.Info("broken: connector paused — run `devctl load` to grow retained WAL")
	return nil
}

func fixConnectorDown(ctx context.Context, s *stack) error {
	_, err := s.do(ctx, "PUT", s.cfg.ConnectURL+"/connectors/"+connectorName+"/resume", nil, 202, 200, 204)
	if err != nil {
		return err
	}
	if err := s.waitConnector(ctx, "RUNNING", 60*time.Second); err != nil {
		return err
	}
	s.log.Info("fixed: connector resumed")
	return nil
}

// consumer-stuck: devconsumer restarts with a poison-pill order id, then that
// order is inserted. The consumer crashes on it again and again; lag grows.
func breakConsumerStuck(ctx context.Context, s *stack) error {
	next, err := s.psql(ctx, "orders", "SELECT last_value + 1 FROM orders_id_seq")
	if err != nil {
		return err
	}
	if _, err := strconv.ParseInt(next, 10, 64); err != nil {
		return fmt.Errorf("devctl: next order id %q: %w", next, err)
	}
	if err := s.compose(ctx, map[string]string{"DEVCONSUMER_FAIL_ON_ID": next}, "up", "-d", "devconsumer"); err != nil {
		return err
	}
	// Wait until the poison-pill consumer rejoined the group, so the inserted
	// row actually reaches it and it starts crash-looping on it.
	if err := s.waitGroup(ctx, "order-sync", "Stable", 1, 60*time.Second); err != nil {
		return fmt.Errorf("devctl: consumer did not rejoin the group: %w", err)
	}
	if _, err := s.psql(ctx, "orders", "INSERT INTO orders (customer_id, amount) VALUES (7, 500)"); err != nil {
		return err
	}
	s.log.Info("broken: devconsumer crashes on order " + next + " — run `devctl load` to grow lag")
	return nil
}

func fixConsumerStuck(ctx context.Context, s *stack) error {
	if err := s.compose(ctx, map[string]string{"DEVCONSUMER_FAIL_ON_ID": "0"}, "up", "-d", "devconsumer"); err != nil {
		return err
	}
	s.log.Info("fixed: devconsumer restarted without the poison-pill flag")
	return nil
}

// long-transaction: a session opens a transaction, writes, and stays open.
// The slot cannot release WAL older than that transaction.
const longTxApp = "piperoom-long-tx"

func breakLongTransaction(ctx context.Context, s *stack) error {
	// docker exec -d keeps the session alive after devctl exits.
	err := runDetached(ctx, "docker", "exec", "-d", "-e", "PGAPPNAME="+longTxApp, pgContainer,
		"psql", "-U", "piperoom", "-d", "orders",
		"-c", "BEGIN",
		"-c", "UPDATE orders SET status = 'on_hold' WHERE id = (SELECT min(id) FROM orders)",
		"-c", "SELECT pg_sleep(600)",
		"-c", "COMMIT")
	if err != nil {
		return err
	}
	var lastErr error
	if err := s.waitFor(ctx, 15*time.Second, func() (bool, error) {
		n, err := s.psql(ctx, "orders", "SELECT count(*) FROM pg_stat_activity WHERE application_name = '"+longTxApp+"' AND xact_start IS NOT NULL")
		if err != nil {
			lastErr = err
			return false, nil
		}
		return n != "0", nil
	}); err != nil {
		if lastErr != nil {
			return fmt.Errorf("devctl: long transaction did not start: %w (last error: %v)", err, lastErr)
		}
		return fmt.Errorf("devctl: long transaction did not start: %w", err)
	}
	s.log.Info("broken: a transaction holds WAL for 10 min — run `devctl load` to grow retained WAL")
	return nil
}

func fixLongTransaction(ctx context.Context, s *stack) error {
	out, err := s.psql(ctx, "orders", "SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity WHERE application_name = '"+longTxApp+"'")
	if err != nil {
		return err
	}
	s.log.Info("fixed: terminated long transactions", "count", strings.TrimSpace(out))
	return nil
}
