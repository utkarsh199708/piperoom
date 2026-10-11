// Command devconsumer is the sample "order-sync" service of the dev pipeline:
// it reads Debezium change events for public.orders from Kafka and upserts
// them into warehouse.analytics.orders. It can be told to fail or slow down,
// so the failure scenarios in dev/scenarios.md have a realistic consumer.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"piperoom/internal/config"
)

type options struct {
	brokers   string
	registry  string
	warehouse string
	topic     string
	group     string
	failOnID  int64
	delay     time.Duration
}

func main() {
	// Read shared settings through the contract config.Load(); brokers,
	// registry and warehouse URL use the same env names as the rest of the repo.
	// TOPIC / GROUP / FAIL_ON_ID / DELAY are devconsumer-only knobs.
	cfg := config.Load()
	var o options
	flag.StringVar(&o.brokers, "brokers", cfg.KafkaBrokers, "Kafka brokers, comma separated")
	flag.StringVar(&o.registry, "registry", cfg.SchemaRegistryURL, "Schema Registry URL")
	flag.StringVar(&o.warehouse, "warehouse", cfg.WarehouseDBURL, "warehouse Postgres URL")
	flag.StringVar(&o.topic, "topic", env("TOPIC", "pg.public.orders"), "topic to consume")
	flag.StringVar(&o.group, "group", env("GROUP", "order-sync"), "consumer group")

	failOnRaw := env("FAIL_ON_ID", "0")
	failOn, err := strconv.ParseInt(failOnRaw, 10, 64)
	if err != nil {
		fmt.Fprintf(os.Stderr, "devconsumer: bad FAIL_ON_ID %q: %v\n", failOnRaw, err)
		os.Exit(2)
	}
	flag.Int64Var(&o.failOnID, "fail-on-id", failOn, "crash when this order id arrives (poison pill); 0 = off")

	delayRaw := env("DELAY", "0s")
	delay, err := time.ParseDuration(delayRaw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "devconsumer: bad DELAY %q: %v\n", delayRaw, err)
		os.Exit(2)
	}
	flag.DurationVar(&o.delay, "delay", delay, "sleep per record (to build lag)")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	// SIGTERM is what `docker stop` sends; handling it lets the client leave
	// the group cleanly instead of leaving a stale member behind.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, o, log); err != nil {
		log.Error("devconsumer stopped", "err", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, o options, log *slog.Logger) error {
	pool, err := pgxpool.New(ctx, o.warehouse)
	if err != nil {
		return fmt.Errorf("devconsumer: connect warehouse: %w", err)
	}
	defer pool.Close()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(o.brokers, ",")...),
		kgo.ConsumerGroup(o.group),
		kgo.ConsumeTopics(o.topic),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return fmt.Errorf("devconsumer: kafka client: %w", err)
	}
	defer cl.Close()

	schemas := newSchemaCache(o.registry)
	log.Info("devconsumer started", "topic", o.topic, "group", o.group, "fail_on_id", o.failOnID, "delay", o.delay)

	for {
		// PollRecords caps each batch, so a large --delay cannot make a single
		// poll hang for minutes or accumulate an unbounded batch in memory.
		fetches := cl.PollRecords(ctx, 100)
		if ctx.Err() != nil {
			return nil
		}
		for _, fe := range fetches.Errors() {
			log.Warn("fetch error", "topic", fe.Topic, "partition", fe.Partition, "err", fe.Err)
		}
		var procErr error
		fetches.EachRecord(func(r *kgo.Record) {
			if procErr != nil {
				return
			}
			procErr = handle(ctx, r, schemas, pool, o)
			if o.delay > 0 {
				// Wait in a ctx-aware way so SIGTERM stops a delayed batch.
				select {
				case <-ctx.Done():
				case <-time.After(o.delay):
				}
			}
		})
		if ctx.Err() != nil {
			// SIGTERM arrived mid-batch (or during a delay): shut down cleanly.
			return nil
		}
		if procErr != nil {
			// Crash without committing: the record is redelivered on restart,
			// which is how a real poison pill behaves.
			return procErr
		}
		if err := cl.CommitUncommittedOffsets(ctx); err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("devconsumer: commit offsets: %w", err)
		}
	}
}

func handle(ctx context.Context, r *kgo.Record, schemas *schemaCache, pool *pgxpool.Pool, o options) error {
	if r.Value == nil {
		return nil // tombstone after a delete
	}
	key, err := schemas.decode(ctx, r.Key)
	if err != nil {
		return fmt.Errorf("devconsumer: key at %s/%d@%d: %w", r.Topic, r.Partition, r.Offset, err)
	}
	val, err := schemas.decode(ctx, r.Value)
	if err != nil {
		return fmt.Errorf("devconsumer: value at %s/%d@%d: %w", r.Topic, r.Partition, r.Offset, err)
	}
	c, err := toChange(key, val)
	if err != nil {
		return fmt.Errorf("devconsumer: record %s/%d@%d: %w", r.Topic, r.Partition, r.Offset, err)
	}
	if o.failOnID != 0 && c.ID == o.failOnID {
		return fmt.Errorf("devconsumer: cannot process order %d at %s/%d@%d (poison pill)", c.ID, r.Topic, r.Partition, r.Offset)
	}
	return apply(ctx, pool, c)
}

func apply(ctx context.Context, pool *pgxpool.Pool, c change) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if c.After == nil {
		if _, err := pool.Exec(ctx, `DELETE FROM analytics.orders WHERE id = $1`, c.ID); err != nil {
			return fmt.Errorf("devconsumer: delete order %d: %w", c.ID, err)
		}
		return nil
	}
	var amount pgtype.Numeric
	if err := amount.Scan(c.After.Amount); err != nil {
		return fmt.Errorf("devconsumer: amount %q: %w", c.After.Amount, err)
	}
	var updated pgtype.Timestamptz
	if !c.After.UpdatedAt.IsZero() {
		updated = pgtype.Timestamptz{Time: c.After.UpdatedAt, Valid: true}
	}
	_, err := pool.Exec(ctx, `
		INSERT INTO analytics.orders (id, customer_id, amount, status, updated_at, loaded_at)
		VALUES ($1, $2, $3, $4, $5, now())
		ON CONFLICT (id) DO UPDATE SET
		  customer_id = EXCLUDED.customer_id,
		  amount      = EXCLUDED.amount,
		  status      = EXCLUDED.status,
		  updated_at  = EXCLUDED.updated_at,
		  loaded_at   = now()`,
		c.After.ID, c.After.CustomerID, amount, c.After.Status, updated)
	if err != nil {
		return fmt.Errorf("devconsumer: upsert order %d: %w", c.ID, err)
	}
	return nil
}

func env(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
