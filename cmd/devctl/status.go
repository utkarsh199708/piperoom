package main

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"os/exec"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// status prints one screen of pipeline health for humans.
func (s *stack) status(ctx context.Context, w io.Writer) error {
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	if st, err := s.connectorStatus(ctx); err != nil {
		fmt.Fprintf(w, "connector   %s: unreachable (%v)\n", connectorName, err)
	} else {
		var tasks []string
		for _, t := range st.Tasks {
			tasks = append(tasks, fmt.Sprintf("%d=%s", t.ID, t.State))
		}
		fmt.Fprintf(w, "connector   %s: %s, tasks %s\n", connectorName, st.Connector.State, strings.Join(tasks, " "))
	}

	if out, err := s.psql(ctx, "orders", "SELECT slot_name || ' active=' || active || ' retained=' || pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) FROM pg_replication_slots"); err != nil {
		fmt.Fprintf(w, "slot        unreachable (%v)\n", err)
	} else {
		fmt.Fprintf(w, "slot        %s\n", out)
	}

	if lag, err := s.groupLag(ctx, "order-sync"); err != nil {
		fmt.Fprintf(w, "consumer    order-sync: unreachable (%v)\n", err)
	} else {
		fmt.Fprintf(w, "consumer    %s\n", lag)
	}

	if out, err := s.psql(ctx, "warehouse", "SELECT count(*) || ' rows, newest updated_at ' || coalesce(to_char(max(updated_at), 'HH24:MI:SS'), '-') || ', stale ' || coalesce(extract(epoch FROM now() - max(updated_at))::int, 0) || ' s' FROM analytics.orders"); err != nil {
		fmt.Fprintf(w, "warehouse   unreachable (%v)\n", err)
	} else {
		fmt.Fprintf(w, "warehouse   analytics.orders: %s\n", out)
	}
	return nil
}

func (s *stack) groupLag(ctx context.Context, group string) (string, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(s.cfg.KafkaBrokers, ",")...))
	if err != nil {
		return "", fmt.Errorf("devctl: kafka client: %w", err)
	}
	defer cl.Close()
	lags, err := kadm.NewClient(cl).Lag(ctx, group)
	if err != nil {
		return "", fmt.Errorf("devctl: lag: %w", err)
	}
	l, ok := lags[group]
	if !ok {
		return "", fmt.Errorf("devctl: group %s not found", group)
	}
	if l.Error() != nil {
		return "", fmt.Errorf("devctl: group %s: %w", group, l.Error())
	}
	return fmt.Sprintf("%s: state %s, members %d, total lag %d", group, l.State, len(l.Members), l.Lag.Total()), nil
}

// load inserts and updates orders at the given rate until ctx ends.
func (s *stack) load(ctx context.Context, rate int) error {
	if rate < 1 {
		rate = 1
	}
	conn, err := s.sourceConn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	t := time.NewTicker(time.Second / time.Duration(rate))
	defer t.Stop()
	s.log.Info("load running — Ctrl+C to stop", "rate_per_s", rate)
	var n int
	for {
		select {
		case <-ctx.Done():
			s.log.Info("load stopped", "changes", n)
			return nil
		case <-t.C:
		}
		var q string
		if rand.IntN(3) == 0 {
			q = "UPDATE orders SET status = 'shipped', updated_at = now() WHERE id = (SELECT id FROM orders WHERE status = 'placed' ORDER BY random() LIMIT 1)"
		} else {
			q = fmt.Sprintf("INSERT INTO orders (customer_id, amount) VALUES (%d, %d)", rand.IntN(1000), 1+rand.IntN(5000))
		}
		qctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		_, err := conn.Exec(qctx, q)
		cancel()
		if err != nil && ctx.Err() == nil {
			return fmt.Errorf("devctl: load: %w", err)
		}
		n++
	}
}

func runDetached(ctx context.Context, name string, args ...string) error {
	out, err := exec.CommandContext(ctx, name, args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("devctl: %s: %w: %s", name, err, strings.TrimSpace(string(out)))
	}
	return nil
}
