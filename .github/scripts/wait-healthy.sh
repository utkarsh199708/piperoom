#!/usr/bin/env bash
# Runs `devctl health` until the pipeline reports healthy, for at most
# timeout seconds (default 180), then fails with the last report. After
# `devctl up` or `devctl fix` the consumer group and the newest warehouse rows
# arrive a little later than the connector state, so one immediate check is
# too strict and an unbounded wait would hide a real failure.
set -uo pipefail
cd "$(dirname "$0")/../.."

timeout="${1:-180}"
bin="${RUNNER_TEMP:-/tmp}/devctl"
go build -o "$bin" ./cmd/devctl || exit 1

deadline=$((SECONDS + timeout))
until "$bin" health; do
  if ((SECONDS >= deadline)); then
    echo "::error::pipeline still unhealthy after ${timeout}s"
    # Facts for the next look: where the slot is and what holds the WAL back.
    pg=(docker compose -f dev/docker-compose.yml exec -T postgres psql -U piperoom -d orders -x -c)
    echo "::group::replication slots"
    "${pg[@]}" "SELECT slot_name, active, restart_lsn, confirmed_flush_lsn, pg_current_wal_lsn(), pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)) AS retained FROM pg_replication_slots" || true
    echo "::endgroup::"
    echo "::group::open transactions"
    "${pg[@]}" "SELECT pid, datname, state, now() - xact_start AS open_for, left(query, 120) AS query FROM pg_stat_activity WHERE xact_start IS NOT NULL ORDER BY xact_start" || true
    echo "::endgroup::"
    exit 1
  fi
  sleep 10
done
