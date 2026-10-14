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
    exit 1
  fi
  sleep 10
done
