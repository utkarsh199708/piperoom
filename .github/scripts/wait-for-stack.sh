#!/usr/bin/env bash
# Waits until the dev stack answers, with a hard time limit. "docker compose
# up" returning (or a container being "started") does not mean Kafka Connect
# can serve its REST API: it starts last and takes the longest. Poll the API
# itself. On timeout, print container state and logs and exit 1.
#
#   wait-for-stack.sh [timeout-seconds]      (default 300)
#
# Reads CONNECT_URL (default http://localhost:8083) and SCHEMA_REGISTRY_URL
# (default http://localhost:8081) like internal/config.
set -uo pipefail
cd "$(dirname "$0")/../.."

timeout="${1:-300}"
connect="${CONNECT_URL:-http://localhost:8083}"
registry="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"
compose=(docker compose -f dev/docker-compose.yml)

dump() {
  echo "::group::docker compose ps"
  "${compose[@]}" ps -a
  echo "::endgroup::"
  echo "::group::docker compose logs (last 200 lines per service)"
  "${compose[@]}" logs --no-color --tail=200
  echo "::endgroup::"
}

ok() { curl -fsS --max-time 5 -o /dev/null "$1"; }

deadline=$((SECONDS + timeout))
for target in "connect|$connect/connectors" "schema-registry|$registry/subjects"; do
  name="${target%%|*}"
  url="${target#*|}"
  until ok "$url"; do
    if ((SECONDS >= deadline)); then
      echo "::error::$name did not answer $url within ${timeout}s"
      dump
      exit 1
    fi
    sleep 3
  done
  echo "$name is answering $url after $((SECONDS))s"
done
