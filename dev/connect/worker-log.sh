#!/bin/bash
# Starts the Kafka Connect worker (the arguments, e.g. the Debezium image's
# "/docker-entrypoint.sh start") and copies everything it prints into
# $WORKER_LOG_DIR/connect.log as well. dev/docker-compose.yml mounts that
# directory from dev/.logs/connect on the host, where CONNECT_WORKER_LOG_FILE and
# the agent's connect_worker_log tool read it (QA G3). Lines are copied as they
# are, so they keep the image's own log4j layout and timestamps; `docker logs`
# still shows the same output.
#
# Size cap: at start the previous run's log moves to connect.log.1; while the
# worker runs, the file moves to connect.log.1 again each time it passes
# WORKER_LOG_MAX_BYTES. The two files together stay under twice that.
#
# The copy never stops the worker: if the directory is not writable (a Linux host
# where the container user may not write into dev/.logs/connect; `devctl up`
# creates it writable), the worker starts with its output in `docker logs` only.
set -u

dir=${WORKER_LOG_DIR:-/piperoom/logs}
max=${WORKER_LOG_MAX_BYTES:-10485760}
log="$dir/connect.log"

if [ "$#" -eq 0 ]; then
  echo "worker-log: no worker command given" >&2
  exit 2
fi
case $max in
  '' | *[!0-9]*) echo "worker-log: WORKER_LOG_MAX_BYTES=$max is not a byte count; using 10485760" >&2; max=10485760 ;;
esac

if ! { mkdir -p "$dir" && touch "$log"; } 2>/dev/null; then
  echo "worker-log: cannot write $log; the worker log is in docker logs only" >&2
  exec "$@"
fi
if [ -s "$log" ]; then
  mv -f "$log" "$log.1" 2>/dev/null || : >"$log"
fi

# copy_log forwards every line to the container's stdout and appends it to the
# file, rotating by size. Byte counting runs in the C locale so ${#line} is bytes.
copy_log() {
  local LC_ALL=C line n=0
  exec 3>>"$log"
  while IFS= read -r line || [ -n "$line" ]; do
    printf '%s\n' "$line"
    printf '%s\n' "$line" >&3 2>/dev/null
    n=$((n + ${#line} + 1))
    if [ "$n" -ge "$max" ]; then
      exec 3>&-
      mv -f "$log" "$log.1" 2>/dev/null
      exec 3>>"$log"
      n=0
    fi
  done
}

# The worker replaces this shell (exec), so it stays PID 1 and gets docker's
# SIGTERM directly; copy_log runs as its child on the other end of the pipe.
exec > >(copy_log) 2>&1
exec "$@"
