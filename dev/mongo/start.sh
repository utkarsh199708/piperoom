#!/usr/bin/env bash
# Starts the dev MongoDB (compose profile "mongo", dev/mongo/README.md).
#
# Auth plus --replSet needs a keyFile (internal member authentication), and
# mongod refuses a keyFile that is group- or world-readable or not owned by the
# user it runs as. A file bind-mounted from a Windows checkout cannot carry
# mode 0400 or the mongodb owner, so the key is generated here, inside the
# container, on first start: random bytes from /dev/urandom, base64 (the only
# alphabet mongod accepts), 756 bytes -> 1008 characters with no padding. It
# lives in the container's own filesystem, not the data volume: a one-member
# set never has to agree on it with another member, so a recreated container
# may get a new one. It is a dev-only key that nothing else reuses.
#
# The arguments are passed on to mongod: the compose service adds
# `--replSet rs0`; `devctl setup-break standalone-mongod` recreates the
# container without it (cmd/devctl/setupbreaks_mongo.go).
set -euo pipefail

keyfile=/tmp/piperoom-mongo-keyfile
if [ ! -s "$keyfile" ]; then
	umask 077
	head -c 756 /dev/urandom | base64 -w0 >"$keyfile"
fi
chown mongodb:mongodb "$keyfile"
chmod 400 "$keyfile"

# docker-entrypoint.sh runs /docker-entrypoint-initdb.d on an empty data
# directory (it strips --replSet and --keyFile for that first start), then
# drops to the mongodb user and execs mongod with these arguments. The
# WiredTiger cache is capped: by default it takes half the host's memory, too
# much for a laptop that also runs Kafka and Connect.
exec docker-entrypoint.sh mongod --bind_ip_all --wiredTigerCacheSizeGB 0.25 --keyFile "$keyfile" "$@"
