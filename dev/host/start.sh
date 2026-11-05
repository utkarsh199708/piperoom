#!/bin/bash
# The fake host's start (dev/host/Dockerfile): it lays the host project down
# from the baked base at every start and then idles. Nothing here runs systemd
# or a Connect worker; the U8c checks read and rewrite the project files
# (cmd/devctl/setupbreaks_host.go).
#
# Copying on start is what makes every break reversible by a container
# restart: `docker compose --profile host restart host` restores all three.
set -euo pipefail

mkdir -p /piperoom-host
rm -rf /piperoom-host/project
cp -a /opt/piperoom/host-base /piperoom-host/project

exec sleep infinity
