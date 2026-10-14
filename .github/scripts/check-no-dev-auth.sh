#!/usr/bin/env bash
# Fails when the release image or the pilot compose file turns dev auth on.
# Dev login has no password, so it must only come from the explicit override
# deploy/docker-compose.dev-auth.yml.
set -euo pipefail
cd "$(dirname "$0")/../.."

fail=0
# 1. The Dockerfile must not set dev auth. Comments are ignored.
if grep -vE '^\s*#' Dockerfile | grep -E 'PIPEROOM_DEV_ALLOW_NONLOOPBACK|PIPEROOM_AUTH_MODE=dev|--allow-remote'; then
  echo "::error file=Dockerfile::the image must not bake in dev auth"
  fail=1
fi
if ! grep -vE '^\s*#' Dockerfile | grep -qE 'PIPEROOM_AUTH_MODE=oidc'; then
  echo "::error file=Dockerfile::the image must default PIPEROOM_AUTH_MODE to oidc"
  fail=1
fi
if grep -vE '^\s*#' Dockerfile | grep -E 'COPY[^#]*users\.yaml|COPY[^#]*dev/'; then
  echo "::error file=Dockerfile::the image must not contain a users file or dev/ files"
  fail=1
fi

# 2. The pilot compose file, resolved with no auth variables set, must be oidc.
export PIPEROOM_POSTGRES_PASSWORD=ci-only SCHEMA_REGISTRY_URL=off
unset PIPEROOM_AUTH_MODE PIPEROOM_DEV_ALLOW_NONLOOPBACK
resolved="$(docker compose -f deploy/docker-compose.pilot.yml config)"
echo "$resolved" | grep -qE 'PIPEROOM_AUTH_MODE: oidc$' \
  || { echo "::error file=deploy/docker-compose.pilot.yml::default auth mode is not oidc"; fail=1; }
echo "$resolved" | grep -qE 'PIPEROOM_DEV_ALLOW_NONLOOPBACK: ""$' \
  || { echo "::error file=deploy/docker-compose.pilot.yml::PIPEROOM_DEV_ALLOW_NONLOOPBACK must default to empty"; fail=1; }

# 3. The override is the only place dev auth turns on, and it must still work.
docker compose -f deploy/docker-compose.pilot.yml -f deploy/docker-compose.dev-auth.yml config \
  | grep -qE 'PIPEROOM_AUTH_MODE: dev$' \
  || { echo "::error file=deploy/docker-compose.dev-auth.yml::override does not select dev mode"; fail=1; }

exit "$fail"
