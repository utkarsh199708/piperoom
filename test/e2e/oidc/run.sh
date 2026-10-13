#!/usr/bin/env bash
# OIDC end-to-end test: certificates, stack up (waiting for health), the Go
# test with -tags e2e, stack down -v. See README.md.
#
#   test/e2e/oidc/run.sh               # build the image, run, tear down
#   NO_BUILD=1 test/e2e/oidc/run.sh    # use an existing $PIPEROOM_E2E_IMAGE (default piperoom:e2e)
#   KEEP=1 test/e2e/oidc/run.sh        # leave the stack up afterwards (down it yourself)
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "$here/../../.." && pwd)"
compose=(docker compose -f "$here/docker-compose.yml")

cd "$root"
echo "== test certificates"
go run ./test/e2e/oidc/certgen -out "$here/.certs"

cleanup() {
	status=$?
	mkdir -p "$here/.logs"
	"${compose[@]}" logs --no-color >"$here/.logs/compose.log" 2>&1 || true
	if [[ "${KEEP:-}" == "1" ]]; then
		echo "== KEEP=1: stack left up; stop it with: ${compose[*]} down -v"
	else
		echo "== stack down"
		"${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
	fi
	exit $status
}
trap cleanup EXIT

build=(--build)
if [[ "${NO_BUILD:-}" == "1" ]]; then
	build=(--no-build)
fi

echo "== stack up (waits for the health checks)"
"${compose[@]}" up -d "${build[@]}" --wait --wait-timeout 300

echo "== go test -tags e2e"
cd "$here"
go test -tags e2e -count=1 -v ./...
