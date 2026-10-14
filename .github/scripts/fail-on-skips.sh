#!/usr/bin/env bash
# Reads `go test -json` output and fails when any test was skipped. In the
# full-stack job every integration test must really run: a skip there means
# the stack was not reachable (testenv.Require skips instead of failing), and
# a green job would then prove nothing.
#
#   fail-on-skips.sh test.json
#
# Only test-level events count; a package with no test files reports a
# package-level skip, which is not a skipped test.
set -euo pipefail

file="${1:?usage: fail-on-skips.sh test.json}"
[ -s "$file" ] || { echo "::error::$file is missing or empty: no tests ran"; exit 1; }

skipped="$(jq -r 'select(.Action == "skip" and (.Test // "") != "") | "\(.Package) \(.Test)"' "$file" | sort -u)"
ran="$(jq -r 'select(.Action == "pass" and (.Test // "") != "") | .Test' "$file" | wc -l)"

if [ -n "$skipped" ]; then
  echo "::error::integration tests were skipped (the stack must make every one run):"
  echo "$skipped"
  exit 1
fi
if [ "$ran" -eq 0 ]; then
  echo "::error::no integration test passed: nothing ran"
  exit 1
fi
echo "integration tests: $ran passed, 0 skipped"
