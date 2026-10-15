#!/usr/bin/env bash
# Reads `go test -json` output and fails when any test was skipped. In the
# full-stack job every integration test must really run: a skip there means
# the stack was not reachable (testenv.Require skips instead of failing), and
# a green job would then prove nothing.
#
#   fail-on-skips.sh test.json [allowlist]
#
# allowlist (default .github/integration-skip-allow.txt) holds reviewed skips
# that are not stack skips: "<package> <Test> | reason | owner | review-by".
# An entry past its review-by date fails the job.
#
# Only test-level events count; a package with no test files reports a
# package-level skip, which is not a skipped test.
set -euo pipefail

file="${1:?usage: fail-on-skips.sh test.json [allowlist]}"
allow="${2:-$(dirname "$0")/../integration-skip-allow.txt}"
[ -s "$file" ] || { echo "::error::$file is missing or empty: no tests ran"; exit 1; }

skipped="$(jq -r 'select(.Action == "skip" and (.Test // "") != "") | "\(.Package) \(.Test)"' "$file" | sort -u)"
ran="$(jq -r 'select(.Action == "pass" and (.Test // "") != "") | .Test' "$file" | wc -l)"

allowed=""
if [ -f "$allow" ]; then
  today="$(date -u +%F)"
  while IFS='|' read -r key _reason _owner review; do
    key="$(echo "$key" | xargs)"; review="$(echo "$review" | xargs)"
    case "$key" in ''|'#'*) continue ;; esac
    if [[ ! "$review" =~ ^[0-9]{4}-[0-9]{2}-[0-9]{2}$ ]] || [[ "$review" < "$today" ]]; then
      echo "::error file=$allow::allowlist entry '$key' is past its review-by date ('$review')"
      exit 1
    fi
    allowed+="$key"$'\n'
  done < "$allow"
fi

unexpected="$(comm -23 <(echo "$skipped" | sed '/^$/d' | sort -u) <(echo "$allowed" | sed '/^$/d' | sort -u))"
if [ -n "$unexpected" ]; then
  echo "::error::integration tests were skipped (the stack must make every one run):"
  echo "$unexpected"
  exit 1
fi
if [ "$ran" -eq 0 ]; then
  echo "::error::no integration test passed: nothing ran"
  exit 1
fi
echo "integration tests: $ran passed, $(echo "$skipped" | sed '/^$/d' | wc -l) skipped (all on the reviewed allowlist)"
