#!/usr/bin/env bash
# Pass/fail gate over an evalrun JSON report (evalrun -out report.json).
#
#   eval-gate.sh report.json
#
# Fails when the overall pass rate is below EVAL_MIN_PASS_RATE (default 0.70),
# when any run errored, or when the seed scenario `schema-change` did not pass
# every run. There is no committed baseline to compare against yet, so this is
# an absolute floor; raise EVAL_MIN_PASS_RATE as the pass rate allows.
set -euo pipefail

report="${1:?usage: eval-gate.sh report.json}"
min="${EVAL_MIN_PASS_RATE:-0.70}"
[ -s "$report" ] || { echo "::error::$report is missing or empty"; exit 1; }

rate="$(jq -r '.overall.pass_rate' "$report")"
errors="$(jq -r '.overall.errors // 0' "$report")"
runs="$(jq -r '.overall.runs' "$report")"
seed="$(jq -r '[.scenarios[] | select(.name == "schema-change")] | if length == 0 then "missing" elif .[0].passes == .[0].runs then "ok" else "failed" end' "$report")"
echo "runs=$runs pass_rate=$rate (min $min) errors=$errors schema-change=$seed"

fail=0
[ "$runs" -gt 0 ] || { echo "::error::the report has no runs"; fail=1; }
jq -e --argjson min "$min" '.overall.pass_rate >= $min' "$report" >/dev/null \
  || { echo "::error::pass rate $rate is below $min"; fail=1; }
[ "$errors" -eq 0 ] || { echo "::error::$errors run(s) errored (API, timeout or harness failure)"; fail=1; }
[ "$seed" = ok ] || { echo "::error::seed scenario schema-change: $seed"; fail=1; }
exit "$fail"
