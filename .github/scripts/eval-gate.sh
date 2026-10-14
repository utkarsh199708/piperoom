#!/usr/bin/env bash
# Hard checks over an evalrun JSON report (evalrun -out report.json). These are
# the checks that are not statistical; regressions against the last nightly
# report are judged separately by `evalrun -compare ... -fail-on-regression`.
#
#   eval-gate.sh report.json
#
# Fails when the report has no runs, when any run errored (API failure, timeout
# or harness fault: the report is then not evidence of anything), or when the
# seed scenario `schema-change` did not pass every run. If EVAL_MIN_PASS_RATE is
# set (for example 0.7) the overall pass rate must also reach it; it is unset by
# default because no committed baseline makes any absolute floor a guess.
set -euo pipefail

report="${1:?usage: eval-gate.sh report.json}"
[ -s "$report" ] || { echo "::error::$report is missing or empty"; exit 1; }

rate="$(jq -r '.overall.pass_rate' "$report")"
errors="$(jq -r '.overall.errors // 0' "$report")"
runs="$(jq -r '.overall.runs' "$report")"
seed="$(jq -r '[.scenarios[] | select(.name == "schema-change")] | if length == 0 then "absent" elif .[0].passes == .[0].runs then "ok" else "failed" end' "$report")"
echo "runs=$runs pass_rate=$rate errors=$errors schema-change=$seed"

fail=0
[ "$runs" -gt 0 ] || { echo "::error::the report has no runs"; fail=1; }
[ "$errors" -eq 0 ] || { echo "::error::$errors run(s) errored (API, timeout or harness failure)"; fail=1; }
# A run that does not include the seed scenario (a custom -only list) is not
# held to it; the nightly and the pull-request subset both include it.
[ "$seed" != failed ] || { echo "::error::seed scenario schema-change did not pass every run"; fail=1; }
if [ -n "${EVAL_MIN_PASS_RATE:-}" ]; then
  jq -e --argjson min "$EVAL_MIN_PASS_RATE" '.overall.pass_rate >= $min' "$report" >/dev/null \
    || { echo "::error::pass rate $rate is below EVAL_MIN_PASS_RATE=$EVAL_MIN_PASS_RATE"; fail=1; }
fi
exit "$fail"
