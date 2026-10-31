#!/usr/bin/env bash
# Renders the two setup releases (the pipeline one for a Postgres and a SQL
# Server source) and compares them with the golden files, then
# runs check.py on each. With -update, rewrites the golden files instead.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
chart="$here/../.."
render() {
  case "$1" in
    pipeline) helm template piperoom-pipeline-orders "$chart" -n data -f "$here/pipeline-values.yaml" ;;
    pipeline-sqlserver) helm template piperoom-pipeline-orders "$chart" -n data -f "$here/pipeline-values.yaml" -f "$here/pipeline-sqlserver-values.yaml" ;;
    runner) helm template piperoom-setup-01jbz3k7q8x9v2m4n6p8r0s2t4 "$chart" -n data -f "$here/runner-values.yaml" ;;
  esac
}
for r in pipeline pipeline-sqlserver runner; do
  if [ "${1:-}" = "-update" ]; then
    render "$r" > "$here/$r.golden.yaml"
  else
    diff -u "$here/$r.golden.yaml" <(render "$r")
  fi
  # The SQL Server render is an overlay that changes only the canary pin,
  # which check.py does not read: it is checked against the base values.
  render "$r" | python3 "$here/check.py" "${r%-sqlserver}" "$here/${r%-sqlserver}-values.yaml"
done
