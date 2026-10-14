#!/bin/bash
# Tests filter_pipeline_env from ../startup.sh against hostile pipeline-env input.
#   bash deploy/terraform/gcp/test/filter_test.sh
set -u

here=$(cd "$(dirname "$0")" && pwd)
src=$here/../startup.sh

# Load only the function, not the rest of the boot script.
fn=$(sed -n '/^# BEGIN filter_pipeline_env/,/^# END filter_pipeline_env/p' "$src")
[ -n "$fn" ] || { echo "FAIL: filter_pipeline_env markers not found in startup.sh"; exit 1; }
eval "$fn"

MANAGED="PIPEROOM_POSTGRES_PASSWORD PIPEROOM_AUTH_MODE PIPEROOM_DEV_ALLOW_NONLOOPBACK PIPEROOM_HTTP_ADDR PIPEROOM_DB_URL"
fail=0

# check <name> <input> <expected output>
check() {
  local got
  got=$(printf '%s\n' "$2" | filter_pipeline_env "$MANAGED" 2>/dev/null)
  if [ "$got" == "$3" ]; then
    echo "ok   $1"
  else
    echo "FAIL $1"
    echo "  input:    $(printf '%s' "$2" | tr '\n' '|')"
    echo "  expected: $(printf '%s' "$3" | tr '\n' '|')"
    echo "  got:      $(printf '%s' "$got" | tr '\n' '|')"
    fail=1
  fi
}

check "plain managed key dropped"          'PIPEROOM_AUTH_MODE=dev'               ''
check "export prefix dropped"              'export PIPEROOM_AUTH_MODE=dev'        ''
check "export, many spaces dropped"        'export    PIPEROOM_AUTH_MODE=dev'     ''
check "export with tab dropped"            $'export\tPIPEROOM_AUTH_MODE=dev'      ''
check "leading blanks dropped"             '   PIPEROOM_AUTH_MODE=dev'            ''
check "leading blanks + export dropped"    $'\t export PIPEROOM_AUTH_MODE=dev'     ''
check "space before = dropped"             'PIPEROOM_AUTH_MODE =dev'              ''
check "dev nonloopback dropped"            'PIPEROOM_DEV_ALLOW_NONLOOPBACK=1'     ''
check "http addr dropped"                  'export PIPEROOM_HTTP_ADDR=127.0.0.1:1' ''
check "db url dropped"                     'PIPEROOM_DB_URL=postgres://x'         ''
check "postgres password dropped"          'PIPEROOM_POSTGRES_PASSWORD=x'         ''
check "quoted value, managed key dropped"  "PIPEROOM_AUTH_MODE='dev'"             ''
check "unrelated key kept"                 'SOURCE_DB_URL=postgres://ro@h/db'     'SOURCE_DB_URL=postgres://ro@h/db'
check "export of unrelated key kept"       'export KAFKA_BROKERS=a:9092'          'export KAFKA_BROKERS=a:9092'
check "prefix-lookalike key kept"          'PIPEROOM_AUTH_MODE_X=1'               'PIPEROOM_AUTH_MODE_X=1'
check "comment kept"                       '# PIPEROOM_AUTH_MODE=dev'             '# PIPEROOM_AUTH_MODE=dev'
check "closed quotes kept"                 'A="x y"'                              'A="x y"'
check "unclosed single quote dropped"      "FOO='abc"                             ''
check "unclosed double quote dropped"      'FOO="abc'                             ''
check "lone quote dropped"                 "FOO='"                                ''
check "mixed input keeps only safe lines"  $'A=1\nexport PIPEROOM_AUTH_MODE=dev\nB=2\nPIPEROOM_HTTP_ADDR=x\n# c' $'A=1\nB=2\n# c'

[ "$fail" -eq 0 ] && echo "all passed" || { echo "FAILED"; exit 1; }
