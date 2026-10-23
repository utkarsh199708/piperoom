# Sourced by the gvenzl entrypoint (set -Eeuo pipefail, user oracle) on the
# container's first start, after the database is open. The work runs in its own
# bash so its `exit` cannot end the entrypoint; a failure still aborts the
# start (dev/oracle/cdc-setup.sh, dev/oracle/README.md).
bash /opt/piperoom/oracle/cdc-setup.sh
