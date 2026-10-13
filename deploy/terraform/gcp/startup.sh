#!/bin/bash
# Piperoom pilot VM start-up script (Debian 12). Runs as root on EVERY boot
# (GCE start-up scripts do), so it is written to be run again safely:
#   sudo google_metadata_script_runner startup
# re-reads Secret Manager and restarts the stack, which is how a rotated secret
# is picked up.
#
# Inputs are instance metadata attributes set by vm.tf. Secret VALUES are never
# in metadata: only their Secret Manager ids are. Values are fetched here with
# the VM's service account and written to /opt/piperoom/.env (mode 0600, root).

set -euo pipefail

APP_DIR=/opt/piperoom
MD=http://metadata.google.internal/computeMetadata/v1

log() { echo "piperoom-startup: $*"; }
die() { log "ERROR: $*"; exit 1; }

md() { # md <path>: instance attribute or empty
  curl -fsS -H 'Metadata-Flavor: Google' "$MD/instance/attributes/$1" 2>/dev/null || true
}

export DEBIAN_FRONTEND=noninteractive

# ---- Packages ---------------------------------------------------------------

if ! command -v docker >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1 || ! command -v jq >/dev/null 2>&1; then
  log "installing Docker Engine, Compose plugin, jq, psql client"
  apt-get update -y
  apt-get install -y ca-certificates curl gnupg jq postgresql-client
  install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/debian/gpg -o /etc/apt/keyrings/docker.asc
  chmod a+r /etc/apt/keyrings/docker.asc
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian bookworm stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update -y
  apt-get install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin
fi

# Container logs go to Cloud Logging (needs roles/logging.logWriter). Docker 20.10+
# keeps a local copy, so "docker logs" still works.
DAEMON_JSON='{"log-driver":"gcplogs","log-opts":{"mode":"non-blocking","max-buffer-size":"4m"}}'
if [ "$(cat /etc/docker/daemon.json 2>/dev/null || true)" != "$DAEMON_JSON" ]; then
  echo "$DAEMON_JSON" > /etc/docker/daemon.json
  systemctl restart docker
fi
systemctl enable --now docker

if [ "$(md piperoom-ops-agent)" = "true" ] && ! dpkg -s google-cloud-ops-agent >/dev/null 2>&1; then
  log "installing the Ops Agent"
  curl -fsSL -o /tmp/add-google-cloud-ops-agent-repo.sh https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh
  bash /tmp/add-google-cloud-ops-agent-repo.sh --also-install
  rm -f /tmp/add-google-cloud-ops-agent-repo.sh
fi

# ---- Secret Manager (REST, no gcloud needed) ---------------------------------

PROJECT=$(curl -fsS -H 'Metadata-Flavor: Google' "$MD/project/project-id")

# access_token: the VM service account's token. It goes to curl through a config
# on stdin so it never appears in the process list.
access_token() {
  curl -fsS -H 'Metadata-Flavor: Google' "$MD/instance/service-accounts/default/token" | jq -r .access_token
}

# secret_to <secret-id> <outfile>: write the latest version's payload to a file.
secret_to() {
  local id=$1 out=$2 tok
  tok=$(access_token)
  printf 'header = "Authorization: Bearer %s"\n' "$tok" |
    curl -fsS -K - "https://secretmanager.googleapis.com/v1/projects/$PROJECT/secrets/$id/versions/latest:access" |
    jq -er .payload.data | base64 -d > "$out" ||
    die "cannot read the latest version of secret '$id'. Add a value (README step 5), then re-run: sudo google_metadata_script_runner startup"
}

umask 077
install -d -m 0755 "$APP_DIR" "$APP_DIR/config" "$APP_DIR/certs"
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

# ---- Compose file and image override ----------------------------------------

md piperoom-compose-b64 | base64 -d > "$APP_DIR/docker-compose.pilot.yml"
chmod 0644 "$APP_DIR/docker-compose.pilot.yml"

IMAGE=$(md piperoom-image)
[ -n "$IMAGE" ] || die "metadata piperoom-image is empty"
# The base file builds from source; a VM has no source tree, so use the image.
# "!reset" needs Docker Compose 2.24 or newer (the Docker apt repo ships it).
cat > "$APP_DIR/docker-compose.override.yml" <<YAML
services:
  piperoom:
    build: !reset null
    image: $IMAGE
    pull_policy: always
YAML
chmod 0644 "$APP_DIR/docker-compose.override.yml"

# ---- Config files -------------------------------------------------------------

secret_to "$(md piperoom-secret-owners)" "$WORK/owners.yaml"
secret_to "$(md piperoom-secret-users)" "$WORK/users.yaml"
# The container runs as uid 65532 and the mount is read-only.
install -m 0440 -o 65532 -g 65532 "$WORK/owners.yaml" "$APP_DIR/config/owners.yaml"
install -m 0440 -o 65532 -g 65532 "$WORK/users.yaml" "$APP_DIR/config/users.yaml"

# ---- Env file -------------------------------------------------------------------

ENVTMP="$WORK/env"
: > "$ENVTMP"
MANAGED_KEYS="PIPEROOM_POSTGRES_PASSWORD"

# quote_val: single-quote a value so $ # spaces stay literal in the compose env
# file; a value with a quote or newline cannot be written safely.
quote_val() {
  case $1 in
    *"'"*) return 1 ;;
  esac
  [ "$(printf '%s' "$1" | wc -l)" -eq 0 ] || return 1
  printf "'%s'" "$1"
}

# Non-secret settings from Terraform.
while IFS= read -r line; do
  [ -n "$line" ] || continue
  k=${line%%=*}
  v=${line#*=}
  q=$(quote_val "$v") || die "setting $k has a quote or newline in its value"
  printf '%s=%s\n' "$k" "$q" >> "$ENVTMP"
  MANAGED_KEYS="$MANAGED_KEYS $k"
done < <(md piperoom-env)

# Secrets that are one variable each.
while IFS= read -r line; do
  [ -n "$line" ] || continue
  k=${line%%=*}
  id=${line#*=}
  secret_to "$id" "$WORK/secret.val"
  v=$(cat "$WORK/secret.val")
  q=$(quote_val "$v") || die "secret '$id' has a quote or newline; use a value without them"
  printf '%s=%s\n' "$k" "$q" >> "$ENVTMP"
  MANAGED_KEYS="$MANAGED_KEYS $k"
done < <(md piperoom-secret-vars)

# The compose file insists on a password for its bundled Postgres. That service
# is not started here (Cloud SQL is used), so a throw-away value is enough.
printf "PIPEROOM_POSTGRES_PASSWORD='%s'\n" "$(head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n')" >> "$ENVTMP"

# Pipeline settings (SOURCE_DB_URL, KAFKA_*, ...). Lines that would override a
# setting managed above are dropped, so this secret cannot switch the auth mode.
secret_to "$(md piperoom-secret-envfile)" "$WORK/pipeline.env"
awk -v managed="$MANAGED_KEYS" '
  BEGIN { n = split(managed, a, " "); for (i = 1; i <= n; i++) skip[a[i]] = 1 }
  /^[[:space:]]*(#|$)/ { print; next }
  { key = $0; sub(/=.*/, "", key); gsub(/[[:space:]]/, "", key)
    if (key in skip) { print "piperoom-startup: ignoring managed setting " key " in pipeline-env" > "/dev/stderr"; next }
    print }
' "$WORK/pipeline.env" >> "$ENVTMP"

install -m 0600 -o root -g root "$ENVTMP" "$APP_DIR/.env"

# ---- Registry login (Artifact Registry / GCR) ------------------------------------

REG_HOST=${IMAGE%%/*}
LOGGED_IN=""
case $REG_HOST in
  *-docker.pkg.dev | docker.pkg.dev | gcr.io | *.gcr.io)
    access_token | docker login -u oauth2accesstoken --password-stdin "https://$REG_HOST" >/dev/null
    LOGGED_IN=$REG_HOST
    ;;
esac

# ---- Run ----------------------------------------------------------------------------

cd "$APP_DIR"
# --no-deps: do not start the bundled postgres service.
docker compose -f docker-compose.pilot.yml -f docker-compose.override.yml up -d --no-deps --pull always piperoom

[ -z "$LOGGED_IN" ] || docker logout "$REG_HOST" >/dev/null 2>&1 || true

log "done. Check: docker compose -f $APP_DIR/docker-compose.pilot.yml -f $APP_DIR/docker-compose.override.yml ps"
