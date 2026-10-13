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

# Exact package versions (variables.tf passes them as metadata; these defaults
# apply when the attribute is absent). apt resolves "name=version" and fails if
# that version is not in the repository, so nothing newer is installed silently.
DOCKER_VERSION=$(md piperoom-docker-version)
DOCKER_VERSION=${DOCKER_VERSION:-5:29.8.2-1~debian.12~bookworm}
CONTAINERD_VERSION=$(md piperoom-containerd-version)
CONTAINERD_VERSION=${CONTAINERD_VERSION:-2.3.6-1~debian.12~bookworm}
COMPOSE_PLUGIN_VERSION=$(md piperoom-compose-version)
COMPOSE_PLUGIN_VERSION=${COMPOSE_PLUGIN_VERSION:-5.5.1-1~debian.12~bookworm}
OPS_AGENT_REPO_SUFFIX=$(md piperoom-ops-agent-suffix)
OPS_AGENT_REPO_SUFFIX=${OPS_AGENT_REPO_SUFFIX:-2}
# Fingerprint of Docker's apt signing key (https://docs.docker.com/engine/install/debian/).
DOCKER_GPG_FPR="9DC858229FC7DD38854AE2D88D81803C0EBFCD88"
MIN_COMPOSE=2.24 # "!reset" in the override file needs this

# check_docker_key <file>: die unless the file holds exactly one key and its
# primary fingerprint is DOCKER_GPG_FPR (stops a swapped or hijacked key file).
check_docker_key() {
  local colons fprs npub
  colons=$(gpg --show-keys --with-colons "$1" 2>/dev/null) || die "cannot read the Docker apt key"
  npub=$(printf '%s\n' "$colons" | grep -c '^pub:' || true)
  [ "$npub" -eq 1 ] || die "the Docker apt key file holds $npub keys, expected 1"
  # The first fpr line after the pub line is the primary key's fingerprint.
  fprs=$(printf '%s\n' "$colons" | awk -F: '/^pub:/{p=1; next} p && /^fpr:/{print $10; exit}')
  [ "$fprs" = "$DOCKER_GPG_FPR" ] ||
    die "Docker apt key fingerprint is '$fprs', expected '$DOCKER_GPG_FPR' (9DC8 5822 9FC7 DD38 854A E2D8 8D81 803C 0EBF CD88)"
}

if ! command -v docker >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1 || ! command -v jq >/dev/null 2>&1; then
  log "installing Docker Engine $DOCKER_VERSION, Compose plugin $COMPOSE_PLUGIN_VERSION, jq, psql client"
  apt-get update -y
  apt-get install -y ca-certificates curl gnupg jq postgresql-client
  install -m 0755 -d /etc/apt/keyrings
  KEY_TMP=$(mktemp)
  curl -fsSL https://download.docker.com/linux/debian/gpg -o "$KEY_TMP"
  check_docker_key "$KEY_TMP"
  install -m 0644 "$KEY_TMP" /etc/apt/keyrings/docker.asc
  rm -f "$KEY_TMP"
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/debian bookworm stable" \
    > /etc/apt/sources.list.d/docker.list
  apt-get update -y
  apt-get install -y \
    "docker-ce=$DOCKER_VERSION" "docker-ce-cli=$DOCKER_VERSION" \
    "containerd.io=$CONTAINERD_VERSION" "docker-compose-plugin=$COMPOSE_PLUGIN_VERSION"
  # Keep unattended-upgrades and "apt upgrade" from moving the pinned packages.
  apt-mark hold docker-ce docker-ce-cli containerd.io docker-compose-plugin
fi

# Compose must be new enough for "!reset" (checked on every boot, installed or not).
COMPOSE_NOW=$(docker compose version --short 2>/dev/null || true)
COMPOSE_NOW=${COMPOSE_NOW#v}
[ -n "$COMPOSE_NOW" ] || die "docker compose is not available"
[ "$(printf '%s\n%s\n' "$MIN_COMPOSE" "$COMPOSE_NOW" | sort -V | head -n1)" = "$MIN_COMPOSE" ] ||
  die "docker compose $COMPOSE_NOW is older than $MIN_COMPOSE ('!reset' in the override file needs $MIN_COMPOSE+)"

# Container logs go to Cloud Logging (needs roles/logging.logWriter). Docker 20.10+
# keeps a local copy, so "docker logs" still works.
DAEMON_JSON='{"log-driver":"gcplogs","log-opts":{"mode":"non-blocking","max-buffer-size":"4m"}}'
if [ "$(cat /etc/docker/daemon.json 2>/dev/null || true)" != "$DAEMON_JSON" ]; then
  echo "$DAEMON_JSON" > /etc/docker/daemon.json
  systemctl restart docker
fi
systemctl enable --now docker

if [ "$(md piperoom-ops-agent)" = "true" ] && ! dpkg -s google-cloud-ops-agent >/dev/null 2>&1; then
  log "installing the Ops Agent from the major-version-$OPS_AGENT_REPO_SUFFIX repository"
  curl -fsSL -o /tmp/add-google-cloud-ops-agent-repo.sh https://dl.google.com/cloudagents/add-google-cloud-ops-agent-repo.sh
  # REPO_SUFFIX selects the repository google-cloud-ops-agent-bookworm-<suffix>,
  # which only holds that major version, so a new major release is not pulled in.
  REPO_SUFFIX="$OPS_AGENT_REPO_SUFFIX" bash /tmp/add-google-cloud-ops-agent-repo.sh --also-install --version="$OPS_AGENT_REPO_SUFFIX.*.*"
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

COMPOSE_B64=$(md piperoom-compose-b64)
[ -n "$COMPOSE_B64" ] || die "metadata piperoom-compose-b64 is empty"
printf '%s' "$COMPOSE_B64" | base64 -d > "$APP_DIR/docker-compose.pilot.yml"
[ -s "$APP_DIR/docker-compose.pilot.yml" ] || die "decoded compose file is empty"
chmod 0644 "$APP_DIR/docker-compose.pilot.yml"

IMAGE=$(md piperoom-image)
[ -n "$IMAGE" ] || die "metadata piperoom-image is empty"
# Only an immutable digest is run: a tag can be re-pushed and would change what
# the VM runs on the next boot. (variables.tf checks this too.)
printf '%s' "$IMAGE" | grep -Eq '^[a-z0-9][a-zA-Z0-9._/:-]*@sha256:[0-9a-f]{64}$' ||
  die "piperoom-image '$IMAGE' is not pinned by digest (<path>@sha256:<64 hex>)"
# The base file builds from source; a VM has no source tree, so use the image.
# "!reset" needs Docker Compose $MIN_COMPOSE or newer (checked above).
# pull_policy missing: pull only if this exact digest is not on the disk. Never
# "always": a digest cannot move, so there is nothing newer to fetch.
cat > "$APP_DIR/docker-compose.override.yml" <<YAML
services:
  piperoom:
    build: !reset null
    image: $IMAGE
    pull_policy: missing
YAML
chmod 0644 "$APP_DIR/docker-compose.override.yml"

# ---- Config files -------------------------------------------------------------

secret_to "$(md piperoom-secret-owners)" "$WORK/owners.yaml"
secret_to "$(md piperoom-secret-users)" "$WORK/users.yaml"
# The container runs as uid 65532 and the mount is read-only.
install -m 0440 -o 65532 -g 65532 "$WORK/owners.yaml" "$APP_DIR/config/owners.yaml"
install -m 0440 -o 65532 -g 65532 "$WORK/users.yaml" "$APP_DIR/config/users.yaml"

# ---- Env file -------------------------------------------------------------------

# Order matters: compose lets a later duplicate win. The pipeline-env secret goes
# FIRST (after the filter below) and the settings this stack manages go LAST, so
# nothing in pipeline-env can override the auth mode, bind address and so on.
ENVTMP="$WORK/managed.env"
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

# BEGIN filter_pipeline_env (extracted and tested by test/filter_test.sh)
# filter_pipeline_env <managed keys, space separated>: stdin -> stdout.
# Drops every line that sets a managed key. The key is read the way the compose
# env-file parser reads it: leading blanks and an optional "export" keyword are
# ignored, as are blanks around the name.
filter_pipeline_env() {
  awk -v managed="$1" '
    BEGIN { n = split(managed, a, " "); for (i = 1; i <= n; i++) skip[a[i]] = 1 }
    /^[[:space:]]*(#|$)/ { print; next }
    { line = $0
      sub(/^[[:space:]]+/, "", line)
      sub(/^export[[:space:]]+/, "", line)
      key = line
      sub(/[[:space:]]*=.*/, "", key)
      if (key in skip) { print "piperoom-startup: ignoring managed setting " key " in pipeline-env" > "/dev/stderr"; next }
      # A quoted value that is not closed on its line could swallow the managed
      # lines that follow (compose allows multi-line quoted values).
      val = line
      sub(/^[^=]*=[[:space:]]*/, "", val)
      sub(/[[:space:]]+$/, "", val)
      c = substr(val, 1, 1)
      if ((c == "\"" || c == "\047") && (length(val) < 2 || substr(val, length(val), 1) != c)) {
        print "piperoom-startup: ignoring line with an unclosed quote for " key " in pipeline-env" > "/dev/stderr"; next }
      print }
  '
}
# END filter_pipeline_env

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

# Pipeline settings (SOURCE_DB_URL, KAFKA_*, ...): filtered, then the managed lines.
secret_to "$(md piperoom-secret-envfile)" "$WORK/pipeline.env"
{
  filter_pipeline_env "$MANAGED_KEYS" < "$WORK/pipeline.env"
  echo
  cat "$ENVTMP"
} > "$WORK/env"

install -m 0600 -o root -g root "$WORK/env" "$APP_DIR/.env"

# ---- Registry login (Artifact Registry / GCR) ------------------------------------

REG_HOST=${IMAGE%%/*}
# The token lives in a throw-away Docker config that the EXIT trap deletes, not
# in /root/.docker.
export DOCKER_CONFIG="$WORK/docker"
install -d -m 0700 "$DOCKER_CONFIG"
case $REG_HOST in
  *-docker.pkg.dev | docker.pkg.dev | gcr.io | *.gcr.io)
    access_token | docker login -u oauth2accesstoken --password-stdin "https://$REG_HOST" >/dev/null
    ;;
esac

# ---- Run ----------------------------------------------------------------------------

cd "$APP_DIR"
# --no-deps: do not start the bundled postgres service.
docker compose -f docker-compose.pilot.yml -f docker-compose.override.yml up -d --no-deps --pull missing piperoom

log "done. Check: docker compose -f $APP_DIR/docker-compose.pilot.yml -f $APP_DIR/docker-compose.override.yml ps"
