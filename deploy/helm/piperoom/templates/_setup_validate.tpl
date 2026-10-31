{{/* Fails a setup release whose values cannot work: both blocks at once, a
missing name, or a connector list the RBAC could not name. The schema checks
each value's shape; this checks the combination. */}}
{{- define "piperoom.setupValidate" -}}
{{- $s := .Values.setup -}}
{{- if and $s.pipeline.enabled $s.runner.enabled -}}
{{- fail "setup.pipeline.enabled and setup.runner.enabled are two releases (piperoom-pipeline-<pipeline> and piperoom-setup-<room>); enable one per release" -}}
{{- end -}}
{{- if $s.pipeline.enabled -}}
  {{- if or (not $s.pipeline.id) (not $s.pipeline.kafka) (not $s.pipeline.connectors) -}}
  {{- fail "setup.pipeline needs id, kafka and at least one connector name" -}}
  {{- end -}}
  {{- if ne .Release.Name (printf "piperoom-pipeline-%s" $s.pipeline.id) -}}
  {{- fail (printf "the pipeline release must be named piperoom-pipeline-%s (the admission policy binding and the rollback card use that name)" $s.pipeline.id) -}}
  {{- end -}}
  {{- if ne (len $s.pipeline.connectors) (len (uniq $s.pipeline.connectors)) -}}
  {{- fail "setup.pipeline.connectors has a name twice" -}}
  {{- end -}}
  {{- include "piperoom.setupNames" (list "setup.pipeline" $s.pipeline.id $s.pipeline.kafka $s.pipeline.connectors) -}}
  {{- if and $s.pipeline.admissionPolicy (not (regexMatch "^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$" ($s.pipeline.topicPrefix | default ""))) -}}
  {{- fail "setup.pipeline.admissionPolicy needs setup.pipeline.topicPrefix, the pipeline's topic prefix (letters, digits, '.', '_', '-')" -}}
  {{- end -}}
  {{- range $s.pipeline.pluginHosts -}}
  {{- if not (regexMatch "^[a-z0-9]([a-z0-9.-]{0,251}[a-z0-9])?(:[0-9]{1,5})?$" .) -}}
  {{- fail (printf "setup.pipeline.pluginHosts: %q is not a host[:port]" .) -}}
  {{- end -}}
  {{- end -}}
  {{- if and $s.pipeline.admissionPolicy (not $s.pipeline.imageRegistries) -}}
  {{- fail "setup.pipeline.admissionPolicy needs setup.pipeline.imageRegistries, the registries (host[:port]) a KafkaConnect build may push to" -}}
  {{- end -}}
  {{- if and $s.pipeline.admissionPolicy (not $s.pipeline.databaseHosts) -}}
  {{- fail "setup.pipeline.admissionPolicy needs setup.pipeline.databaseHosts, the database hosts a KafkaConnector may connect to (the room's source and sink targets)" -}}
  {{- end -}}
  {{- if and .Values.networkPolicy.enabled (not $s.pipeline.networkPolicy.databases.ports) -}}
  {{- fail "networkPolicy.enabled needs setup.pipeline.networkPolicy.databases.ports, the database ports the Connect pods dial (and databases.to, their addresses, for production)" -}}
  {{- end -}}
  {{- range $s.pipeline.imageRegistries -}}
  {{- if not (regexMatch "^[a-z0-9]([a-z0-9.-]{0,251}[a-z0-9])?(:[0-9]{1,5})?$" .) -}}
  {{- fail (printf "setup.pipeline.imageRegistries: %q is not a host[:port]" .) -}}
  {{- end -}}
  {{- end -}}
{{- end -}}
{{- if $s.runner.enabled -}}
  {{- if or (not $s.runner.room) (not $s.runner.pipeline) (not $s.runner.kafka) (not $s.runner.connectors) (not $s.runner.serverURL) -}}
  {{- fail "setup.runner needs room, pipeline, kafka, serverURL and at least one connector name" -}}
  {{- end -}}
  {{- if ne .Release.Name (printf "piperoom-setup-%s" (include "piperoom.setupRoom" .)) -}}
  {{- fail (printf "the runner release must be named piperoom-setup-%s (the close card uninstalls that name)" (include "piperoom.setupRoom" .)) -}}
  {{- end -}}
  {{- if ne (len $s.runner.connectors) (len (uniq $s.runner.connectors)) -}}
  {{- fail "setup.runner.connectors has a name twice" -}}
  {{- end -}}
  {{- include "piperoom.setupNames" (list "setup.runner" $s.runner.pipeline $s.runner.kafka $s.runner.connectors) -}}
  {{- if not (regexMatch "^[0-9A-Za-z]{26}$" $s.runner.room) -}}
  {{- fail "setup.runner.room is not a room id (26 letters and digits, without the r_ prefix)" -}}
  {{- end -}}
  {{- if not (regexMatch `^https?://[^\s/@]+$` $s.runner.serverURL) -}}
  {{- fail "setup.runner.serverURL is not an origin (http[s]://host[:port], no path)" -}}
  {{- end -}}
  {{- /* Cross-check with the pipeline release when the cluster is reachable
  (helm install/upgrade; helm template sees nothing): the shells this Role
  names must be the pipeline release's, so the runner's ownership check
  (meta.helm.sh/release-name) passes and nobody else's object is named. */ -}}
  {{- $want := printf "piperoom-pipeline-%s" $s.runner.pipeline -}}
  {{- $kc := lookup "kafka.strimzi.io/v1" "KafkaConnect" .Release.Namespace (printf "%s-connect" $s.runner.pipeline) -}}
  {{- if $kc -}}
  {{- $rel := dig "metadata" "annotations" "meta.helm.sh/release-name" "" $kc -}}
  {{- $inst := dig "metadata" "labels" "app.kubernetes.io/instance" "" $kc -}}
  {{- if or (ne $rel $want) (ne $inst $want) -}}
  {{- fail (printf "KafkaConnect %s-connect in %s is not the pipeline release %s's (release %q, instance %q): install that release first, with setup.pipeline.id=%s" $s.runner.pipeline .Release.Namespace $want $rel $inst $s.runner.pipeline) -}}
  {{- end -}}
  {{- end -}}
{{- end -}}
{{- end }}

{{/* The names the chart renders unquoted into RBAC and policy objects are
re-checked here (the schema checks them too): a DNS label each, so a value can
never carry YAML or another name. Args: the block, pipeline, kafka, connectors. */}}
{{- define "piperoom.setupNames" -}}
{{- $block := index . 0 -}}
{{- if not (regexMatch "^[a-z0-9]([a-z0-9-]{0,38}[a-z0-9])?$" (index . 1)) -}}
{{- fail (printf "%s: the pipeline id %q is not a DNS label of at most 40 characters" $block (index . 1)) -}}
{{- end -}}
{{- if not (regexMatch "^[a-z0-9]([a-z0-9-]{0,51}[a-z0-9])?$" (index . 2)) -}}
{{- fail (printf "%s.kafka %q is not a Kubernetes object name" $block (index . 2)) -}}
{{- end -}}
{{- range (index . 3) -}}
{{- if not (regexMatch "^[a-z0-9]([a-z0-9-]{0,51}[a-z0-9])?$" .) -}}
{{- fail (printf "%s.connectors: %q is not a Kubernetes object name" $block .) -}}
{{- end -}}
{{- end -}}
{{- end }}
