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
{{- end -}}
{{- end }}
