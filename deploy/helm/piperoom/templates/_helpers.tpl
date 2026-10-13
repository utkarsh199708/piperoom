{{- define "piperoom.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "piperoom.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{- define "piperoom.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{ include "piperoom.selectorLabels" . }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "piperoom.selectorLabels" -}}
app.kubernetes.io/name: {{ include "piperoom.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "piperoom.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "piperoom.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "piperoom.image" -}}
{{- if .Values.image.digest -}}
{{ .Values.image.repository }}@{{ .Values.image.digest }}
{{- else -}}
{{ .Values.image.repository }}:{{ default .Chart.AppVersion .Values.image.tag }}
{{- end -}}
{{- end }}

{{- define "piperoom.configMapName" -}}
{{- default (printf "%s-config" (include "piperoom.fullname" .)) .Values.files.existingConfigMap }}
{{- end }}

{{/* Public URL: explicit value, else https://<ingress host> when ingress has TLS. */}}
{{- define "piperoom.publicURL" -}}
{{- if .Values.env.PIPEROOM_PUBLIC_URL -}}
{{ .Values.env.PIPEROOM_PUBLIC_URL }}
{{- else if and .Values.ingress.enabled .Values.ingress.tlsSecretName -}}
https://{{ .Values.ingress.host }}
{{- end -}}
{{- end }}
