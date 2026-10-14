{{/* Auth mode: an empty value means oidc, never the image's dev default. */}}
{{- define "piperoom.authMode" -}}
{{- $m := toString (default "" .Values.env.PIPEROOM_AUTH_MODE) -}}
{{- if eq $m "" -}}oidc{{- else -}}{{ $m }}{{- end -}}
{{- end }}

{{/* Env values: whole float64 (YAML numbers from -f) as integers, not 2e+06. */}}
{{- define "piperoom.envValue" -}}
{{- if kindIs "float64" . -}}
{{- if eq (floor .) . -}}{{ int64 . }}{{- else -}}{{ . }}{{- end -}}
{{- else -}}{{ . }}{{- end -}}
{{- end }}

{{/* Fails the render on settings that would be unsafe or ignored. */}}
{{- define "piperoom.validate" -}}
{{- $mode := include "piperoom.authMode" . -}}
{{- if ne $mode "oidc" -}}
  {{- if not .Values.allowDevMode -}}
  {{- fail (printf "env.PIPEROOM_AUTH_MODE=%q: only oidc is allowed unless allowDevMode=true (dev login has no password)" $mode) -}}
  {{- end -}}
  {{- if .Values.ingress.enabled -}}
  {{- fail "dev auth mode must not be exposed through an Ingress: disable ingress and use kubectl port-forward" -}}
  {{- end -}}
{{- else -}}
  {{- /* oidc: the server refuses to start without these, so fail at render time. */ -}}
  {{- $pubURL := include "piperoom.publicURL" . -}}
  {{- if not (hasPrefix "https://" (lower $pubURL)) -}}
  {{- fail (printf "oidc mode needs an https PIPEROOM_PUBLIC_URL, got %q: set env.PIPEROOM_PUBLIC_URL=https://<host>, or enable ingress with ingress.tlsSecretName so the chart derives it" $pubURL) -}}
  {{- end -}}
  {{- if eq (toString (default "" .Values.env.PIPEROOM_OIDC_ISSUER)) "" -}}
  {{- fail "oidc mode needs env.PIPEROOM_OIDC_ISSUER (the identity provider issuer URL)" -}}
  {{- end -}}
  {{- if eq (toString (default "" .Values.env.PIPEROOM_OIDC_CLIENT_ID)) "" -}}
  {{- fail "oidc mode needs env.PIPEROOM_OIDC_CLIENT_ID" -}}
  {{- end -}}
{{- end -}}
{{- $s := .Values.secrets -}}
{{- $secretNames := dict -}}
{{- range $s.requiredKeys }}{{ $_ := set $secretNames . true }}{{ end -}}
{{- range $s.optionalKeys }}{{ $_ := set $secretNames . true }}{{ end -}}
{{- range $k, $_ := $s.refs }}{{ $_ := set $secretNames $k true }}{{ end -}}
{{- range $k, $v := .Values.env -}}
  {{- if and (hasKey $secretNames $k) (ne (toString (default "" $v)) "") -}}
  {{- fail (printf "env.%s is also a secret variable (secrets.*): remove it from env" $k) -}}
  {{- end -}}
  {{- if regexMatch "PASSWORD|SECRET|TOKEN$|TOKEN_BEDROCK|_KEY$|ACCESS_KEY|_DB_URL$" $k -}}
  {{- fail (printf "env.%s looks like a secret: put it in the Secret (secrets.requiredKeys/optionalKeys/refs), never in values" $k) -}}
  {{- end -}}
  {{- if and (regexMatch "(?i)_PROXY$" $k) (regexMatch "://[^/?#@]*@" (toString (default "" $v))) -}}
  {{- fail (printf "env.%s has a user name or password in the URL (://user:pass@...): a proxy URL with credentials is a secret, so put it in the Secret and reference it with secrets.refs" $k) -}}
  {{- end -}}
  {{- if has $k (list "PIPEROOM_HTTP_ADDR" "OWNERS_FILE" "USERS_FILE" "PIPEROOM_DEV_ALLOW_NONLOOPBACK" "PIPEROOM_REPLICA_ID") -}}
  {{- fail (printf "env.%s is set by the chart and cannot be overridden" $k) -}}
  {{- end -}}
{{- end -}}
{{- /* A PDB that keeps every replica would block every node drain. */ -}}
{{- $replicas := int .Values.replicaCount -}}
{{- if and (gt $replicas 1) .Values.podDisruptionBudget.enabled (ge (int .Values.podDisruptionBudget.minAvailable) $replicas) -}}
  {{- fail (printf "podDisruptionBudget.minAvailable=%d must be below replicaCount=%d, or no node could ever be drained" (int .Values.podDisruptionBudget.minAvailable) $replicas) -}}
{{- end -}}
{{- if and (gt $replicas 1) (eq (int .Values.rollingUpdate.maxSurge) 0) (eq (int .Values.rollingUpdate.maxUnavailable) 0) -}}
  {{- fail "rollingUpdate.maxSurge and rollingUpdate.maxUnavailable cannot both be 0" -}}
{{- end -}}
{{- $pub := include "piperoom.publicURL" . -}}
{{- if and .Values.ingress.enabled $pub -}}
  {{- $h := (urlParse $pub).hostname -}}
  {{- if and (ne $h .Values.ingress.host) (not (has .Values.ingress.host (splitList "," (toString (default "" .Values.env.PIPEROOM_ALLOWED_HOSTS))))) -}}
  {{- fail (printf "PIPEROOM_PUBLIC_URL host %q differs from ingress.host %q: add %q to env.PIPEROOM_ALLOWED_HOSTS (the server answers 421 otherwise)" $h .Values.ingress.host .Values.ingress.host) -}}
  {{- end -}}
{{- end -}}
{{- end }}
