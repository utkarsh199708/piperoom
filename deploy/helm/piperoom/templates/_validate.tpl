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
  {{- if has $k (list "PIPEROOM_HTTP_ADDR" "OWNERS_FILE" "USERS_FILE" "PIPEROOM_DEV_ALLOW_NONLOOPBACK") -}}
  {{- fail (printf "env.%s is set by the chart and cannot be overridden" $k) -}}
  {{- end -}}
{{- end -}}
{{- $pub := include "piperoom.publicURL" . -}}
{{- if and .Values.ingress.enabled $pub -}}
  {{- $h := (urlParse $pub).hostname -}}
  {{- if and (ne $h .Values.ingress.host) (not (has .Values.ingress.host (splitList "," (toString (default "" .Values.env.PIPEROOM_ALLOWED_HOSTS))))) -}}
  {{- fail (printf "PIPEROOM_PUBLIC_URL host %q differs from ingress.host %q: add %q to env.PIPEROOM_ALLOWED_HOSTS (the server answers 421 otherwise)" $h .Values.ingress.host .Values.ingress.host) -}}
  {{- end -}}
{{- end -}}
{{- end }}
