// Best-effort alert source for a room.created payload. The server stores the
// webhook body verbatim in `room.created.alert` — the normalized alertin.Alert
// (which has an explicit `source`) never reaches the client — so the source is
// inferred from the body shape, mirroring the parsers in internal/alertin.
// Everything here is untrusted input: callers must render it as plain text.

const MAX_SOURCE_CHARS = 32

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v)
}

function nonEmptyString(v: unknown): string | null {
  return typeof v === 'string' && v.trim() !== '' ? v : null
}

/** Source name for an alert webhook body, or null when it cannot be told. */
export function alertSource(alert: unknown): string | null {
  if (!isRecord(alert)) return null

  // PagerDuty V3 webhook: { event: { event_type: "incident.triggered", ... } }.
  if (isRecord(alert.event) && nonEmptyString(alert.event.event_type)) {
    return 'PagerDuty'
  }

  // Datadog webhook template keys (internal/alertin.DatadogPayloadTemplate).
  for (const key of ['alert_id', 'cycle_key', 'aggreg_key', 'transition', 'alert_type']) {
    if (nonEmptyString(alert[key])) return 'Datadog'
  }

  // Alertmanager and Grafana share the group shape (groupKey + alerts); only
  // Grafana adds orgId (and, when configured, title/state).
  if (nonEmptyString(alert.groupKey) && Array.isArray(alert.alerts)) {
    if (typeof alert.orgId === 'number') return 'Grafana'
    if (nonEmptyString(alert.title) && nonEmptyString(alert.state)) return 'Grafana'
    return 'Alertmanager'
  }

  // Generic bodies may name their own source; show it as-is (capped).
  const source = nonEmptyString(alert.source)
  if (source) return source.trim().slice(0, MAX_SOURCE_CHARS)
  return null
}
