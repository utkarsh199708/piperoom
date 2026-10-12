import { describe, expect, it } from 'vitest'
import { alertSource } from './alerts'

describe('alertSource', () => {
  const cases: Array<[string, unknown, string | null]> = [
    [
      'pagerduty',
      { event: { event_type: 'incident.triggered', data: { id: 'P1' } } },
      'PagerDuty',
    ],
    [
      'alertmanager v4 (no orgId)',
      {
        version: '4',
        groupKey: '{}:{alertname="ConsumerLag"}',
        status: 'firing',
        alerts: [{ status: 'firing', labels: { alertname: 'ConsumerLag' } }],
      },
      'Alertmanager',
    ],
    [
      'grafana (orgId)',
      { groupKey: 'g', alerts: [{}], orgId: 1, title: 'T', state: 'alerting' },
      'Grafana',
    ],
    [
      'grafana (title + state, no orgId)',
      { groupKey: 'g', alerts: [{}], title: 'T', state: 'alerting' },
      'Grafana',
    ],
    [
      'datadog template',
      { id: '1234', alert_id: '1234', transition: 'Triggered', alert_type: 'error', title: 'T' },
      'Datadog',
    ],
    ['generic body with a source field', { title: 'T', severity: 'sev2', source: 'checkmk' }, 'checkmk'],
  ]

  it.each(cases)('detects %s', (_name, body, want) => {
    expect(alertSource(body)).toBe(want)
  })

  it('returns null for bodies with no recognizable source', () => {
    expect(alertSource({ title: 'T', severity: 'sev2' })).toBeNull()
    expect(alertSource({})).toBeNull()
    expect(alertSource('plain text alert')).toBeNull()
    expect(alertSource([1, 2])).toBeNull()
    expect(alertSource(42)).toBeNull()
    expect(alertSource(null)).toBeNull()
    expect(alertSource(undefined)).toBeNull()
  })

  it('caps a self-declared source and ignores blank ones', () => {
    expect(alertSource({ source: 'x'.repeat(40) })).toBe('x'.repeat(32))
    expect(alertSource({ source: '   ' })).toBeNull()
  })
})
