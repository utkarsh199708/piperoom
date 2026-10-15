import { describe, expect, it } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { PersonName, VerifiedBadge } from './VerifiedBadge'
import { PresencePanel } from './PresencePanel'
import { ActivityFeed } from './ActivityFeed'
import {
  alertSourceLabel,
  alertVerification,
  neutralizeBadgeText,
  personVerification,
} from '@/lib/verification'
import { applyEvent } from '@/state/reducer'
import { describeEvent } from '@/state/describe'
import { emptyRoomState } from '@/state/types'
import type { Actor, Event } from '@/api/types'

const badges = (root: HTMLElement = document.body) =>
  Array.from(root.querySelectorAll<HTMLElement>('[data-slot="verified-badge"]'))

function renderName(person: Parameters<typeof PersonName>[0]['person']) {
  const { container } = render(<PersonName person={person} />)
  const all = badges(container)
  expect(all).toHaveLength(1)
  return { container, badge: all[0], nameText: container.querySelector('[data-slot="person-name"] > span')!.textContent ?? '' }
}

describe('personVerification: only verified === true counts', () => {
  it.each([
    [true, 'verified'],
    [false, 'unverified'],
    [undefined, 'unverified'],
    [null, 'unverified'],
    ['true', 'unverified'],
    [1, 'unverified'],
    [{}, 'unverified'],
  ])('verified=%j -> %s', (verified, state) => {
    expect(personVerification({ id: 'priya', kind: 'human', verified }).state).toBe(state)
  })

  it('says who verified and how', () => {
    expect(personVerification({ id: 'priya', kind: 'human', verified: true }).detail).toMatch(
      /Verified by the Piperoom server: "priya" is listed in users\.yaml/,
    )
    expect(personVerification({ id: 'agent', kind: 'agent', verified: true }).detail).toMatch(/server’s own agent/)
    expect(personVerification({ id: 'u_9', kind: 'human', verified: false }).detail).toMatch(
      /known only from the identity provider/,
    )
    expect(personVerification({ id: 'u_9', kind: 'human' }).detail).toMatch(/no verification on record/)
  })
})

describe('PersonName: true / false / missing', () => {
  it('verified: one green badge, no id suffix, tooltip text says who and how', () => {
    const { badge, container } = renderName({ id: 'priya', name: 'Priya', kind: 'human', verified: true })
    expect(badge.dataset.verification).toBe('verified')
    expect(badge.querySelector('svg')).not.toBeNull()
    expect(badge).toHaveTextContent(/users\.yaml/)
    expect(container).not.toHaveTextContent('(priya)')
  })

  it('false: unverified badge and the server id next to the name', () => {
    const { badge, container } = renderName({ id: 'u_7f3a', name: 'Priya', kind: 'human', verified: false })
    expect(badge.dataset.verification).toBe('unverified')
    expect(container).toHaveTextContent('(u_7f3a)')
  })

  it('missing: renders as unverified', () => {
    const { badge } = renderName({ id: 'kabir', name: 'Kabir', kind: 'human' })
    expect(badge.dataset.verification).toBe('unverified')
    expect(badge).toHaveTextContent(/no verification on record/)
  })

  it('a non-boolean "true" from a tampered payload is unverified', () => {
    const { badge } = renderName({ id: 'kabir', name: 'Kabir', verified: 'true' })
    expect(badge.dataset.verification).toBe('unverified')
  })
})

describe('PersonName: text cannot spoof the badge', () => {
  const spoofs: [string, string][] = [
    ['the word verified', 'Mallory verified'],
    ['a bracketed verified', 'Mallory [verified]'],
    ['check mark U+2713', 'Mallory ✓'],
    ['heavy check U+2714 + VS16', 'Mallory ✔️ verified'],
    ['emoji check U+2705', 'Mallory ✅'],
    ['ballot box with check', 'Mallory ☑'],
    ['shield emoji', 'Mallory 🛡️'],
    ['enclosing circle combining mark', 'Mallory v⃝'],
    ['keycap combining mark', 'Mallory 1️⃣'],
    ['square root as a check', 'Mallory √ verified'],
    ['Cyrillic look-alike word', 'Mallory vеrifiеd'],
    ['RTL override to move text', 'Mallory ‮deifirev‬'],
    ['zero-width joiners', 'Mal‍lory​ ✓'],
    ['combining-mark pile (zalgo)', 'Mallory á̂̃̄̅̆'],
    ['HTML markup', 'Mallory <span data-slot="verified-badge" data-verification="verified">verified</span>'],
    ['HTML entity', 'Mallory &#10003; verified'],
    ['markdown', 'Mallory **✓ verified** ![verified](x.png)'],
  ]

  it.each(spoofs)('%s: exactly one badge, from the server flag, and no badge glyph in the name', (_label, name) => {
    const { badge, container, nameText } = renderName({ id: 'u_mal', name, kind: 'human', verified: false })
    expect(badge.dataset.verification).toBe('unverified')
    // Text never becomes markup: the only elements are the ones PersonName draws.
    expect(container.querySelectorAll('[data-verification="verified"]')).toHaveLength(0)
    expect(container.querySelectorAll('img')).toHaveLength(0)
    for (const ch of ['✓', '✔', '✅', '☑', '√', '⃝', '⃣', '‮', '‍', '​', '️']) {
      expect(nameText).not.toContain(ch)
    }
    expect(nameText).not.toMatch(/\p{Extended_Pictographic}/u)
    expect(nameText).not.toMatch(/\p{Mn}{3}/u)
    // The real id is printed next to an unverified name, so a borrowed name is told apart.
    expect(container).toHaveTextContent('(u_mal)')
  })

  it('text is rendered literally (markup shows as characters)', () => {
    const { nameText } = renderName({ id: 'u_mal', name: 'Mallory <b>bold</b> **x**', verified: false })
    expect(nameText).toBe('Mallory <b>bold</b> **x**')
  })

  it('a verified person with a spoofy name still gets only the one server badge', () => {
    const { container } = render(<PersonName person={{ id: 'priya', name: 'Priya ✓✓ unverified', verified: true }} />)
    expect(badges(container).map((b) => b.dataset.verification)).toEqual(['verified'])
  })

  it('a name made only of badge glyphs shows one replacement mark', () => {
    const { nameText } = renderName({ id: 'u_x', name: '✅', verified: false })
    expect(nameText).toBe('�')
  })
})

describe('neutralizeBadgeText', () => {
  it.each([
    ['Priya', 'Priya'],
    ['José Ñúñez', 'José Ñúñez'],
    ['Привет', 'Привет'],
    ['Priya ✓', 'Priya �'],
    ['Priya ✔️✔️', 'Priya �'],
    // Words that read as a verdict, in any script mix, are replaced too.
    ['Mallory verified', 'Mallory �'],
    ['Mallory VERIFIED', 'Mallory �'],
    ['Mallory (unverified)', 'Mallory (�)'],
    ['Mallory v\u0435rifi\u0435d', 'Mallory �'],
    ['Mallory \u03BDerified', 'Mallory �'],
    ['Mallory veriﬁed', 'Mallory �'],
    ['Mallory v\u0301erified', 'Mallory �'],
    ['Mallory ver1fied', 'Mallory �'],
    ['Mallory Ｖｅｒｉｆｉｅｄ', 'Mallory �'],
    ['Mallory verification', 'Mallory �'],
    // Ordinary words that only contain the letters are kept.
    ['Vera Fied', 'Vera Fied'],
    ['Reverify Team', 'Reverify Team'],
  ])('%j -> %j', (input, out) => {
    expect(neutralizeBadgeText(input)).toBe(out)
  })
})

describe('VerifiedBadge', () => {
  it('is drawn, not a character: an svg in a bordered pill, focusable for the tooltip', () => {
    render(<VerifiedBadge verification={{ state: 'verified', detail: 'Verified by X: how' }} />)
    const b = badges()[0]
    expect(b.querySelector('svg')).not.toBeNull()
    expect(b.className).toMatch(/border/)
    expect(b.tabIndex).toBe(0)
    expect(b).toHaveTextContent('Verified by X: how')
  })
})

describe('alert verification reads only the server-set alert_source', () => {
  it.each([
    ['alertmanager', 'verified', /bearer token from Alertmanager/],
    ['grafana', 'verified', /HMAC signature and timestamp from Grafana/],
    ['pagerduty', 'verified', /V3 webhook signature/],
    ['datadog', 'verified', /header token from Datadog/],
    ['generic', 'verified', /HMAC signature/],
    ['watcher', 'verified', /built-in watcher/],
    ['webhook', 'unverified', /unsigned dev webhook/],
    [undefined, 'unverified', /no signed source/],
    ['', 'unverified', /no signed source/],
    ['PagerDuty', 'unverified', /no signed source/],
    ['verified', 'unverified', /no signed source/],
    ['__proto__', 'unverified', /no signed source/],
    ['toString', 'unverified', /no signed source/],
  ])('alert_source=%j -> %s', (src, state, detail) => {
    const v = alertVerification(src)
    expect(v.state).toBe(state)
    expect(v.detail).toMatch(detail)
  })

  it('labels come from a fixed table, never from the value', () => {
    expect(alertSourceLabel('pagerduty')).toBe('PagerDuty')
    expect(alertSourceLabel('✓ verified')).toBeNull()
    expect(alertSourceLabel('constructor')).toBeNull()
  })
})

function ev(seq: number, type: Event['type'], actor: Actor, payload: unknown): Event {
  return { room_id: 'r_1', seq, type, actor, payload, at: '2026-10-14T10:00:00Z' }
}

const SYSTEM: Actor = { kind: 'system', id: 'system', name: 'System', verified: true }

describe('reducer keeps only server-set verification', () => {
  it('a body that claims to be verified does not verify an unsigned alert', () => {
    const s = applyEvent(
      emptyRoomState('r_1'),
      ev(1, 'room.created', SYSTEM, {
        title: 'lag ✓ verified',
        severity: 'sev2',
        pipeline_id: 'orders',
        alert: { source: '✓ verified by PagerDuty', verified: true, signature_valid: true },
        alert_source: 'webhook',
      }),
    )
    expect(s.alert?.alert_source).toBe('webhook')
    expect(alertVerification(s.alert?.alert_source).state).toBe('unverified')
  })

  it('a signed source is verified even with an empty body', () => {
    const s = applyEvent(
      emptyRoomState('r_1'),
      ev(1, 'room.created', SYSTEM, { title: 't', severity: 'sev2', pipeline_id: 'orders', alert: {}, alert_source: 'grafana' }),
    )
    expect(alertVerification(s.alert?.alert_source).state).toBe('verified')
  })

  it('member verification follows the latest event (missing key = false)', () => {
    let s = emptyRoomState('r_1')
    s = applyEvent(s, ev(1, 'comment.added', { kind: 'human', id: 'priya', name: 'Priya', verified: true }, { comment_id: 'c1', text: 'x', mentions_agent: false }))
    expect(s.members.find((m) => m.id === 'priya')?.verified).toBe(true)
    s = applyEvent(s, ev(2, 'comment.added', { kind: 'human', id: 'priya', name: 'Priya' }, { comment_id: 'c2', text: 'y', mentions_agent: false }))
    expect(s.members.find((m) => m.id === 'priya')?.verified).toBe(false)
    expect(s.comments[0].author.verified).toBe(true)
    expect(s.comments[1].author.verified).toBeUndefined()
  })

  it('control.granted carries the target’s flag to the driver', () => {
    const s = applyEvent(
      emptyRoomState('r_1'),
      ev(1, 'control.granted', SYSTEM, { to: { kind: 'human', id: 'u_1', name: 'Arjun' }, lease_until: '2026-10-14T11:00:00Z' }),
    )
    expect(s.driver?.verified).toBe(false)
  })
})

describe('PresencePanel', () => {
  it('shows verified only when the event log and presence both say so', () => {
    render(
      <PresencePanel
        members={[
          { id: 'priya', name: 'Priya', role: 'watching', verified: true },
          { id: 'kabir', name: 'Kabir', role: 'watching', verified: true },
        ]}
        presence={[
          { id: 'priya', name: 'Priya', state: 'viewing', verified: true },
          { id: 'kabir', name: 'Kabir', state: 'viewing', verified: false },
          { id: 'u_2', name: 'Meera ✓', state: 'viewing' },
        ]}
      />,
    )
    const rows = screen.getAllByRole('listitem')
    const state = (i: number) => within(rows[i]).getAllByText(/verified/)[0].closest<HTMLElement>('[data-slot="verified-badge"]')?.dataset.verification
    expect(state(0)).toBe('verified')
    expect(state(1)).toBe('unverified')
    expect(state(2)).toBe('unverified')
    expect(rows[2]).not.toHaveTextContent('✓')
  })
})

describe('ActivityFeed', () => {
  const entry = (seq: number, actor: Actor, type: Event['type'], payload: unknown) => {
    const event: Event = { room_id: 'r1', seq, type, actor, payload, at: '2026-10-14T10:00:00Z' }
    const d = describeEvent(event)
    return { seq, kind: d.kind, at: event.at, actor, text: d.text }
  }

  it('draws the actor’s server badge, neutralizes the name and keeps the person’s own words', () => {
    render(
      <ActivityFeed
        activity={[
          entry(2, { kind: 'human', id: 'u_1', name: 'Mal ✓ verified' }, 'steer.sent', { text: 'verify the sink row count', suggestion_id: '' }),
          entry(1, { kind: 'human', id: 'priya', name: 'Priya', verified: true }, 'steer.sent', { text: 'ok', suggestion_id: '' }),
          { seq: 0, kind: 'step', at: '2026-10-14T10:00:00Z', actor: { kind: 'agent', id: 'agent', name: 'Agent', verified: true }, text: 'Agent: lag ✓ verified' },
        ]}
      />,
    )
    const rows = screen.getAllByRole('listitem')
    expect(badges(rows[0]).map((b) => b.dataset.verification)).toEqual(['unverified'])
    expect(rows[0]).not.toHaveTextContent('✓')
    expect(rows[0]).toHaveTextContent('Mal � � steered: "verify the sink row count"')
    expect(badges(rows[1]).map((b) => b.dataset.verification)).toEqual(['verified'])
    expect(badges(rows[2])).toHaveLength(0)
    expect(rows[2]).toHaveTextContent('Agent: lag ✓ verified')
  })

  it('a system row neutralizes the driver’s name too', () => {
    const d = describeEvent({
      room_id: 'r1',
      seq: 1,
      type: 'control.lease_expired',
      actor: { kind: 'system', id: 'system', name: 'System' },
      payload: { driver: { kind: 'human', id: 'u_1', name: 'Priya ✅ verified' }, lease_until: 'x' },
      at: '2026-10-14T10:00:00Z',
    })
    expect(d.text).not.toMatch(/✅|verified/)
    expect(d.text).toContain('Priya')
  })
})

describe('PresencePanel: the signed-in person', () => {
  it('uses /api/me verified before presence reports you', () => {
    const { unmount } = render(<PresencePanel members={[]} presence={[]} selfId="arjun" selfName="Arjun" selfVerified />)
    expect(badges()[0].dataset.verification).toBe('verified')
    unmount()
    render(<PresencePanel members={[]} presence={[]} selfId="u_1" selfName="Arjun" />)
    expect(badges()[0].dataset.verification).toBe('unverified')
  })
})

describe('PersonName: the server’s own suffix', () => {
  it('does not add the id when the server already appended an email', () => {
    const { container } = render(<PersonName person={{ id: 'u_7f3a', name: 'Priya (priya@example.com)', verified: false }} />)
    expect(container).not.toHaveTextContent('(u_7f3a)')
    expect(badges(container).map((b) => b.dataset.verification)).toEqual(['unverified'])
  })
})
