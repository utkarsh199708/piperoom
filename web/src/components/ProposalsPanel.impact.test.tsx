import { describe, expect, it, vi } from 'vitest'
import { render, screen, within } from '@testing-library/react'
import { ProposalsPanel } from './ProposalsPanel'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'
import type { ProposalState, RoomState } from '../state/types'

// Q0 (QA G5): impacted layers, risk level and reasons. QA G12: long unbroken text wraps.
const actions = {
  comment: vi.fn(), steer: vi.fn(), sendSuggestion: vi.fn(), requestControl: vi.fn(), grantControl: vi.fn(),
  close: vi.fn(), approve: vi.fn(), reject: vi.fn(), handoff: vi.fn(), acceptHandoff: vi.fn(), trace: vi.fn(),
  hypothesisVerdict: vi.fn(), nudge: vi.fn(),
} as unknown as RoomActions

// The QA proposal: a decimal.handling.mode change on kafka-connect that changes the data of two more layers.
const money: ProposalState = {
  proposal_id: 'p1',
  title: 'Switch decimal.handling.mode to double',
  steps: [
    {
      action: 'connect_update_config',
      layer: 'kafka-connect',
      params: { connector: 'pg-orders', config: { 'decimal.handling.mode': 'double' } },
      description: 'Change how NUMERIC columns are encoded.',
    },
  ],
  risk: 'Precision loss on money columns.',
  rollback: 'Set it back to precise and restart the task.',
  required_layers: ['consumer', 'kafka-connect', 'warehouse'],
  impacted_layers: ['consumer', 'warehouse'],
  risk_level: 'high',
  impact_reasons: ['decimal.handling.mode=double turns NUMERIC columns into doubles: precision can be lost downstream.'],
  approvals: { consumer: 'pending', 'kafka-connect': 'pending', warehouse: 'pending' },
  status: 'open',
  decisions: {},
  params_hash: 'abc123abc123',
  executed: [],
}
const stranger: CurrentUser = { id: 'sam', name: 'Sam', layers: [], role: 'member' }
const room = (p: ProposalState, over: Partial<RoomState> = {}): RoomState => ({
  ...emptyRoomState('r_1'),
  driver: { id: 'kabir', name: 'Kabir' },
  proposals: [p],
  ...over,
})
const show = (p: ProposalState, over: Partial<RoomState> = {}) =>
  render(<ProposalsPanel state={room(p, over)} user={stranger} actions={actions} />)

const row = (layer: string) => document.querySelector(`[data-layer="${layer}"]`) as HTMLElement

describe('impacted layers (QA G5)', () => {
  it('lists each as a required approval labelled "impacted", apart from the layers the steps write', () => {
    show(money)
    for (const layer of ['consumer', 'warehouse']) {
      expect(within(row(layer)).getByTestId(`impacted-${layer}`)).toHaveTextContent('impacted')
      expect(row(layer)).toHaveAttribute('data-state', 'pending')
    }
    // kafka-connect is where the step writes: a required layer, not an impacted one.
    expect(within(row('kafka-connect')).queryByText('impacted')).not.toBeInTheDocument()
    expect(document.querySelectorAll('[data-layer]')).toHaveLength(3)
  })

  it('never labels a layer a step writes to, even if the list names it', () => {
    show({ ...money, impacted_layers: ['kafka-connect', 'consumer'] })
    expect(within(row('kafka-connect')).queryByText('impacted')).not.toBeInTheDocument()
    expect(within(row('consumer')).getByText('impacted')).toBeInTheDocument()
  })

  it('the label explains itself on hover', () => {
    show(money)
    expect(screen.getByTestId('impacted-consumer')).toHaveAttribute('title', expect.stringContaining('its owner must approve too'))
  })

  it('a proposal without impacted layers has no label', () => {
    show({ ...money, impacted_layers: undefined, required_layers: ['kafka-connect'], approvals: { 'kafka-connect': 'pending' } })
    expect(screen.queryByText('impacted')).not.toBeInTheDocument()
  })
})

describe('risk level and reasons (QA G5)', () => {
  it('shows a "risk high" badge in the warning colour, the risk text, then the reasons under it', () => {
    show(money)
    const badge = screen.getByTestId('risk-level')
    expect(badge).toHaveTextContent('risk high')
    expect(badge.className).toContain('bg-warning-subtle')
    expect(badge.className).not.toContain('bg-destructive-subtle')
    const text = screen.getByTestId('risk-text')
    expect(text).toHaveTextContent('Precision loss on money columns.')
    const reasons = screen.getByTestId('impact-reasons')
    expect(within(reasons).getAllByRole('listitem')).toHaveLength(1)
    expect(reasons).toHaveTextContent('decimal.handling.mode=double turns NUMERIC columns into doubles')
    // The reasons sit under the risk text.
    expect(text.compareDocumentPosition(reasons) & Node.DOCUMENT_POSITION_FOLLOWING).toBeTruthy()
  })

  it('low is green, medium is neutral', () => {
    const low = show({ ...money, risk_level: 'low' })
    expect(screen.getByTestId('risk-level').className).toContain('bg-success-subtle')
    low.unmount()
    show({ ...money, risk_level: 'medium' })
    expect(screen.getByTestId('risk-level')).toHaveTextContent('risk medium')
    expect(screen.getByTestId('risk-level').className).toContain('bg-muted')
  })

  it('a proposal from before Q0 whose risk text is just the level still gets the badge, and no duplicate text', () => {
    show({ ...money, risk: 'low', risk_level: undefined, impact_reasons: undefined, impacted_layers: undefined })
    expect(screen.getByTestId('risk-level')).toHaveTextContent('risk low')
    expect(screen.queryByTestId('risk-text')).not.toBeInTheDocument()
    expect(screen.queryByTestId('impact-reasons')).not.toBeInTheDocument()
  })

  it('a sentence of risk without any level is shown as text only', () => {
    show({ ...money, risk: 'Interrupts CDC for a few seconds.', risk_level: undefined })
    expect(screen.queryByTestId('risk-level')).not.toBeInTheDocument()
    expect(screen.getByTestId('risk-text')).toHaveTextContent('risk: Interrupts CDC for a few seconds.')
  })
})

// jsdom does no layout, so the wrap rules are checked where they live: the classes on the path from the text up
// to the card. A row flex item needs min-w-0 (or it keeps its content's width and runs under the rail), a
// whitespace-nowrap or truncate ancestor keeps a line whole, and the card sets overflow-wrap: anywhere for
// every line it contains.
function wraps(el: HTMLElement) {
  const card = el.closest('article') as HTMLElement
  expect(card).not.toBeNull()
  expect(card.className).toContain('[overflow-wrap:anywhere]')
  expect(card.className).toContain('min-w-0')
  for (let node: HTMLElement | null = el; node && node !== card; node = node.parentElement) {
    expect(node.className, `${node.tagName} keeps its line whole`).not.toMatch(/whitespace-nowrap|truncate|whitespace-pre\b/)
    const parent = node.parentElement as HTMLElement
    const rowFlex = /\bflex\b/.test(parent.className) && !/flex-col/.test(parent.className) && !/\bgrid\b/.test(parent.className)
    if (rowFlex && node !== card) expect(node.className, `${node.tagName} is a row flex item`).toMatch(/min-w-0|w-full|shrink-0/)
  }
}

describe('long unbroken text wraps (QA G12)', () => {
  const long = 'x'.repeat(300)

  it('a 300-character risk', () => {
    show({ ...money, risk: long, risk_level: 'high' })
    const el = screen.getByTestId('risk-text')
    expect(el).toHaveTextContent(long)
    wraps(el)
  })

  it('a 300-character risk with no level (a risk text that used to sit inside a nowrap badge)', () => {
    show({ ...money, risk: long, risk_level: undefined })
    const el = screen.getByTestId('risk-text')
    expect(el).toHaveTextContent(long)
    wraps(el)
  })

  it('a 300-character rollback', () => {
    show({ ...money, rollback: long })
    const el = screen.getByTestId('rollback-text')
    expect(el).toHaveTextContent(long)
    wraps(within(el).getByText(`rollback: ${long}`))
  })

  it('a 300-character step description, title and impact reason', () => {
    show({
      ...money,
      title: long,
      steps: [{ ...money.steps[0], description: long }],
      impact_reasons: [long],
    })
    wraps(screen.getByRole('heading', { level: 3 }))
    wraps(screen.getByText(long, { selector: 'p' }))
    wraps(within(screen.getByTestId('impact-reasons')).getByText(long))
  })

  it('a 300-character withdraw reason', () => {
    show({ ...money, status: 'withdrawn', withdrawn: { reason: long } })
    const el = screen.getByText(`Withdrawn — ${long}`)
    wraps(el)
  })

  it('the status text beside the title can wrap too', () => {
    show({ ...money, required_layers: Array.from({ length: 8 }, (_, i) => `layer-with-a-long-name-${i}`), approvals: {} })
    const status = screen.getByText(/waiting for/)
    wraps(status)
  })
})
