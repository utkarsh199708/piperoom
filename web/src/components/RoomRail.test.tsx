import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, within } from '@testing-library/react'
import { PresencePanel } from './PresencePanel'
import { ActivityFeed } from './ActivityFeed'
import { PROPOSAL_ANCHOR, ProposalsSummary } from './room/ProposalsSummary'
import type { ActivityEntry, MemberState, ProposalState } from '../state/types'

const members: MemberState[] = [
  { id: 'kabir', name: 'Kabir', role: 'driver' },
  { id: 'arjun', name: 'Arjun', role: 'watching' },
  { id: 'meera', name: 'Meera', role: 'watching' },
]

describe('PresencePanel (Who’s here)', () => {
  it('lists people with a count, the Driver badge, "you" and away', () => {
    render(
      <PresencePanel
        members={members}
        presence={[
          { id: 'kabir', name: 'Kabir', state: 'viewing' },
          { id: 'meera', name: 'Meera', state: 'away' },
        ]}
        selfId="arjun"
      />,
    )
    const region = screen.getByRole('region', { name: /here/ })
    expect(within(region).getByText('3')).toBeInTheDocument()
    const kabir = within(region).getByText('Kabir').closest('li') as HTMLElement
    expect(within(kabir).getByText('Driver')).toBeInTheDocument()
    expect(within(kabir).queryByText('you')).not.toBeInTheDocument()
    const arjun = within(region).getByText('Arjun').closest('li') as HTMLElement
    expect(within(arjun).getByText('you')).toBeInTheDocument()
    const meera = within(region).getByText('Meera').closest('li') as HTMLElement
    expect(within(meera).getByText('away')).toBeInTheDocument() // no since: plain away
  })

  it('adds someone who is present but not a member, as watching', () => {
    render(<PresencePanel members={[]} presence={[{ id: 'rohan', name: 'Rohan', state: 'viewing' }]} />)
    expect(screen.getByText('Rohan')).toBeInTheDocument()
    expect(screen.queryByText('Driver')).not.toBeInTheDocument()
  })

  it('a viewer sees themself marked viewer', () => {
    render(<PresencePanel members={members} presence={[]} selfId="meera" selfIsViewer />)
    expect(screen.getByText('viewer')).toBeInTheDocument()
  })
})

describe('PresencePanel layers and state (F9)', () => {
  const people = [
    { id: 'priya', name: 'Priya', layers: ['postgres', 'schema-registry', 'kafka', 'consumer'] },
    { id: 'arjun', name: 'Arjun', layers: ['kafka-connect'] },
  ]

  it('shows layer badges (at most 3, then +N) and away from the server-side since', () => {
    const since = new Date(Date.now() - 12 * 60_000).toISOString()
    render(
      <PresencePanel
        members={members}
        presence={[
          { id: 'kabir', name: 'Kabir', state: 'viewing' },
          { id: 'priya', name: 'Priya', state: 'away', since },
        ]}
        people={people}
        selfId="kabir"
      />,
    )
    const priya = screen.getByText('Priya').closest('li') as HTMLElement
    expect(within(priya).getByLabelText('owns postgres')).toBeInTheDocument()
    expect(within(priya).getByLabelText('owns kafka')).toBeInTheDocument()
    expect(within(priya).queryByLabelText('owns consumer')).not.toBeInTheDocument()
    expect(within(priya).getByText('+1')).toHaveAttribute('title', 'consumer')
    expect(within(priya).getByText('away 12m')).toBeInTheDocument()
  })

  it('says "not here" for a member with no presence entry and "here" for a viewing one', () => {
    render(<PresencePanel members={members} presence={[{ id: 'kabir', name: 'Kabir', state: 'viewing' }]} people={people} selfId="kabir" />)
    const arjun = screen.getByText('Arjun').closest('li') as HTMLElement
    expect(within(arjun).getByText('not here')).toBeInTheDocument()
    expect(within(arjun).getByLabelText('owns kafka-connect')).toBeInTheDocument()
    const kabir = screen.getByText('Kabir').closest('li') as HTMLElement
    expect(within(kabir).getByText('here')).toBeInTheDocument()
  })

  it('shows no layer badges without people (a viewer)', () => {
    render(<PresencePanel members={members} presence={[]} selfId="kabir" />)
    expect(screen.queryByLabelText(/^owns /)).not.toBeInTheDocument()
  })
})

describe('ActivityFeed', () => {
  const activity: ActivityEntry[] = [
    {
      seq: 2,
      at: '2026-10-11T10:02:00Z',
      actor: { kind: 'human', id: 'priya', name: 'Priya' },
      text: 'Priya commented on step 3',
    },
    {
      seq: 1,
      at: '2026-10-11T10:00:00Z',
      actor: { kind: 'system', id: 'alertin', name: 'Alertmanager' },
      text: 'Alertmanager opened the room',
    },
  ]

  it('lists what happened with a time for each', () => {
    render(<ActivityFeed activity={activity} />)
    const list = screen.getByRole('list')
    expect(within(list).getAllByRole('listitem')).toHaveLength(2)
    expect(screen.getByText('Priya commented on step 3')).toBeInTheDocument()
    expect(list.querySelectorAll('time')).toHaveLength(2)
  })

  it('says so when there is nothing yet', () => {
    render(<ActivityFeed activity={[]} />)
    expect(screen.getByText('No activity yet.')).toBeInTheDocument()
  })
})

const proposal = (over: Partial<ProposalState> = {}): ProposalState => ({
  proposal_id: 'p1',
  title: 'Set subject compatibility to NONE, then restart the connector task',
  steps: [],
  risk: 'low',
  rollback: 'undo',
  required_layers: ['schema-registry', 'kafka-connect'],
  approvals: { 'schema-registry': 'approved', 'kafka-connect': 'pending' },
  decisions: {},
  params_hash: 'abc',
  status: 'open',
  executed: [],
  ...over,
})

describe('ProposalsSummary', () => {
  it('with none says no fix is proposed yet', () => {
    render(<ProposalsSummary proposals={[]} />)
    expect(screen.getByText(/No fix proposed yet/)).toBeInTheDocument()
  })

  it('in a shadow room says the agent proposes no fix (it never will)', () => {
    render(<ProposalsSummary proposals={[]} shadow />)
    expect(screen.getByText(/Shadow room: the agent proposes no fix/)).toBeInTheDocument()
    expect(screen.queryByText(/No fix proposed yet/)).not.toBeInTheDocument()
  })

  it('summarises an open proposal: who we wait for and how many approved', () => {
    render(<ProposalsSummary proposals={[proposal()]} />)
    expect(screen.getByText('1 open')).toBeInTheDocument()
    expect(screen.getByText('Waiting for kafka-connect')).toBeInTheDocument()
    expect(screen.getByText('1 of 2')).toBeInTheDocument()
  })

  it('an approved proposal reads Approved', () => {
    render(
      <ProposalsSummary
        proposals={[
          proposal({ status: 'approved', approvals: { 'schema-registry': 'approved', 'kafka-connect': 'approved' } }),
        ]}
      />,
    )
    expect(screen.getByText('Approved')).toBeInTheDocument()
    expect(screen.getByText('2 of 2')).toBeInTheDocument()
  })

  it('jumps to the proposal on the page without touching the hash route', () => {
    const target = document.createElement('div')
    target.id = PROPOSAL_ANCHOR
    target.tabIndex = -1
    target.scrollIntoView = vi.fn()
    document.body.appendChild(target)
    const before = window.location.hash
    render(<ProposalsSummary proposals={[proposal()]} />)
    fireEvent.click(screen.getByRole('button'))
    expect(target.scrollIntoView).toHaveBeenCalled()
    expect(target).toHaveFocus()
    expect(window.location.hash).toBe(before)
    target.remove()
  })
})
