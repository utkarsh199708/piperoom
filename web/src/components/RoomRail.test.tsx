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
    expect(within(meera).getByText('away')).toBeInTheDocument()
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
