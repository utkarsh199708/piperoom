import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import { PresencePanel } from './PresencePanel'
import { ActivityFeed } from './ActivityFeed'

describe('PresencePanel', () => {
  const members = [
    { id: 'kabir', name: 'Kabir', role: 'driver' },
    { id: 'arjun', name: 'Arjun', role: 'watching' },
    { id: 'meera', name: 'Meera', role: 'watching' },
  ]

  it('shows the count, the driver badge and a ringed driver avatar', () => {
    const { container } = render(
      <PresencePanel
        members={members}
        presence={[
          { id: 'kabir', name: 'Kabir', state: 'viewing' },
          { id: 'meera', name: 'Meera', state: 'away' },
        ]}
        selfId="arjun"
      />,
    )
    expect(screen.getByText("Who's here")).toBeInTheDocument()
    expect(container.querySelector('.panel__count')).toHaveTextContent('3')
    expect(screen.getAllByText('driver')).toHaveLength(1)
    expect(container.querySelectorAll('.avatar--driver')).toHaveLength(1)
    expect(screen.getByText('you')).toBeInTheDocument()
  })

  it('dims an away person and marks the dot hollow', () => {
    const { container } = render(
      <PresencePanel members={members} presence={[{ id: 'meera', name: 'Meera', state: 'away' }]} />,
    )
    const away = container.querySelectorAll('.who__row--away')
    expect(away).toHaveLength(1)
    expect(away[0]).toHaveTextContent('Meera')
    expect(away[0].querySelector('.who__dot--away')).not.toBeNull()
    expect(away[0]).toHaveTextContent('away')
  })

  it('says only you are here when nobody is listed', () => {
    render(<PresencePanel members={[]} presence={[]} />)
    expect(screen.getByText('Only you are here.')).toBeInTheDocument()
  })
})

describe('ActivityFeed', () => {
  it('is a polite live list with the agent avatar on agent rows', () => {
    const { container } = render(
      <ActivityFeed
        activity={[
          { seq: 1, at: '2026-10-11T10:00:00Z', actor: { kind: 'human', id: 'arjun', name: 'Arjun' }, text: 'Arjun took control' },
          { seq: 2, at: '2026-10-11T10:01:00Z', actor: { kind: 'agent', id: 'agent', name: 'Agent' }, text: 'Agent started step 1' },
        ]}
      />,
    )
    expect(container.querySelector('ul')).toHaveAttribute('aria-live', 'polite')
    expect(screen.getByText('Arjun took control')).toBeInTheDocument()
    expect(container.querySelectorAll('.avatar--agent')).toHaveLength(1)
    expect(container.querySelectorAll('.activity__row')).toHaveLength(2)
  })

  it('keeps the old empty text', () => {
    render(<ActivityFeed activity={[]} />)
    expect(screen.getByText('No activity yet.')).toBeInTheDocument()
  })
})
