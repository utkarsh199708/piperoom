import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import { PresencePanel } from './PresencePanel'

const members = [
  { id: 'arjun', name: 'Arjun', role: 'driver' as const },
  { id: 'kabir', name: 'Kabir', role: 'watching' as const },
]
const people = [{ id: 'arjun', name: 'Arjun', verified: true, layers: ['kafka-connect', 'kafka', 'schema-registry', 'consumer'] }]

describe('PresencePanel', () => {
  it('shows a Driver chip in an open room', () => {
    render(<PresencePanel members={members} presence={[]} selfId="arjun" selfName="Arjun" people={people} />)
    expect(screen.getByText('Driver')).toBeInTheDocument()
  })

  it('shows no Driver chip in a closed room (Q3 S9)', () => {
    render(<PresencePanel members={members} presence={[]} selfId="arjun" selfName="Arjun" people={people} closed />)
    expect(screen.queryByText('Driver')).toBeNull()
  })

  it('lets the layer chips wrap instead of overflowing the rail (Q3 S9)', () => {
    render(<PresencePanel members={members} presence={[]} selfId="arjun" selfName="Arjun" people={people} />)
    const chip = screen.getByLabelText('owns kafka-connect')
    expect(chip.parentElement?.className).toMatch(/flex-wrap/)
  })
})
