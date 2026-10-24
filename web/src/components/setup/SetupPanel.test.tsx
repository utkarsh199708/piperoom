import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import { SetupPanel } from './SetupPanel'
import { setupState } from '../../test/setupRoom'
import { awaitingChange, rungList, verified } from '../../fixtures/setup'
import type { CurrentUser } from '../../state/permissions'

const driver: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['environment'], role: 'member' }
const runner = { label: 'arjun-laptop', connected: true, tools: {} }

describe('SetupPanel (U1-12b)', () => {
  it('places the controls the setup room needs, in one panel', () => {
    render(<SetupPanel state={setupState({ snapshot: { runner } })} user={driver} />)
    for (const name of ['Targets and classes', 'Pipeline', 'Maintenance window', 'Secret', 'Register the pipeline']) {
      expect(screen.getByRole('region', { name })).toBeInTheDocument()
    }
    // Canary and verified show only when their events exist.
    expect(screen.queryByRole('region', { name: /Make one change/ })).not.toBeInTheDocument()
    expect(screen.queryByRole('region', { name: /Verification/ })).not.toBeInTheDocument()
  })

  it('adds rung 8 and the verified card when the ladder is there', () => {
    const state = setupState({
      snapshot: { runner },
      events: [verified(rungList(), 'pl_1', 3), awaitingChange('public.orders', 'UPDATE public.orders SET note = 1', 4)],
    })
    render(<SetupPanel state={state} user={driver} />)
    expect(screen.getByRole('region', { name: /Make one change/ })).toBeInTheDocument()
    expect(screen.getByRole('region', { name: /Verification/ })).toBeInTheDocument()
  })

  it('renders nothing on an incident room', () => {
    const state = setupState({ snapshot: { runner } })
    delete state.setup
    const { container } = render(<SetupPanel state={state} user={driver} />)
    expect(container).toBeEmptyDOMElement()
  })
})
