import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'

vi.mock('@/api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/api/client')>()
  return { ...actual, isFixtures: () => false, api: { ...actual.api, setupCredentials: vi.fn() } }
})

import { api } from '@/api/client'
import { RegistrationCard } from './RegistrationCard'
import { setupState } from '../../test/setupRoom'
import { SETUP_ROOM_ID, pipelineRegistered } from '../../fixtures/setup'
import type { CurrentUser } from '../../state/permissions'
import type { RoomState } from '../../state/types'
import type { SetupTargetSummary } from '../../api/types'

const setupCredentials = vi.mocked(api.setupCredentials)

const driver: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['environment'], role: 'member' }
const runner = { label: 'arjun-laptop', connected: true, tools: {} }
const productionTarget: SetupTargetSummary = { kind: 'postgres', address: 'db.acme:5432', class: 'production' }
const localTarget: SetupTargetSummary = { kind: 'postgres', address: '127.0.0.1:5432', class: 'local' }

function room(opts: { solo?: boolean; targets?: SetupTargetSummary[] } = {}): RoomState {
  const state = setupState({
    snapshot: { runner, targets: opts.targets ?? [productionTarget] },
  })
  return { ...state, mode: { write: true, solo: opts.solo ?? false, auto_approve: [] } }
}

describe('RegistrationCard (U1-12b)', () => {
  it('says who approves on a team server, and where it will be watched', () => {
    render(<RegistrationCard state={room()} user={driver} />)
    const card = screen.getByRole('region', { name: 'Register the pipeline' })
    expect(screen.getByRole('heading', { name: 'Who approves' })).toBeInTheDocument()
    expect(card).toHaveTextContent('A tenant admin, or the owners of a pipeline that overlaps this one')
    expect(card).toHaveTextContent('always someone other than the person asking')
    expect(card).toHaveTextContent("The driver's own approval does not count on a team server.")
    expect(screen.getByRole('heading', { name: 'Where it will be watched' })).toBeInTheDocument()
    expect(card).toHaveTextContent('Watched by this server once it is registered')
    expect(card).toHaveTextContent('db.acme:5432 · production')
  })

  it('lets the driver approve alone in solo mode', () => {
    render(<RegistrationCard state={room({ solo: true })} user={driver} />)
    expect(screen.getByRole('region', { name: 'Register the pipeline' })).toHaveTextContent(
      'You approve it: this server is in solo mode',
    )
  })

  it('says a local pipeline is watched locally, and registers no credentials with a team server', () => {
    render(<RegistrationCard state={room({ targets: [localTarget] })} user={driver} />)
    const card = screen.getByRole('region', { name: 'Register the pipeline' })
    expect(card).toHaveTextContent('Watched locally while piperoom-setup runs — a team server cannot reach 127.0.0.1.')
    expect(card).toHaveTextContent('.piperoom')
    expect(screen.queryByRole('button', { name: 'Register read credentials' })).not.toBeInTheDocument()
  })

  it('offers a disabled Register pipeline button and invents no call', () => {
    render(<RegistrationCard state={room()} user={driver} />)
    const register = screen.getByRole('button', { name: 'Register pipeline' })
    expect(register).toBeDisabled()
    const description = register.getAttribute('aria-describedby')
    expect(description).toBeTruthy()
    expect(document.getElementById(description as string)).toHaveTextContent('Not available in this build')
    expect(document.getElementById(description as string)).toHaveTextContent('U1-14')
    fireEvent.click(register)
    expect(setupCredentials).not.toHaveBeenCalled()
  })

  it('its Register read credentials button only asks the runner and says where to confirm', async () => {
    setupCredentials.mockResolvedValue(undefined)
    render(<RegistrationCard state={room()} user={driver} />)
    fireEvent.click(screen.getByRole('button', { name: 'Register read credentials' }))
    await waitFor(() => expect(setupCredentials).toHaveBeenCalledTimes(1))
    expect(setupCredentials.mock.calls[0]).toEqual([SETUP_ROOM_ID])
    expect(await screen.findByText(/confirm on the runner's terminal/i)).toBeInTheDocument()
  })

  it('shows the registered pipeline and where it is watched once pipeline.registered lands', () => {
    const base = room()
    const state = setupState({
      snapshot: { runner, targets: [productionTarget] },
      events: [pipelineRegistered('server', 3)],
    })
    render(<RegistrationCard state={{ ...state, mode: base.mode }} user={driver} />)
    const card = screen.getByRole('region', { name: /Register the pipeline/ })
    expect(card).toHaveTextContent('registered')
    expect(card).toHaveTextContent('Watched by this server.')
    expect(screen.queryByRole('button', { name: 'Register pipeline' })).not.toBeInTheDocument()
  })
})
