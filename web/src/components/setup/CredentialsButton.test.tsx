import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'

vi.mock('@/api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/api/client')>()
  return {
    ...actual,
    isFixtures: () => false,
    api: { ...actual.api, setupCredentials: vi.fn(), comment: vi.fn(), setupSecret: vi.fn() },
  }
})

import { api } from '@/api/client'
import { CredentialsButton } from './CredentialsButton'
import { setupState } from '../../test/setupRoom'
import { SETUP_ROOM_ID, credentialsRegistered } from '../../fixtures/setup'
import type { CurrentUser } from '../../state/permissions'

const setupCredentials = vi.mocked(api.setupCredentials)
const comment = vi.mocked(api.comment)
const setupSecret = vi.mocked(api.setupSecret)

const driver: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['environment'], role: 'member' }
const watcher: CurrentUser = { id: 'priya', name: 'Priya', layers: [], role: 'member' }
const runner = { label: 'arjun-laptop', connected: true, tools: {} }

const room = () => setupState({ snapshot: { runner } })

describe('CredentialsButton (U1-12b)', () => {
  it('only asks the runner: one argument, no body, and says to confirm on its terminal', async () => {
    setupCredentials.mockResolvedValue(undefined)
    render(<CredentialsButton state={room()} user={driver} />)
    fireEvent.click(screen.getByRole('button', { name: 'Register read credentials' }))

    await waitFor(() => expect(setupCredentials).toHaveBeenCalledTimes(1))
    // One argument: the room id. There is no second argument and so no request body.
    expect(setupCredentials.mock.calls[0]).toEqual([SETUP_ROOM_ID])
    expect(comment).not.toHaveBeenCalled()
    expect(setupSecret).not.toHaveBeenCalled()
    expect(await screen.findByText(/confirm on the runner's terminal/i)).toBeInTheDocument()
  })

  it('cannot be asked before a runner is paired', () => {
    render(<CredentialsButton state={setupState()} user={driver} />)
    expect(screen.getByRole('button', { name: 'Register read credentials' })).toBeDisabled()
    expect(screen.getByText(/Pair a runner first/)).toBeInTheDocument()
  })

  it('shows the server’s refusal when the ask fails', async () => {
    setupCredentials.mockRejectedValue(new Error('no_runner'))
    render(<CredentialsButton state={room()} user={driver} />)
    fireEvent.click(screen.getByRole('button', { name: 'Register read credentials' }))
    expect(await screen.findByRole('alert')).toHaveTextContent('no_runner')
  })

  it('shows the names the runner registered, never a value', () => {
    const state = setupState({
      snapshot: { runner },
      events: [credentialsRegistered(['piperoom_ro', 'piperoom_ro_kafka'], 'arjun-laptop', 3)],
    })
    render(<CredentialsButton state={state} user={driver} />)
    expect(screen.getByText('piperoom_ro')).toBeInTheDocument()
    expect(screen.getByText('piperoom_ro_kafka')).toBeInTheDocument()
    expect(screen.getByText(/from arjun-laptop/)).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Register read credentials' })).not.toBeInTheDocument()
  })

  it('is not offered to someone who is not the driver', () => {
    render(<CredentialsButton state={room()} user={watcher} />)
    expect(screen.getByText(/Only the driver can ask the runner/)).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Register read credentials' })).not.toBeInTheDocument()
  })

  it('renders nothing on an incident room', () => {
    const state = room()
    delete state.setup
    const { container } = render(<CredentialsButton state={state} user={driver} />)
    expect(container).toBeEmptyDOMElement()
  })
})
