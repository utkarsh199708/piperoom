import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'

vi.mock('@/api/client', async (importOriginal) => {
  const actual = await importOriginal<typeof import('@/api/client')>()
  return {
    ...actual,
    isFixtures: () => false,
    api: { ...actual.api, setupSecret: vi.fn(), comment: vi.fn() },
  }
})

import { api } from '@/api/client'
import { SecretField } from './SecretField'
import { setupState } from '../../test/setupRoom'
import { SETUP_ROOM_ID, secretSet } from '../../fixtures/setup'
import type { CurrentUser } from '../../state/permissions'

const setupSecret = vi.mocked(api.setupSecret)
const comment = vi.mocked(api.comment)

const driver: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['environment'], role: 'member' }
const watcher: CurrentUser = { id: 'priya', name: 'Priya', layers: [], role: 'member' }
const runner = { label: 'arjun-laptop', connected: true, tools: {} }

const room = () => setupState({ snapshot: { runner } })

describe('SecretField (U1-12b)', () => {
  it('posts the name and value to the secret route and never to a comment', async () => {
    setupSecret.mockResolvedValue(undefined)
    render(<SecretField state={room()} user={driver} />)
    fireEvent.change(screen.getByLabelText('Secret name'), { target: { value: 'DB_PASSWORD' } })
    fireEvent.change(screen.getByLabelText('Secret value'), { target: { value: 's3cr3t!' } })
    fireEvent.click(screen.getByRole('button', { name: 'Send secret' }))

    await waitFor(() =>
      expect(setupSecret).toHaveBeenCalledWith(SETUP_ROOM_ID, { name: 'DB_PASSWORD', value: 's3cr3t!' }),
    )
    expect(comment).not.toHaveBeenCalled()
    // The form clears, and the value is nowhere on the page or in the confirmation.
    expect(screen.getByLabelText('Secret value')).toHaveValue('')
    expect(await screen.findByText(/Secret DB_PASSWORD sent to the runner/)).toBeInTheDocument()
    expect(document.body.textContent).not.toContain('s3cr3t!')
    expect(setupSecret.mock.calls).toHaveLength(1)
  })

  it('refuses a value shorter than 6 bytes before any request', () => {
    render(<SecretField state={room()} user={driver} />)
    fireEvent.change(screen.getByLabelText('Secret name'), { target: { value: 'DB_PASSWORD' } })
    fireEvent.change(screen.getByLabelText('Secret value'), { target: { value: 'abc' } })
    fireEvent.click(screen.getByRole('button', { name: 'Send secret' }))
    expect(screen.getByText('A secret value is at least 6 bytes.')).toBeInTheDocument()
    expect(screen.getByLabelText('Secret value')).toHaveAttribute('aria-invalid', 'true')
    expect(setupSecret).not.toHaveBeenCalled()
  })

  it('refuses a name the server would refuse', () => {
    render(<SecretField state={room()} user={driver} />)
    fireEvent.change(screen.getByLabelText('Secret name'), { target: { value: '9password' } })
    fireEvent.change(screen.getByLabelText('Secret value'), { target: { value: 'long-enough' } })
    fireEvent.click(screen.getByRole('button', { name: 'Send secret' }))
    expect(screen.getByText(/A secret name starts with a letter or underscore/)).toBeInTheDocument()
    expect(setupSecret).not.toHaveBeenCalled()
  })

  it('shows the server’s refusal', async () => {
    setupSecret.mockRejectedValue(new Error('no_runner'))
    render(<SecretField state={room()} user={driver} />)
    fireEvent.change(screen.getByLabelText('Secret name'), { target: { value: 'DB_PASSWORD' } })
    fireEvent.change(screen.getByLabelText('Secret value'), { target: { value: 's3cr3t!' } })
    fireEvent.click(screen.getByRole('button', { name: 'Send secret' }))
    expect(await screen.findByRole('alert')).toHaveTextContent('no_runner')
    // The value stays in the form, so the driver can retry once the runner is back; it is not shown.
    expect(screen.getByLabelText('Secret value')).toHaveValue('s3cr3t!')
    expect(document.body.textContent).not.toContain('s3cr3t!')
  })

  it('lists only the names the room saw set, never a value', () => {
    setupSecret.mockResolvedValue(undefined)
    const state = setupState({ snapshot: { runner }, events: [secretSet('DB_PASSWORD', 3)] })
    render(<SecretField state={state} user={driver} />)
    expect(screen.getByText('DB_PASSWORD')).toBeInTheDocument()
  })

  it('waits for a runner before offering the field', () => {
    render(<SecretField state={setupState()} user={driver} />)
    expect(screen.getByText(/Pair a runner first/)).toBeInTheDocument()
    expect(screen.queryByLabelText('Secret value')).not.toBeInTheDocument()
  })

  it('is read-only for someone who is not the driver', () => {
    render(<SecretField state={room()} user={watcher} />)
    expect(screen.getByText(/Only the driver can send a secret/)).toBeInTheDocument()
    expect(screen.queryByLabelText('Secret value')).not.toBeInTheDocument()
  })
})
