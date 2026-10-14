import { describe, expect, it, vi } from 'vitest'
import { useState } from 'react'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { SecretField } from './SecretField'
import type { SecretsLookup } from './SecretField'
import { ApiClientError } from '../../api/client'
import type { SecretFieldState } from '../../lib/integrations'
import { secretWrite } from '../../lib/integrations'
import type { Integration } from '../../api/integrations'
import { field, vaultRow } from '../../test/integrationsData'

const pw = field('password', 'secret', { required: true, help: 'The role password' })
const optionalPw = field('password', 'secret', { help: 'The role password' })

function Harness({
  initial,
  managers = [vaultRow],
  lookup,
  onState,
  f = pw,
}: {
  f?: typeof pw
  initial: Partial<SecretFieldState>
  managers?: Integration[]
  lookup?: SecretsLookup
  onState?: (s: SecretFieldState) => void
}) {
  const [state, setState] = useState<SecretFieldState>({ mode: 'value', value: '', ref: '', stored: false, hasStored: false, clear: false, ...initial })
  return (
    <SecretField
      field={f}
      state={state}
      managers={managers}
      lookup={lookup ?? { secretNames: vi.fn(), secretKeys: vi.fn() }}
      onChange={(s) => {
        setState(s)
        onState?.(s)
      }}
    />
  )
}

describe('SecretField toggle', () => {
  it('switches between "From a secret manager" and "Enter value"', () => {
    render(<Harness initial={{ mode: 'value' }} />)
    const manager = screen.getByRole('button', { name: 'From a secret manager' })
    const value = screen.getByRole('button', { name: 'Enter value' })
    expect(value).toHaveAttribute('aria-pressed', 'true')
    expect(screen.getByLabelText('Password value')).toBeInTheDocument()

    fireEvent.click(manager)
    expect(manager).toHaveAttribute('aria-pressed', 'true')
    expect(screen.queryByLabelText('Password value')).not.toBeInTheDocument()
    expect(screen.getByLabelText('Secret manager')).toHaveValue('vault-corp')
    expect(screen.getByLabelText(/Reference/)).toBeInTheDocument()
  })

  it('a stored typed-in value shows "Set", never the value, and blank keeps it', () => {
    const onState = vi.fn()
    render(<Harness initial={{ mode: 'value', stored: true }} onState={onState} />)
    expect(screen.getByText('Set')).toBeInTheDocument()
    const input = screen.getByLabelText('Password value')
    expect(input).toHaveValue('')
    expect(input).toHaveAttribute('placeholder', expect.stringMatching(/keep the stored value/))
    // Required field: no "remove" option.
    expect(screen.queryByLabelText('Remove the stored value')).not.toBeInTheDocument()
    fireEvent.change(input, { target: { value: 'new' } })
    expect(secretWrite(onState.mock.calls.at(-1)![0])).toBe('new')
  })

  it('without a connected manager, a secret:// reference can still be pasted', () => {
    const onState = vi.fn()
    render(<Harness initial={{ mode: 'manager' }} managers={[]} onState={onState} />)
    expect(screen.queryByLabelText('Secret manager')).not.toBeInTheDocument()
    fireEvent.change(screen.getByLabelText('Paste a secret:// reference'), { target: { value: 'secret://files/etc/pw' } })
    expect(secretWrite(onState.mock.calls.at(-1)![0])).toEqual({ $ref: 'secret://files/etc/pw' })
  })

  it('finds names under a prefix, picks one, then a key: the reference is built from them', async () => {
    const lookup: SecretsLookup = {
      secretNames: vi.fn().mockResolvedValue({ names: ['database/creds/piperoom-ro', 'database/other'], list_supported: true }),
      secretKeys: vi.fn().mockResolvedValue({ keys: ['username', 'password'] }),
    }
    const onState = vi.fn()
    render(<Harness initial={{ mode: 'manager' }} lookup={lookup} onState={onState} />)
    fireEvent.change(screen.getByLabelText('Secret name'), { target: { value: 'database/' } })
    fireEvent.click(screen.getByRole('button', { name: 'Find' }))
    expect(lookup.secretNames).toHaveBeenCalledWith('int_vault', 'database/')
    fireEvent.click(await screen.findByRole('button', { name: 'database/creds/piperoom-ro' }))
    expect(lookup.secretKeys).toHaveBeenCalledWith('int_vault', 'secret://vault-corp/database/creds/piperoom-ro')
    const key = await screen.findByLabelText('Key')
    fireEvent.change(key, { target: { value: 'password' } })
    expect(onState.mock.calls.at(-1)![0].ref).toBe('secret://vault-corp/database/creds/piperoom-ro#password')
    expect(screen.getByLabelText(/Reference/)).toHaveValue('secret://vault-corp/database/creds/piperoom-ro#password')
  })

  it('a manager that cannot list says so; a manager that is down shows an error', async () => {
    const lookup: SecretsLookup = {
      secretNames: vi
        .fn()
        .mockResolvedValueOnce({ names: [], list_supported: false })
        .mockRejectedValueOnce(new ApiClientError(502, 'provider_unreachable', 'down')),
      secretKeys: vi.fn(),
    }
    render(<Harness initial={{ mode: 'manager' }} lookup={lookup} />)
    fireEvent.click(screen.getByRole('button', { name: 'Find' }))
    expect(await screen.findByText(/cannot list secrets/)).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Find' }))
    expect(await screen.findByRole('alert')).toHaveTextContent(/did not answer/)
  })

  it('a pasted reference fills the pickers, also for a provider that is not a connected manager', async () => {
    render(<Harness initial={{ mode: 'manager' }} />)
    fireEvent.change(screen.getByLabelText(/Reference/), { target: { value: 'secret://aws-env/prod/db#pw' } })
    await waitFor(() => expect(screen.getByLabelText('Secret manager')).toHaveValue('aws-env'))
    expect(screen.getByRole('option', { name: /aws-env \(not a connected manager\)/ })).toBeInTheDocument()
    expect(screen.getByLabelText('Secret name')).toHaveValue('prod/db')
    // Browsing needs a connected manager.
    expect(screen.getByRole('button', { name: 'Find' })).toBeDisabled()
  })

  it('an optional field held as a reference can be removed: the write is null', () => {
    const onState = vi.fn()
    render(<Harness f={optionalPw} initial={{ mode: 'manager', ref: 'secret://vault-corp/db#pw', hasStored: true }} onState={onState} />)
    fireEvent.click(screen.getByLabelText('Remove the stored value'))
    expect(secretWrite(onState.mock.calls.at(-1)![0])).toBeNull()
    expect(screen.getByLabelText(/Reference/)).toBeDisabled()
  })

  it('a new field without a stored value offers no remove option', () => {
    render(<Harness f={optionalPw} initial={{ mode: 'manager' }} />)
    expect(screen.queryByLabelText('Remove the stored value')).not.toBeInTheDocument()
  })

  it('help text is linked to the input only, not repeated on the fieldset', () => {
    render(<Harness initial={{ mode: 'value' }} />)
    expect(screen.getByLabelText('Password value')).toHaveAccessibleDescription('The role password')
    expect(screen.getByRole('group', { name: /^Password/ })).not.toHaveAttribute('aria-describedby')
  })

  it('a key lookup during a Find does not leave Find stuck busy; typing a name fetches no keys', async () => {
    let resolveNames: (v: { names: string[]; list_supported: boolean }) => void = () => {}
    const lookup: SecretsLookup = {
      secretNames: vi.fn().mockReturnValue(new Promise((r) => (resolveNames = r))),
      secretKeys: vi.fn().mockResolvedValue({ keys: ['password'] }),
    }
    render(<Harness initial={{ mode: 'manager' }} lookup={lookup} />)
    const name = screen.getByLabelText('Secret name')
    fireEvent.change(name, { target: { value: 'data' } })
    fireEvent.blur(name)
    expect(lookup.secretKeys).not.toHaveBeenCalled()
    fireEvent.click(screen.getByRole('button', { name: 'Find' }))
    expect(screen.getByRole('button', { name: 'Finding…' })).toBeDisabled()
    // A full reference pasted while Find runs looks up keys.
    fireEvent.change(screen.getByLabelText(/Reference/), { target: { value: 'secret://vault-corp/database/x' } })
    await screen.findByLabelText('Key')
    resolveNames({ names: ['database/x'], list_supported: true })
    expect(await screen.findByRole('button', { name: 'Find' })).toBeEnabled()
  })

  it('a manager that cannot list can still look up keys of a typed name', async () => {
    const lookup: SecretsLookup = {
      secretNames: vi.fn().mockResolvedValue({ names: [], list_supported: false }),
      secretKeys: vi.fn().mockResolvedValue({ keys: ['username', 'password'] }),
    }
    render(<Harness initial={{ mode: 'manager' }} lookup={lookup} />)
    fireEvent.change(screen.getByLabelText('Secret name'), { target: { value: 'prod/db' } })
    fireEvent.click(screen.getByRole('button', { name: 'Find' }))
    fireEvent.click(await screen.findByRole('button', { name: 'look up its keys' }))
    expect(lookup.secretKeys).toHaveBeenCalledWith('int_vault', 'secret://vault-corp/prod/db')
    expect(await screen.findByLabelText('Key')).toBeInTheDocument()
  })
})
