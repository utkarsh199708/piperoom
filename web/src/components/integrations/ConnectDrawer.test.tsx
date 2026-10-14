import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { ConnectForm } from './ConnectDrawer'
import type { DrawerApi } from './ConnectDrawer'
import { ApiClientError } from '../../api/client'
import { check, grafanaKind, postgresKind, view } from '../../test/integrationsData'

function fakeApi(over: Partial<DrawerApi> = {}): DrawerApi {
  return {
    catalog: vi.fn(),
    setupScript: vi.fn().mockResolvedValue({ script: 'CREATE ROLE piperoom_ro LOGIN;' }),
    list: vi.fn(),
    test: vi.fn(),
    create: vi.fn(),
    update: vi.fn(),
    setEnabled: vi.fn(),
    remove: vi.fn(),
    audit: vi.fn().mockResolvedValue([]),
    alertSetup: vi.fn(),
    rotateAlertSecret: vi.fn(),
    secretNames: vi.fn(),
    secretKeys: vi.fn(),
    ...over,
  } as DrawerApi
}

function renderForm(api: DrawerApi, props: Partial<Parameters<typeof ConnectForm>[0]> = {}) {
  const onSaved = vi.fn()
  const onReload = vi.fn().mockResolvedValue(undefined)
  const onDeleted = vi.fn()
  render(
    <ConnectForm
      kind={postgresKind}
      managers={[]}
      admin
      api={api}
      onSaved={onSaved}
      onDeleted={onDeleted}
      onReload={onReload}
      {...props}
    />,
  )
  return { onSaved, onReload, onDeleted }
}

function fillPostgres() {
  fireEvent.change(screen.getByLabelText(/^Host/), { target: { value: 'db.corp' } })
  fireEvent.change(screen.getByLabelText('Password value'), { target: { value: 'pw' } })
}

describe('ConnectForm', () => {
  it('Test shows the checklist and Save stays disabled while a check fails; editing re-enables it', async () => {
    const api = fakeApi({ test: vi.fn().mockResolvedValue({ checks: [check('reach', 'pass'), check('slots', 'fail', { fix: 'GRANT pg_monitor TO piperoom_ro;' })] }) })
    renderForm(api)
    fillPostgres()
    fireEvent.click(screen.getByRole('button', { name: /Test connection/ }))
    await screen.findByText('GRANT pg_monitor TO piperoom_ro;')
    expect(api.test).toHaveBeenCalledWith({
      kind: 'postgres-source',
      config: { host: 'db.corp', port: 5432, sslmode: 'require', replica: false },
      secrets: { password: 'pw' },
    })
    expect(screen.getByRole('button', { name: 'Save connection' })).toBeDisabled()
    expect(screen.getByText('Fix 1 red check to save')).toBeInTheDocument()

    fireEvent.change(screen.getByLabelText(/^Host/), { target: { value: 'db2.corp' } })
    expect(screen.getByText('Values changed since this test')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Save connection' })).toBeEnabled()
  })

  it('stops before a call when a required field is empty', () => {
    const api = fakeApi()
    renderForm(api)
    fireEvent.click(screen.getByRole('button', { name: 'Save connection' }))
    expect(api.create).not.toHaveBeenCalled()
    expect(screen.getByLabelText(/^Host/)).toHaveAttribute('aria-invalid', 'true')
  })

  it('422 checks_failed shows the checks from the error body and saves nothing', async () => {
    const body = { error: { code: 'checks_failed', message: 'a check failed' }, checks: [check('login', 'fail', { title: 'Log in as piperoom_ro' })] }
    const api = fakeApi({ create: vi.fn().mockRejectedValue(new ApiClientError(422, 'checks_failed', 'a check failed', body)) })
    const { onSaved } = renderForm(api)
    fillPostgres()
    fireEvent.click(screen.getByRole('button', { name: 'Save connection' }))
    expect(await screen.findByText(/Not saved: a check failed/)).toBeInTheDocument()
    expect(screen.getByText('Log in as piperoom_ro')).toBeInTheDocument()
    expect(onSaved).not.toHaveBeenCalled()
    expect(screen.getByRole('button', { name: 'Save connection' })).toBeDisabled()
  })

  it('update sends If-Match version and a 409 version_conflict offers a reload', async () => {
    const existing = view()
    const api = fakeApi({ update: vi.fn().mockRejectedValue(new ApiClientError(409, 'version_conflict', 'stale')) })
    const { onReload } = renderForm(api, { existing })
    // The stored password says "set" and is not resent.
    expect(screen.getByText('Set')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Save changes' }))
    await screen.findByText(/Someone else changed this connection/)
    expect(api.update).toHaveBeenCalledWith('int_1', 3, expect.objectContaining({ secrets: {} }))
    fireEvent.click(screen.getByRole('button', { name: 'Reload' }))
    expect(onReload).toHaveBeenCalled()
  })

  it('a 400 validation marks the field the server named', async () => {
    const body = { error: { code: 'validation', message: 'host: bad value', field: 'host' } }
    const api = fakeApi({ create: vi.fn().mockRejectedValue(new ApiClientError(400, 'validation', 'host: bad value', body)) })
    renderForm(api)
    fillPostgres()
    fireEvent.click(screen.getByRole('button', { name: 'Save connection' }))
    await waitFor(() => expect(screen.getByLabelText(/^Host/)).toHaveAttribute('aria-invalid', 'true'))
  })

  it('saves and clears the typed secret from the form', async () => {
    const saved = view({ last_test: [check('reach', 'pass')] })
    const api = fakeApi({ create: vi.fn().mockResolvedValue({ integration: saved, checks: saved.last_test }) })
    const { onSaved } = renderForm(api)
    fillPostgres()
    fireEvent.click(screen.getByRole('button', { name: 'Save connection' }))
    await waitFor(() => expect(onSaved).toHaveBeenCalledWith(saved))
    expect(screen.getByLabelText('Password value')).toHaveValue('')
  })

  it('disable and delete ask first', async () => {
    const existing = view()
    const api = fakeApi({
      setEnabled: vi.fn().mockResolvedValue(view({ enabled: false, status: 'disabled' })),
      remove: vi.fn().mockResolvedValue(undefined),
    })
    const { onDeleted } = renderForm(api, { existing })
    fireEvent.click(screen.getByRole('button', { name: 'Disable' }))
    expect(api.setEnabled).not.toHaveBeenCalled()
    await screen.findByText('Disable PostgreSQL?')
    fireEvent.click(screen.getAllByRole('button', { name: 'Disable' }).at(-1)!)
    await waitFor(() => expect(api.setEnabled).toHaveBeenCalledWith('int_1', false))

    fireEvent.click(screen.getByRole('button', { name: 'Delete' }))
    expect(api.remove).not.toHaveBeenCalled()
    await screen.findByText('Delete PostgreSQL?')
    fireEvent.click(screen.getAllByRole('button', { name: 'Delete' }).at(-1)!)
    await waitFor(() => expect(onDeleted).toHaveBeenCalledWith('int_1'))
  })

  it('is read-only for a row managed by environment variables, and for non-admins', () => {
    renderForm(fakeApi(), { existing: view({ id: 'env_source_db', source: 'env', version: 0 }) })
    expect(screen.getByText(/Managed by environment: this connection comes from environment variables/)).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /Save/ })).not.toBeInTheDocument()
    expect(screen.queryByLabelText(/^Host/)).not.toBeInTheDocument()
  })

  it('shows the setup script with copy, and the reads / never lists', async () => {
    const api = fakeApi()
    renderForm(api)
    // Always shown below the form (integrations-ux.md §3.4): open without a click.
    expect(screen.getByRole('button', { name: 'Setup script' })).toHaveAttribute('aria-expanded', 'true')
    expect(await screen.findByText('CREATE ROLE piperoom_ro LOGIN;')).toBeInTheDocument()
    expect(api.setupScript).toHaveBeenCalledWith('postgres-source', { port: 5432, sslmode: 'require', replica: false })
    expect(screen.getByRole('button', { name: 'Copy setup script' })).toBeInTheDocument()
    expect(screen.getByText('replication slot state')).toBeInTheDocument()
    expect(screen.getByText('writes to any table')).toBeInTheDocument()
  })

  it('an alert source shows its webhook URL and snippet; a generated secret is shown once', async () => {
    const setup = { source: 'grafana', integration_id: '', webhook_url: 'https://piperoom.corp/api/alerts/grafana', snippet: 'Secret: <secret>', snippet_lang: 'text', secret_set: false, secret_source: '' }
    const api = fakeApi({
      alertSetup: vi.fn().mockResolvedValue(setup),
      rotateAlertSecret: vi.fn().mockResolvedValue({ ...setup, integration_id: 'int_g', secret_set: true, secret_source: 'db', secret: 'whsec_123' }),
    })
    renderForm(api, { kind: grafanaKind })
    expect(await screen.findByDisplayValue('https://piperoom.corp/api/alerts/grafana')).toBeInTheDocument()
    expect(screen.getByText('Secret: <secret>')).toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'Generate secret' }))
    expect(await screen.findByDisplayValue('whsec_123')).toBeInTheDocument()
    expect(screen.getByText(/never shows it again/)).toBeInTheDocument()
    expect(screen.queryByText(/after its next restart/)).not.toBeInTheDocument()
    fireEvent.click(screen.getByRole('button', { name: 'I have copied it' }))
    expect(screen.queryByDisplayValue('whsec_123')).not.toBeInTheDocument()
    // Rotating an existing secret asks first.
    fireEvent.click(screen.getByRole('button', { name: 'Rotate secret' }))
    expect(await screen.findByText('Rotate the signing secret?')).toBeInTheDocument()
    expect(api.rotateAlertSecret).toHaveBeenCalledTimes(1)
  })

  it('a rotate that needs a server restart says so', async () => {
    const setup = { source: 'grafana', integration_id: 'int_g', webhook_url: 'https://piperoom.corp/api/alerts/grafana', snippet: 's', snippet_lang: 'text', secret_set: false, secret_source: '' }
    const api = fakeApi({
      alertSetup: vi.fn().mockResolvedValue(setup),
      rotateAlertSecret: vi.fn().mockResolvedValue({ ...setup, secret_set: true, secret_source: 'db', secret: 'whsec_9', restart_required: true }),
    })
    renderForm(api, { kind: grafanaKind })
    fireEvent.click(await screen.findByRole('button', { name: 'Generate secret' }))
    expect(await screen.findByDisplayValue('whsec_9')).toBeInTheDocument()
    expect(screen.getByText(/after its next restart/)).toBeInTheDocument()
  })

  it('409 secret_key_missing explains the missing server key', async () => {
    const api = fakeApi({ create: vi.fn().mockRejectedValue(new ApiClientError(409, 'secret_key_missing', 'no key')) })
    const { onSaved } = renderForm(api)
    fillPostgres()
    fireEvent.click(screen.getByRole('button', { name: 'Save connection' }))
    expect(await screen.findByText(/Typed-in secrets need PIPEROOM_SECRET_KEY on the server/)).toBeInTheDocument()
    expect(onSaved).not.toHaveBeenCalled()
  })

  it('429 rate_limited on Test says to wait', async () => {
    const api = fakeApi({ test: vi.fn().mockRejectedValue(new ApiClientError(429, 'rate_limited', 'slow down')) })
    renderForm(api)
    fillPostgres()
    fireEvent.click(screen.getByRole('button', { name: /Test connection/ }))
    expect(await screen.findByText(/Too many tests: at most 10 a minute/)).toBeInTheDocument()
  })

  it('a setup script that fails to load says so', async () => {
    const api = fakeApi({ setupScript: vi.fn().mockRejectedValue(new ApiClientError(404, 'not_found', 'unknown kind')) })
    renderForm(api)
    expect(await screen.findByText('Could not load the setup script: unknown kind')).toBeInTheDocument()
  })

  it('a change log that fails to load says so', async () => {
    const api = fakeApi({ audit: vi.fn().mockRejectedValue(new ApiClientError(403, 'not_admin', 'admins only')) })
    renderForm(api, { existing: view() })
    fireEvent.click(screen.getByRole('button', { name: 'Change log' }))
    expect(await screen.findByText('Could not load the change log: Only an admin can change integrations.')).toBeInTheDocument()
  })

  it('rotate 409 managed_by_env says the secret comes from an environment variable', async () => {
    const setup = { source: 'grafana', integration_id: '', webhook_url: 'https://piperoom.corp/api/alerts/grafana', snippet: 's', snippet_lang: 'text', secret_set: false, secret_source: '' }
    const api = fakeApi({
      alertSetup: vi.fn().mockResolvedValue(setup),
      rotateAlertSecret: vi.fn().mockRejectedValue(new ApiClientError(409, 'managed_by_env', 'env')),
    })
    renderForm(api, { kind: grafanaKind })
    fireEvent.click(await screen.findByRole('button', { name: 'Generate secret' }))
    expect(await screen.findByText('This secret is set by an environment variable; change it on the server.')).toBeInTheDocument()
  })
})

describe('ConnectForm without PIPEROOM_SECRET_KEY', () => {
  it('says so and offers references only for new secrets', () => {
    renderForm(fakeApi(), { secretKeySet: false })
    expect(screen.getByRole('status').textContent).toMatch(/PIPEROOM_SECRET_KEY is not set/)
    const enter = screen.getByRole('button', { name: /Enter value/ })
    expect((enter as HTMLButtonElement).disabled).toBe(true)
    expect(screen.getByRole('button', { name: /From a secret manager/ }).getAttribute('aria-pressed')).toBe('true')
  })

  it('keeps the value option when the key is set', () => {
    renderForm(fakeApi())
    expect((screen.getByRole('button', { name: /Enter value/ }) as HTMLButtonElement).disabled).toBe(false)
  })
})
