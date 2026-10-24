import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import type { ModelPanel, ModelRole, SupportedModel } from '../../api/types'
import { FirstRunModelGate } from './FirstRunModelGate'
import { ModelPopover } from './ModelPopover'
import { ModelSection } from './ModelSection'

const SECRET = 'sk-test-0123456789-DO-NOT-ECHO'

const role = (over: Partial<ModelRole> = {}): ModelRole => ({
  role: 'main',
  provider: 'openai',
  flavor: 'groq',
  model: 'openai/gpt-oss-120b',
  base_url_host: 'api.groq.com',
  status: 'supported',
  prompt_cache: false,
  price: { input: 0.15, output: 0.6 },
  key_set: true,
  ...over,
})

const supported: SupportedModel[] = [
  { provider: 'anthropic', model: 'claude-opus-5-5', status: 'supported', alias: false, pass_rate: 0.92, root_cause_rate: 0.9, cost_per_run_usd: 1.234, gated_at: '2026-11-01', sunset: '' },
  { provider: 'openai', flavor: 'ollama', model: 'qwen3:32b', status: 'supported', alias: false, pass_rate: null, root_cause_rate: null, cost_per_run_usd: null, gated_at: '', sunset: '' },
  { provider: 'openai', flavor: 'groq', model: 'llama-4-scout', status: 'experimental', alias: false, pass_rate: 0.5, root_cause_rate: 0.4, cost_per_run_usd: 0.05, gated_at: '', sunset: '' },
]

const panel = (over: Partial<ModelPanel> = {}): ModelPanel => ({
  mode: 'team',
  state: 'ready',
  roles: [role()],
  supported,
  ...over,
})
const noModel = (): ModelPanel => panel({ mode: 'standalone', state: 'no_model', roles: [] })

interface Call {
  method: string
  path: string
  body: string
  contentType: string | null
}
let calls: Call[]

/** Route fetch: GET /api/models answers `get`, PUT answers each of `puts` in turn. */
function stubApi(get: ModelPanel, puts: Array<{ status: number; body: unknown }> = []) {
  calls = []
  const queue = [...puts]
  vi.stubGlobal(
    'fetch',
    vi.fn(async (input: RequestInfo | URL, init: RequestInit = {}) => {
      const headers = new Headers(init.headers)
      calls.push({
        method: (init.method ?? 'GET').toUpperCase(),
        path: String(input),
        body: typeof init.body === 'string' ? init.body : '',
        contentType: headers.get('Content-Type'),
      })
      if ((init.method ?? 'GET').toUpperCase() === 'GET') return new Response(JSON.stringify(get), { status: 200 })
      const next = queue.shift() ?? { status: 200, body: panel({ mode: 'standalone' }) }
      return new Response(JSON.stringify(next.body), { status: next.status })
    }),
  )
}

const apiErr = (status: number, code: string, message: string) => ({ status, body: { error: { code, message } } })

beforeEach(() => {
  calls = []
})
afterEach(() => vi.unstubAllGlobals())

async function openFirstRun() {
  stubApi(noModel(), [])
  render(<FirstRunModelGate />)
  return await screen.findByRole('dialog', { name: /choose a model to get started/i })
}

describe('Model panel (read only)', () => {
  it('a team server shows provider, model, host, price and cache, and offers no picker', async () => {
    stubApi(panel({ roles: [role({ region: 'eu-west-1', key_set: true })] }))
    render(<ModelSection />)
    const row = await screen.findByTestId('model-role-main')
    expect(within(row).getByText('openai/gpt-oss-120b')).toBeInTheDocument()
    expect(within(row).getByText('openai (groq)')).toBeInTheDocument()
    expect(within(row).getByText('api.groq.com')).toBeInTheDocument()
    expect(within(row).getByText('eu-west-1')).toBeInTheDocument()
    expect(within(row).getByText('$0.15 in · $0.6 out per 1M tokens')).toBeInTheDocument()
    expect(within(row).getByText('Prompt cache').nextSibling).toHaveTextContent('No')
    expect(within(row).getByText('Supported')).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /change model|choose a model/i })).toBeNull()
    expect(document.querySelector('input[type="password"]')).toBeNull()
    expect(screen.getByText(/read only/i)).toBeInTheDocument()
  })

  it('shows the unverified model badge and an unpriced role', async () => {
    stubApi(panel({ roles: [role({ status: 'unverified', price: null })] }))
    render(<ModelSection />)
    const row = await screen.findByTestId('model-role-main')
    expect(within(row).getByText('Unverified model')).toBeInTheDocument()
    expect(within(row).getByText('Not priced')).toBeInTheDocument()
  })

  it('shows a sunset warning only when the role has a sunset date', async () => {
    stubApi(panel({ roles: [role({ sunset: '2027-03-01' }), role({ role: 'judge', model: 'other' })] }))
    render(<ModelSection />)
    const main = await screen.findByTestId('model-role-main')
    expect(within(main).getByRole('note')).toHaveTextContent('2027-03-01')
    expect(within(screen.getByTestId('model-role-judge')).queryByRole('note')).toBeNull()
  })

  it('never shows a base URL path, only the host the server sent', async () => {
    stubApi(panel({ roles: [role({ base_url_host: 'gw.example.com' })] }))
    render(<ModelSection />)
    await screen.findByTestId('model-role-main')
    expect(screen.getByText('gw.example.com')).toBeInTheDocument()
    expect(document.body.textContent).not.toMatch(/https?:\/\//)
  })
})

describe('Standalone first-run picker', () => {
  it('opens by itself on state no_model, cannot be dismissed, and lists pass rate and cost', async () => {
    const dialog = await openFirstRun()
    expect(within(dialog).queryByRole('button', { name: 'Close' })).toBeNull()
    expect(within(dialog).queryByRole('button', { name: 'Cancel' })).toBeNull()
    fireEvent.keyDown(dialog, { key: 'Escape' })
    expect(screen.getByRole('dialog')).toBeInTheDocument()

    const claude = within(dialog).getByRole('radio', { name: /claude-opus-5-5/ })
    expect(claude).toBeChecked()
    expect(within(dialog).getByText('Pass rate: 92%')).toBeInTheDocument()
    expect(within(dialog).getByText('Cost: $1.23 per run')).toBeInTheDocument()
    // null numbers are "not measured yet", never 0
    expect(within(dialog).getAllByText(/not measured yet/)).toHaveLength(2)
    // experimental stays behind a warning
    expect(within(dialog).queryByRole('radio', { name: /llama-4-scout/ })).toBeNull()
    fireEvent.click(within(dialog).getByRole('button', { name: /show experimental models/i }))
    expect(within(dialog).getByRole('note')).toHaveTextContent(/not passed the eval gate/i)
    expect(within(dialog).getByRole('radio', { name: /llama-4-scout/ })).toBeInTheDocument()
  })

  it('does not open on a team server or when a model is ready', async () => {
    stubApi(panel({ mode: 'standalone', state: 'ready' }))
    render(<FirstRunModelGate />)
    await waitFor(() => expect(calls.length).toBe(1))
    expect(screen.queryByRole('dialog')).toBeNull()
  })

  it('submits the choice to PUT /api/models as JSON with a password-type key field, then closes', async () => {
    stubApi(noModel(), [{ status: 200, body: panel({ mode: 'standalone', state: 'ready' }) }])
    render(<FirstRunModelGate />)
    const dialog = await screen.findByRole('dialog')
    const key = within(dialog).getByLabelText('API key') as HTMLInputElement
    expect(key.type).toBe('password')
    fireEvent.change(key, { target: { value: SECRET } })
    fireEvent.click(within(dialog).getByRole('button', { name: 'Use this model' }))
    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull())

    const puts = calls.filter((c) => c.method === 'PUT')
    expect(puts).toHaveLength(1)
    expect(puts[0].path).toBe('/api/models')
    expect(puts[0].contentType).toBe('application/json')
    expect(JSON.parse(puts[0].body)).toEqual({ provider: 'anthropic', model: 'claude-opus-5-5', key: SECRET })
  })

  it('shows 400 key_required next to the key field and keeps the dialog open', async () => {
    stubApi(noModel(), [apiErr(400, 'key_required', 'key required')])
    render(<FirstRunModelGate />)
    const dialog = await screen.findByRole('dialog')
    fireEvent.click(within(dialog).getByRole('button', { name: 'Use this model' }))
    const key = within(dialog).getByLabelText('API key')
    const alert = await within(dialog).findByRole('alert')
    expect(alert).toHaveTextContent(/enter the key/i)
    expect(key).toHaveAttribute('aria-invalid', 'true')
    expect(key.getAttribute('aria-describedby')).toContain(alert.id)
    await waitFor(() => expect(key).toHaveFocus())
    expect(screen.getByRole('dialog')).toBeInTheDocument()
  })

  it('shows 400 validation next to the field it names', async () => {
    stubApi(noModel(), [apiErr(400, 'validation', 'base_url: must be an http or https address')])
    render(<FirstRunModelGate />)
    const dialog = await screen.findByRole('dialog')
    fireEvent.change(within(dialog).getByLabelText(/base url/i), { target: { value: 'ftp://x' } })
    fireEvent.change(within(dialog).getByLabelText('API key'), { target: { value: SECRET } })
    fireEvent.click(within(dialog).getByRole('button', { name: 'Use this model' }))
    const alert = await within(dialog).findByRole('alert')
    expect(alert).toHaveTextContent('base_url: must be an http or https address')
    const url = within(dialog).getByLabelText(/base url/i)
    expect(url).toHaveAttribute('aria-invalid', 'true')
    expect(url.getAttribute('aria-describedby')).toContain(alert.id)
  })

  it('shows a model validation message next to the model list', async () => {
    stubApi(noModel(), [apiErr(400, 'validation', 'model: not supported on this provider')])
    render(<FirstRunModelGate />)
    const dialog = await screen.findByRole('dialog')
    fireEvent.click(within(dialog).getByRole('button', { name: 'Use this model' }))
    const alert = await within(dialog).findByRole('alert')
    expect(alert).toHaveTextContent('model: not supported on this provider')
    expect(alert.closest('fieldset')).not.toBeNull()
  })

  it('never echoes the key: emptied after submit, in no text, no storage, and only in the PUT body', async () => {
    stubApi(noModel(), [apiErr(400, 'validation', 'model: not supported on this provider')])
    render(<FirstRunModelGate />)
    const dialog = await screen.findByRole('dialog')
    const key = within(dialog).getByLabelText('API key') as HTMLInputElement
    fireEvent.change(key, { target: { value: SECRET } })
    fireEvent.click(within(dialog).getByRole('button', { name: 'Use this model' }))
    await within(dialog).findByRole('alert')

    expect(key.value).toBe('')
    expect(document.body.innerHTML).not.toContain(SECRET)
    expect(JSON.stringify({ ...localStorage })).not.toContain(SECRET)
    expect(JSON.stringify({ ...sessionStorage })).not.toContain(SECRET)
    // The only request that carries it is PUT /api/models (same-origin, relative path).
    const carrying = calls.filter((c) => c.body.includes(SECRET) || c.path.includes(SECRET))
    expect(carrying).toHaveLength(1)
    expect(carrying[0]).toMatchObject({ method: 'PUT', path: '/api/models' })
    expect(calls.every((c) => c.path.startsWith('/api/models'))).toBe(true)
  })

  it('asks for the key again when the provider changes: typed text is dropped', async () => {
    stubApi(noModel(), [])
    render(<FirstRunModelGate />)
    const dialog = await screen.findByRole('dialog')
    const key = within(dialog).getByLabelText('API key') as HTMLInputElement
    fireEvent.change(key, { target: { value: SECRET } })
    fireEvent.click(within(dialog).getByRole('radio', { name: /qwen3:32b/ }))
    expect(key.value).toBe('')
    expect(within(dialog).getByText(/asked again because the provider, flavor or address changed/i)).toBeInTheDocument()
  })

  it('a change of model only on the same provider keeps the stored key: the key may stay empty', async () => {
    const ready = panel({
      mode: 'standalone',
      roles: [role({ provider: 'anthropic', flavor: undefined, model: 'claude-opus-5-5', base_url_host: undefined })],
    })
    stubApi(ready, [{ status: 200, body: ready }])
    render(<ModelSection />)
    fireEvent.click(await screen.findByRole('button', { name: 'Change model' }))
    const dialog = await screen.findByRole('dialog', { name: 'Change the model' })
    expect(within(dialog).getByText(/leave empty to keep the key/i)).toBeInTheDocument()
    fireEvent.click(within(dialog).getByRole('button', { name: 'Use this model' }))
    await waitFor(() => expect(calls.some((c) => c.method === 'PUT')).toBe(true))
    expect(JSON.parse(calls.find((c) => c.method === 'PUT')!.body)).toEqual({ provider: 'anthropic', model: 'claude-opus-5-5' })
  })
})

describe('Model popover', () => {
  it('opens the read-only panel from the room header and Escape closes it, returning focus', async () => {
    stubApi(panel())
    render(<ModelPopover canEdit />)
    const button = screen.getByRole('button', { name: /model/i })
    expect(button).toHaveAttribute('aria-expanded', 'false')
    fireEvent.click(button)
    expect(button).toHaveAttribute('aria-expanded', 'true')
    expect(await screen.findByTestId('model-role-main')).toBeInTheDocument()
    fireEvent.keyDown(document, { key: 'Escape' })
    expect(screen.queryByRole('region', { name: 'Model' })).toBeNull()
    expect(button).toHaveFocus()
  })
})
