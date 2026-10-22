import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'

// The menu exists only in a dev build (AppShell also loads it behind
// import.meta.env.DEV). The component's own isDev gate is what the tests toggle.
const flags = vi.hoisted(() => ({ dev: true }))
vi.mock('@/lib/env', () => ({
  get isDev() {
    return flags.dev
  },
}))

import { BreakSomethingMenu } from './BreakSomethingMenu'

const fetchMock = vi.fn()

function jsonResponse(status: number, body: unknown): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

function deferred<T>() {
  let resolve!: (v: T) => void
  let reject!: (e: unknown) => void
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

const scenariosResponse = (...names: string[]) => jsonResponse(200, { scenarios: names })

function openMenu() {
  render(<BreakSomethingMenu />)
  fireEvent.keyDown(screen.getByRole('button', { name: 'Break something' }), { key: 'Enter' })
}

function postsTo(url: string) {
  return fetchMock.mock.calls.filter(([u, init]) => u === url && (init as RequestInit)?.method === 'POST')
}

beforeEach(() => {
  flags.dev = true
  fetchMock.mockReset()
  vi.stubGlobal('fetch', fetchMock)
})

afterEach(() => {
  vi.unstubAllGlobals()
})

describe('BreakSomethingMenu gate', () => {
  it('renders nothing and makes no request when isDev is false', () => {
    flags.dev = false
    render(<BreakSomethingMenu />)
    expect(screen.queryByRole('button', { name: 'Break something' })).not.toBeInTheDocument()
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('renders the trigger when isDev is true', () => {
    render(<BreakSomethingMenu />)
    expect(screen.getByRole('button', { name: 'Break something' })).toBeInTheDocument()
    // The list is fetched lazily: not until the menu is opened.
    expect(fetchMock).not.toHaveBeenCalled()
  })
})

describe('BreakSomethingMenu scenarios', () => {
  it('lists scenarios with Break and Fix buttons', async () => {
    fetchMock.mockImplementation((url: string) =>
      url === '/devctl/scenarios'
        ? Promise.resolve(scenariosResponse('schema-change', 'connector-down'))
        : Promise.reject(new Error(`unexpected ${url}`)),
    )
    openMenu()
    expect(await screen.findByText('schema-change')).toBeInTheDocument()
    expect(screen.getByText('connector-down')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Break schema-change' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Fix schema-change' })).toBeInTheDocument()
  })

  it('renders scenario names as text, not HTML', async () => {
    fetchMock.mockResolvedValue(scenariosResponse('<b>schema</b>'))
    const { container } = render(<BreakSomethingMenu />)
    fireEvent.keyDown(screen.getByRole('button', { name: 'Break something' }), { key: 'Enter' })
    expect(await screen.findByText('<b>schema</b>')).toBeInTheDocument()
    expect(container.querySelector('b')).toBeNull()
  })
})

describe('BreakSomethingMenu when devctl serve is down', () => {
  // What the Vite proxy now answers for a refused connection, and what it used
  // to answer before the fix (an empty 500). Both must be actionable.
  it.each([
    ['the 503 the proxy plugin writes', jsonResponse(503, { error: 'devctl_not_running' })],
    ['an empty 500 from the proxy', new Response(null, { status: 500 })],
  ])('explains how to start devctl on %s', async (_label, response) => {
    fetchMock.mockResolvedValue(response)
    openMenu()
    expect(await screen.findByText('devctl serve is not running.')).toBeInTheDocument()
    expect(screen.getByText('go run ./cmd/devctl serve')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Retry' })).toBeInTheDocument()
    expect(screen.queryByText(/Could not load scenarios/)).not.toBeInTheDocument()
  })

  it('shows the same hint when the request itself fails (dev server proxy gone)', async () => {
    fetchMock.mockRejectedValue(new TypeError('Failed to fetch'))
    openMenu()
    expect(await screen.findByText('devctl serve is not running.')).toBeInTheDocument()
    expect(screen.getByText('go run ./cmd/devctl serve')).toBeInTheDocument()
  })

  it('copies the start command', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    fetchMock.mockResolvedValue(jsonResponse(503, { error: 'devctl_not_running' }))
    openMenu()
    fireEvent.click(await screen.findByRole('button', { name: 'Copy start command' }))
    expect(writeText).toHaveBeenCalledWith('go run ./cmd/devctl serve')
    expect(await screen.findByRole('button', { name: 'Copied start command' })).toBeInTheDocument()
  })

  it('Retry reloads and shows the scenarios once devctl is up', async () => {
    fetchMock.mockResolvedValueOnce(jsonResponse(503, { error: 'devctl_not_running' }))
    openMenu()
    await screen.findByText('devctl serve is not running.')
    fetchMock.mockResolvedValueOnce(scenariosResponse('schema-change'))
    fireEvent.click(screen.getByRole('button', { name: 'Retry' }))
    expect(await screen.findByRole('button', { name: 'Break schema-change' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Fix schema-change' })).toBeInTheDocument()
    expect(screen.queryByText('devctl serve is not running.')).not.toBeInTheDocument()
  })

  it('keeps the plain error (with its message) for a real non-5xx answer', async () => {
    fetchMock.mockResolvedValue(jsonResponse(404, { error: 'no such route' }))
    openMenu()
    expect(await screen.findByText(/Could not load scenarios: no such route/)).toBeInTheDocument()
    expect(screen.queryByText('devctl serve is not running.')).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Retry' })).toBeInTheDocument()
  })
})

describe('BreakSomethingMenu actions', () => {
  it('clicking Break posts one POST to /devctl/break/{name}', async () => {
    fetchMock.mockImplementation((url: string, init?: RequestInit) => {
      if (url === '/devctl/scenarios') return Promise.resolve(scenariosResponse('schema-change'))
      if (url === '/devctl/break/schema-change' && init?.method === 'POST') {
        return Promise.resolve(jsonResponse(200, { status: 'ok', scenario: 'schema-change', action: 'break' }))
      }
      return Promise.reject(new Error(`unexpected ${url}`))
    })
    openMenu()
    fireEvent.click(await screen.findByRole('button', { name: 'Break schema-change' }))
    await screen.findByText('schema-change') // the list stays put after the run
    expect(postsTo('/devctl/break/schema-change')).toHaveLength(1)
  })

  it('disables Break and Fix while a run is in flight, until it resolves', async () => {
    const run = deferred<Response>()
    fetchMock.mockImplementation((url: string, init?: RequestInit) => {
      if (url === '/devctl/scenarios') return Promise.resolve(scenariosResponse('schema-change'))
      if (init?.method === 'POST') return run.promise
      return Promise.reject(new Error(`unexpected ${url}`))
    })
    openMenu()
    const breakBtn = await screen.findByRole('button', { name: 'Break schema-change' })
    const fixBtn = screen.getByRole('button', { name: 'Fix schema-change' })
    fireEvent.click(breakBtn)
    expect(breakBtn).toBeDisabled()
    expect(fixBtn).toBeDisabled()
    run.resolve(jsonResponse(200, { status: 'ok', scenario: 'schema-change', action: 'break' }))
    await vi.waitFor(() => expect(breakBtn).not.toBeDisabled())
    expect(fixBtn).not.toBeDisabled()
  })

  it('shows busy on a 409 without crashing', async () => {
    fetchMock.mockImplementation((url: string, init?: RequestInit) => {
      if (url === '/devctl/scenarios') return Promise.resolve(scenariosResponse('schema-change'))
      if (init?.method === 'POST') return Promise.resolve(jsonResponse(409, { status: 'busy' }))
      return Promise.reject(new Error(`unexpected ${url}`))
    })
    openMenu()
    fireEvent.click(await screen.findByRole('button', { name: 'Break schema-change' }))
    expect(await screen.findByText(/busy/i)).toBeInTheDocument()
    expect(screen.getByText('schema-change')).toBeInTheDocument()
  })

  it.each([
    ['404 unknown scenario', 404, { status: 'error', error: 'unknown scenario nope' }, /unknown scenario nope/],
    ['500 run failure', 500, { status: 'error', error: 'boom' }, /boom/],
  ])('shows an error on %s without crashing', async (_label, status, body, matcher) => {
    fetchMock.mockImplementation((url: string, init?: RequestInit) => {
      if (url === '/devctl/scenarios') return Promise.resolve(scenariosResponse('schema-change'))
      if (init?.method === 'POST') return Promise.resolve(jsonResponse(status, body))
      return Promise.reject(new Error(`unexpected ${url}`))
    })
    openMenu()
    fireEvent.click(await screen.findByRole('button', { name: 'Break schema-change' }))
    expect(await screen.findByText(matcher)).toBeInTheDocument()
    expect(screen.getByText('schema-change')).toBeInTheDocument()
  })

  it('shows an error on a network failure without crashing', async () => {
    fetchMock.mockImplementation((url: string, init?: RequestInit) => {
      if (url === '/devctl/scenarios') return Promise.resolve(scenariosResponse('schema-change'))
      if (init?.method === 'POST') return Promise.reject(new TypeError('Failed to fetch'))
      return Promise.reject(new Error(`unexpected ${url}`))
    })
    openMenu()
    fireEvent.click(await screen.findByRole('button', { name: 'Break schema-change' }))
    expect(await screen.findByText(/Failed to fetch/)).toBeInTheDocument()
    expect(screen.getByText('schema-change')).toBeInTheDocument()
  })
})
