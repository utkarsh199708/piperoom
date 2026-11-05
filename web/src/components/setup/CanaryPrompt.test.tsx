import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { CanaryPrompt } from './CanaryPrompt'
import { setupState } from '../../test/setupRoom'
import { awaitingChange } from '../../fixtures/setup'

const runner = { label: 'arjun-laptop', connected: true, tools: {} }

function awaited(table = 'public.orders', sql?: string) {
  return setupState({
    snapshot: { runner },
    events: [awaitingChange(table, sql, 3)],
  })
}

describe('CanaryPrompt — rung 8 (U1-12b)', () => {
  it('names the table and shows the example change as data', () => {
    render(<CanaryPrompt state={awaited()} />)
    const card = screen.getByRole('region', { name: /Make one change/ })
    expect(card).toHaveTextContent('public.orders')
    expect(screen.getByLabelText('Example change')).toHaveTextContent("UPDATE public.orders SET note = 'ping' WHERE id = 1")
    expect(card).toHaveTextContent('Rung 8 is yours')
  })

  it('copies the statement with a visible confirmation', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    const sql = "UPDATE public.orders SET note = 'ping' WHERE id = 1"
    render(<CanaryPrompt state={awaited('public.orders', sql)} />)
    fireEvent.click(screen.getByRole('button', { name: 'Copy example change' }))
    await waitFor(() => expect(writeText).toHaveBeenCalledWith(sql))
    expect(await screen.findByText('Example change copied')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Copied example change' })).toBeInTheDocument()
  })

  it('renders untrusted text as text, never as markup', () => {
    render(<CanaryPrompt state={awaited('public.orders', '<script>alert(1)</script>')} />)
    expect(document.querySelector('script')).toBeNull()
    expect(screen.getByLabelText('Example change')).toHaveTextContent('<script>alert(1)</script>')
  })

  it('renders nothing when the ladder is not waiting on a change', () => {
    const { container } = render(<CanaryPrompt state={setupState({ snapshot: { runner } })} />)
    expect(container).toBeEmptyDOMElement()
  })
})
