import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import type { StepState } from '@/state/types'
import { AgentText, StepNumbersProvider } from './AgentText'

// Real step ids: "s_" and 26 characters of the ULID alphabet.
const A = 's_01M4Y9X0ZPZ0NPJW00JGWVY8VP'
const B = 's_01M4Y9X0ZPZ0NPJW00JGWVY8VQ'
const UNKNOWN = 's_11111111111111111111111111'

const steps: StepState[] = [
  { id: A, title: 'connect_status', tool_calls: [], messages: [] },
  { id: B, title: 'pg_recent_ddl', tool_calls: [], messages: [] },
]

// Renders the prose in a paragraph, under the room's steps unless withSteps is false.
function prose(text: string, withSteps = true) {
  const body = (
    <p>
      <AgentText text={text} />
    </p>
  )
  const { container } = render(withSteps ? <StepNumbersProvider steps={steps}>{body}</StepNumbersProvider> : body)
  const p = container.querySelector('p')
  if (!p) throw new Error('no paragraph rendered')
  return p
}

describe('AgentText', () => {
  it('shows a known step id as a link to that step, by its place in the room', () => {
    const p = prose(`Checked ${B} first.`)
    expect(screen.getByRole('link', { name: 'step 2' })).toHaveAttribute('href', `#step-${B}`)
    expect(p).toHaveTextContent('Checked step 2 first.')
    expect(p).not.toHaveTextContent(B)
  })

  it('keeps an unknown id as plain text', () => {
    const p = prose(`Look at ${UNKNOWN}.`)
    expect(screen.queryByRole('link')).toBeNull()
    expect(p).toHaveTextContent(`Look at ${UNKNOWN}.`)
  })

  it('links several ids, each to its own step, and keeps the unknown one as text', () => {
    const p = prose(`${A} then ${UNKNOWN} then ${B}`)
    const links = screen.getAllByRole('link')
    expect(links.map((l) => l.textContent)).toEqual(['step 1', 'step 2'])
    expect(links.map((l) => l.getAttribute('href'))).toEqual([`#step-${A}`, `#step-${B}`])
    expect(p).toHaveTextContent(`then ${UNKNOWN} then`)
  })

  it('renders markup in the prose as text, never as elements', () => {
    const p = prose(`<img src=x onerror="alert(1)"> after ${A}`)
    expect(p.querySelector('img')).toBeNull()
    expect(p).toHaveTextContent('<img src=x onerror="alert(1)"> after step 1')
  })

  it('is plain text when no room provides its steps', () => {
    const p = prose(`see ${A}`, false)
    expect(screen.queryByRole('link')).toBeNull()
    expect(p).toHaveTextContent(`see ${A}`)
  })
})
