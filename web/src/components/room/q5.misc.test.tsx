import { fireEvent, render, screen } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { AgentText, StepNumbersProvider } from './AgentText'
import { CollapsibleText } from './CollapsibleText'
import { splitProposalRefs } from '@/lib/stepRefs'
import { REVEAL_PROPOSAL_EVENT } from '@/lib/triage'
import { plainText } from '@/lib/title'

const P = 'p_01M4Y9X0ZPZ0NPJW00JGWVY8VP'
const UNKNOWN = 'p_11111111111111111111111111'

afterEach(() => vi.useRealTimers())

describe('Q5 T12: proposal ids in agent prose', () => {
  it('splits a known id and leaves an unknown one as text', () => {
    expect(splitProposalRefs(`see ${P} and ${UNKNOWN}`, [P])).toEqual(['see ', { proposalId: P, n: 1 }, ` and ${UNKNOWN}`])
  })

  it('renders a link to the proposal card that asks the room to show it', () => {
    const seen = vi.fn()
    window.addEventListener(REVEAL_PROPOSAL_EVENT, seen)
    render(
      <StepNumbersProvider steps={[]} proposalIds={[P]}>
        <AgentText text={`Approve ${P} now`} />
      </StepNumbersProvider>,
    )
    fireEvent.click(screen.getByRole('link', { name: 'proposal 1' }))
    expect(seen).toHaveBeenCalledTimes(1)
    window.removeEventListener(REVEAL_PROPOSAL_EVENT, seen)
  })

  it('is plain text without a provider', () => {
    render(<AgentText text={`Approve ${P}`} />)
    expect(screen.queryByRole('link')).toBeNull()
  })
})

describe('Q5 T7: CollapsibleText fits a narrow column', () => {
  it('lets the clamp span shrink and wrap', () => {
    const { container } = render(<CollapsibleText text={'x'.repeat(400)} />)
    const root = container.firstElementChild as HTMLElement
    expect(root.className).toContain('w-full')
    expect(root.className).toContain('min-w-0')
    const clamp = root.firstElementChild as HTMLElement
    expect(clamp.className).toContain('min-w-0')
    expect(clamp.className).toContain('max-w-full')
    expect(clamp.className).toContain('[overflow-wrap:anywhere]')
  })
})

describe('Q5 T8: bidi override characters', () => {
  it('plainText removes them, with no length limit when asked', () => {
    expect(plainText('orders ‮gal‬ missing', 80)).not.toMatch(/[‪-‮⁦-⁩]/)
    expect(plainText('a⁦b', Infinity)).toBe('a b')
  })
})
