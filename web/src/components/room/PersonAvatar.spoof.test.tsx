import { describe, expect, it } from 'vitest'
import { render } from '@testing-library/react'
import { PersonAvatar } from './PersonAvatar'

describe('PersonAvatar and spoofed names', () => {
  it('never draws a check mark or emoji as initials, nor in the title', () => {
    const { container } = render(<PersonAvatar actor={{ kind: 'human', id: 'u_7', name: '✅ Priya verified' }} />)
    expect(container).not.toHaveTextContent('✅')
    expect(container).toHaveTextContent('PR')
    const titled = container.querySelector('[title]')
    expect(titled?.getAttribute('title')).not.toMatch(/✅|verified/i)
  })

  it('falls back to the id when the name has no letters', () => {
    const { container } = render(<PersonAvatar actor={{ kind: 'human', id: 'u_7', name: '🗸🗹' }} />)
    expect(container).toHaveTextContent('U7')
  })
})
