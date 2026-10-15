import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import { CollapsibleText } from './CollapsibleText'

describe('CollapsibleText', () => {
  // jsdom has no layout: this pins the classes that keep the clamp inside its column at 390 px
  // (QA T7: one long token made the text 701 px wide in a 330 px column).
  it('caps long text at its column width so it wraps instead of overflowing', () => {
    const text = 'x'.repeat(400)
    const { container } = render(<CollapsibleText text={text} />)
    const outer = container.firstElementChild as HTMLElement
    expect(outer).toHaveClass('max-w-full', 'min-w-0')
    expect(screen.getByText(text)).toHaveClass('max-w-full', 'break-words', 'line-clamp-3')
  })
})
