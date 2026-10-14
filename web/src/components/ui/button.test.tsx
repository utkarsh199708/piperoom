import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import { Button } from './button'

// QA9: on a phone Approve, Reject, the nav, Invite, the theme toggle, Trace and Suggest were under
// 44px. Every Button gets a 44px floor below 600px, even when a screen sets a smaller h-*.
describe('Button touch targets', () => {
  it('has a 44px minimum height below 600px, over a caller height', () => {
    render(
      <Button size="sm" className="h-7">
        Approve
      </Button>,
    )
    expect(screen.getByRole('button', { name: 'Approve' })).toHaveClass('h-7', 'max-[599px]:min-h-11')
  })

  it('an icon button is 44px wide too', () => {
    render(<Button size="icon" aria-label="Invite" />)
    expect(screen.getByRole('button', { name: 'Invite' })).toHaveClass('max-[599px]:min-h-11', 'max-[599px]:min-w-11')
  })
})
