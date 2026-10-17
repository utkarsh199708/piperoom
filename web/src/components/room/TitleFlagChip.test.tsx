import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import { TitleFlagChip } from './TitleFlagChip'

describe('TitleFlagChip', () => {
  it('shows nothing for a clean title', () => {
    for (const flags of [undefined, null, []]) {
      const { container, unmount } = render(<TitleFlagChip flags={flags} />)
      expect(container).toBeEmptyDOMElement()
      unmount()
    }
  })

  it('warns with a chip whose tooltip names the flags', () => {
    render(<TitleFlagChip flags={['instruction_override', 'role_play']} />)
    expect(screen.getByText('suspicious alert text')).toHaveAttribute(
      'title',
      'Flagged in the alert text: instruction_override, role_play',
    )
  })
})
