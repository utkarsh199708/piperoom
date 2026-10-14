import { describe, expect, it } from 'vitest'
import { render } from '@testing-library/react'
import { Kbd } from './kbd'

describe('Kbd', () => {
  it('carries the data-slot the coarse-pointer CSS hides', () => {
    const { container } = render(<Kbd>H</Kbd>)
    expect(container.querySelector('kbd')?.getAttribute('data-slot')).toBe('kbd')
  })
})
