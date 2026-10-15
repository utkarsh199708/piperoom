import { describe, expect, it } from 'vitest'
import { render, screen } from '@testing-library/react'
import { HypothesisCard } from './HypothesisCard'
import type { StepState } from '@/state/types'

// Real step id: "s_" and 26 characters of the ULID alphabet.
const STEP = 's_01M4Y9X0ZPZ0NPJW00JGWVY8VP'
const UNKNOWN = 's_11111111111111111111111111'
const steps: StepState[] = [{ id: STEP, title: 'connect_status', tool_calls: [], messages: [] }]

describe('HypothesisCard step links', () => {
  it('links a step id in the hypothesis to its step and keeps an unknown id as text', () => {
    render(
      <HypothesisCard
        hypothesis={{
          text: `Lag began at ${STEP}; ${UNKNOWN} is not a step`,
          based_on_steps: [],
          confidence: 'high',
          confirmed: false,
          seq: 1,
          verdicts: [],
        }}
        steps={steps}
      />,
    )
    expect(screen.getByRole('link', { name: 'step 1' })).toHaveAttribute('href', `#step-${STEP}`)
    expect(document.body.textContent).toContain(`Lag began at step 1; ${UNKNOWN} is not a step`)
    expect(document.body.textContent).not.toContain(STEP)
  })
})
