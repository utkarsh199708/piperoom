import { createContext, Fragment, useContext, useMemo, type ReactNode } from 'react'
import { splitStepRefs, stepNumbers, type StepNumbers } from '@/lib/stepRefs'
import { goToStep, stepAnchor } from '@/lib/triage'
import type { StepState } from '@/state/types'

const NO_STEPS: StepNumbers = new Map()
const StepNumbersContext = createContext<StepNumbers>(NO_STEPS)

/**
 * Lets the agent prose under it name the room's steps: a step id in that prose
 * becomes a "step N" link to the step. Without a provider the prose is plain text.
 */
export function StepNumbersProvider({ steps, children }: { steps: readonly StepState[]; children: ReactNode }) {
  const numbers = useMemo(() => stepNumbers(steps), [steps])
  return <StepNumbersContext.Provider value={numbers}>{children}</StepNumbersContext.Provider>
}

/**
 * Agent prose as text. Step ids of the room are shown as "step N" links; everything
 * else, including markup-looking text, is rendered as text.
 */
export function AgentText({ text }: { text: string }) {
  const numbers = useContext(StepNumbersContext)
  return (
    <>
      {splitStepRefs(text, numbers).map((part, i) =>
        typeof part === 'string' ? (
          <Fragment key={i}>{part}</Fragment>
        ) : (
          <a
            key={i}
            href={`#${stepAnchor(part.id)}`}
 onClick={(e) => { e.preventDefault(); goToStep(part.id) }}
            className="rounded-sm font-medium text-foreground underline-offset-2 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
          >
            step {part.n}
          </a>
        ),
      )}
    </>
  )
}
