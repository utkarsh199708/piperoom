import { createContext, Fragment, useContext, useMemo, type ReactNode } from 'react'
import { splitStepRefs, stepNumbers, type StepNumbers } from '@/lib/stepRefs'
import { goToStep, stepAnchor } from '@/lib/triage'
import type { StepState } from '@/state/types'

const NO_STEPS: StepNumbers = new Map()
const StepNumbersContext = createContext<StepNumbers>(NO_STEPS)
const StepFollowContext = createContext<(() => void) | undefined>(undefined)

/**
 * Lets the agent prose under it name the room's steps: a step id in that prose
 * becomes a "step N" link to the step. Without a provider the prose is plain text.
 * `onFollow` runs before a link scrolls, for a dialog that covers the page (QA R17):
 * it closes the dialog, and the step is shown after it has closed.
 */
export function StepNumbersProvider({
  steps,
  onFollow,
  children,
}: {
  steps: readonly StepState[]
  onFollow?: () => void
  children: ReactNode
}) {
  const numbers = useMemo(() => stepNumbers(steps), [steps])
  return (
    <StepNumbersContext.Provider value={numbers}>
      <StepFollowContext.Provider value={onFollow}>{children}</StepFollowContext.Provider>
    </StepNumbersContext.Provider>
  )
}

function followStep(stepId: string, onFollow: (() => void) | undefined): void {
  if (!onFollow) {
    goToStep(stepId)
    return
  }
  onFollow()
  // The dialog's overlay hides the page until it has unmounted, so the scroll waits for that.
  window.setTimeout(() => goToStep(stepId), 0)
}

/**
 * Agent prose as text. Step ids of the room are shown as "step N" links; everything
 * else, including markup-looking text, is rendered as text.
 */
export function AgentText({ text }: { text: string }) {
  const numbers = useContext(StepNumbersContext)
  const onFollow = useContext(StepFollowContext)
  return (
    <>
      {splitStepRefs(text, numbers).map((part, i) =>
        typeof part === 'string' ? (
          <Fragment key={i}>{part}</Fragment>
        ) : (
          <a
            key={i}
            href={`#${stepAnchor(part.id)}`}
            onClick={(e) => {
              e.preventDefault()
              followStep(part.id, onFollow)
            }}
            className="rounded-sm font-medium text-foreground underline-offset-2 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
          >
            step {part.n}
          </a>
        ),
      )}
    </>
  )
}
