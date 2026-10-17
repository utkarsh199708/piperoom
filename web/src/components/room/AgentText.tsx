import { createContext, Fragment, useContext, useMemo, type ReactNode } from 'react'
import { splitProposalRefs, splitStepRefs, stepNumbers, type StepNumbers } from '@/lib/stepRefs'
import { goToProposal, goToStep, proposalCardAnchor, stepAnchor } from '@/lib/triage'
import type { StepState } from '@/state/types'

const NO_STEPS: StepNumbers = new Map()
const StepNumbersContext = createContext<StepNumbers>(NO_STEPS)
const NO_PROPOSALS: readonly string[] = []
const ProposalIdsContext = createContext<readonly string[]>(NO_PROPOSALS)
const StepFollowContext = createContext<(() => void) | undefined>(undefined)

/**
 * Lets the agent prose under it name the room's steps: a step id in that prose
 * becomes a "step N" link to the step. Without a provider the prose is plain text.
 * `onFollow` runs before a link scrolls, for a dialog that covers the page (QA R17):
 * it closes the dialog, and the step is shown after it has closed.
 */
export function StepNumbersProvider({
  steps,
  proposalIds,
  onFollow,
  children,
}: {
  steps: readonly StepState[]
  /** Proposal ids of the room, in creation order: a raw id in the prose becomes a "proposal N" link (T12). */
  proposalIds?: readonly string[]
  onFollow?: () => void
  children: ReactNode
}) {
  const numbers = useMemo(() => stepNumbers(steps), [steps])
  return (
    <StepNumbersContext.Provider value={numbers}>
      <ProposalIdsContext.Provider value={proposalIds ?? NO_PROPOSALS}>
        <StepFollowContext.Provider value={onFollow}>{children}</StepFollowContext.Provider>
      </ProposalIdsContext.Provider>
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
  const proposalIds = useContext(ProposalIdsContext)
  const link = 'rounded-sm font-medium text-foreground underline-offset-2 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring'
  return (
    <>
      {splitStepRefs(text, numbers).map((part, i) =>
        typeof part === 'string' ? (
          <Fragment key={i}>
            {splitProposalRefs(part, proposalIds).map((q, j) =>
              typeof q === 'string' ? (
                <Fragment key={j}>{q}</Fragment>
              ) : (
                <a
                  key={j}
                  href={`#${proposalCardAnchor(q.proposalId)}`}
                  onClick={(e) => {
                    e.preventDefault()
                    onFollow?.()
                    goToProposal(q.proposalId)
                  }}
                  className={link}
                >
                  proposal {q.n}
                </a>
              ),
            )}
          </Fragment>
        ) : (
          <a
            key={i}
            href={`#${stepAnchor(part.id)}`}
            onClick={(e) => {
              e.preventDefault()
              followStep(part.id, onFollow)
            }}
            className={link}
          >
            step {part.n}
          </a>
        ),
      )}
    </>
  )
}
