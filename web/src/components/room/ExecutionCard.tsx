import type { DeliveredListener, SendOptions } from '../../hooks/useRoom'
import { CircleCheck, CircleX, LoaderCircle, TriangleAlert } from 'lucide-react'
import type { ProposalState } from '../../state/types'
import { executionPhase } from '../../lib/execution'
import { shortHash } from '../../lib/hash'
import { Badge } from '@/components/ui/badge'
import { Progress } from '@/components/ui/progress'
import { RollbackButton } from './RollbackButton'
import { cn } from '@/lib/utils'

function headerText(proposal: ProposalState): string {
  const { phase, step, of } = executionPhase(proposal)
  const action = proposal.steps[step - 1]?.action
  switch (phase) {
    case 'running':
      return `Running · step ${step} of ${of}${action ? ` · ${action}` : ''}`
    case 'done':
      return proposal.verification ? `Ran ${of} of ${of}` : `Ran ${of} of ${of} · verifying…`
    case 'failed':
      return `Stopped at step ${step} of ${of}`
    case 'unknown':
      return `Stopped at step ${step} of ${of} · outcome unknown`
    default:
      return ''
  }
}

// What happened to an approved proposal's steps, one row each. The header is the card's single
// live region. The "hash differs" badge and exec-hash test ids carry over from the old list.
export function ExecutionCard({
  proposal,
  rollback,
}: {
  proposal: ProposalState
  /** Absent for a viewer. The driver's steer goes to the agent; a member's is a suggestion. */
  rollback?: {
    asDriver: boolean
    onSteer: (text: string, opts?: SendOptions) => Promise<boolean>
    onDelivered?: (listener: DeliveredListener) => () => void
  }
}) {
  const info = executionPhase(proposal)
  if (proposal.executed.length === 0 && info.phase !== 'running') return null
  const byStep = new Map(proposal.executed.map((x) => [x.step_index, x]))
  const running = info.phase === 'running'
  const verifying = info.phase === 'done' && !proposal.verification
  return (
    <div className="flex flex-col gap-2 px-3.5 py-2.5 text-sm">
      <p
        aria-live="polite"
        className={cn('m-0 text-xs font-medium', running || verifying ? 'text-agent-foreground' : 'text-muted-foreground')}
      >
        {headerText(proposal)}
      </p>
      {(running || verifying) && <Progress aria-label="Execution in progress" className="h-0.5" />}
      <ol className="m-0 flex list-none flex-col gap-1.5 p-0">
        {Array.from({ length: Math.max(proposal.steps.length, ...proposal.executed.map((x) => x.step_index + 1)) }, (_, i) => {
          const s = proposal.steps[i]
          const ex = byStep.get(i)
          const isRunning = running && !ex && i === info.step - 1
          const differs = !!ex?.params_hash && !!proposal.params_hash && ex.params_hash !== proposal.params_hash
          return (
            <li key={i} className="flex flex-wrap items-center gap-2" data-testid={`exec-step-${i}`}>
              {ex?.outcome_unknown ? (
                <TriangleAlert className="size-4 text-warning-foreground" role="img" aria-label="outcome unknown" />
              ) : ex?.ok ? (
                <CircleCheck className="size-4 text-success-foreground motion-safe:animate-check" role="img" aria-label="succeeded" />
              ) : ex ? (
                <CircleX className="size-4 text-destructive-fg" role="img" aria-label="failed" />
              ) : isRunning ? (
                <LoaderCircle className="size-4 text-agent-foreground motion-safe:animate-spin" role="img" aria-label="running" />
              ) : (
                <span className="inline-flex size-4 items-center justify-center text-subtle-foreground" role="img" aria-label="not run yet">
                  ○
                </span>
              )}
              <span className="font-mono text-xs text-subtle-foreground">step {i + 1}</span>
              {s && <span className="font-mono text-xs">{s.action}</span>}
              {ex?.outcome_unknown ? (
                <span className="text-xs text-warning-foreground">Result unknown — check the target before retrying</span>
              ) : (
                ex?.output && <span className="min-w-0 break-words">{ex.output}</span>
              )}
              {ex?.params_hash && (
                <code
                  className="mono exec__hash rounded-md border border-input bg-muted px-1.5 py-0.5 font-mono text-xs"
                  data-testid={`exec-hash-${ex.step_index}`}
                  title={ex.params_hash}
                >
                  {shortHash(ex.params_hash)}
                </code>
              )}
              {differs && (
                <Badge variant="destructive" title="The step ran with a different params_hash than this proposal shows">
                  <TriangleAlert className="size-3" aria-hidden="true" />
                  hash differs from proposal
                </Badge>
              )}
            </li>
          )
        })}
      </ol>
      {rollback && (info.phase === 'done' || info.phase === 'failed' || info.phase === 'unknown') && (
        <div className="flex">
          <RollbackButton
            title={proposal.title}
            proposalId={proposal.proposal_id}
            rollback={proposal.rollback}
            asDriver={rollback.asDriver}
            onSteer={rollback.onSteer}
            onDelivered={rollback.onDelivered}
          />
        </div>
      )}
    </div>
  )
}
