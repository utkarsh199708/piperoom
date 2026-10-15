import type { ProposalState } from '@/state/types'
import { cn } from '@/lib/utils'
import { StatusDot } from './StatusDot'
import type { DotTone } from './StatusDot'

/** The id the full proposal card (ProposalsPanel) is wrapped in, so the rail can jump to it. */
export const PROPOSAL_ANCHOR = 'proposal'

function describe(p: ProposalState): { tone: DotTone; word: string; count: string } {
  const total = p.required_layers.length
  const done = p.required_layers.filter((l) => p.approvals[l] === 'approved').length
  const count = total > 0 ? `${done} of ${total}` : ''
  if (p.status === 'approved') return { tone: 'ok', word: 'Approved', count }
  if (p.status === 'rejected') return { tone: 'fail', word: 'Rejected', count }
  // Q0: ended without running; it waits on nobody and is not counted as open.
  if (p.status === 'withdrawn') return { tone: 'muted', word: 'Withdrawn', count: '' }
  const waiting = p.required_layers.find((l) => p.approvals[l] === 'pending')
  return { tone: 'warn', word: waiting ? `Waiting for ${waiting}` : 'Open', count }
}

// A proposal is not a link target in the URL (the app uses the hash for its own routes),
// so the card scrolls the full proposal into view and moves focus there.
function jumpToProposal() {
  const el = document.getElementById(PROPOSAL_ANCHOR)
  if (!el) return
  const calm = window.matchMedia?.('(prefers-reduced-motion: reduce)').matches
  el.scrollIntoView?.({ behavior: calm ? 'auto' : 'smooth', block: 'start' })
  el.focus({ preventScroll: true })
}

// The rail's Proposals block: a count and one small card per proposal. The proposal itself
// (steps, approvals, buttons) is the ProposalsPanel in the main column.
export function ProposalsSummary({ proposals, shadow = false }: { proposals: ProposalState[]; shadow?: boolean }) {
  const open = proposals.filter((p) => p.status === 'open').length
  return (
    <section aria-labelledby="proposals-summary-heading" className="flex flex-col gap-2.5 px-4 py-3.5">
      <h2 id="proposals-summary-heading" className="m-0 flex items-center gap-1.5 text-sm font-medium">
        Proposals
        <span className="font-mono text-[11px] font-medium text-subtle-foreground">
          {open > 0 ? `${open} open` : proposals.length}
        </span>
      </h2>
      {proposals.length === 0 ? (
        <p className="m-0 text-muted-foreground">
          {shadow
            ? 'Shadow room: the agent proposes no fix here.'
            : 'No fix proposed yet. The agent proposes once the hypothesis is confident; each layer owner approves their part.'}
        </p>
      ) : (
        <ul className="m-0 flex list-none flex-col gap-2 p-0">
          {proposals.map((p, i) => {
            const d = describe(p)
            return (
              <li key={p.proposal_id}>
                <button
                  type="button"
                  onClick={jumpToProposal}
                  className={cn(
                    'flex w-full flex-col gap-1.5 rounded-lg border border-border bg-card px-3 py-2.5 text-left transition-colors coarse:min-h-11',
                    'hover:bg-accent focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring',
                  )}
                >
                  <span className="flex items-center gap-1.5">
                    <StatusDot tone={d.tone} />
                    <span className="text-xs font-medium text-muted-foreground">{d.word}</span>
                    {d.count && <span className="ml-auto font-mono text-xs text-subtle-foreground">{d.count}</span>}
                  </span>
                  <span className="line-clamp-2 font-medium">
                    {i + 1}. {p.title}
                  </span>
                </button>
              </li>
            )
          })}
        </ul>
      )}
    </section>
  )
}
