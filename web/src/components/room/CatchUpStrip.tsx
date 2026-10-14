import { X } from 'lucide-react'
import type { CatchUpStrip as Strip } from '@/hooks/useCatchUp'
import type { CatchUpTarget } from '@/lib/catchup'
import { Button } from '@/components/ui/button'
import { PROPOSAL_ANCHOR } from './ProposalsSummary'

const TARGET_ID: Record<CatchUpTarget, string> = {
  proposal: PROPOSAL_ANCHOR,
  hypothesis: 'hypothesis-heading',
  steps: 'investigation-heading',
}

function showMe(target: CatchUpTarget) {
  const el = document.getElementById(TARGET_ID[target]) ?? document.getElementById(TARGET_ID.steps)
  const calm = window.matchMedia?.('(prefers-reduced-motion: reduce)').matches
  el?.scrollIntoView?.({ behavior: calm ? 'auto' : 'smooth', block: 'start' })
}

/** One line under the header: what happened while you were away, or the connection catching up. */
export function CatchUpStrip({ strip, onDismiss }: { strip: Strip | null; onDismiss: () => void }) {
  if (!strip) return null
  return (
    <div
      role="status"
      tabIndex={-1}
      onKeyDown={(e) => {
        if (e.key === 'Escape' && strip.kind === 'away') onDismiss()
      }}
      className="flex min-h-9 flex-wrap items-center gap-x-3 gap-y-1 border-b border-border bg-primary-subtle px-4 py-1.5 text-sm text-primary-fg sm:px-6"
    >
      <span className="min-w-0 flex-1">{strip.text}</span>
      {strip.kind === 'away' && (
        <>
          <Button variant="ghost" size="sm" onClick={() => showMe(strip.target)}>
            Show me
          </Button>
          <Button variant="ghost" size="sm" aria-label="Dismiss" onClick={onDismiss}>
            <X className="size-3.5" aria-hidden="true" />
          </Button>
        </>
      )}
    </div>
  )
}
