import { useId } from 'react'
import type { RoomState } from '@/state/types'
import { costBadge, headerUsage } from '@/lib/insight'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'

// "$1.42 est." in the room header (Q0, QA G14: cost was invisible). "≥ $1.42 est." while some calls had no
// price; nothing while no call is priced, and nothing in a shadow room (its numbers are revealed after
// close). Tokens and calls are in the tooltip, which is also the button's description, not a live region.
export function CostBadge({ state }: { state: RoomState }) {
  const tipId = useId()
  if (state.shadow) return null
  const badge = costBadge(headerUsage(state))
  if (!badge) return null
  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <button
            type="button"
            data-testid="cost-badge"
            aria-describedby={tipId}
            className="inline-flex h-6 cursor-default items-center whitespace-nowrap rounded-full border border-border bg-card px-2 font-mono text-xs tabular-nums text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
          >
            {badge.text}
          </button>
        </TooltipTrigger>
        <TooltipContent>{badge.title}</TooltipContent>
      </Tooltip>
      <span id={tipId} className="sr-only">
        {badge.title}
      </span>
    </TooltipProvider>
  )
}
