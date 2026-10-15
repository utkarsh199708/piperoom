import { useId } from 'react'
import type { RoomState } from '@/state/types'
import { chipText, chipTooltip } from '@/lib/insight'
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from '@/components/ui/tooltip'
import { useNow } from './useNow'

// "$0.42 · 11 steps · 2m 10s" in the header facts line. Plain information: the
// button opens nothing and is not a live region. The duration runs while the
// room is open and is frozen at closed_at afterwards. Hidden in a shadow room
// (its numbers are revealed after close).
export function CostChip({ state }: { state: RoomState }) {
  const now = useNow(1000)
  const tipId = useId()
  if (state.shadow) return null
  const opened = Date.parse(state.opened_at)
  if (Number.isNaN(opened)) return null
  const end = state.closed_at ? Date.parse(state.closed_at) : now
  const ms = Math.max(0, (Number.isNaN(end) ? now : end) - opened)
  const text = chipText(state.insight, ms)
  const tip = chipTooltip(state.insight)
  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <button
            type="button"
            data-testid="cost-chip"
            aria-describedby={tipId}
            className="cursor-default whitespace-nowrap rounded-sm border-0 bg-transparent p-0 font-mono text-xs tabular-nums text-subtle-foreground"
          >
            {text}
          </button>
        </TooltipTrigger>
        <TooltipContent>{tip}</TooltipContent>
      </Tooltip>
      <span id={tipId} className="sr-only">
        {tip}
      </span>
    </TooltipProvider>
  )
}
