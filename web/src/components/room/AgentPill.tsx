import { agentPill } from '@/lib/agentPill'
import type { PillTone } from '@/lib/agentPill'
import { cn } from '@/lib/utils'
import type { RoomState } from '@/state/types'
import { StatusDot } from './StatusDot'

const TEXT: Record<PillTone, string> = {
  ok: 'text-success-foreground',
  warn: 'text-warning-foreground',
  fail: 'text-destructive-fg',
  agent: 'text-agent-foreground',
  muted: 'text-muted-foreground',
}

/**
 * "Agent working · step 5". The live region (aria-live polite, not role=status: the screen
 * already has banners with that role) holds only the state words, so a screen reader hears
 * "Agent working" once and "resolved — verified" at the end, not every new step. The
 * "· step N" is drawn next to it, hidden from assistive technology.
 */
export function AgentPill({ state }: { state: RoomState }) {
  const p = agentPill(state)
  return (
    <span
      data-tone={p.tone}
      title={p.title}
      className={cn(
        'inline-flex h-6 items-center gap-1.5 whitespace-nowrap rounded-full border border-border bg-card px-2 text-xs font-medium',
        TEXT[p.tone],
      )}
    >
      <StatusDot tone={p.tone} />
      <span aria-live="polite">{p.text}</span>
      {p.step != null && (
        <span aria-hidden="true" data-testid="agent-step" className="-ml-1">
          · step {p.step}
        </span>
      )}
    </span>
  )
}
