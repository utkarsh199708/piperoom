// The verified card (U1-12b; docs/api.md "setup.verified"). The rungs come from the server's
// ladder runner only — the agent's sink refuses the type — so this card states what the server
// checked, not what the model says. Registration (U1-14) needs rungs 1 to 7 green.
import { useId } from 'react'
import { Check, ShieldCheck, TriangleAlert } from 'lucide-react'
import type { RoomState } from '@/state/types'
import { stripBidi } from '@/lib/title'
import { Badge } from '@/components/ui/badge'
import { Card } from '@/components/ui/card'

export function VerifiedCard({ state }: { state: RoomState }) {
  const id = useId()
  const rungs = state.setup?.verified ?? []
  if (rungs.length === 0) return null
  const green = rungs.filter((rung) => rung.ok).length
  const allGreen = green === rungs.length
  return (
    <Card
      role="region"
      aria-labelledby={`${id}-title`}
      className="mx-4 mt-2 flex min-w-0 flex-col gap-2 p-3 sm:mx-6"
    >
      <h2 id={`${id}-title`} className="m-0 flex items-center gap-2 text-sm font-semibold">
        <ShieldCheck className="size-4 shrink-0 text-muted-foreground" aria-hidden="true" />
        Verification
        <Badge variant={allGreen ? 'success' : 'warning'}>
          {green} of {rungs.length} rungs green
        </Badge>
      </h2>
      <ol aria-label="Verification rungs" className="m-0 flex list-none flex-col gap-1 p-0">
        {rungs.map((rung, i) => (
          <li key={`${i}-${rung.name}`} className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-0.5">
            {rung.ok ? (
              <Check className="size-3.5 shrink-0 text-success" aria-hidden="true" />
            ) : (
              <TriangleAlert className="size-3.5 shrink-0 text-destructive-fg" aria-hidden="true" />
            )}
            <code className="font-mono text-xs text-foreground">{stripBidi(rung.name)}</code>
            <span className="text-xs text-muted-foreground">{rung.ok ? 'ok' : 'failed'}</span>
            {rung.note && (
              <span className="min-w-0 truncate text-xs text-subtle-foreground">{stripBidi(rung.note)}</span>
            )}
          </li>
        ))}
      </ol>
      <p className="m-0 text-xs text-subtle-foreground">
        Written by the ladder runner on this server; the agent cannot claim a verification. Registering
        the pipeline needs rungs 1 to 7 green; rung 8 is the change you make.
      </p>
    </Card>
  )
}
