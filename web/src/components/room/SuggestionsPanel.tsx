import type { SuggestionState, SuggestionStatus } from '../../state/types'
import { Clock } from './Clock'
import { Badge } from '@/components/ui/badge'
import { PersonAvatar } from './PersonAvatar'

const SHOW_RESOLVED = 5

const LABEL: Record<SuggestionStatus, string> = {
  pending: 'Pending',
  accepted: 'Sent to agent',
  declined: 'Declined',
}
const VARIANT: Record<SuggestionStatus, 'warning' | 'success' | 'destructive'> = {
  pending: 'warning',
  accepted: 'success',
  declined: 'destructive',
}

// Everyone sees what happened to each suggestion: waiting for the driver, sent to the agent.
// Pending first, then the latest few settled ones, so a room open for hours does not grow a wall.
export function SuggestionsPanel({ suggestions }: { suggestions: SuggestionState[] }) {
  if (suggestions.length === 0) return null
  const pending = suggestions.filter((s) => s.status === 'pending')
  const settled = suggestions.filter((s) => s.status !== 'pending').slice(-SHOW_RESOLVED).reverse()
  const hidden = suggestions.length - pending.length - settled.length
  return (
    <section aria-labelledby="suggestions-heading" className="flex flex-col gap-2 px-4 py-3.5">
      <h2 id="suggestions-heading" className="m-0 text-sm font-medium">
        Suggestions
      </h2>
      <ul className="m-0 flex list-none flex-col gap-2 p-0">
        {[...pending, ...settled].map((s) => (
          <li key={s.suggestion_id} className="flex items-start gap-2">
            <PersonAvatar actor={s.from} size={20} className="mt-0.5" />
            <div className="min-w-0 flex-1">
              <div className="flex flex-wrap items-center gap-x-2 gap-y-0.5">
                <strong className="font-medium">{s.from.name}</strong>
                <Badge variant={VARIANT[s.status]}>{LABEL[s.status]}</Badge>
                {s.at && <Clock iso={s.at} className="font-mono text-xs text-subtle-foreground" />}
              </div>
              <p className="m-0 break-words text-muted-foreground">{s.text}</p>
            </div>
          </li>
        ))}
      </ul>
      {hidden > 0 && <p className="m-0 text-xs text-subtle-foreground">{hidden} older suggestions not shown</p>}
    </section>
  )
}
