import { Link } from 'react-router-dom'
import type { StepState } from '@/state/types'
import { durationLabel, latestSimilarHit, resolutionText } from '@/lib/f8'
import { Card } from '@/components/ui/card'

const MONTHS = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

function dayLabel(iso: string): string {
  const t = Date.parse(iso)
  if (Number.isNaN(t)) return ''
  const d = new Date(t)
  return `${d.getUTCDate()} ${MONTHS[d.getUTCMonth()]}` // fixed names: locale data spells September "Sept"
}

// "orders had this on 30 Sep · fixed by connect_resume_connector · 12 min · closed by Kabir".
// The closer's name is shown only when the server sent one, which it does for a verified closer.
export function SimilarCard({ steps }: { steps: StepState[] }) {
  const hit = latestSimilarHit(steps)
  if (!hit) return null
  const resolution = resolutionText(hit)
  const day = dayLabel(hit.date)
  const fix = hit.fix_that_worked || hit.fix_actions.join(', ')
  const parts = [
    `${hit.title || 'A past room'} had this${day ? ` on ${day}` : ''}`,
    fix && `fixed by ${fix}`,
    hit.duration_s > 0 && durationLabel(hit.duration_s),
    hit.closed_by && `closed by ${hit.closed_by}`,
  ].filter(Boolean)
  return (
    <Card role="region" aria-label="Similar incident" className="flex flex-col gap-1.5 px-3.5 py-3">
      <h2 className="m-0 text-sm font-medium">Seen before</h2>
      <p className="m-0 break-words text-sm">{parts.join(' · ')}</p>
      {resolution && <p className="m-0 line-clamp-2 break-words text-xs text-muted-foreground">{resolution}</p>}
      <Link
        to={`/rooms/${encodeURIComponent(hit.room_id)}`}
        className="w-fit rounded-sm text-xs font-medium text-primary-fg underline-offset-2 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      >
        Open room ↗
      </Link>
    </Card>
  )
}
