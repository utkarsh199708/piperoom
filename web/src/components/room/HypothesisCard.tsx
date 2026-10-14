import { useState } from 'react'
import { Check, X } from 'lucide-react'
import type { HypothesisHistoryEntry, HypothesisState, StepState } from '@/state/types'
import type { HypothesisVerdict } from '@/api/types'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { clockTime } from '@/lib/time'
import { PersonAvatar } from './PersonAvatar'

const AGENT = { kind: 'agent', id: 'agent', name: 'Agent' } as const
const MAX_NOTE = 500

// "based on steps 1, 3, 4": step ids become the numbers the timeline shows.
function basedOn(ids: string[], steps: StepState[]): string {
  const nums = ids.map((id) => {
    const i = steps.findIndex((s) => s.id === id)
    return i >= 0 ? String(i + 1) : id
  })
  return `based on ${nums.length === 1 ? 'step' : 'steps'} ${nums.join(', ')}`
}

export function HypothesisCard({
  hypothesis,
  steps,
  history = [],
  selfId,
  canDecide = false,
  onVerdict,
}: {
  hypothesis: HypothesisState
  steps: StepState[]
  /** The confidence path; shown from two entries on. */
  history?: HypothesisHistoryEntry[]
  selfId?: string
  /** A member of an open room: Confirm and Dispute show. */
  canDecide?: boolean
  onVerdict?: (verdict: HypothesisVerdict, note?: string) => void
}) {
  const based = hypothesis.based_on_steps ?? []
  const verdicts = hypothesis.verdicts ?? []
  const mine = verdicts.find((v) => v.by.id === selfId)?.verdict
  const [disputing, setDisputing] = useState(false)
  const [note, setNote] = useState('')

  // The server answers 409 already_decided to a repeat, so a pressed button is a no-op.
  const decide = (verdict: HypothesisVerdict, text?: string) => {
    if (mine === verdict) return
    onVerdict?.(verdict, text)
  }
  const sendDispute = () => {
    decide('disputed', note.trim() || undefined)
    setDisputing(false)
    setNote('')
  }

  return (
    <Card className="flex flex-col gap-2 px-3.5 py-3" role="region" aria-labelledby="hypothesis-heading">
      <div className="flex flex-wrap items-center gap-2">
        <PersonAvatar actor={AGENT} size={20} />
        <h2 id="hypothesis-heading" className="m-0 text-sm font-medium">
          Hypothesis
        </h2>
        <Badge variant="agent">{hypothesis.confidence} confidence</Badge>
        {verdicts.map((v) => {
          const name = v.by.name || v.by.id
          const when = clockTime(v.at)
          const ok = v.verdict === 'confirmed'
          return (
            <Badge
              key={v.by.id}
              variant={ok ? 'success' : 'destructive'}
              title={v.note || undefined}
              data-testid={`verdict-${v.by.id}`}
            >
              {ok ? <Check className="size-3" aria-hidden="true" /> : <X className="size-3" aria-hidden="true" />}
              {v.verdict} by {name} {when}
              {v.note && <span className="sr-only">: {v.note}</span>}
            </Badge>
          )
        })}
        {based.length > 0 && <span className="ml-auto text-xs text-subtle-foreground">{basedOn(based, steps)}</span>}
      </div>
      {history.length > 1 && (
        <p className="m-0 text-xs tabular-nums text-subtle-foreground" data-testid="confidence-path">
          {history.map((h) => `${h.confidence} ${clockTime(h.at)}`).join(' → ')}
        </p>
      )}
      <p className="m-0 max-w-[72ch] text-base">{hypothesis.text}</p>
      {canDecide && onVerdict && (
        <div className="flex flex-wrap items-center justify-end gap-2">
          {disputing ? (
            <form
              className="flex min-w-0 flex-1 basis-72 items-center justify-end gap-2"
              onSubmit={(e) => {
                e.preventDefault()
                sendDispute()
              }}
            >
              <Input
                value={note}
                maxLength={MAX_NOTE}
                onChange={(e) => setNote(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Escape') {
                    e.stopPropagation()
                    setDisputing(false)
                    setNote('')
                  }
                }}
                placeholder="Why? (optional, 500 chars)"
                aria-label="Why do you dispute the hypothesis?"
                className="min-w-0 flex-1"
                autoComplete="off"
                autoFocus
              />
            </form>
          ) : (
            <>
              <Button variant="ghost" size="sm" aria-pressed={mine === 'confirmed'} onClick={() => decide('confirmed')}>
                <Check className="size-3.5" aria-hidden="true" />
                Confirm
              </Button>
              <Button
                variant="ghost"
                size="sm"
                aria-pressed={mine === 'disputed'}
                onClick={() => (mine === 'disputed' ? undefined : setDisputing(true))}
              >
                <X className="size-3.5" aria-hidden="true" />
                Dispute…
              </Button>
            </>
          )}
        </div>
      )}
    </Card>
  )
}
