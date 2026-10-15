import { useState } from 'react'
import { Check, X } from 'lucide-react'
import type { AgentStats, HypothesisVerdict } from '@/api/types'
import type { HypothesisHistoryEntry, HypothesisState, InsightState, StepState } from '@/state/types'
import { useAgentStats } from '@/hooks/useAgentStats'
import { recordText } from '@/lib/insight'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { clockTime } from '@/lib/time'
import { stepAnchor } from '@/lib/triage'
import { markTerms } from '@/lib/unbacked'
import { PersonName } from '@/components/VerifiedBadge'
import { PersonAvatar } from './PersonAvatar'
import { AgentText, StepNumbersProvider } from './AgentText'

const AGENT = { kind: 'agent', id: 'agent', name: 'Agent' } as const
const MAX_NOTE = 500

/** The DOM id of a step row, shared with the timeline. */
function rowOf(id: string) {
  return typeof document === 'undefined' ? null : document.getElementById(stepAnchor(id))
}

// "based on" + one chip per cited step: the number the timeline shows, a button that jumps to
// the row. An id the log does not hold (a snapshot before replay, a stale citation) is a
// disabled "step ?": never the raw id. Hovering a chip marks its row without scrolling.
function CitationChips({ ids, steps, onCite }: { ids: string[]; steps: StepState[]; onCite?: (stepId: string) => void }) {
  return (
    <span className="flex flex-wrap items-center gap-1" data-testid="citations">
      <span>based on</span>
      {ids.map((id, k) => {
        const i = steps.findIndex((s) => s.id === id)
        const cls =
          'inline-flex h-5 items-center rounded-md border border-agent/40 px-1.5 font-mono text-[10px] leading-none text-agent-foreground enabled:hover:bg-agent-subtle focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:cursor-not-allowed disabled:opacity-60 coarse:h-7'
        if (i < 0) {
          return (
            <button key={`${id}-${k}`} type="button" disabled className={cls} title="This step is not in the room log">
              step ?
            </button>
          )
        }
        return (
          <button
            key={`${id}-${k}`}
            type="button"
            className={cls}
            aria-label={`Go to step ${i + 1}: ${steps[i].title}`}
            onClick={() => onCite?.(id)}
            onMouseEnter={() => rowOf(id)?.setAttribute('data-cited', '')}
            onMouseLeave={() => rowOf(id)?.removeAttribute('data-cited')}
            onFocus={() => rowOf(id)?.setAttribute('data-cited', '')}
            onBlur={() => rowOf(id)?.removeAttribute('data-cited')}
          >
            step {i + 1}
          </button>
        )
      })}
    </span>
  )
}

// The hypothesis text with the terms the cited steps do not show underlined (dotted, warning tone).
// The rest goes through AgentText, so step and proposal references are links.
function HypothesisText({ text, terms }: { text: string; terms: string[] }) {
  return (
    <p className="m-0 max-w-[72ch] text-base">
      {markTerms(text, terms).map((r, i) =>
        r.unbacked ? (
          <mark
            key={i}
            className="unbacked bg-transparent text-inherit underline decoration-warning decoration-dotted underline-offset-4"
            title="Not found in the cited steps"
          >
            {r.text}
          </mark>
        ) : (
          <span key={i}>
            <AgentText text={r.text} />
          </span>
        ),
      )}
    </p>
  )
}

function recordTitle(stats: AgentStats): string {
  const rows = stats.by_confidence.map((b) => `${b.confidence} ${b.correct}/${b.runs}`)
  if (stats.median_time_to_first_hypothesis_s >= 0) {
    rows.push(`median first insight ${stats.median_time_to_first_hypothesis_s} s`)
  }
  return rows.join(' · ')
}

export function HypothesisCard({
  hypothesis,
  steps,
  history = [],
  selfId,
  canDecide = false,
  onVerdict,
  pipelineId = '',
  insight,
  onCite,
}: {
  hypothesis: HypothesisState
  steps: StepState[]
  /** The confidence path; shown from two entries on. */
  history?: HypothesisHistoryEntry[]
  selfId?: string
  /** A member of an open room: Confirm and Dispute show. */
  canDecide?: boolean
  onVerdict?: (verdict: HypothesisVerdict, note?: string) => void
  /** The pipeline whose track record is shown (F11). */
  pipelineId?: string
  /** The room's insight: the first-hypothesis time (F11). */
  insight?: InsightState
  /** A citation chip was clicked: scroll to and open that step (F3). */
  onCite?: (stepId: string) => void
}) {
  const stats = useAgentStats(pipelineId)
  const record = stats ? recordText(stats, hypothesis.confidence) : null
  const noRecord = stats !== null && stats.graded === 0
  const firstS = insight?.time_to_first_hypothesis_s ?? -1
  const based = hypothesis.based_on_steps ?? []
  const unbacked = hypothesis.unbacked_terms ?? []
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
        <Badge variant="agent" title={noRecord ? 'no labelled rooms yet' : undefined}>
          {hypothesis.confidence} confidence
        </Badge>
        {record && stats && (
          <Badge variant="outline" title={recordTitle(stats)}>
            {record}
          </Badge>
        )}
        {verdicts.map((v) => {
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
              {v.verdict} by <PersonName person={v.by} /> {when}
              {v.note && <span className="sr-only">: {v.note}</span>}
            </Badge>
          )
        })}
        {unbacked.length > 0 && (
          <Badge variant="warning" title="Terms the cited steps do not show">
            {unbacked.length} unbacked
          </Badge>
        )}
        {(based.length > 0 || firstS >= 0) && (
          <span className="ml-auto flex flex-wrap gap-x-2 text-xs text-subtle-foreground">
            {firstS >= 0 && <span>first hypothesis in {firstS} s</span>}
            {based.length > 0 && <CitationChips ids={based} steps={steps} onCite={onCite} />}
          </span>
        )}
      </div>
      {history.length > 1 && (
        <p className="m-0 text-xs tabular-nums text-subtle-foreground" data-testid="confidence-path">
          {history.map((h) => `${h.confidence} ${clockTime(h.at)}`).join(' → ')}
        </p>
      )}
      <StepNumbersProvider steps={steps}>
        <HypothesisText text={hypothesis.text} terms={unbacked} />
      </StepNumbersProvider>
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
