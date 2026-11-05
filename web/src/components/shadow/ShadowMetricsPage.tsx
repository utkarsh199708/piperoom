import { useContext, useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { useShadowMetrics } from '@/hooks/useShadowMetrics'
import { formatSeconds, formatTokens, metricsHeadline, sinceParam, statusLabel } from '@/lib/shadow'
import type { ShadowMetrics, ShadowStatus } from '@/api/types'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Skeleton } from '@/components/ui/skeleton'
import { RoomListContext, useInShell } from '@/components/shell/context'
import { cn } from '@/lib/utils'

// Shadow results for the pilot: "N of M matched, median X vs Y". M counts every closed
// shadow room, so an ungraded or failed run lowers the number rather than hiding.
export function ShadowMetricsPage() {
  const [pipeline, setPipeline] = useState('')
  const [date, setDate] = useState('')
  const [copied, setCopied] = useState(false)
  // One request per pause in typing, not per keystroke.
  const [debounced, setDebounced] = useState('')
  useEffect(() => {
    const t = window.setTimeout(() => setDebounced(pipeline.trim()), 300)
    return () => window.clearTimeout(t)
  }, [pipeline])
  const view = useShadowMetrics(debounced, sinceParam(date))
  const inShell = useInShell()

  const copy = (text: string) => {
    if (!navigator.clipboard) return
    void navigator.clipboard.writeText(text).then(
      () => {
        setCopied(true)
        window.setTimeout(() => setCopied(false), 1500)
      },
      () => undefined,
    )
  }

  return (
    <div className="mx-auto flex w-full max-w-5xl flex-col gap-4 px-4 py-6 text-sm sm:px-6">
      {/* In the app the top bar carries the page title; a second heading would repeat it. */}
      {!inShell && <h1 className="m-0 text-lg font-semibold">Shadow results</h1>}
      <form className="flex flex-wrap items-end gap-3" onSubmit={(e) => e.preventDefault()}>
        <div className="flex flex-col gap-1">
          <label htmlFor="shadow-pipeline" className="text-xs font-medium">
            Pipeline
          </label>
          <Input
            id="shadow-pipeline"
            value={pipeline}
            onChange={(e) => setPipeline(e.target.value)}
            placeholder="all pipelines"
            className="w-48"
          />
        </div>
        <div className="flex flex-col gap-1">
          <label htmlFor="shadow-since" className="text-xs font-medium">
            Closed since
          </label>
          <Input id="shadow-since" type="date" value={date} onChange={(e) => setDate(e.target.value)} className="w-40" />
        </div>
      </form>

      {view.phase === 'loading' && <ResultsSkeleton />}
      {view.phase === 'error' && (
        <p role="alert" className="m-0 font-medium text-destructive-fg">
          {view.message}
        </p>
      )}
      {view.phase === 'ready' && view.metrics.rooms === 0 && (
        <Card role="status" className="flex flex-col items-center gap-1.5 px-6 py-12 text-center">
          <p className="m-0 font-medium">No closed shadow rooms match</p>
          <p className="m-0 max-w-[52ch] text-muted-foreground">
            A room counts here once it closes on a pipeline that runs in shadow mode. An admin turns shadow mode on
            per pipeline in the owners file.
          </p>
        </Card>
      )}
      {view.phase === 'ready' && view.metrics.rooms > 0 && (
        <Results metrics={view.metrics} copied={copied} onCopy={copy} />
      )}
    </div>
  )
}

function Results({ metrics: m, copied, onCopy }: { metrics: ShadowMetrics; copied: boolean; onCopy: (t: string) => void }) {
  // The metrics name rooms by id; the room list (when the shell has it) knows their titles.
  const rooms = useContext(RoomListContext)?.rooms
  const titles = new Map((rooms ?? []).map((r) => [r.id, r.title] as const))
  const headline = metricsHeadline(m)
  const statuses = Object.entries(m.by_status).filter(([, n]) => (n ?? 0) > 0) as [ShadowStatus, number][]
  return (
    <>
      <Card className="flex flex-wrap items-center gap-3 p-4">
        <p className="m-0 text-base font-medium" data-testid="shadow-headline">
          {headline}
        </p>
        <Button type="button" variant="outline" size="sm" className="ml-auto" onClick={() => onCopy(headline)}>
          {copied ? 'Copied' : 'Copy line'}
        </Button>
        <p className="m-0 w-full text-xs text-subtle-foreground">
          Matched = layer judged correct, out of every closed shadow room (errors and ungraded rooms count as not
          matched). Agent time is to its final hypothesis, from when the room opened; human time is open to close.
        </p>
      </Card>

      {/* One summary strip, as on the room list: four numbers with a caption each, not eight boxes. */}
      <Card className="grid grid-cols-2 md:grid-cols-4" aria-label="Summary" role="group">
        <Stat
          label="Closed shadow rooms"
          value={String(m.rooms)}
          caption={m.tokens_available ? `${formatTokens(m.total_tokens)} tokens` : 'tokens not counted'}
        />
        <Stat label="Layer correct" value={`${m.layer_correct} of ${m.rooms}`} caption={`${m.graded} graded · ${m.labelled} labelled`} />
        <Stat
          label="Agent, median"
          value={formatSeconds(m.median_time_to_final_s)}
          caption={`to final hypothesis · first ${formatSeconds(m.median_time_to_first_s)}`}
        />
        <Stat label="Humans, median" value={formatSeconds(m.median_human_mttr_s)} caption="open to resolve" />
      </Card>

      <div className="flex flex-wrap gap-x-6 gap-y-2">
        <div className="flex flex-wrap items-center gap-1.5">
          <span className="text-xs text-subtle-foreground">Root cause found</span>
          <Badge variant="success">yes {m.root_cause.yes}</Badge>
          <Badge variant="warning">partly {m.root_cause.partial}</Badge>
          <Badge variant="destructive">no {m.root_cause.no}</Badge>
        </div>
        <div className="flex flex-wrap items-center gap-1.5">
          <span className="text-xs text-subtle-foreground">Run status</span>
          {statuses.map(([s, n]) => (
            <Badge key={s} variant="neutral">
              {statusLabel(s)} {n}
            </Badge>
          ))}
        </div>
      </div>

      {/* On a phone the table keeps room, run and grade; the rest is on the room itself. */}
      <Card className="overflow-hidden">
        <table className="w-full border-collapse text-left">
          <caption className="sr-only">Closed shadow rooms</caption>
          <thead>
            <tr className="border-b border-border text-xs text-subtle-foreground">
              <th scope="col" className={cn(TH, 'pl-3')}>Room</th>
              <th scope="col" className={cn(TH, 'max-md:hidden')}>Pipeline</th>
              <th scope="col" className={cn(TH, 'max-lg:hidden')}>Source</th>
              <th scope="col" className={TH}>Run</th>
              <th scope="col" className={TH}>Grade</th>
              <th scope="col" className={cn(TH, 'text-right max-md:hidden')}>Agent</th>
              <th scope="col" className={cn(TH, 'text-right max-md:hidden')}>Human</th>
              <th scope="col" className={cn(TH, 'pr-3 text-right max-lg:hidden')}>Tokens</th>
            </tr>
          </thead>
          <tbody>
            {m.per_room.map((r) => (
              <tr key={r.room_id} className="border-b border-border last:border-b-0">
                <td className={cn(TD, 'pl-3')}>
                  <Link
                    to={`/rooms/${encodeURIComponent(r.room_id)}`}
                    title={`Room ${r.room_id}`}
                    className="font-medium text-foreground hover:underline"
                  >
                    {titles.get(r.room_id) ?? r.room_id}
                  </Link>
                </td>
                <td className={cn(TD, 'font-mono text-muted-foreground max-md:hidden')}>{r.pipeline_id}</td>
                <td className={cn(TD, 'text-muted-foreground max-lg:hidden')}>{r.alert_source || '–'}</td>
                <td className={TD}>{statusLabel(r.status)}</td>
                <td className={TD}>{gradeText(r)}</td>
                <td className={cn(TD, 'text-right tabular-nums max-md:hidden')}>{formatSeconds(r.time_to_final_s)}</td>
                <td className={cn(TD, 'text-right tabular-nums max-md:hidden')}>{formatSeconds(r.human_mttr_s)}</td>
                <td className={cn(TD, 'pr-3 text-right tabular-nums max-lg:hidden')}>{r.tokens > 0 ? formatTokens(r.tokens) : '–'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </Card>
    </>
  )
}

const TH = 'py-2 pr-3 font-medium'
const TD = 'py-2 pr-3 align-top'

const CAUSE: Record<string, string> = { yes: 'cause found', partial: 'cause partly found', no: 'cause missed' }

function gradeText(r: ShadowMetrics['per_room'][number]): string {
  if (r.graded) {
    const cause = r.root_cause ? `, ${CAUSE[r.root_cause] ?? r.root_cause}` : ''
    return `${r.layer_correct ? 'Right layer' : 'Wrong layer'}${cause}`
  }
  return r.labelled ? 'Labelled, not graded' : 'Not labelled'
}

// The room list's stat cell: two columns on a phone, four from md, borders following the grid.
const CELL = 'max-md:[&:nth-child(even)]:border-l max-md:[&:nth-child(n+3)]:border-t md:[&:nth-child(n+2)]:border-l'

function Stat({ label, value, caption }: { label: string; value: string; caption: string }) {
  return (
    <div className={cn('flex min-w-0 flex-col gap-1 border-border p-4', CELL)}>
      <span className="text-xs text-muted-foreground">{label}</span>
      <span className="text-xl font-semibold leading-6 tracking-tight tabular-nums">{value}</span>
      <span className="truncate text-xs text-subtle-foreground">{caption}</span>
    </div>
  )
}

function ResultsSkeleton() {
  return (
    <div role="status" aria-label="Loading shadow results" className="flex flex-col gap-4">
      <span className="sr-only">Loading shadow results…</span>
      <Card className="p-4" aria-hidden="true">
        <Skeleton className="h-5 w-2/3" />
      </Card>
      <Card className="grid grid-cols-2 md:grid-cols-4" aria-hidden="true">
        {[0, 1, 2, 3].map((i) => (
          <div key={i} className={cn('flex flex-col gap-2 border-border p-4', CELL)}>
            <Skeleton className="h-3 w-20" />
            <Skeleton className="h-6 w-12" />
            <Skeleton className="h-3 w-24" />
          </div>
        ))}
      </Card>
      <Card className="flex flex-col" aria-hidden="true">
        {[0, 1, 2].map((i) => (
          <div key={i} className="flex h-10 items-center gap-4 border-b border-border px-3 last:border-b-0">
            <Skeleton className="h-4 w-1/3" />
            <Skeleton className="h-4 w-16" />
            <Skeleton className="ml-auto h-4 w-10" />
          </div>
        ))}
      </Card>
    </div>
  )
}
