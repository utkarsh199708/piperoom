import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { useShadowMetrics } from '@/hooks/useShadowMetrics'
import { formatSeconds, formatTokens, metricsHeadline, sinceParam, STATUS_LABEL } from '@/lib/shadow'
import type { ShadowMetrics, ShadowStatus } from '@/api/types'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Input } from '@/components/ui/input'

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
      <h1 className="m-0 text-lg font-semibold">Shadow results</h1>
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

      {view.phase === 'loading' && (
        <p role="status" className="m-0 text-muted-foreground">
          Loading…
        </p>
      )}
      {view.phase === 'error' && (
        <p role="alert" className="m-0 font-medium text-destructive-fg">
          {view.message}
        </p>
      )}
      {view.phase === 'ready' && view.metrics.rooms === 0 && (
        <p role="status" className="m-0 text-muted-foreground">
          No closed shadow rooms match. A pipeline needs <code className="font-mono">shadow: {'{'}enabled: true{'}'}</code>{' '}
          in owners.yaml, and a room counts once it is closed.
        </p>
      )}
      {view.phase === 'ready' && view.metrics.rooms > 0 && (
        <Results metrics={view.metrics} copied={copied} onCopy={copy} />
      )}
    </div>
  )
}

function Results({ metrics: m, copied, onCopy }: { metrics: ShadowMetrics; copied: boolean; onCopy: (t: string) => void }) {
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

      <dl className="m-0 grid grid-cols-2 gap-3 sm:grid-cols-4">
        <Tile label="Closed shadow rooms" value={String(m.rooms)} />
        <Tile label="Labelled" value={`${m.labelled} of ${m.rooms}`} />
        <Tile label="Graded" value={`${m.graded} of ${m.rooms}`} />
        <Tile label="Layer correct" value={`${m.layer_correct} of ${m.rooms}`} />
        <Tile label="Median agent, first hypothesis" value={formatSeconds(m.median_time_to_first_s)} />
        <Tile label="Median agent, final hypothesis" value={formatSeconds(m.median_time_to_final_s)} />
        <Tile label="Median human, to resolve" value={formatSeconds(m.median_human_mttr_s)} />
        <Tile label="Tokens" value={m.tokens_available ? formatTokens(m.total_tokens) : 'not counted'} />
      </dl>

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
              {STATUS_LABEL[s] ?? s} {n}
            </Badge>
          ))}
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full min-w-[720px] border-collapse text-left">
          <caption className="sr-only">Closed shadow rooms</caption>
          <thead>
            <tr className="border-b border-border text-xs text-subtle-foreground">
              <th scope="col" className="py-2 pr-3 font-medium">Room</th>
              <th scope="col" className="py-2 pr-3 font-medium">Pipeline</th>
              <th scope="col" className="py-2 pr-3 font-medium">Source</th>
              <th scope="col" className="py-2 pr-3 font-medium">Run</th>
              <th scope="col" className="py-2 pr-3 font-medium">Grade</th>
              <th scope="col" className="py-2 pr-3 text-right font-medium">Agent</th>
              <th scope="col" className="py-2 pr-3 text-right font-medium">Human</th>
              <th scope="col" className="py-2 text-right font-medium">Tokens</th>
            </tr>
          </thead>
          <tbody>
            {m.per_room.map((r) => (
              <tr key={r.room_id} className="border-b border-border">
                <td className="py-2 pr-3">
                  <Link to={`/rooms/${encodeURIComponent(r.room_id)}`} className="font-mono text-primary-fg hover:underline">
                    {r.room_id}
                  </Link>
                </td>
                <td className="py-2 pr-3">{r.pipeline_id}</td>
                <td className="py-2 pr-3">{r.alert_source || '–'}</td>
                <td className="py-2 pr-3">{STATUS_LABEL[r.status] ?? r.status}</td>
                <td className="py-2 pr-3">{gradeText(r)}</td>
                <td className="py-2 pr-3 text-right tabular-nums">{formatSeconds(r.time_to_final_s)}</td>
                <td className="py-2 pr-3 text-right tabular-nums">{formatSeconds(r.human_mttr_s)}</td>
                <td className="py-2 text-right tabular-nums">{r.tokens > 0 ? formatTokens(r.tokens) : '–'}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </>
  )
}

function gradeText(r: ShadowMetrics['per_room'][number]): string {
  if (r.graded) return `${r.layer_correct ? 'layer ok' : 'layer wrong'}, cause ${r.root_cause || '?'}`
  return r.labelled ? 'labelled, not graded' : 'not labelled'
}

function Tile({ label, value }: { label: string; value: string }) {
  return (
    <Card className="flex flex-col gap-0.5 px-3 py-2">
      <dt className="text-xs text-subtle-foreground">{label}</dt>
      <dd className="m-0 text-base font-medium tabular-nums">{value}</dd>
    </Card>
  )
}
