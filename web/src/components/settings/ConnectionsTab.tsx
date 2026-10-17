import { useRef, useState } from 'react'
import {
  Cable,
  Check,
  CircleCheck,
  CircleMinus,
  CircleX,
  Copy,
  Database,
  LoaderCircle,
  Minus,
  Plug,
  Server,
  TriangleAlert,
  Warehouse,
  Waypoints,
} from 'lucide-react'
import type { LucideIcon } from 'lucide-react'
import { api } from '@/api/client'
import type { AdminConnection, ConnectionSetting, ProbeCheck, ProbeResult, ProbeStatus } from '@/api/types'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { knownStatus, kindLabel, lines, probeSummary, scopeLabel, settingValue, STATUS_LABEL, testErrorText } from '@/lib/settings'
import { cn } from '@/lib/utils'

const KIND_ICON: Record<string, LucideIcon> = {
  source_db: Database,
  kafka: Waypoints,
  connect: Cable,
  schema_registry: Server,
  warehouse: Warehouse,
}

// Shape and a word tell the statuses apart, so colour is only a hint: check, cross, triangle, minus.
const STATUS_ICON: Record<ProbeStatus, { icon: LucideIcon; className: string }> = {
  ok: { icon: CircleCheck, className: 'text-success-foreground' },
  fail: { icon: CircleX, className: 'text-destructive-fg' },
  warn: { icon: TriangleAlert, className: 'text-warning-foreground' },
  skip: { icon: CircleMinus, className: 'text-subtle-foreground' },
}

type TestState =
  | { phase: 'idle' }
  | { phase: 'running' }
  | { phase: 'done'; result: ProbeResult }
  | { phase: 'error'; message: string }

/** One card per connection the server runs with, each with its settings and a Test connection button. */
export function ConnectionsTab({ connections }: { connections: readonly AdminConnection[] }) {
  if (connections.length === 0) {
    return (
      <p role="status" className="m-0 text-muted-foreground">
        The server has no connections configured. A connection appears once its address (the database DSN, the Kafka
        brokers or a URL) is set.
      </p>
    )
  }
  return (
    <ul className="m-0 flex list-none flex-col gap-4 p-0">
      {connections.map((c) => (
        <li key={c.id}>
          <ConnectionCard connection={c} />
        </li>
      ))}
    </ul>
  )
}

function ConnectionCard({ connection: c }: { connection: AdminConnection }) {
  const [test, setTest] = useState<TestState>({ phase: 'idle' })
  // A second click while a test runs, or an answer that arrives after the card was re-run, is dropped.
  const run = useRef(0)
  const Icon = KIND_ICON[c.kind] ?? Plug
  const titleId = `conn-${c.id}`
  const usedBy = lines(c.used_by)

  const start = () => {
    if (test.phase === 'running') return
    const mine = ++run.current
    setTest({ phase: 'running' })
    api
      .testConnection(c.id)
      .then((result) => mine === run.current && setTest({ phase: 'done', result }))
      .catch((e: unknown) => mine === run.current && setTest({ phase: 'error', message: testErrorText(e) }))
  }

  return (
    <Card className="flex flex-col gap-3 p-4" aria-labelledby={titleId} role="group">
      <div className="flex flex-wrap items-center gap-x-3 gap-y-2">
        <Icon className="size-5 shrink-0 text-muted-foreground" aria-hidden="true" />
        <div className="flex min-w-0 flex-col">
          <h2 id={titleId} className="m-0 text-base font-semibold">
            {kindLabel(c.kind)}
          </h2>
          <span className="font-mono text-xs text-subtle-foreground">{c.id}</span>
        </div>
        <Badge variant={c.scope === 'pipeline' ? 'accent' : 'neutral'}>{scopeLabel(c)}</Badge>
        <Button
          type="button"
          variant="outline"
          size="sm"
          className="ml-auto"
          onClick={start}
          disabled={test.phase === 'running'}
          aria-label={`Test connection: ${kindLabel(c.kind)}, ${scopeLabel(c)}`}
        >
          {test.phase === 'running' ? (
            <LoaderCircle className="animate-spin" aria-hidden="true" />
          ) : (
            <Plug aria-hidden="true" />
          )}
          {test.phase === 'running' ? 'Testing…' : 'Test connection'}
        </Button>
      </div>

      <div className="flex flex-wrap items-center gap-1.5 text-xs text-muted-foreground">
        <span>Used by</span>
        {usedBy.length === 0 ? (
          <span className="text-subtle-foreground">no pipeline</span>
        ) : (
          usedBy.map((p) => (
            <Badge key={p} variant="outline" mono>
              {p}
            </Badge>
          ))
        )}
      </div>

      <SettingsTable id={c.id} settings={lines(c.settings)} />

      {/* Always in the page, so a screen reader announces the text that arrives in it. */}
      <div role="status" aria-live="polite" className="flex flex-col gap-2">
        {test.phase === 'running' && <p className="m-0 text-muted-foreground">Testing {kindLabel(c.kind)}…</p>}
        {test.phase === 'error' && (
          <p role="alert" className="m-0 font-medium text-destructive-fg">
            {test.message}
          </p>
        )}
        {test.phase === 'done' && <TestResult result={test.result} />}
      </div>
    </Card>
  )
}

function SettingsTable({ id, settings }: { id: string; settings: readonly ConnectionSetting[] }) {
  if (settings.length === 0) return <p className="m-0 text-xs text-subtle-foreground">No settings reported.</p>
  return (
    <div className="overflow-x-auto">
      <table className="w-full min-w-[480px] border-collapse text-left">
        <caption className="sr-only">Settings of {id}</caption>
        <thead>
          <tr className="border-b border-border text-xs text-subtle-foreground">
            <th scope="col" className="py-1.5 pr-3 font-medium">Setting</th>
            <th scope="col" className="py-1.5 pr-3 font-medium">Value</th>
            <th scope="col" className="py-1.5 font-medium">From</th>
          </tr>
        </thead>
        <tbody>
          {settings.map((s) => {
            const v = settingValue(s)
            return (
              <tr key={s.key} className="border-b border-border last:border-0">
                <th scope="row" className="py-1.5 pr-3 font-mono text-xs font-normal">
                  {s.key}
                </th>
                <td className="py-1.5 pr-3">
                  {v.kind === 'secret' ? (
                    <Badge variant={v.set ? 'success' : 'warning'}>
                      {v.set ? <Check className="size-3" aria-hidden="true" /> : <Minus className="size-3" aria-hidden="true" />}
                      {v.set ? 'set' : 'not set'}
                    </Badge>
                  ) : v.text ? (
                    <span className="break-all font-mono text-xs">{v.text}</span>
                  ) : (
                    <span className="text-subtle-foreground">
                      <span aria-hidden="true">–</span>
                      <span className="sr-only">empty</span>
                    </span>
                  )}
                </td>
                <td className="py-1.5 font-mono text-xs text-muted-foreground">{s.from || '–'}</td>
              </tr>
            )
          })}
        </tbody>
      </table>
    </div>
  )
}

function TestResult({ result }: { result: ProbeResult }) {
  const checks = lines(result.checks)
  return (
    <>
      <p
        className={cn('m-0 flex items-center gap-1.5 font-medium', result.ok ? 'text-success-foreground' : 'text-destructive-fg')}
        data-testid="probe-summary"
      >
        {result.ok ? (
          <CircleCheck className="size-4 shrink-0" aria-hidden="true" />
        ) : (
          <CircleX className="size-4 shrink-0" aria-hidden="true" />
        )}
        {probeSummary(result)}
      </p>
      <ol className="m-0 flex list-none flex-col gap-1.5 p-0" aria-label="Checks">
        {checks.map((c) => (
          <CheckRow key={c.id} check={c} />
        ))}
      </ol>
    </>
  )
}

function CheckRow({ check: c }: { check: ProbeCheck }) {
  const status = knownStatus(c.status)
  const { icon: Icon, className } = STATUS_ICON[status]
  return (
    <li className="flex flex-col gap-1 rounded-md border border-border px-3 py-2" data-status={status}>
      <div className="flex items-center gap-2">
        <Icon className={cn('size-4 shrink-0', className)} aria-hidden="true" />
        <span className="font-mono text-xs">{c.id}</span>
        {/* The word, not only the icon and colour. */}
        <span className="text-xs font-medium">{STATUS_LABEL[status]}</span>
        <span className="ml-auto font-mono text-xs tabular-nums text-subtle-foreground">{c.ms} ms</span>
      </div>
      {c.detail && <p className="m-0 text-xs text-muted-foreground">{c.detail}</p>}
      {c.problem && <p className="m-0 text-sm">{c.problem}</p>}
      {c.fix && <Fix id={c.id} text={c.fix} />}
    </li>
  )
}

function Fix({ id, text }: { id: string; text: string }) {
  const [copied, setCopied] = useState(false)
  const copy = () => {
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
    <div className="flex items-start gap-2">
      <span className="mt-0.5 text-xs font-medium text-muted-foreground">Fix</span>
      <code className="min-w-0 flex-1 whitespace-pre-wrap break-words rounded bg-muted px-2 py-1 font-mono text-xs">
        {text}
      </code>
      <Button type="button" variant="ghost" size="sm" onClick={copy} aria-label={`Copy fix for ${id}`}>
        {copied ? <Check aria-hidden="true" /> : <Copy aria-hidden="true" />}
        {copied ? 'Copied' : 'Copy'}
      </Button>
    </div>
  )
}
