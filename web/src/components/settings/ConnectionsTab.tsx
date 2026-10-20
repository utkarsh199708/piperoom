import { useCallback, useRef, useState } from 'react'
import {
  Cable,
  Check,
  ChevronDown,
  CircleCheck,
  CircleMinus,
  CircleX,
  Copy,
  Database,
  LoaderCircle,
  Play,
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
import {
  checkLabel,
  endpointSummary,
  healthLabel,
  kindLabel,
  kindRole,
  knownStatus,
  lines,
  probeSummary,
  resultHealth,
  scopeLabel,
  settingLabel,
  settingSource,
  settingValue,
  STATUS_LABEL,
  testErrorText,
  type ConnectionHealth,
} from '@/lib/settings'
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

const HEALTH_STYLE: Record<ConnectionHealth, { dot: string; text: string }> = {
  untested: { dot: 'bg-subtle-foreground/50', text: 'text-muted-foreground' },
  testing: { dot: 'bg-primary animate-pulse', text: 'text-muted-foreground' },
  healthy: { dot: 'bg-success', text: 'text-success-foreground' },
  warnings: { dot: 'bg-warning', text: 'text-warning-foreground' },
  failed: { dot: 'bg-destructive', text: 'text-destructive-fg' },
  error: { dot: 'bg-destructive', text: 'text-destructive-fg' },
}

type TestState =
  | { phase: 'idle' }
  | { phase: 'running' }
  | { phase: 'done'; result: ProbeResult }
  | { phase: 'error'; message: string }

function healthOf(t: TestState | undefined): ConnectionHealth {
  if (!t || t.phase === 'idle') return 'untested'
  if (t.phase === 'running') return 'testing'
  if (t.phase === 'error') return 'error'
  return resultHealth(t.result)
}

/**
 * Every connection the server runs with, as one list: a summary strip on top, then a row per connection
 * with its status, a Test button and details that open in place.
 */
export function ConnectionsTab({ connections }: { connections: readonly AdminConnection[] }) {
  const [tests, setTests] = useState<Record<string, TestState>>({})
  // An answer that arrives after the same connection was tested again is dropped.
  const runs = useRef<Record<string, number>>({})
  const [runningAll, setRunningAll] = useState(false)

  const runTest = useCallback(async (id: string) => {
    const mine = (runs.current[id] ?? 0) + 1
    runs.current[id] = mine
    setTests((t) => ({ ...t, [id]: { phase: 'running' } }))
    try {
      const result = await api.testConnection(id)
      if (runs.current[id] === mine) setTests((t) => ({ ...t, [id]: { phase: 'done', result } }))
    } catch (e: unknown) {
      if (runs.current[id] === mine) setTests((t) => ({ ...t, [id]: { phase: 'error', message: testErrorText(e) } }))
    }
  }, [])

  // One at a time: each test makes the server dial out, and the server rate-limits them per admin.
  const testAll = async () => {
    if (runningAll) return
    setRunningAll(true)
    try {
      for (const c of connections) {
        if (tests[c.id]?.phase === 'running') continue
        await runTest(c.id)
      }
    } finally {
      setRunningAll(false)
    }
  }

  if (connections.length === 0) {
    return (
      <Card className="flex flex-col items-center gap-2 px-6 py-12 text-center">
        <Plug className="size-6 text-subtle-foreground" aria-hidden="true" />
        <p role="status" className="m-0 max-w-md text-muted-foreground">
          No connections are configured yet. A connection appears once its address is set: the database connection
          string, the Kafka bootstrap servers, or a Connect or Schema Registry URL.
        </p>
      </Card>
    )
  }

  const healths = connections.map((c) => healthOf(tests[c.id]))
  const count = (...hs: ConnectionHealth[]) => healths.filter((h) => hs.includes(h)).length
  const anyRunning = runningAll || healths.includes('testing')

  return (
    <div className="flex flex-col gap-4">
      <SummaryStrip
        total={connections.length}
        healthy={count('healthy')}
        attention={count('warnings', 'failed', 'error')}
        untested={count('untested', 'testing')}
      />

      <Card className="overflow-hidden">
        <div className="flex flex-wrap items-center gap-3 border-b border-border px-4 py-3">
          <div className="min-w-0 flex-1">
            <h2 className="m-0 text-sm font-semibold">Connection tests</h2>
            <p className="m-0 text-xs text-muted-foreground">
              A test checks reachability, login and every permission Piperoom needs. It only reads.
            </p>
          </div>
          <Button type="button" variant="outline" size="sm" onClick={testAll} disabled={anyRunning}>
            {runningAll ? <LoaderCircle className="animate-spin" aria-hidden="true" /> : <Play aria-hidden="true" />}
            {runningAll ? 'Testing all…' : 'Test all'}
          </Button>
        </div>
        <ul className="m-0 list-none divide-y divide-border p-0">
          {connections.map((c) => (
            <ConnectionRow key={c.id} connection={c} test={tests[c.id] ?? { phase: 'idle' }} onTest={() => void runTest(c.id)} />
          ))}
        </ul>
      </Card>
    </div>
  )
}

function SummaryStrip({ total, healthy, attention, untested }: { total: number; healthy: number; attention: number; untested: number }) {
  // Two columns on a phone, four from md; the borders follow the grid (as the room list's strip).
  const cell = 'max-md:[&:nth-child(even)]:border-l max-md:[&:nth-child(n+3)]:border-t md:[&:nth-child(n+2)]:border-l'
  return (
    <Card className="grid grid-cols-2 md:grid-cols-4" role="group" aria-label="Connection summary">
      <Stat className={cell} label="Connections" value={total} caption={total === 1 ? '1 system' : `${total} systems`} />
      <Stat className={cell} label="Healthy" dot="bg-success" value={healthy} caption={healthy === total ? 'all passing' : 'passed every check'} />
      <Stat
        className={cell}
        label="Needs attention"
        dot="bg-warning"
        value={attention}
        valueClass={attention > 0 ? 'text-warning-foreground' : undefined}
        caption={attention > 0 ? 'warnings or failures' : 'nothing flagged'}
      />
      <Stat className={cell} label="Not tested" value={untested} caption={untested > 0 ? 'run Test all' : 'all tested'} />
    </Card>
  )
}

function Stat({
  label,
  dot,
  value,
  caption,
  valueClass,
  className,
}: {
  label: string
  dot?: string
  value: number
  caption: string
  valueClass?: string
  className?: string
}) {
  return (
    <div className={cn('flex min-w-0 flex-col gap-1 border-border p-4', className)}>
      <span className="inline-flex items-center gap-1.5 text-xs text-muted-foreground">
        {dot && <span className={cn('size-1.5 rounded-full', dot)} aria-hidden="true" />}
        {label}
      </span>
      <span className={cn('text-xl font-semibold leading-6 tracking-tight tabular-nums', valueClass)}>{value}</span>
      <span className="truncate text-xs text-subtle-foreground">{caption}</span>
    </div>
  )
}

function HealthPill({ health, result }: { health: ConnectionHealth; result?: ProbeResult }) {
  const s = HEALTH_STYLE[health]
  return (
    <span className="inline-flex h-6 shrink-0 items-center gap-1.5 whitespace-nowrap rounded-full border border-border bg-card px-2 text-xs font-medium">
      <span className={cn('size-1.5 rounded-full', s.dot)} aria-hidden="true" />
      <span className={s.text}>{healthLabel(health, result)}</span>
    </span>
  )
}

function ConnectionRow({ connection: c, test, onTest }: { connection: AdminConnection; test: TestState; onTest: () => void }) {
  const [open, setOpen] = useState(false)
  const Icon = KIND_ICON[c.kind] ?? Plug
  const titleId = `conn-${c.id}`
  const detailsId = `conn-details-${c.id}`
  const health = healthOf(test)
  const result = test.phase === 'done' ? test.result : undefined
  const usedBy = lines(c.used_by)
  const label = kindLabel(c.kind)
  const endpoint = endpointSummary(c)

  const start = () => {
    if (test.phase === 'running') return
    // A test that finds something opens the details, so the fix is in view.
    setOpen(true)
    onTest()
  }

  return (
    <li role="group" aria-labelledby={titleId}>
      <div className="flex flex-wrap items-center gap-x-4 gap-y-2 px-4 py-3">
        <span className="grid size-9 shrink-0 place-items-center rounded-md border border-border bg-muted/60">
          <Icon className="size-[18px] text-muted-foreground" aria-hidden="true" />
        </span>
        <div className="flex min-w-0 flex-1 flex-col gap-0.5">
          <div className="flex min-w-0 flex-wrap items-center gap-2">
            <h3 id={titleId} className="m-0 text-sm font-semibold">
              {label}
            </h3>
            <span className="text-xs text-subtle-foreground">{kindRole(c.kind)}</span>
            {c.scope === 'pipeline' && <Badge variant="accent">{scopeLabel(c)}</Badge>}
          </div>
          <p className="m-0 truncate text-xs text-muted-foreground">
            <span className={cn(endpoint.address && 'font-mono')}>{endpoint.text}</span>
            <span className="px-1.5 text-subtle-foreground" aria-hidden="true">
              ·
            </span>
            {usedBy.length === 0 ? 'no pipeline uses it' : `used by ${usedBy.join(', ')}`}
          </p>
        </div>
        <HealthPill health={health} result={result} />
        <div className="flex items-center gap-1">
          <Button
            type="button"
            variant="outline"
            size="sm"
            onClick={start}
            disabled={test.phase === 'running'}
            aria-label={`Test connection: ${label}, ${scopeLabel(c)}`}
          >
            {/* The pill already says "Testing…"; the button only turns its icon into a spinner. */}
            {test.phase === 'running' ? <LoaderCircle className="animate-spin" aria-hidden="true" /> : <Plug aria-hidden="true" />}
            Test
          </Button>
          <Button
            type="button"
            variant="ghost"
            size="icon"
            onClick={() => setOpen((o) => !o)}
            aria-expanded={open}
            aria-controls={detailsId}
            aria-label={`${open ? 'Hide' : 'Show'} details: ${label}`}
          >
            <ChevronDown className={cn('size-4 transition-transform', open && 'rotate-180')} aria-hidden="true" />
          </Button>
        </div>
      </div>

      {/* Always in the page, so a screen reader announces the outcome even with the details closed. */}
      <div role="status" aria-live="polite" className="sr-only">
        {test.phase === 'running' && `Testing ${label}…`}
        {test.phase === 'done' && probeSummary(test.result)}
      </div>
      {test.phase === 'error' && (
        <p role="alert" className="m-0 px-4 pb-3 text-sm font-medium text-destructive-fg sm:pl-[68px]">
          {test.message}
        </p>
      )}

      <div id={detailsId} hidden={!open} className="border-t border-border bg-muted/25 px-4 py-4 sm:pl-[68px]">
        <div className="grid gap-6 lg:grid-cols-[minmax(0,1fr)_minmax(0,1.15fr)]">
          <section aria-label={`Configuration of ${label}`} className="flex min-w-0 flex-col gap-2">
            <h4 className="m-0 text-xs font-medium uppercase tracking-wide text-subtle-foreground">Configuration</h4>
            <SettingsList settings={lines(c.settings)} />
            <p className="m-0 text-[11px] text-subtle-foreground">
              ID <span className="font-mono">{c.id}</span>
            </p>
          </section>
          <section aria-label={`Last test of ${label}`} className="flex min-w-0 flex-col gap-2">
            <h4 className="m-0 text-xs font-medium uppercase tracking-wide text-subtle-foreground">Last test</h4>
            {test.phase === 'idle' && (
              <p className="m-0 text-sm text-muted-foreground">Not tested yet. Run a test to check reachability, login and permissions.</p>
            )}
            {test.phase === 'running' && <p className="m-0 text-sm text-muted-foreground">Testing…</p>}
            {test.phase === 'error' && <p className="m-0 text-sm text-muted-foreground">The test could not run.</p>}
            {test.phase === 'done' && <TestResult result={test.result} />}
          </section>
        </div>
      </div>
    </li>
  )
}

function SettingsList({ settings }: { settings: readonly ConnectionSetting[] }) {
  if (settings.length === 0) return <p className="m-0 text-sm text-subtle-foreground">No settings reported.</p>
  return (
    <dl className="m-0 divide-y divide-border rounded-md border border-border bg-card">
      {settings.map((s) => {
        const v = settingValue(s)
        const src = settingSource(s.from)
        return (
          <div key={s.key} className="grid grid-cols-[minmax(0,10rem)_minmax(0,1fr)] gap-3 px-3 py-2" data-setting={s.key}>
            <dt className="text-sm text-muted-foreground" title={s.key}>
              {settingLabel(s.key)}
            </dt>
            <dd className="m-0 flex min-w-0 flex-col gap-0.5">
              {v.kind === 'secret' ? (
                <span className={cn('inline-flex items-center gap-1 text-sm', v.set ? 'text-foreground' : 'text-warning-foreground')}>
                  {v.set ? (
                    <>
                      <Check className="size-3.5 text-success-foreground" aria-hidden="true" />
                      <span>set</span>
                      <span className="sr-only">(hidden)</span>
                    </>
                  ) : (
                    <>
                      <TriangleAlert className="size-3.5" aria-hidden="true" />
                      <span>not set</span>
                    </>
                  )}
                </span>
              ) : v.text ? (
                <span className="break-all font-mono text-[13px]">{v.text}</span>
              ) : (
                <span className="text-subtle-foreground">
                  <span aria-hidden="true">–</span>
                  <span className="sr-only">empty</span>
                </span>
              )}
              {src.label && (
                <span className="text-xs text-subtle-foreground">
                  {src.label}
                  {src.name && (
                    <>
                      {' '}
                      <span className="font-mono">{src.name}</span>
                    </>
                  )}
                </span>
              )}
            </dd>
          </div>
        )
      })}
    </dl>
  )
}

function TestResult({ result }: { result: ProbeResult }) {
  const checks = lines(result.checks)
  const health = resultHealth(result)
  return (
    <div className="flex flex-col gap-2">
      <p className={cn('m-0 text-sm font-medium', HEALTH_STYLE[health].text)} data-testid="probe-summary">
        {probeSummary(result)}
      </p>
      <ol className="m-0 list-none divide-y divide-border rounded-md border border-border bg-card p-0" aria-label="Checks">
        {checks.map((c) => (
          <CheckRow key={c.id} check={c} />
        ))}
      </ol>
    </div>
  )
}

function CheckRow({ check: c }: { check: ProbeCheck }) {
  const status = knownStatus(c.status)
  const { icon: Icon, className } = STATUS_ICON[status]
  const label = checkLabel(c.id)
  const flagged = status === 'warn' || status === 'fail'
  return (
    <li className="flex flex-col gap-1.5 px-3 py-2" data-status={status} data-check={c.id}>
      <div className="flex items-start gap-2.5">
        <Icon className={cn('mt-0.5 size-4 shrink-0', className)} aria-hidden="true" />
        <div className="flex min-w-0 flex-1 flex-col">
          <span className="text-sm" title={c.id}>
            {label}
            {/* The word, not only the icon and colour. */}
            <span className="sr-only"> {STATUS_LABEL[status]}</span>
          </span>
          {c.detail && !flagged && <span className="text-xs text-muted-foreground">{c.detail}</span>}
          {c.problem && <span className="text-sm text-foreground">{c.problem}</span>}
        </div>
        {c.ms > 0 && <span className="shrink-0 font-mono text-[11px] tabular-nums text-subtle-foreground">{c.ms} ms</span>}
      </div>
      {c.fix && <Fix label={label} text={c.fix} />}
    </li>
  )
}

function Fix({ label, text }: { label: string; text: string }) {
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
    <div className="ml-[26px] flex items-stretch overflow-hidden rounded-md border border-border bg-muted/50">
      <code className="min-w-0 flex-1 whitespace-pre-wrap break-words px-2.5 py-1.5 font-mono text-xs">{text}</code>
      <Button
        type="button"
        variant="ghost"
        size="sm"
        className="h-auto shrink-0 rounded-none border-l border-border"
        onClick={copy}
        aria-label={`Copy fix: ${label}`}
      >
        {copied ? <Check aria-hidden="true" /> : <Copy aria-hidden="true" />}
        {copied ? 'Copied' : 'Copy'}
      </Button>
    </div>
  )
}
