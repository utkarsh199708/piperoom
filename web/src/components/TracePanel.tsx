import { useEffect, useRef, useState } from 'react'
import type { ChangeEvent, FormEvent } from 'react'
import type { TraceState } from '../state/types'
import type { RoomActions } from '../hooks/useRoom'
import { cn } from '@/lib/utils'
import { Badge, layerVariant } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { traceVerdict } from '../lib/trace'
import { factLabel, factValue } from '../lib/facts'
import { StatusDot } from './room/StatusDot'
import { RoomDialogClose } from './room/RoomDialogClose'
import { useRestoreFocus } from './room/useRestoreFocus'

const STATUS: Record<string, { variant: 'destructive' | 'success' | 'neutral' | 'warning'; word: string }> = {
  stopped_here: { variant: 'destructive', word: 'stopped here' },
  seen: { variant: 'success', word: 'seen' },
  not_seen: { variant: 'neutral', word: 'not seen' },
}

// A shadcn Dialog: Esc and the X close it, focus is trapped inside and goes back to
// whatever opened it. The stages reuse the pipeline node look; the stage where the
// record stopped gets the only coloured border.
export function TracePanel({
  trace,
  actions,
  onClose,
}: {
  trace: TraceState | null
  actions: RoomActions
  onClose: () => void
}) {
  const [table, setTable] = useState('')
  const [key, setKey] = useState('')
  const [busy, setBusy] = useState(false)
  // Why the last trace failed. Shown here, not in the room banner: the dialog's overlay covers the banner.
  const [error, setError] = useState<string | null>(null)
  const restoreFocus = useRestoreFocus()
  // Which submit may still report back. An edit or closing the panel moves it on, so an answer for words
  // the person has since changed (or for a closed panel) is dropped instead of landing next to new input.
  const attempt = useRef(0)
  useEffect(
    () => () => {
      attempt.current += 1
    },
    [],
  )

  const edit = (set: (value: string) => void) => (e: ChangeEvent<HTMLInputElement>) => {
    attempt.current += 1
    setBusy(false)
    setError(null)
    set(e.target.value)
  }

  const submit = async (e: FormEvent) => {
    e.preventDefault()
    if (busy || !table.trim() || !key.trim()) return
    const mine = ++attempt.current
    setBusy(true)
    setError(null)
    const result = await actions.trace(table.trim(), key.trim())
    if (mine !== attempt.current) return
    setBusy(false)
    if (!result.ok) setError(result.error ?? 'The trace failed.')
  }

  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent
        hideClose
        className="left-auto right-0 top-0 h-full max-h-none w-full max-w-md content-start translate-x-0 translate-y-0 overflow-y-auto rounded-none border-l data-[state=closed]:slide-out-to-right data-[state=open]:slide-in-from-right data-[state=closed]:zoom-out-100 data-[state=open]:zoom-in-100"
        onCloseAutoFocus={restoreFocus}
      >
        <RoomDialogClose label="Close trace panel" />
        <DialogHeader>
          <DialogTitle>Trace a record</DialogTitle>
          <DialogDescription>
            Follow one row from the source table to the warehouse and see where it stops.
          </DialogDescription>
        </DialogHeader>

        <form className="flex flex-wrap items-end gap-3" onSubmit={submit}>
          <div className="flex min-w-[10rem] flex-1 flex-col gap-1.5">
            <label htmlFor="trace-table" className="text-sm font-medium">
              Table
            </label>
            <Input
              id="trace-table"
              className="font-mono"
              value={table}
              onChange={edit(setTable)}
              placeholder="schema.table"
              required
              aria-invalid={error ? true : undefined}
              aria-describedby={error ? 'trace-error' : undefined}
            />
          </div>
          <div className="flex min-w-[8rem] flex-1 flex-col gap-1.5">
            <label htmlFor="trace-key" className="text-sm font-medium">
              Key
            </label>
            <Input
              id="trace-key"
              className="font-mono"
              value={key}
              onChange={edit(setKey)}
              placeholder="primary key value"
              required
              aria-invalid={error ? true : undefined}
              aria-describedby={error ? 'trace-error' : undefined}
            />
          </div>
          <Button type="submit" disabled={busy || !table.trim() || !key.trim()}>
            Trace
          </Button>
        </form>

        {error && (
          <p id="trace-error" role="alert" className="m-0 text-sm font-medium text-destructive-fg">
            {error}
          </p>
        )}

        {trace && (
          <div className="flex min-w-0 flex-col gap-2">
            {(() => {
              const v = traceVerdict(trace)
              return (
                <p role="status" className="m-0 flex items-start gap-2 text-sm font-medium">
                  <StatusDot tone={v.tone} className="mt-1.5" />
                  <span>{v.text}</span>
                </p>
              )
            })()}
            <p className="m-0 text-xs text-muted-foreground">
              <span className="font-mono">{trace.table}</span> · key <span className="font-mono">{trace.key}</span>
            </p>
            <div role="group" aria-label="Trace stages, source to sink">
              <ol className="m-0 flex list-none flex-col p-0">
                {(trace.stages ?? []).map((s, i, all) => {
                  const st = STATUS[s.status] ?? { variant: 'warning' as const, word: s.status.replace('_', ' ') }
                  const stopped = s.status === 'stopped_here'
                  const last = i === all.length - 1
                  return (
                    <li
                      key={i}
                      data-status={s.status}
                      data-worst={stopped ? 'true' : undefined}
                      className={cn('trace-stage flex min-w-0 items-stretch gap-3', `trace-stage--${s.status}`)}
                    >
                      {/* The rail: a numbered dot per stage and a line to the next one. */}
                      <div className="flex w-6 shrink-0 flex-col items-center">
                        <span
                          aria-label={`Stage ${i + 1} of ${all.length}`}
                          className={cn(
                            'inline-flex size-6 shrink-0 items-center justify-center rounded-full border text-xs font-medium tabular-nums',
                            stopped
                              ? 'border-destructive bg-destructive-subtle text-destructive-fg'
                              : s.status === 'seen'
                                ? 'border-success/40 bg-success-subtle text-success-foreground'
                                : 'border-border bg-card text-subtle-foreground',
                            s.status === 'unknown' && 'border-dashed',
                          )}
                        >
                          {i + 1}
                        </span>
                        {!last && <span aria-hidden="true" className="w-px flex-1 bg-border" />}
                      </div>
                      <div className={cn('flex min-w-0 flex-1 flex-col gap-1', last ? 'pb-0' : 'pb-4')}>
                        <div className="flex min-w-0 items-center gap-2">
                          <span className="min-w-0 break-all font-mono text-sm font-medium">{s.name}</span>
                          <Badge variant={st.variant} className="ml-auto shrink-0">
                            {st.word}
                          </Badge>
                        </div>
                        <div className="flex flex-wrap items-center gap-x-2 gap-y-1 text-xs text-muted-foreground">
                          <Badge variant={layerVariant(s.layer)} mono>
                            {s.layer}
                          </Badge>
                          {s.facts && (
                            <dl className="m-0 contents">
                              {Object.entries(s.facts).map(([k, v]) => (
                                <div key={k} className="flex min-w-0 gap-1 tabular-nums" title={k}>
                                  <dt className="text-subtle-foreground">{factLabel(k)}</dt>
                                  <dd className="m-0 break-all">{factValue(v)}</dd>
                                </div>
                              ))}
                            </dl>
                          )}
                        </div>
                      </div>
                    </li>
                  )
                })}
              </ol>
            </div>
          </div>
        )}
      </DialogContent>
    </Dialog>
  )
}
