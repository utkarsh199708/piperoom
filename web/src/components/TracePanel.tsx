import { useEffect, useRef, useState } from 'react'
import type { FormEvent } from 'react'
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
  const restoreFocus = useRestoreFocus()
  const busyTimer = useRef<number | undefined>(undefined)
  // A timer that outlives the panel would set state on an unmounted component (and in tests, after teardown).
  useEffect(() => () => window.clearTimeout(busyTimer.current), [])

  const submit = (e: FormEvent) => {
    e.preventDefault()
    if (!table.trim() || !key.trim()) return
    setBusy(true)
    actions.trace(table.trim(), key.trim())
    window.clearTimeout(busyTimer.current)
    busyTimer.current = window.setTimeout(() => setBusy(false), 400)
  }

  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent
        hideClose
        className="left-auto right-0 top-0 h-full max-h-none w-full max-w-md translate-x-0 translate-y-0 overflow-y-auto rounded-none border-l data-[state=closed]:slide-out-to-right data-[state=open]:slide-in-from-right data-[state=closed]:zoom-out-100 data-[state=open]:zoom-in-100"
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
              onChange={(e) => setTable(e.target.value)}
              placeholder="schema.table"
              required
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
              onChange={(e) => setKey(e.target.value)}
              placeholder="primary key value"
              required
            />
          </div>
          <Button type="submit" disabled={busy || !table.trim() || !key.trim()}>
            Trace
          </Button>
        </form>

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
            <div
              role="group"
              aria-label="Trace stages, source to sink"
              className="grid grid-cols-[repeat(auto-fill,minmax(160px,1fr))] gap-2"
            >
              {(trace.stages ?? []).map((s, i, all) => {
                const st = STATUS[s.status] ?? { variant: 'warning' as const, word: s.status.replace('_', ' ') }
                return (
                  <div
                    key={i}
                    data-status={s.status}
                    data-worst={s.status === 'stopped_here' ? 'true' : undefined}
                    className={cn(
                      'trace-stage flex min-w-0 flex-col gap-1.5 rounded-lg border bg-card px-3 py-2.5',
                      `trace-stage--${s.status}`,
                      s.status === 'stopped_here' ? 'border-destructive' : 'border-border',
                      s.status === 'unknown' && 'border-dashed',
                    )}
                  >
                    <div className="flex items-center justify-between gap-2">
                      <Badge variant={layerVariant(s.layer)} mono className="w-fit">
                        {s.layer}
                      </Badge>
                      <span
                        aria-label={`Stage ${i + 1} of ${all.length}`}
                        className="font-mono text-xs text-subtle-foreground"
                      >
                        {i + 1}
                      </span>
                    </div>
                    <div className="break-all font-mono text-sm font-medium">{s.name}</div>
                    <Badge variant={st.variant} className="w-fit">
                      {st.word}
                    </Badge>
                    {s.facts && (
                      <dl className="m-0 flex flex-col gap-0.5 font-mono text-xs text-muted-foreground">
                        {Object.entries(s.facts).map(([k, v]) => (
                          <div key={k} className="flex gap-1.5">
                            <dt className="text-subtle-foreground">{k}</dt>
                            <dd className="m-0 break-all">{v}</dd>
                          </div>
                        ))}
                      </dl>
                    )}
                  </div>
                )
              })}
            </div>
          </div>
        )}
      </DialogContent>
    </Dialog>
  )
}
