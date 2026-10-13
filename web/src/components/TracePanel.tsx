import { useState } from 'react'
import type { FormEvent } from 'react'
import { ArrowRight } from 'lucide-react'
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
  const [table, setTable] = useState('public.orders')
  const [key, setKey] = useState('10042')
  const [busy, setBusy] = useState(false)
  const restoreFocus = useRestoreFocus()

  const submit = (e: FormEvent) => {
    e.preventDefault()
    if (!table.trim() || !key.trim()) return
    setBusy(true)
    actions.trace(table.trim(), key.trim())
    window.setTimeout(() => setBusy(false), 400)
  }

  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent hideClose className="max-h-[85vh] max-w-3xl overflow-y-auto" onCloseAutoFocus={restoreFocus}>
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
            <Input id="trace-table" className="font-mono" value={table} onChange={(e) => setTable(e.target.value)} required />
          </div>
          <div className="flex min-w-[8rem] flex-1 flex-col gap-1.5">
            <label htmlFor="trace-key" className="text-sm font-medium">
              Key
            </label>
            <Input id="trace-key" className="font-mono" value={key} onChange={(e) => setKey(e.target.value)} required />
          </div>
          <Button type="submit" disabled={busy || !table.trim() || !key.trim()}>
            Trace
          </Button>
        </form>

        {trace && (
          <div className="flex min-w-0 flex-col gap-2">
            <p className="m-0 text-xs text-muted-foreground">
              <span className="font-mono">{trace.table}</span> · key <span className="font-mono">{trace.key}</span>
            </p>
            <div
              role="group"
              aria-label="Trace stages, source to sink"
              tabIndex={0}
              className="flex items-stretch gap-1 overflow-x-auto rounded-md pb-1 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            >
              {(trace.stages ?? []).map((s, i) => {
                const st = STATUS[s.status] ?? { variant: 'warning' as const, word: s.status.replace('_', ' ') }
                return (
                  <div
                    key={i}
                    data-status={s.status}
                    className={cn('trace-stage flex items-stretch gap-1', `trace-stage--${s.status}`)}
                  >
                    {i > 0 && (
                      <ArrowRight className="size-4 shrink-0 self-center text-subtle-foreground" aria-hidden="true" />
                    )}
                    <div
                      className={cn(
                        'flex min-w-[160px] flex-col gap-1.5 rounded-lg border bg-card px-3 py-2.5',
                        s.status === 'stopped_here' ? 'border-destructive' : 'border-border',
                        s.status === 'unknown' && 'border-dashed',
                      )}
                    >
                      <Badge variant={layerVariant(s.layer)} mono className="w-fit">
                        {s.layer}
                      </Badge>
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
