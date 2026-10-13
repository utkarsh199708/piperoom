import { Fragment, useEffect, useRef, useState } from 'react'
import type { FormEvent } from 'react'
import type { TraceState } from '../state/types'
import type { RoomActions } from '../hooks/useRoom'
import { Chip } from './ui'
// TEMP until P1: LayerTag comes from './ui' once it lands.
import { LayerTag } from './p2Standins'
import '../styles/flow.css'

/** Trace stage status → the node health it borrows its look from (§7.11). */
const STAGE_HEALTH: Record<string, string> = {
  seen: 'ok',
  not_seen: 'unknown',
  stopped_here: 'fail',
}

function stageTone(status: string): string {
  if (status === 'stopped_here') return 'fail'
  if (status === 'seen') return 'ok'
  if (status === 'not_seen') return 'muted'
  return 'warn'
}

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
  const tableRef = useRef<HTMLInputElement>(null)
  const onCloseRef = useRef(onClose)
  useEffect(() => {
    onCloseRef.current = onClose
  })

  // Open on the first field, close on Esc, and give focus back to whatever opened the dialog.
  useEffect(() => {
    const opener = document.activeElement instanceof HTMLElement ? document.activeElement : null
    tableRef.current?.focus()
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onCloseRef.current()
    }
    document.addEventListener('keydown', onKey)
    return () => {
      document.removeEventListener('keydown', onKey)
      opener?.focus()
    }
  }, [])

  const submit = (e: FormEvent) => {
    e.preventDefault()
    if (!table.trim() || !key.trim()) return
    setBusy(true)
    actions.trace(table.trim(), key.trim())
    window.setTimeout(() => setBusy(false), 400)
  }

  const stages = trace?.stages ?? []

  return (
    <div className="overlay" role="dialog" aria-modal="true" aria-label="Trace a record">
      <div className="dialog trace">
        <header className="dialog__head">
          <h2 className="dialog__title">Trace a record</h2>
          <button type="button" className="icon-btn" onClick={onClose} aria-label="Close trace panel">
            ×
          </button>
        </header>

        <form className="trace__form" onSubmit={submit}>
          <div className="field">
            <label htmlFor="trace-table">Table</label>
            <input
              id="trace-table"
              ref={tableRef}
              value={table}
              onChange={(e) => setTable(e.target.value)}
              required
            />
          </div>
          <div className="field">
            <label htmlFor="trace-key">Key</label>
            <input id="trace-key" value={key} onChange={(e) => setKey(e.target.value)} required />
          </div>
          <button type="submit" className="btn btn--primary" disabled={busy || !table.trim() || !key.trim()}>
            Trace
          </button>
        </form>

        {trace && (
          <div className="trace__stages">
            {stages.map((s, i) => (
              <Fragment key={i}>
                {i > 0 && (
                  <div
                    className={`edge ${stages[i - 1].status === 'stopped_here' ? 'edge--stalled' : 'edge--flow'}`}
                    aria-hidden="true"
                  >
                    <div className="edge__line" />
                  </div>
                )}
                <div
                  className={`trace-stage trace-stage--${s.status} node-card node-card--${STAGE_HEALTH[s.status] ?? 'warn'}`}
                >
                  <div className="node-card__top">
                    <LayerTag layer={s.layer} />
                  </div>
                  <div className="node-card__name" title={s.name}>
                    {s.name}
                  </div>
                  {s.facts && Object.keys(s.facts).length > 0 && (
                    <dl className="node-card__facts">
                      {Object.entries(s.facts).map(([k, v]) => (
                        <Fragment key={k}>
                          <dt>{k}</dt>
                          <dd>{v}</dd>
                        </Fragment>
                      ))}
                    </dl>
                  )}
                  <div className="node-card__foot">
                    <Chip tone={stageTone(s.status)}>{s.status.replace('_', ' ')}</Chip>
                  </div>
                </div>
              </Fragment>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
