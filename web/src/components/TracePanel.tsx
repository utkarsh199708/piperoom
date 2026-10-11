import { useState } from 'react'
import type { FormEvent } from 'react'
import type { TraceState } from '../state/types'
import type { RoomActions } from '../hooks/useRoom'
import { Chip } from './ui'

export function TracePanel({
  trace,
  actions,
  onClose,
}: {
  trace: TraceState | null
  actions: RoomActions
  onClose: () => void
}) {
  const [table, setTable] = useState('orders')
  const [key, setKey] = useState('id=10042')
  const [busy, setBusy] = useState(false)

  const submit = (e: FormEvent) => {
    e.preventDefault()
    if (!table.trim() || !key.trim()) return
    setBusy(true)
    actions.trace(table.trim(), key.trim())
    window.setTimeout(() => setBusy(false), 400)
  }

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
            <input id="trace-table" value={table} onChange={(e) => setTable(e.target.value)} required />
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
            {(trace.stages ?? []).map((s, i) => (
              <div key={i} className={`trace-stage trace-stage--${s.status}`}>
                {i > 0 && <span className="trace-stage__arrow" aria-hidden="true">→</span>}
                <div className="trace-stage__card">
                  <div className="trace-stage__name">{s.name}</div>
                  <div className="trace-stage__meta muted">{s.layer}</div>
                  <Chip
                    tone={
                      s.status === 'stopped_here'
                        ? 'fail'
                        : s.status === 'seen'
                          ? 'ok'
                          : s.status === 'not_seen'
                            ? 'muted'
                            : 'warn'
                    }
                  >
                    {s.status.replace('_', ' ')}
                  </Chip>
                  {s.facts && (
                    <dl className="trace-stage__facts">
                      {Object.entries(s.facts).map(([k, v]) => (
                        <div key={k}>
                          <dt>{k}</dt>
                          <dd>{v}</dd>
                        </div>
                      ))}
                    </dl>
                  )}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  )
}
