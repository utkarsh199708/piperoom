// The maintenance-window control (U1-12b; docs/api.md POST
// /api/rooms/{id}/setup/maintenance-window). A T3 step whose guard row needs a window — a
// restart, wal_level or binlog_format — is created and run only inside the latest one.
import { useId, useState } from 'react'
import type { FormEvent } from 'react'
import { CalendarClock } from 'lucide-react'
import { api } from '@/api/client'
import type { RoomState } from '@/state/types'
import type { CurrentUser } from '@/state/permissions'
import { canSteer } from '@/state/permissions'
import { SetupControlError, windowFromInputs } from '@/lib/setupControls'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { FIELD_ERROR, FIELD_LABEL, SetupAlert, SetupStatus } from './SetupFields'
import { useSetupAction } from './useSetupAction'

function localTime(iso: string): string {
  const date = new Date(iso)
  return Number.isNaN(date.getTime()) ? iso : date.toLocaleString()
}

export function MaintenanceWindow({ state, user }: { state: RoomState; user: CurrentUser }) {
  const setup = state.setup
  const formId = useId()
  const canEdit = canSteer(state, user)
  const { busy, error, done, run } = useSetupAction()
  const [start, setStart] = useState('')
  const [end, setEnd] = useState('')
  const [fieldError, setFieldError] = useState<{ field: string; message: string } | null>(null)
  if (!setup) return null

  const submit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault()
    let window
    try {
      window = windowFromInputs(start, end)
    } catch (err) {
      if (err instanceof SetupControlError) {
        setFieldError({ field: err.field, message: err.message })
        return
      }
      throw err
    }
    setFieldError(null)
    const ok = await run(
      () => api.setupMaintenanceWindow(state.id, window),
      'Maintenance window set. A guarded step waits for it.',
    )
    if (ok) {
      setStart('')
      setEnd('')
    }
  }

  const errorFor = (field: string) => (fieldError?.field === field ? fieldError.message : undefined)

  return (
    <Card
      role="region"
      aria-labelledby={`${formId}-title`}
      className="mx-4 mt-2 flex min-w-0 flex-col gap-3 p-3 sm:mx-6"
    >
      <div className="flex min-w-0 flex-wrap items-center gap-2">
        <CalendarClock className="size-4 shrink-0 text-muted-foreground" aria-hidden="true" />
        <h2 id={`${formId}-title`} className="m-0 text-sm font-semibold">
          Maintenance window
        </h2>
      </div>
      <p className="m-0 text-xs text-muted-foreground">
        A step that restarts a database or changes <code>wal_level</code> or <code>binlog_format</code>{' '}
        runs only inside the latest window. At most four hours, starting at most 24 hours from now.
      </p>
      {setup.maintenance_window && (
        <p className="m-0 text-xs text-foreground">
          Current window:{' '}
          <time dateTime={setup.maintenance_window.start}>{localTime(setup.maintenance_window.start)}</time>
          {' – '}
          <time dateTime={setup.maintenance_window.end}>{localTime(setup.maintenance_window.end)}</time>
        </p>
      )}

      {canEdit ? (
        <form className="flex min-w-0 flex-col gap-2" onSubmit={submit} aria-label="Set a maintenance window">
          <div className="grid min-w-0 gap-2 sm:grid-cols-2">
            <div className="grid gap-1">
              <label htmlFor={`${formId}-start`} className={FIELD_LABEL}>
                Window starts
              </label>
              <Input
                id={`${formId}-start`}
                type="datetime-local"
                value={start}
                onChange={(e) => setStart(e.target.value)}
                disabled={busy}
                aria-invalid={errorFor('start') ? true : undefined}
                aria-describedby={errorFor('start') ? `${formId}-start-error` : undefined}
              />
              {errorFor('start') && (
                <span id={`${formId}-start-error`} className={FIELD_ERROR}>
                  {errorFor('start')}
                </span>
              )}
            </div>
            <div className="grid gap-1">
              <label htmlFor={`${formId}-end`} className={FIELD_LABEL}>
                Window ends
              </label>
              <Input
                id={`${formId}-end`}
                type="datetime-local"
                value={end}
                onChange={(e) => setEnd(e.target.value)}
                disabled={busy}
                aria-invalid={errorFor('end') ? true : undefined}
                aria-describedby={errorFor('end') ? `${formId}-end-error` : undefined}
              />
              {errorFor('end') && (
                <span id={`${formId}-end-error`} className={FIELD_ERROR}>
                  {errorFor('end')}
                </span>
              )}
            </div>
          </div>
          <div>
            <Button type="submit" variant="outline" size="sm" className="coarse:h-11" disabled={busy}>
              Set maintenance window
            </Button>
          </div>
        </form>
      ) : (
        <p className="m-0 text-xs text-subtle-foreground">Only the driver can set a maintenance window.</p>
      )}

      {error && <SetupAlert>{error}</SetupAlert>}
      <SetupStatus>{done ?? ''}</SetupStatus>
    </Card>
  )
}
