// The targets control (U1-12b; docs/api.md POST /api/rooms/{id}/setup/targets and
// .../targets/{t}/class). The room's targets and their classes, a form to add one, and a raise
// per target that offers only classes above its current floor. The server combines the class
// with the one it derived and refuses a lower one, so the select never shows one.
import { useId, useState } from 'react'
import type { FormEvent } from 'react'
import { Plus, ShieldAlert } from 'lucide-react'
import { api } from '@/api/client'
import type { RoomState } from '@/state/types'
import type { CurrentUser } from '@/state/permissions'
import { canSteer } from '@/state/permissions'
import { classesAbove } from '@/lib/setupControls'
import { TARGET_KIND_LABELS, TARGET_KINDS } from '@/lib/setupForm'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { FIELD_LABEL, SELECT, SetupAlert, SetupStatus } from './SetupFields'
import { useSetupAction } from './useSetupAction'

const ADD_CLASSES = ['shared', 'production'] as const

export function TargetControls({ state, user }: { state: RoomState; user: CurrentUser }) {
  const setup = state.setup
  const formId = useId()
  const canEdit = canSteer(state, user)
  const { busy, error, done, run } = useSetupAction()
  const [kind, setKind] = useState('postgres')
  const [address, setAddress] = useState('')
  const [addClass, setAddClass] = useState('')
  const [raises, setRaises] = useState<Record<string, string>>({})
  const [addressError, setAddressError] = useState<string | null>(null)
  if (!setup) return null

  const addTarget = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault()
    const trimmed = address.trim()
    if (!trimmed) {
      setAddressError('Target address is required.')
      return
    }
    setAddressError(null)
    // The server resolves the address and refuses loopback of another host, link-local and
    // cloud metadata endpoints (400 address_refused); the form only asks for a non-empty one.
    const ok = await run(
      () => api.setupTarget(state.id, { kind, address: trimmed, ...(addClass ? { class: addClass } : {}) }),
      `Target ${trimmed} added.`,
    )
    if (ok) {
      setAddress('')
      setAddClass('')
    }
  }

  const raiseClass = async (target: string, current: string) => {
    const options = classesAbove(current)
    const cls = raises[target] ?? options[0]
    if (!cls) return
    const ok = await run(
      () => api.setupTargetClass(state.id, target, { class: cls }),
      `Class raised to ${cls} for ${target}.`,
    )
    if (ok) setRaises((all) => ({ ...all, [target]: '' }))
  }

  return (
    <Card
      role="region"
      aria-labelledby={`${formId}-title`}
      className="mx-4 mt-2 flex min-w-0 flex-col gap-3 p-3 sm:mx-6"
    >
      <div className="flex min-w-0 flex-wrap items-center gap-2">
        <ShieldAlert className="size-4 shrink-0 text-muted-foreground" aria-hidden="true" />
        <h2 id={`${formId}-title`} className="m-0 text-sm font-semibold">
          Targets and classes
        </h2>
      </div>
      <p className="m-0 text-xs text-muted-foreground">
        Every host or account the runner may reach. A class is a floor: a raise makes a step ask a
        person sooner, and the server never lets one drop.
      </p>

      {setup.targets.length === 0 ? (
        <p className="m-0 text-xs text-subtle-foreground">No targets registered yet.</p>
      ) : (
        <ul aria-label="Setup targets" className="m-0 flex list-none flex-col gap-2 p-0">
          {setup.targets.map((target) => {
            const options = classesAbove(target.class)
            const selectId = `${formId}-class-${target.address}`
            return (
              <li
                key={target.address}
                className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1 rounded-md border border-border px-2 py-1.5"
              >
                <span className="text-xs text-muted-foreground">{TARGET_KIND_LABELS[target.kind] ?? target.kind}</span>
                <code className="min-w-0 truncate font-mono text-xs text-foreground">{target.address}</code>
                <Badge variant="outline" mono>
                  {target.class || 'shared'}
                </Badge>
                {canEdit &&
                  (options.length > 0 ? (
                    <span className="ml-auto flex min-w-0 items-center gap-1.5">
                      <label htmlFor={selectId} className="text-xs text-muted-foreground">
                        New class for {target.address}
                      </label>
                      <select
                        id={selectId}
                        className={`${SELECT} w-auto`}
                        value={raises[target.address] ?? options[0]}
                        onChange={(e) => setRaises((all) => ({ ...all, [target.address]: e.target.value }))}
                        disabled={busy}
                      >
                        {options.map((cls) => (
                          <option key={cls} value={cls}>
                            {cls}
                          </option>
                        ))}
                      </select>
                      <Button
                        variant="outline"
                        size="sm"
                        className="coarse:h-11"
                        disabled={busy}
                        onClick={() => raiseClass(target.address, target.class)}
                        aria-label={`Raise class for ${target.address}`}
                      >
                        Raise
                      </Button>
                    </span>
                  ) : (
                    <span className="ml-auto text-xs text-subtle-foreground">Highest class</span>
                  ))}
              </li>
            )
          })}
        </ul>
      )}

      {canEdit ? (
        <form className="flex min-w-0 flex-col gap-2" onSubmit={addTarget} aria-label="Add a target">
          <div className="grid min-w-0 gap-2 sm:grid-cols-[9rem_minmax(0,1fr)_9rem_auto] sm:items-start">
            <div className="grid gap-1">
              <label htmlFor={`${formId}-kind`} className={FIELD_LABEL}>
                Kind
              </label>
              <select
                id={`${formId}-kind`}
                className={SELECT}
                value={kind}
                onChange={(e) => setKind(e.target.value)}
                disabled={busy}
              >
                {TARGET_KINDS.map((k) => (
                  <option key={k} value={k}>
                    {TARGET_KIND_LABELS[k] ?? k}
                  </option>
                ))}
              </select>
            </div>
            <div className="grid gap-1">
              <label htmlFor={`${formId}-address`} className={FIELD_LABEL}>
                New target address
              </label>
              <Input
                id={`${formId}-address`}
                value={address}
                onChange={(e) => setAddress(e.target.value)}
                placeholder="kafka.acme:9092"
                autoComplete="off"
                disabled={busy}
                aria-invalid={addressError ? true : undefined}
                aria-describedby={addressError ? `${formId}-address-error` : undefined}
              />
              {addressError && (
                <span id={`${formId}-address-error`} className="text-xs text-destructive-fg">
                  {addressError}
                </span>
              )}
            </div>
            <div className="grid gap-1">
              <label htmlFor={`${formId}-class`} className={FIELD_LABEL}>
                New target class
              </label>
              <select
                id={`${formId}-class`}
                className={SELECT}
                value={addClass}
                onChange={(e) => setAddClass(e.target.value)}
                disabled={busy}
              >
                <option value="">server decides</option>
                {ADD_CLASSES.map((cls) => (
                  <option key={cls} value={cls}>
                    {cls}
                  </option>
                ))}
              </select>
            </div>
            <div className="flex items-end">
              <Button type="submit" variant="outline" size="sm" className="coarse:h-11" disabled={busy}>
                <Plus aria-hidden="true" />
                Add target
              </Button>
            </div>
          </div>
          <p className="m-0 text-xs text-subtle-foreground">
            Addresses resolve on the server: loopback of another host, link-local and cloud metadata
            endpoints are refused.
          </p>
        </form>
      ) : (
        <p className="m-0 text-xs text-subtle-foreground">
          Only the driver can add targets or raise a class. Take control to change the setup.
        </p>
      )}

      {error && <SetupAlert>{error}</SetupAlert>}
      <SetupStatus>{done ?? ''}</SetupStatus>
    </Card>
  )
}
