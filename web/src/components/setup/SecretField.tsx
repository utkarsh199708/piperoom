// The secret field (U1-12b; docs/api.md POST /api/rooms/{id}/setup/secret). The driver types a
// name and a value; the value goes to the paired runner and is stored nowhere on the server, so
// it is never in an event, a log line or a comment. The form clears as soon as the runner has it.
import { useId, useState } from 'react'
import type { FormEvent } from 'react'
import { KeyRound } from 'lucide-react'
import { api } from '@/api/client'
import type { RoomState } from '@/state/types'
import type { CurrentUser } from '@/state/permissions'
import { canSteer } from '@/state/permissions'
import { secretNameError, secretValueError } from '@/lib/setupControls'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { FIELD_ERROR, FIELD_LABEL, SetupAlert, SetupStatus } from './SetupFields'
import { useSetupAction } from './useSetupAction'

export function SecretField({ state, user }: { state: RoomState; user: CurrentUser }) {
  const setup = state.setup
  const formId = useId()
  const canEdit = canSteer(state, user)
  const runnerReady = setup?.runner?.connected === true
  const { busy, error, done, run } = useSetupAction()
  const [name, setName] = useState('')
  const [value, setValue] = useState('')
  const [fieldError, setFieldError] = useState<{ field: string; message: string } | null>(null)
  if (!setup) return null

  const submit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault()
    const nameProblem = secretNameError(name.trim())
    const valueProblem = secretValueError(value)
    const problem = nameProblem
      ? { field: 'name', message: nameProblem }
      : valueProblem
        ? { field: 'value', message: valueProblem }
        : null
    if (problem) {
      setFieldError(problem)
      return
    }
    setFieldError(null)
    const sent = name.trim()
    const ok = await run(
      () => api.setupSecret(state.id, { name: sent, value }),
      // The name only: the value is not repeated anywhere the browser keeps.
      `Secret ${sent} sent to the runner. It is in no event and no comment.`,
    )
    if (ok) {
      setName('')
      setValue('')
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
        <KeyRound className="size-4 shrink-0 text-muted-foreground" aria-hidden="true" />
        <h2 id={`${formId}-title`} className="m-0 text-sm font-semibold">
          Secret
        </h2>
      </div>
      <p className="m-0 text-xs text-muted-foreground">
        A value the setup needs — a database password, an API key. It goes straight to the paired
        runner in one frame; the server stores it nowhere, writes it in no event and keeps it out of
        its request log. The room only ever names it.
      </p>
      {setup.secret_names.length > 0 && (
        <p className="m-0 flex min-w-0 flex-wrap items-center gap-1.5 text-xs">
          <span className="text-muted-foreground">Sent to the runner:</span>
          {setup.secret_names.map((secret) => (
            <Badge key={secret} variant="outline" mono>
              {secret}
            </Badge>
          ))}
        </p>
      )}

      {!runnerReady ? (
        <p className="m-0 text-xs text-subtle-foreground">
          Pair a runner first: a secret value goes straight to it and nowhere else.
        </p>
      ) : !canEdit ? (
        <p className="m-0 text-xs text-subtle-foreground">
          Only the driver can send a secret. Take control to enter one.
        </p>
      ) : (
        <form className="flex min-w-0 flex-col gap-2" onSubmit={submit} aria-label="Send a secret">
          <div className="grid min-w-0 gap-2 sm:grid-cols-2">
            <div className="grid gap-1">
              <label htmlFor={`${formId}-name`} className={FIELD_LABEL}>
                Secret name
              </label>
              <Input
                id={`${formId}-name`}
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="DB_PASSWORD"
                autoComplete="off"
                spellCheck={false}
                disabled={busy}
                aria-invalid={errorFor('name') ? true : undefined}
                aria-describedby={errorFor('name') ? `${formId}-name-error` : `${formId}-name-hint`}
              />
              <span id={`${formId}-name-hint`} className="text-xs text-subtle-foreground">
                Letters, digits and underscores; it starts with a letter or underscore.
              </span>
              {errorFor('name') && (
                <span id={`${formId}-name-error`} className={FIELD_ERROR}>
                  {errorFor('name')}
                </span>
              )}
            </div>
            <div className="grid gap-1">
              <label htmlFor={`${formId}-value`} className={FIELD_LABEL}>
                Secret value
              </label>
              <Input
                id={`${formId}-value`}
                type="password"
                value={value}
                onChange={(e) => setValue(e.target.value)}
                autoComplete="new-password"
                spellCheck={false}
                disabled={busy}
                aria-invalid={errorFor('value') ? true : undefined}
                aria-describedby={errorFor('value') ? `${formId}-value-error` : undefined}
              />
              {errorFor('value') && (
                <span id={`${formId}-value-error`} className={FIELD_ERROR}>
                  {errorFor('value')}
                </span>
              )}
            </div>
          </div>
          <div>
            <Button type="submit" variant="outline" size="sm" className="coarse:h-11" disabled={busy}>
              Send secret
            </Button>
          </div>
        </form>
      )}

      {error && <SetupAlert>{error}</SetupAlert>}
      <SetupStatus>{done ?? ''}</SetupStatus>
    </Card>
  )
}
