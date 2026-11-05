// "New pipeline" → Set up (U1-12a): the form that opens a setup room (POST /api/setup) and
// hands the person the pairing command. Every field is the server's SetupRequest; the rules a
// first pass can check live in lib/setupForm.ts, and the server stays the authority.
import { useId, useState } from 'react'
import type { FormEvent } from 'react'
import { Link, useNavigate } from 'react-router-dom'
import { ArrowLeft, Plus, Trash2 } from 'lucide-react'
import { api, isFixtures } from '@/api/client'
import { addFixtureRoom } from '@/fixtures/player'
import { setupCreateResponse } from '@/fixtures/setup'
import { useCurrentUser } from '@/context/user'
import { isViewer } from '@/state/permissions'
import { errorText } from '@/lib/errors'
import { rememberSetupPairing } from '@/lib/setup'
import {
  EMPTY_SETUP_FORM,
  SETUP_CLASSES,
  SETUP_TARGETS,
  SOURCE_KINDS,
  SetupFormError,
  TARGET_KIND_LABELS,
  TARGET_KINDS,
  setupRequestFrom,
} from '@/lib/setupForm'
import type { SetupFormValues, SetupTargetValues } from '@/lib/setupForm'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Input } from '@/components/ui/input'

const FIELD_LABEL = 'text-xs font-medium text-muted-foreground'
const SELECT =
  'h-8 w-full rounded-md border border-input-border bg-background px-2 text-sm text-foreground focus-visible:border-ring focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/30'
const FIELD_ERROR = 'text-xs text-destructive-fg'

const SOURCE_LABEL: Record<string, string> = {
  postgres: 'PostgreSQL',
  mysql: 'MySQL',
  sqlserver: 'SQL Server',
  mongodb: 'MongoDB',
  oracle: 'Oracle',
}

const TARGET_LABEL: Record<string, string> = {
  compose: 'Docker Compose',
  host: 'Plain host',
  strimzi: 'Strimzi',
  msk: 'Amazon MSK',
  confluent: 'Confluent Cloud',
  aiven: 'Aiven',
  dbzserver: 'Debezium Server',
  embedded: 'Embedded',
}

const KIND_LABEL = TARGET_KIND_LABELS

/** The Set up page: a viewer gets the same message the server would answer with. */
export function SetupPage() {
  const { user } = useCurrentUser()
  if (isViewer(user)) {
    return (
      <div className="mx-auto flex w-full max-w-3xl flex-col gap-4 px-4 py-6">
        <h1 className="text-xl font-semibold tracking-tight">Set up a pipeline</h1>
        <Card className="px-6 py-8 text-sm text-muted-foreground">
          Viewers cannot open a setup room. Ask a member of the tenant to run it.
        </Card>
      </div>
    )
  }
  return <SetupForm />
}

function SetupForm() {
  const navigate = useNavigate()
  const formId = useId()
  const [values, setValues] = useState<SetupFormValues>(EMPTY_SETUP_FORM)
  const [error, setError] = useState<{ field: string; message: string } | null>(null)
  const [busy, setBusy] = useState(false)
  const [sendError, setSendError] = useState<string | null>(null)

  const set = <K extends keyof SetupFormValues>(key: K, value: SetupFormValues[K]) =>
    setValues((v) => ({ ...v, [key]: value }))

  const setTarget = (i: number, patch: Partial<SetupTargetValues>) =>
    setValues((v) => ({ ...v, targets: v.targets.map((t, j) => (j === i ? { ...t, ...patch } : t)) }))

  const addTarget = () =>
    setValues((v) => ({ ...v, targets: [...v.targets, { kind: 'kafka', address: '', class: '' }] }))

  const removeTarget = (i: number) =>
    setValues((v) => ({ ...v, targets: v.targets.filter((_, j) => j !== i) }))

  const fieldError = (field: string) => (error?.field === field ? error.message : undefined)
  const invalid = (field: string) => (error?.field === field ? true : undefined)

  const focusField = (field: string) =>
    window.requestAnimationFrame(() => document.getElementById(`${formId}-${field}`)?.focus())

  const onSubmit = async (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault()
    setSendError(null)
    let req
    try {
      req = setupRequestFrom(values)
    } catch (err) {
      if (err instanceof SetupFormError) {
        setError({ field: err.field, message: err.message })
        focusField(err.field)
        return
      }
      throw err
    }
    setError(null)
    setBusy(true)
    try {
      if (isFixtures()) {
        const res = setupCreateResponse()
        addFixtureRoom(res.room)
        rememberSetupPairing(res.room.id, { command: res.command, expires_at: res.expires_at })
        navigate(`/rooms/${res.room.id}`)
        return
      }
      const res = await api.createSetup(req)
      rememberSetupPairing(res.room.id, { command: res.command, expires_at: res.expires_at })
      navigate(`/rooms/${res.room.id}`)
    } catch (err) {
      setSendError(errorText(err))
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="mx-auto flex w-full max-w-3xl flex-col gap-4 px-4 py-6">
      <div className="flex flex-col gap-1">
        <Link to="/fleet" className="inline-flex items-center gap-1 text-xs text-muted-foreground hover:text-foreground">
          <ArrowLeft className="size-3.5" aria-hidden="true" />
          Fleet
        </Link>
        <h1 className="m-0 text-xl font-semibold tracking-tight">Set up a pipeline</h1>
        <p className="m-0 text-sm text-muted-foreground">
          The agent plans the setup, a runner on your machine runs it, and a step that needs a person waits for
          approval. You get a pairing command when the room opens.
        </p>
      </div>

      {sendError && (
        <div role="alert" className="rounded-md border border-border bg-destructive-subtle px-3 py-2 text-sm text-destructive-fg">
          {sendError}
        </div>
      )}

      <form className="flex flex-col gap-5" onSubmit={onSubmit} aria-label="Set up a pipeline">
        <Card className="grid gap-3 p-4 sm:grid-cols-2">
          <div className="grid gap-1.5">
            <label htmlFor={`${formId}-tenant`} className={FIELD_LABEL}>
              Tenant
            </label>
            <Input
              id={`${formId}-tenant`}
              value={values.tenant}
              onChange={(e) => set('tenant', e.target.value)}
              placeholder="acme"
              autoComplete="off"
            />
            <span className="text-xs text-subtle-foreground">Only on a server with tenants; leave empty otherwise.</span>
          </div>
          <div className="grid gap-1.5">
            <label htmlFor={`${formId}-pipelineId`} className={FIELD_LABEL}>
              Pipeline id
            </label>
            <Input
              id={`${formId}-pipelineId`}
              value={values.pipelineId}
              onChange={(e) => set('pipelineId', e.target.value)}
              placeholder="orders"
              required
              autoComplete="off"
              aria-invalid={invalid('pipelineId')}
              aria-describedby={fieldError('pipelineId') ? `${formId}-pipelineId-error` : undefined}
            />
            {fieldError('pipelineId') && (
              <span id={`${formId}-pipelineId-error`} className={FIELD_ERROR}>
                {fieldError('pipelineId')}
              </span>
            )}
          </div>
          <div className="grid gap-1.5 sm:col-span-2">
            <label htmlFor={`${formId}-title`} className={FIELD_LABEL}>
              Title
            </label>
            <Input
              id={`${formId}-title`}
              value={values.title}
              onChange={(e) => set('title', e.target.value)}
              placeholder="Watch orders end to end"
              required
              aria-invalid={invalid('title')}
              aria-describedby={fieldError('title') ? `${formId}-title-error` : undefined}
            />
            {fieldError('title') && (
              <span id={`${formId}-title-error`} className={FIELD_ERROR}>
                {fieldError('title')}
              </span>
            )}
          </div>
          <div className="grid gap-1.5">
            <label htmlFor={`${formId}-sourceKind`} className={FIELD_LABEL}>
              Source
            </label>
            <select
              id={`${formId}-sourceKind`}
              className={SELECT}
              value={values.sourceKind}
              onChange={(e) => set('sourceKind', e.target.value)}
            >
              {SOURCE_KINDS.map((k) => (
                <option key={k} value={k}>
                  {SOURCE_LABEL[k] ?? k}
                </option>
              ))}
            </select>
          </div>
          <div className="grid gap-1.5">
            <label htmlFor={`${formId}-target`} className={FIELD_LABEL}>
              Target
            </label>
            <select
              id={`${formId}-target`}
              className={SELECT}
              value={values.target}
              onChange={(e) => set('target', e.target.value)}
            >
              {SETUP_TARGETS.map((k) => (
                <option key={k} value={k}>
                  {TARGET_LABEL[k] ?? k}
                </option>
              ))}
            </select>
          </div>
          <div className="grid gap-1.5">
            <label htmlFor={`${formId}-topicPrefix`} className={FIELD_LABEL}>
              Topic prefix
            </label>
            <Input
              id={`${formId}-topicPrefix`}
              value={values.topicPrefix}
              onChange={(e) => set('topicPrefix', e.target.value)}
              placeholder="orders"
              required
              aria-invalid={invalid('topicPrefix')}
              aria-describedby={fieldError('topicPrefix') ? `${formId}-topicPrefix-error` : undefined}
            />
            {fieldError('topicPrefix') && (
              <span id={`${formId}-topicPrefix-error`} className={FIELD_ERROR}>
                {fieldError('topicPrefix')}
              </span>
            )}
          </div>
          <div className="grid gap-1.5 sm:col-span-2">
            <label htmlFor={`${formId}-tables`} className={FIELD_LABEL}>
              Source tables
            </label>
            <textarea
              id={`${formId}-tables`}
              className="min-h-20 w-full rounded-md border border-input-border bg-background px-2.5 py-2 font-mono text-sm text-foreground focus-visible:border-ring focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/30 aria-invalid:border-destructive"
              value={values.tables}
              onChange={(e) => set('tables', e.target.value)}
              placeholder={'public.orders\npublic.order_items'}
              required
              aria-invalid={invalid('tables')}
              aria-describedby={`${formId}-tables-hint${fieldError('tables') ? ` ${formId}-tables-error` : ''}`}
            />
            <span id={`${formId}-tables-hint`} className="text-xs text-subtle-foreground">
              One schema.table per line.
            </span>
            {fieldError('tables') && (
              <span id={`${formId}-tables-error`} className={FIELD_ERROR}>
                {fieldError('tables')}
              </span>
            )}
          </div>
        </Card>

        <Card className="flex flex-col gap-3 p-4">
          <fieldset className="m-0 flex min-w-0 flex-col gap-2 border-0 p-0">
            <legend className="mb-1 text-sm font-semibold">Targets and classes</legend>
            <p className="m-0 text-xs text-muted-foreground">
              Every host or account the runner may reach. The class is a floor: a class above what the server derives
              makes the step ask a person (or refuse on production). Leave it on "server decides" unless you know.
            </p>
            {values.targets.map((t, i) => (
              <div key={i} className="grid min-w-0 gap-2 sm:grid-cols-[9rem_minmax(0,1fr)_9rem_auto] sm:items-start">
                <div className="grid gap-1">
                  <label htmlFor={`${formId}-target-${i}-kind`} className={FIELD_LABEL}>
                    Kind {i + 1}
                  </label>
                  <select
                    id={`${formId}-target-${i}-kind`}
                    className={SELECT}
                    value={t.kind}
                    onChange={(e) => setTarget(i, { kind: e.target.value })}
                    aria-invalid={invalid(`target-${i}-kind`)}
                  >
                    {TARGET_KINDS.map((k) => (
                      <option key={k} value={k}>
                        {KIND_LABEL[k] ?? k}
                      </option>
                    ))}
                  </select>
                </div>
                <div className="grid gap-1">
                  <label htmlFor={`${formId}-target-${i}-address`} className={FIELD_LABEL}>
                    Address {i + 1}
                  </label>
                  <Input
                    id={`${formId}-target-${i}-address`}
                    value={t.address}
                    onChange={(e) => setTarget(i, { address: e.target.value })}
                    placeholder="db.acme:5432"
                    autoComplete="off"
                    aria-invalid={invalid(`target-${i}-address`)}
                    aria-describedby={fieldError(`target-${i}-address`) ? `${formId}-target-${i}-address-error` : undefined}
                  />
                  {fieldError(`target-${i}-address`) && (
                    <span id={`${formId}-target-${i}-address-error`} className={FIELD_ERROR}>
                      {fieldError(`target-${i}-address`)}
                    </span>
                  )}
                </div>
                <div className="grid gap-1">
                  <label htmlFor={`${formId}-target-${i}-class`} className={FIELD_LABEL}>
                    Class {i + 1}
                  </label>
                  <select
                    id={`${formId}-target-${i}-class`}
                    className={SELECT}
                    value={t.class}
                    onChange={(e) => setTarget(i, { class: e.target.value })}
                  >
                    <option value="">server decides</option>
                    {SETUP_CLASSES.map((c) => (
                      <option key={c} value={c}>
                        {c}
                      </option>
                    ))}
                  </select>
                </div>
                <div className="flex items-end sm:pt-5">
                  <Button
                    variant="ghost"
                    size="icon"
                    aria-label={`Remove target ${i + 1}`}
                    disabled={values.targets.length === 1}
                    onClick={() => removeTarget(i)}
                  >
                    <Trash2 aria-hidden="true" />
                  </Button>
                </div>
              </div>
            ))}
            <div>
              <Button variant="outline" size="sm" onClick={addTarget}>
                <Plus aria-hidden="true" />
                Add target
              </Button>
            </div>
          </fieldset>
        </Card>

        <div className="flex flex-wrap items-center gap-3">
          <Button type="submit" disabled={busy}>
            {busy ? 'Opening the room…' : 'Open the setup room'}
          </Button>
          <Link to="/fleet" className="text-sm text-muted-foreground hover:text-foreground">
            Cancel
          </Link>
        </div>
      </form>
    </div>
  )
}
