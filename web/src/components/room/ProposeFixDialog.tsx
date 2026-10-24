import { useEffect, useMemo, useRef, useState } from 'react'
import type { FormEvent, KeyboardEvent } from 'react'
import { api } from '@/api/client'
import type { ActionInfo, Event } from '@/api/types'
import { formSpec, initialValues, paramsFrom } from '@/lib/paramsForm'
import type { ParamsValues } from '@/lib/paramsForm'
import { EMPTY_DRAFT, RISKS, proposalBody, proposeErrorText } from '@/lib/proposeFix'
import type { ProposeFixDraft } from '@/lib/proposeFix'
import { errorText } from '@/lib/errors'
import { modKeyLabel } from '@/lib/envLabel'
import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Kbd } from '@/components/ui/kbd'
import { RoomDialogClose } from './RoomDialogClose'
import { useRestoreFocus } from './useRestoreFocus'
import { ParamsFields } from './ParamsFields'

const LABEL = 'text-sm font-medium'
const SELECT =
  'h-8 w-full min-w-0 rounded-md border border-input-border bg-background px-2 text-sm text-foreground focus-visible:border-ring focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/30 disabled:cursor-not-allowed disabled:opacity-50 aria-invalid:border-destructive'

type Load = { kind: 'loading' } | { kind: 'error'; text: string } | { kind: 'ready'; actions: ActionInfo[] }

// "Propose a fix yourself" (P3d-2): the driver picks a registered action (GET /api/actions), fills
// its params through the generated form (P3d-1) and sends the fix (POST /api/rooms/{id}/proposals).
// The new card arrives over the socket as proposal.created with origin "driver", and its preview
// fills in from proposal.previewed exactly as for the agent's. There is no free-form command: only
// registered actions with their params. A refusal (403, 429, 400) stays in the dialog with the
// typed values, so a second try costs nothing.
export function ProposeFixDialog({
  roomId,
  onClose,
  onCreated,
}: {
  roomId: string
  onClose: () => void
  /** The server took it; the dialog has closed itself by then. */
  onCreated?: (e: Event) => void
}) {
  const [load, setLoad] = useState<Load>({ kind: 'loading' })
  const [actionName, setActionName] = useState('')
  const [values, setValues] = useState<ParamsValues>({})
  const [draft, setDraft] = useState<ProposeFixDraft>(EMPTY_DRAFT)
  const [fieldErrors, setFieldErrors] = useState<Record<string, string>>({})
  const [formError, setFormError] = useState<{ field: 'action' | 'title'; text: string } | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const restoreFocus = useRestoreFocus()
  const first = useRef<HTMLSelectElement>(null)

  useEffect(() => {
    let live = true
    api
      .actions()
      .then((list) => {
        if (live) setLoad({ kind: 'ready', actions: Array.isArray(list) ? list : [] })
      })
      .catch((e: unknown) => {
        if (live) setLoad({ kind: 'error', text: errorText(e) })
      })
    return () => {
      live = false
    }
  }, [])

  const actions = load.kind === 'ready' ? load.actions : []
  const action = actions.find((a) => a.name === actionName)
  const spec = useMemo(() => (action ? formSpec(action.params_schema) : null), [action])

  // Focus the action list once it exists: Radix's own autofocus ran while it was still loading.
  useEffect(() => {
    if (load.kind === 'ready') first.current?.focus()
  }, [load.kind])

  const pick = (name: string) => {
    setActionName(name)
    const a = actions.find((x) => x.name === name)
    setValues(a ? initialValues(a.params_schema) : {})
    setFieldErrors({})
    setFormError(null)
    setError(null)
  }
  const set = (patch: Partial<ProposeFixDraft>) => setDraft((d) => ({ ...d, ...patch }))

  const submit = async (e: FormEvent) => {
    e.preventDefault()
    if (busy) return
    setError(null)
    if (!action || !spec) {
      setFormError({ field: 'action', text: 'Choose an action.' })
      return
    }
    const params = paramsFrom(spec, values)
    setFieldErrors(params.ok ? {} : params.errors)
    const body = proposalBody(action, params.ok ? params.params : {}, draft)
    if (!body.ok) {
      setFormError({ field: body.field, text: body.error })
      return
    }
    setFormError(null)
    if (!params.ok) return
    setBusy(true)
    try {
      const ev = await api.proposeFix(roomId, body.body)
      onCreated?.(ev)
      onClose()
    } catch (err) {
      setError(proposeErrorText(err))
    } finally {
      setBusy(false)
    }
  }
  const onKeyDown = (e: KeyboardEvent) => {
    // Ctrl+Enter from anywhere in the dialog, as the other room dialogs do; the multi-line params
    // fields submit on it themselves.
    if (e.key === 'Enter' && (e.metaKey || e.ctrlKey) && !e.nativeEvent.isComposing && !(e.target instanceof HTMLTextAreaElement)) {
      e.preventDefault()
      ;(e.currentTarget.querySelector('form') as HTMLFormElement | null)?.requestSubmit()
    }
  }

  const titleErr = formError?.field === 'title' ? formError.text : undefined
  const actionErr = formError?.field === 'action' ? formError.text : undefined

  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent
        hideClose
        className="max-h-[calc(100dvh-2rem)] overflow-y-auto"
        onKeyDown={onKeyDown}
        onOpenAutoFocus={(e) => {
          e.preventDefault()
          first.current?.focus()
        }}
        onCloseAutoFocus={restoreFocus}
      >
        <RoomDialogClose label="Cancel proposing a fix" />
        <DialogHeader>
          <DialogTitle>Propose a fix yourself</DialogTitle>
          <DialogDescription>
            Pick one of the registered actions and fill in its params. The fix is checked and previewed like the agent&apos;s, and
            runs only after every required layer owner approves.
          </DialogDescription>
        </DialogHeader>
        {load.kind === 'loading' && (
          <p role="status" className="m-0 text-sm text-muted-foreground">
            Loading actions…
          </p>
        )}
        {load.kind === 'error' && (
          <p role="alert" className="m-0 text-sm font-medium text-destructive-fg">
            The actions could not be loaded: {load.text}
          </p>
        )}
        {load.kind === 'ready' && actions.length === 0 && (
          <p className="m-0 text-sm text-muted-foreground">This server has no registered write actions.</p>
        )}
        {load.kind === 'ready' && actions.length > 0 && (
          <form className="flex flex-col gap-4" noValidate onSubmit={(e) => void submit(e)}>
            <div className="flex flex-col gap-1.5">
              <label htmlFor="propose-action" className={LABEL}>
                Action
              </label>
              <select
                id="propose-action"
                ref={first}
                value={actionName}
                aria-required
                aria-invalid={actionErr ? true : undefined}
                aria-describedby={actionErr ? 'propose-action-error' : action ? 'propose-action-describe' : undefined}
                onChange={(e) => pick(e.target.value)}
                className={SELECT}
              >
                <option value="">Choose an action…</option>
                {actions.map((a) => (
                  <option key={a.name} value={a.name}>
                    {a.name} · {a.layer_by_params ? 'layer from the files it changes' : a.layer}
                  </option>
                ))}
              </select>
              {action && (
                <p id="propose-action-describe" className="m-0 text-xs text-muted-foreground">
                  {action.describe}
                  {!action.preview && ' This action has no preview: approvers see its params only.'}
                  {action.deferred && ' It opens a pull request: nothing changes until a reviewer merges it, and no policy approves it.'}
                </p>
              )}
              {actionErr && (
                <p id="propose-action-error" className="m-0 text-xs font-medium text-destructive-fg">
                  {actionErr}
                </p>
              )}
            </div>

            {spec && (
              <fieldset className="m-0 flex flex-col gap-3 border-0 p-0">
                <legend className="mb-1 p-0 text-sm font-medium">Params</legend>
                <ParamsFields
                  key={actionName}
                  spec={spec}
                  values={values}
                  onChange={setValues}
                  errors={fieldErrors}
                  idPrefix="propose-param"
                  disabled={busy}
                />
                {spec.mode === 'fields' && spec.fields.length === 0 && (
                  <p className="m-0 text-xs text-muted-foreground">This action takes no params.</p>
                )}
              </fieldset>
            )}

            <div className="flex flex-col gap-1.5">
              <label htmlFor="propose-title" className={LABEL}>
                Title
              </label>
              <Input
                id="propose-title"
                aria-required
                maxLength={200}
                value={draft.title}
                aria-invalid={titleErr ? true : undefined}
                aria-describedby={titleErr ? 'propose-title-error' : undefined}
                onChange={(e) => set({ title: e.target.value })}
                placeholder="e.g. Resume the orders connector"
              />
              {titleErr && (
                <p id="propose-title-error" className="m-0 text-xs font-medium text-destructive-fg">
                  {titleErr}
                </p>
              )}
            </div>
            <div className="grid gap-3 sm:grid-cols-[8rem_minmax(0,1fr)]">
              <div className="flex flex-col gap-1.5">
                <label htmlFor="propose-risk" className={LABEL}>
                  Risk
                </label>
                <select id="propose-risk" value={draft.risk} onChange={(e) => set({ risk: e.target.value })} className={SELECT}>
                  {RISKS.map((r) => (
                    <option key={r} value={r}>
                      {r}
                    </option>
                  ))}
                </select>
              </div>
              <div className="flex flex-col gap-1.5">
                <label htmlFor="propose-rollback" className={LABEL}>
                  Rollback
                </label>
                <Input
                  id="propose-rollback"
                  maxLength={500}
                  value={draft.rollback}
                  onChange={(e) => set({ rollback: e.target.value })}
                  placeholder="How to undo it"
                />
              </div>
            </div>
            <div className="flex flex-col gap-1.5">
              <label htmlFor="propose-description" className={LABEL}>
                Step description <span className="font-normal text-muted-foreground">(optional)</span>
              </label>
              <Input
                id="propose-description"
                maxLength={500}
                value={draft.description}
                onChange={(e) => set({ description: e.target.value })}
              />
            </div>

            {error && (
              <p role="alert" className="m-0 text-sm font-medium text-destructive-fg">
                {error}
              </p>
            )}
            {Object.keys(fieldErrors).length > 0 && (
              <p className="sr-only" role="alert">
                Some params need fixing.
              </p>
            )}
            <DialogFooter>
              <Button type="button" variant="ghost" onClick={onClose}>
                Cancel
              </Button>
              <Button type="submit" disabled={busy}>
                {busy ? 'Proposing…' : 'Propose fix'}
                <Kbd aria-hidden="true">{modKeyLabel()}⏎</Kbd>
              </Button>
            </DialogFooter>
          </form>
        )}
      </DialogContent>
    </Dialog>
  )
}
