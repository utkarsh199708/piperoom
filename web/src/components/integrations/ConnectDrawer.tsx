import { useEffect, useMemo, useState } from 'react'
import type { ReactNode } from 'react'
import * as DialogPrimitive from '@radix-ui/react-dialog'
import { ChevronDown, ChevronRight, Download, Power, ShieldCheck, Trash2, X } from 'lucide-react'
import { toast } from 'sonner'
import { ApiClientError } from '../../api/client'
import { integrationsApi, isSecretField } from '../../api/integrations'
import type { CatalogKind, Check, Integration } from '../../api/integrations'
import {
  CATEGORY_LABEL,
  alertSourceOf,
  anyFailed,
  buildWriteBody,
  configBody,
  countFailed,
  initialForm,
  validateForm,
  verifiedReadOnly,
} from '../../lib/integrations'
import type { FormState } from '../../lib/integrations'
import { checksOf, fieldOf, integrationErrorText } from '../../lib/integrationErrors'
import { Button } from '../ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogOverlay,
  DialogTitle,
} from '../ui/dialog'
import { AlertSetupPanel } from './AlertSetupPanel'
import { AuditList } from './AuditList'
import { CheckList } from './CheckList'
import { CopyButton } from './CopyButton'
import { IntegrationForm } from './IntegrationForm'
import { EnvPill, StatusPill } from './StatusPill'

export type DrawerApi = typeof integrationsApi

function Collapsible({ title, defaultOpen = false, children }: { title: string; defaultOpen?: boolean; children: ReactNode }) {
  const [open, setOpen] = useState(defaultOpen)
  const id = `section-${title.replace(/\W+/g, '-').toLowerCase()}`
  return (
    <section className="rounded-lg border border-border bg-sidebar">
      <h3 className="m-0">
        <button
          type="button"
          className="flex w-full items-center gap-2 rounded-lg px-3.5 py-3 text-left text-sm font-medium hover:bg-accent focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
          aria-expanded={open}
          aria-controls={id}
          onClick={() => setOpen((o) => !o)}
        >
          {open ? <ChevronDown className="size-4" aria-hidden="true" /> : <ChevronRight className="size-4" aria-hidden="true" />}
          {title}
        </button>
      </h3>
      {open && (
        <div id={id} className="px-3.5 pb-3.5">
          {children}
        </div>
      )}
    </section>
  )
}

function ReadsList({ kind }: { kind: CatalogKind }) {
  if (kind.reads.length === 0 && kind.never.length === 0) return null
  return (
    <Collapsible title="What Piperoom reads / never reads" defaultOpen>
      <div className="flex flex-col gap-3 text-sm">
        {kind.reads.length > 0 && (
          <div>
            <h4 className="m-0 mb-1 text-xs font-medium text-muted-foreground">Piperoom reads</h4>
            <ul className="m-0 pl-5 text-muted-foreground">
              {kind.reads.map((r) => (
                <li key={r}>{r}</li>
              ))}
            </ul>
          </div>
        )}
        {kind.never.length > 0 && (
          <div>
            <h4 className="m-0 mb-1 text-xs font-medium text-muted-foreground">Piperoom never</h4>
            <ul className="m-0 pl-5 text-muted-foreground">
              {kind.never.map((r) => (
                <li key={r}>{r}</li>
              ))}
            </ul>
          </div>
        )}
      </div>
    </Collapsible>
  )
}

function SetupScript({ kind, config, api }: { kind: CatalogKind; config: string; api: DrawerApi }) {
  const [script, setScript] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  // The script follows the non-secret fields as they are typed, a moment after the last key.
  useEffect(() => {
    let live = true
    const t = setTimeout(() => {
      api.setupScript(kind.id, JSON.parse(config) as Record<string, string | number | boolean>).then(
        (r) => {
          if (!live) return
          setScript(r.script)
          setError(null)
        },
        (e: unknown) => {
          if (live) setError(`Could not load the setup script: ${integrationErrorText(e)}`)
        },
      )
    }, 400)
    return () => {
      live = false
      clearTimeout(t)
    }
  }, [api, kind.id, config])

  const download = () => {
    if (!script || typeof URL.createObjectURL !== 'function') return
    const url = URL.createObjectURL(new Blob([script], { type: 'text/plain' }))
    const a = document.createElement('a')
    a.href = url
    a.download = `piperoom-${kind.id}-setup.txt`
    a.click()
    URL.revokeObjectURL(url)
  }

  return (
    <Collapsible title="Setup script" defaultOpen>
      <p className="m-0 mb-2 text-xs text-muted-foreground">
        For the person who creates the account: the least-privilege grants, filled from the fields above.
      </p>
      {error && (
        <p role="alert" className="m-0 text-xs text-destructive-fg">
          {error}
        </p>
      )}
      {script === null && !error && <p className="m-0 text-xs text-muted-foreground">Loading…</p>}
      {script !== null && (
        <>
          <pre className="m-0 mb-2 max-h-72 overflow-auto whitespace-pre-wrap rounded-md border border-border bg-background p-3 font-mono text-xs">
            {script || 'This card has no script.'}
          </pre>
          {script && (
            <div className="flex gap-2">
              <CopyButton text={script} label="Copy setup script" />
              <Button variant="secondary" size="sm" onClick={download}>
                <Download aria-hidden="true" />
                Download
              </Button>
            </div>
          )}
        </>
      )}
    </Collapsible>
  )
}

/** A confirm dialog for disable, delete and the like. */
function Confirm({
  open,
  title,
  body,
  action,
  onCancel,
  onConfirm,
}: {
  open: boolean
  title: string
  body: string
  action: string
  onCancel: () => void
  onConfirm: () => void
}) {
  return (
    <Dialog open={open} onOpenChange={(o) => !o && onCancel()}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>{title}</DialogTitle>
          <DialogDescription>{body}</DialogDescription>
        </DialogHeader>
        <DialogFooter>
          <Button variant="secondary" onClick={onCancel}>
            Cancel
          </Button>
          <Button variant="destructive" onClick={onConfirm}>
            {action}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

export interface ConnectFormProps {
  kind: CatalogKind
  existing?: Integration
  managers: Integration[]
  admin: boolean
  api?: DrawerApi
  onSaved: (i: Integration) => void
  onDeleted: (id: string) => void
  /** Reload the list after a version conflict; the form then starts again from the new row. */
  onReload: () => Promise<void>
  /** Reload the list without resetting the form (an alert secret was generated). */
  onRefresh?: () => void
  /** PIPEROOM_SECRET_KEY is set (GET /api/integrations/status); false = references only. */
  secretKeySet?: boolean
}

/**
 * The body of the connect drawer for one card: form, setup script, reads list, the live
 * test checklist and the save / enable / delete actions. Read-only for non-admins and for
 * rows set by environment variables.
 */
export function ConnectForm({
  kind,
  existing,
  managers,
  admin,
  api = integrationsApi,
  onSaved,
  onDeleted,
  onReload,
  onRefresh,
  secretKeySet = true,
}: ConnectFormProps) {
  const env = existing?.source === 'env'
  const readOnly = !admin || env
  const hasSecretFields = kind.fields.some((f) => isSecretField(f.type))
  const [form, setFormState] = useState<FormState>(() => {
    const f = initialForm(kind, existing, managers.length > 0)
    if (secretKeySet) return f
    // Without the key a typed value cannot be saved: start new values on the reference.
    const secrets = Object.fromEntries(
      Object.entries(f.secrets).map(([name, s]) => [name, s.mode === 'value' && !s.stored ? { ...s, mode: 'manager' as const } : s]),
    )
    return { ...f, secrets }
  })
  const [checks, setChecks] = useState<Check[] | null>(existing && existing.last_test.length > 0 ? existing.last_test : null)
  const [stale, setStale] = useState(false)
  const [errors, setErrors] = useState<Record<string, string>>({})
  const [busy, setBusy] = useState<'test' | 'save' | 'toggle' | 'delete' | null>(null)
  const [notice, setNotice] = useState<string | null>(null)
  const [conflict, setConflict] = useState(false)
  const [confirm, setConfirm] = useState<'disable' | 'delete' | null>(null)

  const setForm = (next: FormState) => {
    setFormState(next)
    if (checks) setStale(true)
  }

  const configJson = useMemo(() => JSON.stringify(configBody(kind, form)), [kind, form])
  const source = alertSourceOf(kind.id)
  const isAlert = kind.category === 'alerts'

  const fail = (e: unknown) => {
    const failedChecks = checksOf(e)
    if (failedChecks) {
      setChecks(failedChecks)
      setStale(false)
      setNotice('Not saved: a check failed. Fix the red lines and test again.')
      return
    }
    if (e instanceof ApiClientError && e.code === 'version_conflict') {
      setConflict(true)
      setNotice(null)
      return
    }
    const field = fieldOf(e)
    if (field) setErrors({ [field]: e instanceof Error ? e.message : 'Invalid value' })
    setNotice(integrationErrorText(e))
  }

  const precheck = (): boolean => {
    const errs = validateForm(kind, form, existing)
    setErrors(errs)
    if (Object.keys(errs).length > 0) {
      setNotice('Some fields need a value or have the wrong format.')
      return false
    }
    setNotice(null)
    return true
  }

  const test = async () => {
    if (!precheck()) return
    setBusy('test')
    try {
      const res = await api.test(buildWriteBody(kind, form, existing?.id))
      setChecks(res.checks)
      setStale(false)
    } catch (e) {
      fail(e)
    } finally {
      setBusy(null)
    }
  }

  const save = async () => {
    if (!precheck()) return
    setBusy('save')
    try {
      const body = buildWriteBody(kind, form)
      const res = existing ? await api.update(existing.id, existing.version, body) : await api.create(body)
      setChecks(res.checks)
      setStale(false)
      // Typed-in secrets leave memory; the saved row now says "set" for them.
      setFormState(initialForm(kind, res.integration, managers.length > 0))
      setErrors({})
      setNotice(null)
      setConflict(false)
      toast.success(`${kind.title} saved`)
      onSaved(res.integration)
    } catch (e) {
      fail(e)
    } finally {
      setBusy(null)
    }
  }

  const setEnabled = async (enabled: boolean) => {
    if (!existing) return
    setConfirm(null)
    setBusy('toggle')
    setNotice(null)
    try {
      onSaved(await api.setEnabled(existing.id, enabled))
    } catch (e) {
      fail(e)
    } finally {
      setBusy(null)
    }
  }

  const remove = async () => {
    if (!existing) return
    setConfirm(null)
    setBusy('delete')
    setNotice(null)
    try {
      await api.remove(existing.id)
      toast.success(`${kind.title} removed`)
      onDeleted(existing.id)
    } catch (e) {
      fail(e)
      setBusy(null)
    }
  }

  const failedNow = checks !== null && !stale && anyFailed(checks)
  const saveDisabled = busy !== null || failedNow
  const showForm = kind.fields.length > 0

  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div className="flex min-h-0 flex-1 flex-col gap-5 overflow-y-auto px-6 py-5">
        {existing && (
          <div className="flex flex-wrap items-center gap-2">
            {env && <EnvPill />}
            <StatusPill
              status={existing.status === 'connected' ? 'connected' : existing.status === 'failing' ? 'attention' : existing.status}
              readOnlyVerified={verifiedReadOnly(existing)}
            />
            {existing.updated_by && (
              <span className="text-xs text-muted-foreground">
                Last change by {existing.updated_by}
              </span>
            )}
          </div>
        )}

        {!admin && (
          <p className="m-0 rounded-md border border-border bg-sidebar p-3 text-sm text-muted-foreground">
            Only an admin can connect or change integrations. This is what the card asks for and what Piperoom reads.
          </p>
        )}
        {env && (
          <p className="m-0 rounded-md border border-border bg-sidebar p-3 text-sm text-muted-foreground">
            Managed by environment: this connection comes from environment variables on the server and cannot be changed
            here.
          </p>
        )}

        {isAlert && admin && <AlertSetupPanel source={source} ownsSecret={kind.fields.length > 0} api={api} onChanged={onRefresh} />}

        {showForm &&
          (readOnly ? (
            <section aria-label="Fields">
              <h3 className="m-0 mb-2 text-sm font-semibold">Fields</h3>
              <dl className="m-0 grid grid-cols-[minmax(0,1fr)_minmax(0,2fr)] gap-x-4 gap-y-1.5 text-sm">
                {kind.fields.map((f) => {
                  const v = existing?.config[f.name]
                  let shown = '—'
                  if (v && typeof v === 'object') shown = v.$ref
                  else if (v !== undefined) shown = String(v)
                  else if (existing?.secrets_set.includes(f.name)) shown = 'set'
                  return (
                    <div key={f.name} className="contents">
                      <dt className="text-muted-foreground">{f.label}</dt>
                      <dd className="m-0 truncate font-mono text-xs leading-5">{existing ? shown : f.type}</dd>
                    </div>
                  )
                })}
              </dl>
            </section>
          ) : (
            <>
              {!secretKeySet && hasSecretFields && (
                <p role="status" className="m-0 mb-3 rounded-md border border-border bg-sidebar px-3 py-2 text-sm text-muted-foreground">
                  PIPEROOM_SECRET_KEY is not set on the server, so typed-in secrets cannot be saved. Use a secret manager
                  reference, or ask the operator to set the key.
                </p>
              )}
              <IntegrationForm
                kind={kind}
                form={form}
                onChange={setForm}
                errors={errors}
                managers={managers}
                lookup={api}
                disabled={busy !== null}
                typedSecretsOff={!secretKeySet}
              />
            </>
          ))}

        {kind.has_setup_script && admin && <SetupScript kind={kind} config={configJson} api={api} />}
        <ReadsList kind={kind} />

        {checks !== null && !isAlert && <CheckList checks={checks} stale={stale} />}

        {conflict && (
          <div role="alert" className="flex flex-col gap-2 rounded-md border border-warning bg-warning-subtle p-3 text-sm">
            <p className="m-0 text-warning-foreground">
              Someone else changed this connection since you opened it. Reload to see their version; your edits here are
              dropped.
            </p>
            <div>
              <Button
                variant="secondary"
                size="sm"
                onClick={() => {
                  void onReload()
                }}
              >
                Reload
              </Button>
            </div>
          </div>
        )}
        {notice && (
          <p role="alert" className="m-0 text-sm text-destructive-fg">
            {notice}
          </p>
        )}

        {existing && admin && !env && (
          <div className="flex flex-col gap-3 border-t border-border pt-4">
            <div className="flex flex-wrap gap-2">
              {existing.enabled ? (
                <Button variant="secondary" size="sm" disabled={busy !== null} onClick={() => setConfirm('disable')}>
                  <Power aria-hidden="true" />
                  Disable
                </Button>
              ) : (
                <Button variant="secondary" size="sm" disabled={busy !== null} onClick={() => void setEnabled(true)}>
                  <Power aria-hidden="true" />
                  Enable
                </Button>
              )}
              <Button variant="ghost-danger" size="sm" disabled={busy !== null} onClick={() => setConfirm('delete')}>
                <Trash2 aria-hidden="true" />
                Delete
              </Button>
            </div>
            <AuditList integrationId={existing.id} api={api} />
          </div>
        )}
      </div>

      {!readOnly && showForm && (
        <footer className="flex flex-wrap items-center justify-end gap-2.5 border-t border-border bg-card px-6 py-4">
          <span className="mr-auto text-sm text-muted-foreground" aria-live="polite">
            {failedNow
              ? `Fix ${countFailed(checks)} red ${countFailed(checks) === 1 ? 'check' : 'checks'} to save`
              : busy === 'test'
                ? 'Testing…'
                : busy === 'save'
                  ? 'Testing and saving…'
                  : ''}
          </span>
          {!isAlert && (
            <Button variant="secondary" onClick={() => void test()} disabled={busy !== null}>
              <ShieldCheck aria-hidden="true" />
              {checks ? 'Test again' : 'Test connection'}
            </Button>
          )}
          <Button onClick={() => void save()} disabled={saveDisabled}>
            {existing ? 'Save changes' : 'Save connection'}
          </Button>
        </footer>
      )}

      <Confirm
        open={confirm === 'disable'}
        title={`Disable ${kind.title}?`}
        body="Piperoom keeps the settings, but tools answer “not connected” until it is enabled again."
        action="Disable"
        onCancel={() => setConfirm(null)}
        onConfirm={() => void setEnabled(false)}
      />
      <Confirm
        open={confirm === 'delete'}
        title={`Delete ${kind.title}?`}
        body="This removes the connection and its stored secrets. The change log stays."
        action="Delete"
        onCancel={() => setConfirm(null)}
        onConfirm={() => void remove()}
      />
    </div>
  )
}

/** The right-side drawer around ConnectForm, so the checklist stays in view. */
export function ConnectDrawer({
  open,
  onOpenChange,
  ...props
}: ConnectFormProps & { open: boolean; onOpenChange: (open: boolean) => void }) {
  const { kind } = props
  // After a version conflict the form starts again from the reloaded row.
  const [resetKey, setResetKey] = useState(0)
  const reload = async () => {
    await props.onReload()
    setResetKey((k) => k + 1)
  }
  return (
    <DialogPrimitive.Root open={open} onOpenChange={onOpenChange}>
      <DialogPrimitive.Portal>
        <DialogOverlay />
        <DialogPrimitive.Content
          className="fixed inset-y-0 right-0 z-50 flex w-[620px] max-w-full flex-col border-l border-border bg-card text-card-foreground shadow-lg duration-150 data-[state=closed]:animate-out data-[state=closed]:slide-out-to-right data-[state=open]:animate-in data-[state=open]:slide-in-from-right"
        >
          <header className="flex items-start gap-3.5 border-b border-border px-6 py-5">
            <span
              aria-hidden="true"
              className="flex size-10 shrink-0 items-center justify-center rounded-lg border border-input bg-secondary font-mono text-xs font-semibold uppercase"
            >
              {(kind.logo || kind.category).slice(0, 2)}
            </span>
            <div className="min-w-0 flex-1">
              <DialogPrimitive.Title className="m-0 text-lg font-semibold">
                {kind.title} · {CATEGORY_LABEL[kind.category].toLowerCase()}
              </DialogPrimitive.Title>
              <DialogPrimitive.Description className="m-0 text-sm text-muted-foreground">
                {kind.summary}
              </DialogPrimitive.Description>
            </div>
            <DialogPrimitive.Close className="inline-flex size-8 items-center justify-center rounded-md text-muted-foreground hover:bg-accent hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring">
              <X className="size-4" aria-hidden="true" />
              <span className="sr-only">Close</span>
            </DialogPrimitive.Close>
          </header>
          <ConnectForm key={`${kind.id}:${resetKey}`} {...props} onReload={reload} />
        </DialogPrimitive.Content>
      </DialogPrimitive.Portal>
    </DialogPrimitive.Root>
  )
}
