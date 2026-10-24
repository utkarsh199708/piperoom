import { useId, useMemo, useRef, useState } from 'react'
import type { FormEvent } from 'react'
import { AlertTriangle } from 'lucide-react'
import { api } from '@/api/client'
import type { ModelChoice, ModelPanel, SupportedModel } from '@/api/types'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import {
  costPerRun,
  entryKey,
  keyRequired,
  percent,
  pickerError,
  providerLabel,
  roles,
  supportedList,
  usuallyKeyless,
} from '@/lib/models'
import type { PickerError, PickerField } from '@/lib/models'

interface Props {
  panel: ModelPanel
  open: boolean
  onOpenChange: (open: boolean) => void
  /** Called with the panel PUT /api/models answered. */
  onApplied: (panel: ModelPanel) => void
  /** First run: there is no model to fall back to, so the dialog cannot be dismissed. */
  required?: boolean
}

// The standalone model picker (V8; docs/design/model-providers.md section 11). The key goes to
// PUT /api/models only: it is read from the field when the form is submitted, never held in React state,
// never written to browser storage, and the field is emptied as soon as the request has been sent.
export function ModelPicker({ panel, open, onOpenChange, onApplied, required = false }: Props) {
  return (
    <Dialog open={open} onOpenChange={(o) => (required && !o ? undefined : onOpenChange(o))}>
      <DialogContent
        hideClose={required}
        className="max-h-[90dvh] max-w-xl overflow-y-auto p-4 sm:p-6"
        onEscapeKeyDown={(e) => required && e.preventDefault()}
        onPointerDownOutside={(e) => required && e.preventDefault()}
        onInteractOutside={(e) => required && e.preventDefault()}
      >
        <PickerForm panel={panel} required={required} onApplied={onApplied} onCancel={() => onOpenChange(false)} />
      </DialogContent>
    </Dialog>
  )
}

function PickerForm({
  panel,
  required,
  onApplied,
  onCancel,
}: {
  panel: ModelPanel
  required: boolean
  onApplied: (p: ModelPanel) => void
  onCancel: () => void
}) {
  const ids = useId()
  const list = supportedList(panel)
  const supported = list.filter((m) => m.status !== 'experimental')
  const experimental = list.filter((m) => m.status === 'experimental')
  const current = roles(panel).find((r) => r.role === 'main')

  const [selected, setSelected] = useState<string>(() => {
    const cur = current ? list.find((m) => m.provider === current.provider && (m.flavor ?? '') === (current.flavor ?? '') && m.model === current.model) : undefined
    const first = cur ?? supported[0] ?? experimental[0]
    return first ? entryKey(first) : ''
  })
  const [showExperimental, setShowExperimental] = useState(() => {
    const cur = current && experimental.some((m) => m.provider === current.provider && m.model === current.model)
    return Boolean(cur)
  })
  const [baseUrl, setBaseUrl] = useState('')
  const [noKey, setNoKey] = useState(false)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<PickerError | null>(null)
  // The key lives in the DOM field only (uncontrolled), so it is in no state, no storage and no re-render.
  const keyRef = useRef<HTMLInputElement>(null)

  const entry = useMemo(() => list.find((m) => entryKey(m) === selected), [list, selected])
  const needKey = entry ? keyRequired(current, { provider: entry.provider, flavor: entry.flavor, baseUrl }) : true

  const clearKey = () => {
    if (keyRef.current) keyRef.current.value = ''
  }
  // A new target must be given its own key: whatever was typed for the old one is dropped.
  const retarget = (next: () => void) => {
    clearKey()
    setError(null)
    next()
  }

  const fieldError = (f: PickerField) => (error?.field === f ? error.message : null)

  const submit = async (e: FormEvent) => {
    e.preventDefault()
    if (!entry || busy) return
    const typed = keyRef.current?.value ?? ''
    const choice: ModelChoice = { provider: entry.provider, model: entry.model }
    if (entry.flavor) choice.flavor = entry.flavor
    if (baseUrl.trim()) choice.base_url = baseUrl.trim()
    if (typed !== '') choice.key = typed
    else if (needKey && noKey) choice.key = '' // this target takes no key
    setBusy(true)
    setError(null)
    let result: ModelPanel | null = null
    try {
      result = await api.putModels(choice)
    } catch (err) {
      const pe = pickerError(err)
      setError(pe)
      if (pe.field === 'key') queueMicrotask(() => keyRef.current?.focus())
    } finally {
      // Sent or refused, the key does not stay in the page.
      clearKey()
      setBusy(false)
    }
    if (result) onApplied(result)
  }

  const keyId = `${ids}-key`
  const urlId = `${ids}-url`
  const keyErr = fieldError('key')
  const urlErr = fieldError('base_url')
  const modelErr = fieldError('model')

  return (
    <form onSubmit={submit} className="grid gap-4" noValidate>
      <DialogHeader>
        <DialogTitle>{required ? 'Choose a model to get started' : 'Change the model'}</DialogTitle>
        <DialogDescription>
          {required
            ? 'Piperoom needs a model to investigate incidents. Pick one and give its key. The key stays on this machine.'
            : 'Pick a model and, if the provider or address changes, give its key.'}
        </DialogDescription>
      </DialogHeader>

      <fieldset className="m-0 grid min-w-0 gap-2 border-0 p-0" aria-describedby={modelErr ? `${ids}-model-err` : undefined}>
        <legend className="mb-1 p-0 text-sm font-medium">Model</legend>
        {supported.length === 0 && experimental.length === 0 && (
          <p className="m-0 text-sm text-muted-foreground">The server sent no supported models.</p>
        )}
        {supported.map((m) => (
          <ModelOption key={entryKey(m)} m={m} checked={selected === entryKey(m)} onSelect={() => retarget(() => setSelected(entryKey(m)))} />
        ))}
        {experimental.length > 0 && (
          <div className="grid gap-2">
            <Button
              type="button"
              variant="ghost"
              size="sm"
              className="justify-start"
              aria-expanded={showExperimental}
              aria-controls={`${ids}-exp`}
              onClick={() => setShowExperimental((v) => !v)}
            >
              {showExperimental ? 'Hide' : 'Show'} experimental models ({experimental.length})
            </Button>
            {showExperimental && (
              <div id={`${ids}-exp`} className="grid gap-2">
                <p role="note" className="m-0 flex items-start gap-1.5 text-xs text-warning-foreground">
                  <AlertTriangle className="mt-px size-3.5 shrink-0" aria-hidden="true" />
                  <span>
                    Experimental models have not passed the eval gate against the Claude reference. They may miss root
                    causes or call tools badly. Use them to try Piperoom, not on a real incident.
                  </span>
                </p>
                {experimental.map((m) => (
                  <ModelOption key={entryKey(m)} m={m} checked={selected === entryKey(m)} onSelect={() => retarget(() => setSelected(entryKey(m)))} />
                ))}
              </div>
            )}
          </div>
        )}
        {modelErr && (
          <p id={`${ids}-model-err`} role="alert" className="m-0 text-xs text-destructive-fg">
            {modelErr}
          </p>
        )}
      </fieldset>

      <div className="grid gap-1">
        <label htmlFor={urlId} className="text-sm font-medium">
          Base URL <span className="font-normal text-subtle-foreground">(optional)</span>
        </label>
        <Input
          id={urlId}
          type="url"
          inputMode="url"
          autoComplete="off"
          spellCheck={false}
          placeholder="Leave empty for the provider's address"
          value={baseUrl}
          onChange={(e) => retarget(() => setBaseUrl(e.target.value))}
          aria-invalid={urlErr ? true : undefined}
          aria-describedby={urlErr ? `${urlId}-err` : undefined}
          className="font-mono"
        />
        {urlErr && (
          <p id={`${urlId}-err`} role="alert" className="m-0 text-xs text-destructive-fg">
            {urlErr}
          </p>
        )}
      </div>

      <div className="grid gap-1">
        <label htmlFor={keyId} className="text-sm font-medium">
          API key
        </label>
        <Input
          id={keyId}
          ref={keyRef}
          type="password"
          name="model-api-key"
          autoComplete="new-password"
          spellCheck={false}
          data-lpignore="true"
          aria-invalid={keyErr ? true : undefined}
          aria-describedby={`${keyId}-hint${keyErr ? ` ${keyId}-err` : ''}`}
          className="font-mono"
        />
        <p id={`${keyId}-hint`} className="m-0 text-xs text-subtle-foreground">
          {needKey
            ? 'Asked again because the provider, flavor or address changed. It is sent once to this server and never shown again.'
            : 'Leave empty to keep the key already stored for this provider.'}
        </p>
        {keyErr && (
          <p id={`${keyId}-err`} role="alert" className="m-0 text-xs text-destructive-fg">
            {keyErr}
          </p>
        )}
        {needKey && entry && (
          <label className="mt-1 flex items-center gap-2 text-xs text-muted-foreground">
            <input
              type="checkbox"
              checked={noKey}
              onChange={(e) => setNoKey(e.target.checked)}
              className="size-4"
            />
            <span>
              This address needs no key{usuallyKeyless(entry.flavor) ? ' (usual for a local model)' : ''}
            </span>
          </label>
        )}
      </div>

      {error?.field === 'form' && (
        <p role="alert" className="m-0 text-sm text-destructive-fg">
          {error.message}
        </p>
      )}

      <DialogFooter>
        {!required && (
          <Button type="button" variant="outline" onClick={onCancel}>
            Cancel
          </Button>
        )}
        <Button type="submit" disabled={busy || !entry}>
          {busy ? 'Checking the model…' : 'Use this model'}
        </Button>
      </DialogFooter>
    </form>
  )
}

function ModelOption({ m, checked, onSelect }: { m: SupportedModel; checked: boolean; onSelect: () => void }) {
  return (
    <label
      className={`flex min-w-0 cursor-pointer items-start gap-3 rounded-md border p-3 text-sm focus-within:ring-2 focus-within:ring-ring ${
        checked ? 'border-primary bg-primary-subtle' : 'border-border'
      }`}
    >
      <input type="radio" name="model" checked={checked} onChange={onSelect} className="mt-1 size-4 shrink-0" />
      <span className="grid min-w-0 flex-1 gap-1">
        <span className="flex flex-wrap items-center gap-x-2 gap-y-1">
          <span className="break-all font-mono text-[13px] font-medium">{m.model}</span>
          {m.status === 'experimental' && <Badge variant="warning">Experimental</Badge>}
          {m.alias && <Badge variant="neutral">Alias</Badge>}
        </span>
        <span className="text-xs text-muted-foreground">{providerLabel(m)}</span>
        <span className="flex flex-wrap gap-x-4 gap-y-0.5 text-xs text-muted-foreground">
          <span>Pass rate: {percent(m.pass_rate)}</span>
          <span>Cost: {costPerRun(m.cost_per_run_usd)}</span>
        </span>
        {m.sunset && <span className="text-xs text-warning-foreground">Sunset {m.sunset}</span>}
      </span>
    </label>
  )
}
