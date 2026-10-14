import { useEffect, useId, useRef, useState } from 'react'
import type { ChangeEvent } from 'react'
import { KeyRound, Lock } from 'lucide-react'
import { ApiClientError } from '../../api/client'
import { integrationsApi } from '../../api/integrations'
import type { Field, Integration } from '../../api/integrations'
import { buildRef, managerName, parseRef, refBase } from '../../lib/integrations'
import type { SecretFieldState, SecretMode } from '../../lib/integrations'
import { errorText } from '../../lib/errors'
import { cn } from '../../lib/utils'
import { Badge } from '../ui/badge'
import { Button } from '../ui/button'
import { Input } from '../ui/input'

/** The two secret listing calls, injectable for tests. */
export type SecretsLookup = Pick<typeof integrationsApi, 'secretNames' | 'secretKeys'>

const selectClass =
  'h-8 w-full rounded-md border border-input-border bg-background px-2 text-sm text-foreground focus-visible:border-ring focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/30 disabled:opacity-50'
const textareaClass =
  'min-h-24 w-full rounded-md border border-input-border bg-background px-2.5 py-2 font-mono text-xs text-foreground placeholder:text-subtle-foreground focus-visible:border-ring focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/30 aria-invalid:border-destructive'

function lookupError(e: unknown): string {
  if (e instanceof ApiClientError && e.code === 'provider_unreachable') {
    return 'The secret manager did not answer. Check its card, or paste the reference.'
  }
  return errorText(e)
}

/**
 * Pick a secret in a connected secret manager: the manager, the secret name (listed under
 * a prefix when the manager can list), and the key of a JSON / key-value secret. The
 * `secret://` reference is the single value; it can also be pasted directly (for the
 * built-in `files` provider or a provider set by environment variables).
 * Only names and keys are ever fetched, never values.
 */
export function SecretRefPicker({
  idBase,
  value,
  onChange,
  managers,
  lookup,
  disabled,
  invalid,
  describedBy,
}: {
  idBase: string
  value: string
  onChange: (ref: string) => void
  managers: Integration[]
  lookup: SecretsLookup
  disabled?: boolean
  invalid?: boolean
  describedBy?: string
}) {
  const parsed = parseRef(value)
  const [provider, setProvider] = useState(parsed?.provider ?? (managers[0] ? managerName(managers[0]) : ''))
  const [path, setPath] = useState(parsed?.path ?? '')
  const [key, setKey] = useState(parsed?.key ?? '')
  const [names, setNames] = useState<string[] | null>(null)
  const [listSupported, setListSupported] = useState(true)
  const [keys, setKeys] = useState<string[] | null>(null)
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  // Ignore an answer that arrives after a newer lookup of the same kind started. Names and
  // keys count separately so one never strands the other's busy state.
  const nameSeq = useRef(0)
  const keySeq = useRef(0)

  const manager = managers.find((m) => managerName(m) === provider)
  const version = parsed?.version ?? ''

  const emit = (p: string, n: string, k: string) => {
    if (p && n) onChange(buildRef({ provider: p, path: n, key: k, version }))
    else onChange('')
  }

  const loadKeys = async (p: string, n: string) => {
    const m = managers.find((x) => managerName(x) === p)
    const mine = ++keySeq.current
    if (!m || !n) {
      setKeys(null)
      return
    }
    setError(null)
    try {
      const res = await lookup.secretKeys(m.id, refBase({ provider: p, path: n, key: '', version: '' }))
      if (mine === keySeq.current) setKeys(res.keys)
    } catch (e) {
      if (mine === keySeq.current) {
        setKeys(null)
        setError(lookupError(e))
      }
    }
  }

  const findNames = async () => {
    if (!manager) return
    const mine = ++nameSeq.current
    setBusy(true)
    setError(null)
    try {
      const res = await lookup.secretNames(manager.id, path)
      if (mine !== nameSeq.current) return
      setListSupported(res.list_supported)
      setNames(res.names)
    } catch (e) {
      if (mine === nameSeq.current) {
        setNames(null)
        setError(lookupError(e))
      }
    } finally {
      // The button is disabled while busy, so no newer Find can be in flight.
      setBusy(false)
    }
  }

  const pickName = (n: string) => {
    setPath(n)
    setKey('')
    setNames(null)
    emit(provider, n, '')
    void loadKeys(provider, n)
  }

  const onPaste = (e: ChangeEvent<HTMLInputElement>) => {
    const text = e.target.value
    onChange(text)
    const p = parseRef(text)
    if (p) {
      setProvider(p.provider)
      setPath(p.path)
      setKey(p.key)
      // A complete reference to a connected manager: offer its keys.
      setKeys(null)
      void loadKeys(p.provider, p.path)
    } else {
      keySeq.current++
      setKeys(null)
    }
  }

  // An unknown provider in a pasted reference still shows in the select.
  const providerKnown = provider === '' || managers.some((m) => managerName(m) === provider)
  const ids = {
    manager: `${idBase}-manager`,
    name: `${idBase}-name`,
    key: `${idBase}-key`,
    ref: `${idBase}-ref`,
    names: `${idBase}-names`,
  }

  return (
    <div className="flex flex-col gap-3">
      {managers.length > 0 && (
        <>
          <div className="flex flex-col gap-1.5">
            <label htmlFor={ids.manager} className="text-xs font-medium text-muted-foreground">
              Secret manager
            </label>
            <select
              id={ids.manager}
              className={selectClass}
              value={provider}
              disabled={disabled}
              onChange={(e) => {
                setProvider(e.target.value)
                setNames(null)
                setKeys(null)
                emit(e.target.value, path, key)
              }}
            >
              {managers.map((m) => (
                <option key={m.id} value={managerName(m)}>
                  {managerName(m)} · {m.title}
                </option>
              ))}
              {!providerKnown && <option value={provider}>{provider} (not a connected manager)</option>}
            </select>
          </div>
          <div className="flex flex-col gap-1.5">
            <label htmlFor={ids.name} className="text-xs font-medium text-muted-foreground">
              Secret name
            </label>
            <div className="flex gap-2">
              <Input
                id={ids.name}
                className="font-mono"
                value={path}
                placeholder="database/creds/piperoom-ro"
                disabled={disabled || !manager}
                autoComplete="off"
                onChange={(e) => {
                  setPath(e.target.value)
                  keySeq.current++
                  setKeys(null)
                  emit(provider, e.target.value, key)
                }}
              />
              <Button variant="secondary" size="sm" onClick={() => void findNames()} disabled={disabled || !manager || busy}>
                {busy ? 'Finding…' : 'Find'}
              </Button>
            </div>
            {names !== null && (
              <div id={ids.names}>
                {!listSupported ? (
                  <p className="m-0 text-xs text-muted-foreground">
                    This manager cannot list secrets. Type the full name, then{' '}
                    <button
                      type="button"
                      className="underline underline-offset-2 hover:text-foreground disabled:opacity-50"
                      disabled={disabled || path.trim() === ''}
                      onClick={() => void loadKeys(provider, path.trim())}
                    >
                      look up its keys
                    </button>
                    .
                  </p>
                ) : names.length === 0 ? (
                  <p className="m-0 text-xs text-muted-foreground">No secrets under “{path}”.</p>
                ) : (
                  <ul aria-label="Secrets found" className="m-0 flex max-h-48 list-none flex-col overflow-y-auto rounded-md border border-border p-1">
                    {names.map((n) => (
                      <li key={n}>
                        <button
                          type="button"
                          className="w-full truncate rounded px-2 py-1 text-left font-mono text-xs hover:bg-accent focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                          onClick={() => pickName(n)}
                        >
                          {n}
                        </button>
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            )}
          </div>
          {keys !== null && (
            <div className="flex flex-col gap-1.5">
              <label htmlFor={ids.key} className="text-xs font-medium text-muted-foreground">
                Key
              </label>
              {keys.length === 0 ? (
                <p className="m-0 text-xs text-muted-foreground">A plain secret: its whole value is used.</p>
              ) : (
                <select
                  id={ids.key}
                  className={cn(selectClass, 'font-mono')}
                  value={key}
                  disabled={disabled}
                  onChange={(e) => {
                    setKey(e.target.value)
                    emit(provider, path, e.target.value)
                  }}
                >
                  <option value="">(whole secret)</option>
                  {keys.map((k) => (
                    <option key={k} value={k}>
                      {k}
                    </option>
                  ))}
                </select>
              )}
            </div>
          )}
        </>
      )}
      {error && (
        <p role="alert" className="m-0 text-xs text-destructive-fg">
          {error}
        </p>
      )}
      <div className="flex flex-col gap-1.5">
        <label htmlFor={ids.ref} className="text-xs font-medium text-muted-foreground">
          {managers.length > 0 ? 'Reference (or paste one)' : 'Paste a secret:// reference'}
        </label>
        <Input
          id={ids.ref}
          className="font-mono"
          value={value}
          placeholder="secret://vault-corp/database/creds/piperoom-ro#password"
          disabled={disabled}
          autoComplete="off"
          spellCheck={false}
          aria-invalid={invalid || undefined}
          aria-describedby={describedBy}
          onChange={onPaste}
        />
      </div>
    </div>
  )
}

/**
 * One secret field (`secret`, `pem`, `json_file`) with a toggle between "From a secret
 * manager" (a `secret://` reference) and "Enter value" (write-only: the stored value is never
 * shown, only "set"; leaving it blank keeps it).
 */
export function SecretField({
  field,
  state,
  onChange,
  managers,
  lookup = integrationsApi,
  error,
  disabled,
  typedOff = false,
}: {
  field: Field
  state: SecretFieldState
  onChange: (next: SecretFieldState) => void
  managers: Integration[]
  lookup?: SecretsLookup
  error?: string
  disabled?: boolean
  /** PIPEROOM_SECRET_KEY is not set: a typed-in value cannot be saved, references still can. */
  typedOff?: boolean
}) {
  const id = useId()
  const helpId = `${id}-help`
  const errId = `${id}-err`
  const describedBy = [field.help ? helpId : '', error ? errId : ''].filter(Boolean).join(' ') || undefined
  const set = (patch: Partial<SecretFieldState>) => onChange({ ...state, ...patch })
  const setMode = (mode: SecretMode) => set({ mode })
  const valueId = `${id}-value`

  // Read a key file into the value; it stays in memory only and is sent once, write-only.
  const [fileName, setFileName] = useState('')
  useEffect(() => {
    if (state.value === '') setFileName('')
  }, [state.value])
  const onFile = (e: ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return
    void file.text().then((text) => {
      setFileName(file.name)
      set({ value: text, clear: false })
    })
  }

  const placeholder = state.stored ? 'Set · leave blank to keep the stored value' : field.type === 'pem' ? '-----BEGIN …-----' : ''

  return (
    <fieldset className="m-0 flex min-w-0 flex-col gap-2 border-0 p-0" data-field={field.name}>
      <legend className="mb-1.5 flex items-center gap-2 p-0 text-sm font-medium">
        <span>
          {field.label}
          {field.required && (
            <span aria-hidden="true" className="text-destructive-fg">
              {' '}
              *
            </span>
          )}
          {field.required && <span className="sr-only"> (required)</span>}
        </span>
        {state.stored && state.mode === 'value' && !state.clear && (
          <Badge variant="success" className="rounded-full">
            <Lock className="size-3" aria-hidden="true" />
            Set
          </Badge>
        )}
      </legend>

      <div role="group" aria-label={`Where ${field.label} comes from`} className="inline-flex w-fit gap-1 rounded-md border border-border bg-background p-0.5">
        {(
          [
            ['manager', 'From a secret manager'],
            ['value', 'Enter value'],
          ] as const
        ).map(([mode, text]) => (
          <button
            key={mode}
            type="button"
            aria-pressed={state.mode === mode}
            disabled={disabled || (mode === 'value' && typedOff)}
            title={mode === 'value' && typedOff ? 'Typed-in secrets need PIPEROOM_SECRET_KEY on the server' : undefined}
            onClick={() => setMode(mode)}
            className={cn(
              'inline-flex h-7 items-center gap-1.5 rounded px-2.5 text-xs font-medium text-muted-foreground transition-colors',
              'hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:opacity-50',
              state.mode === mode && 'bg-accent text-foreground',
            )}
          >
            {mode === 'manager' ? <KeyRound className="size-3.5" aria-hidden="true" /> : <Lock className="size-3.5" aria-hidden="true" />}
            {text}
          </button>
        ))}
      </div>

      {state.mode === 'manager' ? (
        <SecretRefPicker
          idBase={id}
          value={state.ref}
          onChange={(ref) => set({ ref })}
          managers={managers}
          lookup={lookup}
          disabled={disabled || state.clear}
          invalid={!!error}
          describedBy={describedBy}
        />
      ) : (
        <div className="flex flex-col gap-1.5">
          <label htmlFor={valueId} className="sr-only">
            {field.label} value
          </label>
          {field.type === 'secret' ? (
            <Input
              id={valueId}
              type="password"
              autoComplete="new-password"
              value={state.value}
              placeholder={placeholder}
              disabled={disabled || state.clear}
              aria-invalid={!!error || undefined}
              aria-describedby={describedBy}
              onChange={(e) => set({ value: e.target.value, clear: false })}
            />
          ) : (
            <>
              {field.type === 'json_file' && (
                <div className="flex items-center gap-2">
                  <label htmlFor={`${id}-file`} className="text-xs text-muted-foreground">
                    Choose a JSON key file
                  </label>
                  <input
                    id={`${id}-file`}
                    type="file"
                    accept="application/json,.json"
                    disabled={disabled || state.clear}
                    onChange={onFile}
                    className="text-xs text-muted-foreground file:mr-2 file:rounded file:border file:border-border file:bg-secondary file:px-2 file:py-1 file:text-xs file:text-foreground"
                  />
                  {fileName && <span className="truncate text-xs text-muted-foreground">{fileName} read</span>}
                </div>
              )}
              <textarea
                id={valueId}
                className={textareaClass}
                value={state.value}
                placeholder={field.type === 'json_file' ? (state.stored ? placeholder : 'Or paste the JSON') : placeholder}
                disabled={disabled || state.clear}
                spellCheck={false}
                autoComplete="off"
                aria-invalid={!!error || undefined}
                aria-describedby={describedBy}
                onChange={(e) => set({ value: e.target.value, clear: false })}
              />
            </>
          )}
          {typedOff ? (
            <p className="m-0 text-xs text-destructive-fg">
              Typed-in secrets cannot be saved: PIPEROOM_SECRET_KEY is not set on the server. Use a secret manager reference.
            </p>
          ) : (
            <p className="m-0 text-xs text-subtle-foreground">Write-only: Piperoom never shows a stored secret again.</p>
          )}
        </div>
      )}

      {state.hasStored && !field.required && (
        <label className="flex items-center gap-2 text-xs text-muted-foreground">
          <input
            type="checkbox"
            checked={state.clear}
            disabled={disabled}
            onChange={(e) => set({ clear: e.target.checked, value: '' })}
          />
          Remove the stored value
        </label>
      )}

      {field.help && (
        <p id={helpId} className="m-0 text-xs text-muted-foreground">
          {field.help}
        </p>
      )}
      {error && (
        <p id={errId} className="m-0 text-xs text-destructive-fg">
          {error}
        </p>
      )}
    </fieldset>
  )
}
