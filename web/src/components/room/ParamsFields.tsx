import type { KeyboardEvent, ReactNode } from 'react'
import type { ParamField, ParamsFormSpec, ParamsValues } from '@/lib/paramsForm'
import { JSON_KEY } from '@/lib/paramsForm'
import { Input } from '@/components/ui/input'

// The fields of an action's params (P3d-1), generated from its params_schema by lib/paramsForm.
// Controlled by the caller, which owns the <form>: Enter in a one-line field submits it as any form
// does, and Ctrl+Enter (Cmd+Enter) submits from a multi-line one, where Enter is a new line. Tab
// order is the schema's property order; nothing sets a tabIndex.

const LABEL = 'text-sm font-medium'
const HINT = 'm-0 text-xs text-muted-foreground'
const ERROR = 'm-0 text-xs font-medium text-destructive-fg'
// The bordered textarea: the shared Textarea is the composer's borderless style.
const AREA =
  'w-full min-w-0 rounded-md border border-input-border bg-background px-2.5 py-1.5 font-mono text-sm text-foreground placeholder:text-subtle-foreground focus-visible:border-ring focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/30 disabled:cursor-not-allowed disabled:opacity-50 aria-invalid:border-destructive'
const SELECT =
  'h-8 w-full min-w-0 rounded-md border border-input-border bg-background px-2 text-sm text-foreground focus-visible:border-ring focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/30 disabled:cursor-not-allowed disabled:opacity-50 aria-invalid:border-destructive'

function submitOnModEnter(e: KeyboardEvent<HTMLTextAreaElement>) {
  if (e.key === 'Enter' && (e.ctrlKey || e.metaKey)) {
    e.preventDefault()
    e.currentTarget.form?.requestSubmit()
  }
}

// Ids joined for aria-describedby; undefined when none.
function ids(...xs: (string | false | undefined)[]): string | undefined {
  const out = xs.filter(Boolean).join(' ')
  return out === '' ? undefined : out
}

// The visible mark; screen readers hear "required" from aria-required on the control.
function Required() {
  return (
    <span aria-hidden="true" title="required" className="ml-0.5 text-destructive-fg">
      *
    </span>
  )
}

function optionLabel(v: string | number | boolean): string {
  return typeof v === 'string' ? v : JSON.stringify(v)
}

export function ParamsFields({
  spec,
  values,
  onChange,
  errors = {},
  idPrefix = 'param',
  disabled = false,
}: {
  spec: ParamsFormSpec
  values: ParamsValues
  onChange: (v: ParamsValues) => void
  /** Messages by field name (JSON_KEY in JSON mode); the caller shows them after a submit attempt. */
  errors?: Record<string, string>
  idPrefix?: string
  disabled?: boolean
}) {
  const set = (name: string, v: string | boolean) => onChange({ ...values, [name]: v })

  if (spec.mode === 'json') {
    const id = `${idPrefix}-json`
    const err = errors[JSON_KEY]
    return (
      <div className="flex flex-col gap-1.5">
        <label htmlFor={id} className={LABEL}>
          Params (edit as JSON)
          <Required />
        </label>
        <p id={`${id}-hint`} className={HINT}>
          This action&apos;s params have no form here: write them as one JSON object. Ctrl+Enter submits.
        </p>
        <textarea
          id={id}
          rows={8}
          spellCheck={false}
          aria-required
          disabled={disabled}
          value={typeof values[JSON_KEY] === 'string' ? (values[JSON_KEY] as string) : ''}
          onChange={(e) => set(JSON_KEY, e.target.value)}
          onKeyDown={submitOnModEnter}
          aria-invalid={err ? true : undefined}
          aria-describedby={ids(`${id}-hint`, !!err && `${id}-error`)}
          className={AREA}
        />
        {err && (
          <p id={`${id}-error`} className={ERROR}>
            {err}
          </p>
        )}
      </div>
    )
  }

  if (spec.fields.length === 0) return null
  return (
    <div className="flex flex-col gap-3">
      {spec.fields.map((f) => (
        <Field
          key={f.name}
          field={f}
          id={`${idPrefix}-${f.name}`}
          value={values[f.name]}
          error={errors[f.name]}
          disabled={disabled}
          onChange={(v) => set(f.name, v)}
        />
      ))}
    </div>
  )
}

function Field({
  field: f,
  id,
  value,
  error,
  disabled,
  onChange,
}: {
  field: ParamField
  id: string
  value: string | boolean | undefined
  error?: string
  disabled: boolean
  onChange: (v: string | boolean) => void
}) {
  const hintId = `${id}-hint`
  const errId = `${id}-error`
  const hintText =
    f.kind === 'string[]'
      ? [f.description, 'One per line.'].filter(Boolean).join(' ')
      : f.kind === 'json'
        ? [f.description, 'Edit as JSON. Ctrl+Enter submits.'].filter(Boolean).join(' ')
        : f.description
  const describedBy = ids(!!hintText && hintId, !!error && errId)
  const common = {
    id,
    disabled,
    // aria-required, not required: the caller's form shows its own messages (lib/paramsForm), so
    // the browser's validation bubble must not stop the submit first.
    'aria-required': f.required || undefined,
    'aria-invalid': error ? true : undefined,
    'aria-describedby': describedBy,
  }
  const text = typeof value === 'string' ? value : ''

  if (f.kind === 'boolean') {
    return (
      <div className="flex flex-col gap-1">
        <label htmlFor={id} className="flex min-h-8 items-center gap-2 text-sm font-medium">
          <input
            type="checkbox"
            {...common}
            checked={value === true}
            onChange={(e) => onChange(e.target.checked)}
            className="size-4"
          />
          <code className="font-mono">{f.name}</code>
          {f.required && <Required />}
        </label>
        {hintText && (
          <p id={hintId} className={HINT}>
            {hintText}
          </p>
        )}
        {error && (
          <p id={errId} className={ERROR}>
            {error}
          </p>
        )}
      </div>
    )
  }

  let control: ReactNode
  switch (f.kind) {
    case 'enum':
      control = (
        <select {...common} value={text} onChange={(e) => onChange(e.target.value)} className={SELECT}>
          <option value="">{f.required ? 'Choose…' : '(not set)'}</option>
          {(f.options ?? []).map((o, i) => (
            <option key={i} value={String(i)}>
              {optionLabel(o)}
            </option>
          ))}
        </select>
      )
      break
    case 'integer':
      control = (
        <Input
          {...common}
          type="text"
          inputMode="numeric"
          autoComplete="off"
          value={text}
          onChange={(e) => onChange(e.target.value)}
          className="font-mono"
        />
      )
      break
    case 'string[]':
    case 'json':
      control = (
        <textarea
          {...common}
          rows={f.kind === 'json' ? 5 : 3}
          spellCheck={false}
          value={text}
          onChange={(e) => onChange(e.target.value)}
          onKeyDown={submitOnModEnter}
          className={AREA}
        />
      )
      break
    default:
      control = (
        <Input
          {...common}
          type="text"
          autoComplete="off"
          spellCheck={false}
          maxLength={f.maxLength}
          value={text}
          onChange={(e) => onChange(e.target.value)}
          className="font-mono"
        />
      )
  }

  return (
    <div className="flex flex-col gap-1.5">
      <label htmlFor={id} className={LABEL}>
        <code className="font-mono">{f.name}</code>
        {f.required && <Required />}
      </label>
      {hintText && (
        <p id={hintId} className={HINT}>
          {hintText}
        </p>
      )}
      {control}
      {error && (
        <p id={errId} className={ERROR}>
          {error}
        </p>
      )}
    </div>
  )
}
