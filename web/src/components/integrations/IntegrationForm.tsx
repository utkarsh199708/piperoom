import { useId } from 'react'
import { isSecretField } from '../../api/integrations'
import type { CatalogKind, Field, Integration } from '../../api/integrations'
import type { FormState } from '../../lib/integrations'
import { cn } from '../../lib/utils'
import { Input } from '../ui/input'
import { SecretField } from './SecretField'
import type { SecretsLookup } from './SecretField'

const selectClass =
  'h-8 w-full rounded-md border border-input-border bg-background px-2 text-sm text-foreground focus-visible:border-ring focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring/30 aria-invalid:border-destructive disabled:opacity-50'

/** One non-secret field: text, number, select or bool, with label, help and error wired by id. */
export function FieldInput({
  field,
  value,
  onChange,
  error,
  disabled,
}: {
  field: Field
  value: string | boolean
  onChange: (v: string | boolean) => void
  error?: string
  disabled?: boolean
}) {
  const id = useId()
  const helpId = `${id}-help`
  const errId = `${id}-err`
  const describedBy = [field.help ? helpId : '', error ? errId : ''].filter(Boolean).join(' ') || undefined
  const common = {
    id,
    disabled,
    'aria-invalid': error ? true : undefined,
    'aria-describedby': describedBy,
    'aria-required': field.required || undefined,
  }

  const label = (
    <>
      {field.label}
      {field.required && (
        <span aria-hidden="true" className="text-destructive-fg">
          {' '}
          *
        </span>
      )}
    </>
  )

  let control
  if (field.type === 'bool') {
    return (
      <div className="flex flex-col gap-1" data-field={field.name}>
        <label htmlFor={id} className="flex items-center gap-2 text-sm font-medium">
          <input {...common} type="checkbox" checked={value === true} onChange={(e) => onChange(e.target.checked)} />
          {label}
        </label>
        {field.help && (
          <p id={helpId} className="m-0 ml-6 text-xs text-muted-foreground">
            {field.help}
          </p>
        )}
        {error && (
          <p id={errId} className="m-0 ml-6 text-xs text-destructive-fg">
            {error}
          </p>
        )}
      </div>
    )
  }
  const text = typeof value === 'string' ? value : ''
  if (field.type === 'select') {
    control = (
      <select {...common} className={selectClass} value={text} onChange={(e) => onChange(e.target.value)}>
        {!field.required || text === '' ? <option value="">Choose…</option> : null}
        {field.options.map((o) => (
          <option key={o} value={o}>
            {o}
          </option>
        ))}
      </select>
    )
  } else {
    control = (
      <Input
        {...common}
        type={field.type === 'number' ? 'number' : 'text'}
        inputMode={field.type === 'number' ? 'numeric' : undefined}
        className={cn(field.type === 'text' && 'font-mono')}
        value={text}
        autoComplete="off"
        spellCheck={false}
        placeholder={field.default || undefined}
        onChange={(e) => onChange(e.target.value)}
      />
    )
  }
  return (
    <div className="flex flex-col gap-1.5" data-field={field.name}>
      <label htmlFor={id} className="text-sm font-medium">
        {label}
        {field.required && <span className="sr-only"> (required)</span>}
      </label>
      {control}
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
    </div>
  )
}

/**
 * The connect form, rendered from the card's field schema (GET /api/integrations/catalog).
 * Non-secret fields come first in schema order, each secret field with its own source toggle.
 */
export function IntegrationForm({
  kind,
  form,
  onChange,
  errors = {},
  managers,
  lookup,
  disabled,
}: {
  kind: CatalogKind
  form: FormState
  onChange: (next: FormState) => void
  errors?: Record<string, string>
  managers: Integration[]
  lookup?: SecretsLookup
  disabled?: boolean
}) {
  if (kind.fields.length === 0) {
    return <p className="m-0 text-sm text-muted-foreground">This card has nothing to fill in.</p>
  }
  return (
    <div className="flex flex-col gap-4">
      {kind.fields.map((f) =>
        isSecretField(f.type) ? (
          <SecretField
            key={f.name}
            field={f}
            state={form.secrets[f.name] ?? { mode: 'value', value: '', ref: '', stored: false, hasStored: false, clear: false }}
            onChange={(s) => onChange({ ...form, secrets: { ...form.secrets, [f.name]: s } })}
            managers={managers}
            lookup={lookup}
            error={errors[f.name]}
            disabled={disabled}
          />
        ) : (
          <FieldInput
            key={f.name}
            field={f}
            value={form.config[f.name] ?? (f.type === 'bool' ? false : '')}
            onChange={(v) => onChange({ ...form, config: { ...form.config, [f.name]: v } })}
            error={errors[f.name]}
            disabled={disabled}
          />
        ),
      )}
    </div>
  )
}
