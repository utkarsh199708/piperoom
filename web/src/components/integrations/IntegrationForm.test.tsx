import { describe, expect, it, vi } from 'vitest'
import { useState } from 'react'
import { fireEvent, render, screen } from '@testing-library/react'
import { IntegrationForm } from './IntegrationForm'
import { buildWriteBody, initialForm } from '../../lib/integrations'
import type { FormState } from '../../lib/integrations'
import type { CatalogKind } from '../../api/integrations'
import { postgresKind, snowflakeKind } from '../../test/integrationsData'

function Harness({ kind, errors, onForm }: { kind: CatalogKind; errors?: Record<string, string>; onForm?: (f: FormState) => void }) {
  const [form, setForm] = useState(() => initialForm(kind, undefined, false))
  return (
    <IntegrationForm
      kind={kind}
      form={form}
      onChange={(f) => {
        setForm(f)
        onForm?.(f)
      }}
      errors={errors}
      managers={[]}
    />
  )
}

describe('IntegrationForm (schema renderer)', () => {
  it('renders one labelled control per field type', () => {
    render(<Harness kind={postgresKind} />)
    const host = screen.getByLabelText(/^Host/)
    expect(host).toHaveAttribute('type', 'text')
    expect(host).toHaveAttribute('aria-required', 'true')
    expect(host).toHaveAccessibleDescription('Database host name')
    expect(screen.getByLabelText(/^Port/)).toHaveAttribute('type', 'number')
    expect(screen.getByLabelText(/^Port/)).toHaveValue(5432)
    const ssl = screen.getByLabelText(/^Sslmode/) as HTMLSelectElement
    expect(ssl.tagName).toBe('SELECT')
    expect(Array.from(ssl.options).map((o) => o.value)).toEqual(['disable', 'require', 'verify-full'])
    expect(ssl.value).toBe('require')
    expect(screen.getByLabelText(/Read from a replica/)).toHaveAttribute('type', 'checkbox')
    // A secret field is a fieldset with its own toggle, and its input is a password field.
    expect(screen.getByRole('group', { name: /^Password/ })).toBeInTheDocument()
    expect(screen.getByLabelText('Password value')).toHaveAttribute('type', 'password')
  })

  it('pem and json_file render as write-only text areas, json_file with a file picker', () => {
    render(<Harness kind={snowflakeKind} />)
    expect(screen.getByLabelText('Private_key value').tagName).toBe('TEXTAREA')
    expect(screen.getByLabelText('Creds value').tagName).toBe('TEXTAREA')
    expect(screen.getByLabelText('Choose a JSON key file')).toHaveAttribute('type', 'file')
  })

  it('edits flow into the write body with their JSON types', () => {
    const onForm = vi.fn()
    render(<Harness kind={postgresKind} onForm={onForm} />)
    fireEvent.change(screen.getByLabelText(/^Host/), { target: { value: 'db.corp' } })
    fireEvent.change(screen.getByLabelText(/^Port/), { target: { value: '6543' } })
    fireEvent.change(screen.getByLabelText(/^Sslmode/), { target: { value: 'verify-full' } })
    fireEvent.click(screen.getByLabelText(/Read from a replica/))
    fireEvent.change(screen.getByLabelText('Password value'), { target: { value: 's3cret' } })
    const last = onForm.mock.calls.at(-1)![0] as FormState
    expect(buildWriteBody(postgresKind, last)).toEqual({
      kind: 'postgres-source',
      config: { host: 'db.corp', port: 6543, sslmode: 'verify-full', replica: true },
      secrets: { password: 's3cret' },
    })
  })

  it('shows field errors and marks the control invalid', () => {
    render(<Harness kind={postgresKind} errors={{ host: 'Required', password: 'Required' }} />)
    const host = screen.getByLabelText(/^Host/)
    expect(host).toHaveAttribute('aria-invalid', 'true')
    expect(host).toHaveAccessibleDescription(/Required/)
    expect(screen.getByLabelText('Password value')).toHaveAttribute('aria-invalid', 'true')
  })

  it('says so when a card has no fields', () => {
    render(<Harness kind={{ ...postgresKind, fields: [] }} />)
    expect(screen.getByText('This card has nothing to fill in.')).toBeInTheDocument()
  })
})
