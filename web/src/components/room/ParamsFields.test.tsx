import { describe, expect, it, vi } from 'vitest'
import { useState } from 'react'
import { fireEvent, render, screen } from '@testing-library/react'
import { ParamsFields } from './ParamsFields'
import { JSON_KEY, formSpec, initialValues, paramsFrom } from '@/lib/paramsForm'

// The caller's side, as the propose dialog does it: a <form> that builds the params on submit and
// shows the field messages only after a failed attempt.
function Harness({ schema, onParams }: { schema: unknown; onParams: (p: Record<string, unknown>) => void }) {
  const spec = formSpec(schema)
  const [values, setValues] = useState(() => initialValues(schema, spec))
  const [errors, setErrors] = useState<Record<string, string>>({})
  return (
    <form
      noValidate
      onSubmit={(e) => {
        e.preventDefault()
        const r = paramsFrom(spec, values)
        if (r.ok) {
          setErrors({})
          onParams(r.params)
        } else setErrors(r.errors)
      }}
    >
      <ParamsFields spec={spec} values={values} onChange={setValues} errors={errors} />
      <button type="submit">Propose</button>
    </form>
  )
}

const obj = (properties: Record<string, unknown>, required: string[] = []) => ({ type: 'object', properties, required })

function setup(schema: unknown) {
  const onParams = vi.fn()
  render(<Harness schema={schema} onParams={onParams} />)
  const submit = () => fireEvent.click(screen.getByRole('button', { name: 'Propose' }))
  return { onParams, submit }
}

describe('ParamsFields', () => {
  it('string: a labelled text field, required marked, pattern checked', () => {
    const { onParams, submit } = setup(obj({ connector: { type: 'string', description: 'connector name', pattern: '^[a-z-]+$' } }, ['connector']))
    const input = screen.getByRole('textbox', { name: 'connector' })
    expect(input).toHaveAttribute('aria-required', 'true')
    expect(input).toHaveAccessibleDescription('connector name')
    submit()
    expect(onParams).not.toHaveBeenCalled()
    expect(input).toHaveAttribute('aria-invalid', 'true')
    expect(input).toHaveAccessibleDescription('connector name Required.')
    fireEvent.change(input, { target: { value: 'Bad Name' } })
    submit()
    expect(screen.getByText('Does not match the expected format.')).toBeInTheDocument()
    fireEvent.change(input, { target: { value: 'orders-src' } })
    submit()
    expect(onParams).toHaveBeenCalledWith({ connector: 'orders-src' })
  })

  it('integer: a numeric text field that sends a number', () => {
    const { onParams, submit } = setup(obj({ partitions: { type: 'integer', minimum: 1 } }))
    const input = screen.getByRole('textbox', { name: 'partitions' })
    expect(input).toHaveAttribute('inputmode', 'numeric')
    expect(input).not.toHaveAttribute('aria-required')
    fireEvent.change(input, { target: { value: '0' } })
    submit()
    expect(screen.getByText('Must be at least 1.')).toBeInTheDocument()
    fireEvent.change(input, { target: { value: '12' } })
    submit()
    expect(onParams).toHaveBeenCalledWith({ partitions: 12 })
  })

  it('boolean: a checkbox, omitted while optional and off', () => {
    const { onParams, submit } = setup(obj({ force: { type: 'boolean', description: 'when the slot is lost' } }))
    const box = screen.getByRole('checkbox', { name: 'force' })
    submit()
    expect(onParams).toHaveBeenLastCalledWith({})
    fireEvent.click(box)
    submit()
    expect(onParams).toHaveBeenLastCalledWith({ force: true })
  })

  it('enum: a select of the schema values, in order', () => {
    const { onParams, submit } = setup(obj({ reset_to: { enum: ['earliest', 'latest', 0] } }, ['reset_to']))
    const select = screen.getByRole('combobox', { name: 'reset_to' })
    expect([...select.querySelectorAll('option')].map((o) => o.textContent)).toEqual(['Choose…', 'earliest', 'latest', '0'])
    submit()
    expect(screen.getByText('Required.')).toBeInTheDocument()
    fireEvent.change(select, { target: { value: '2' } })
    submit()
    expect(onParams).toHaveBeenCalledWith({ reset_to: 0 })
  })

  it('string[]: one item per line, items checked', () => {
    const { onParams, submit } = setup(
      obj({ tables: { type: 'array', items: { type: 'string', pattern: '^[a-z]+\\.[a-z]+$' }, minItems: 1, description: 'source tables' } }, ['tables']),
    )
    const area = screen.getByRole('textbox', { name: 'tables' })
    expect(area.tagName).toBe('TEXTAREA')
    expect(area).toHaveAccessibleDescription('source tables One per line.')
    fireEvent.change(area, { target: { value: 'public.orders\norders' } })
    submit()
    expect(screen.getByText('"orders" does not match the expected format.')).toBeInTheDocument()
    fireEvent.change(area, { target: { value: 'public.orders\n\npublic.lines\n' } })
    submit()
    expect(onParams).toHaveBeenCalledWith({ tables: ['public.orders', 'public.lines'] })
  })

  it('an unsupported property is edited as JSON, validated', () => {
    const { onParams, submit } = setup(
      obj({ key_range: { type: 'object', properties: { from: { type: 'string' } } } }, ['key_range']),
    )
    const area = screen.getByRole('textbox', { name: 'key_range' })
    expect(area).toHaveAccessibleDescription('Edit as JSON. Ctrl+Enter submits.')
    fireEvent.change(area, { target: { value: '{from: 1}' } })
    submit()
    expect(screen.getByText(/^Not valid JSON/)).toBeInTheDocument()
    fireEvent.change(area, { target: { value: '{"from": "1"}' } })
    submit()
    expect(onParams).toHaveBeenCalledWith({ key_range: { from: '1' } })
  })

  it('an unsupported schema is edited as one JSON object', () => {
    const { onParams, submit } = setup({ oneOf: [{ type: 'object' }] })
    const area = screen.getByRole('textbox', { name: /edit as JSON/ })
    expect(area).toHaveValue('{}')
    fireEvent.change(area, { target: { value: '[1, 2]' } })
    submit()
    expect(screen.getByText('Params must be a JSON object ({ … }).')).toBeInTheDocument()
    fireEvent.change(area, { target: { value: '{"slot": "dbz"}' } })
    submit()
    expect(onParams).toHaveBeenCalledWith({ slot: 'dbz' })
    expect(initialValues('x')).toEqual({ [JSON_KEY]: '{}' })
  })

  describe('keyboard', () => {
    const schema = obj(
      {
        connector: { type: 'string' },
        partitions: { type: 'integer' },
        force: { type: 'boolean' },
        mode: { enum: ['a', 'b'] },
        tables: { type: 'array', items: { type: 'string' } },
        range: { type: 'object' },
      },
      ['connector'],
    )

    it('every control has a label, and tab order is the schema order (no tabIndex)', () => {
      const { container } = render(<Harness schema={schema} onParams={vi.fn()} />)
      const controls = [...container.querySelectorAll('input, select, textarea')]
      expect(controls.map((c) => c.id)).toEqual(['param-connector', 'param-partitions', 'param-force', 'param-mode', 'param-tables', 'param-range'])
      for (const c of controls) {
        expect(c).not.toHaveAttribute('tabindex')
        expect(container.querySelector(`label[for="${c.id}"]`)).not.toBeNull()
      }
    })

    it('Ctrl+Enter or Cmd+Enter in a multi-line field submits; plain Enter does not', () => {
      const onParams = vi.fn()
      render(<Harness schema={schema} onParams={onParams} />)
      fireEvent.change(screen.getByRole('textbox', { name: 'connector' }), { target: { value: 'c' } })
      const tables = screen.getByRole('textbox', { name: 'tables' })
      fireEvent.keyDown(tables, { key: 'Enter' })
      expect(onParams).not.toHaveBeenCalled()
      fireEvent.keyDown(tables, { key: 'Enter', ctrlKey: true })
      expect(onParams).toHaveBeenCalledTimes(1)
      fireEvent.keyDown(screen.getByRole('textbox', { name: 'range' }), { key: 'Enter', metaKey: true })
      expect(onParams).toHaveBeenCalledTimes(2)
      expect(onParams).toHaveBeenLastCalledWith({ connector: 'c' })
    })

    it('Enter in a one-line field submits the form (implicit submission)', () => {
      const onParams = vi.fn()
      render(<Harness schema={schema} onParams={onParams} />)
      const input = screen.getByRole('textbox', { name: 'connector' })
      fireEvent.change(input, { target: { value: 'c' } })
      // jsdom does not perform implicit submission on a key press; the browser does for any form
      // with a submit button. Assert the precondition: the field is a plain text input in the form.
      expect(input).toHaveAttribute('type', 'text')
      expect((input as HTMLInputElement).form?.querySelector('button[type="submit"]')).not.toBeNull()
      fireEvent.submit((input as HTMLInputElement).form as HTMLFormElement)
      expect(onParams).toHaveBeenCalledWith({ connector: 'c' })
    })
  })
})
