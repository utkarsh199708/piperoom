import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { CheckList } from './CheckList'
import { SetupChecklist } from './SetupChecklist'
import { checklistRows } from '../../lib/integrations'
import { check, connectKind, grafanaKind, postgresKind, snowflakeKind, vaultRow, view } from '../../test/integrationsData'

describe('CheckList', () => {
  const checks = [
    check('reach', 'pass', { title: 'Can reach host' }),
    check('slots', 'fail', { title: 'Can read pg_replication_slots', detail: 'permission denied', fix: 'GRANT pg_monitor TO piperoom_ro;' }),
    check('timeout', 'warn', { title: 'statement_timeout not set', fix: "ALTER ROLE piperoom_ro SET statement_timeout = '10s';" }),
    check('write', 'skipped', { title: 'Cannot write' }),
  ]

  it('lists every check in order with its status in text, not only colour', () => {
    render(<CheckList checks={checks} />)
    const items = screen.getAllByRole('listitem')
    expect(items.map((li) => li.getAttribute('data-status'))).toEqual(['pass', 'fail', 'warn', 'skipped'])
    expect(items[0]).toHaveTextContent('Passed: Can reach host')
    expect(items[1]).toHaveTextContent('Failed: Can read pg_replication_slots')
    expect(items[2]).toHaveTextContent('Warning:')
    expect(items[3]).toHaveTextContent('Skipped:')
    expect(screen.getByRole('status')).toHaveTextContent('1 failed')
  })

  it('a failing check opens with its fix; a warning expands on click; the fix can be copied', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', { value: { writeText }, configurable: true })
    render(<CheckList checks={checks} />)
    const [, fail, warn] = screen.getAllByRole('listitem')
    expect(within(fail).getByText('GRANT pg_monitor TO piperoom_ro;')).toBeInTheDocument()
    expect(within(fail).getByText('permission denied')).toBeInTheDocument()

    const toggle = within(warn).getByRole('button', { name: /Fix for statement_timeout/ })
    expect(toggle).toHaveAttribute('aria-expanded', 'false')
    fireEvent.click(toggle)
    expect(toggle).toHaveAttribute('aria-expanded', 'true')
    expect(within(warn).getByText(/statement_timeout = '10s'/)).toBeInTheDocument()

    fireEvent.click(within(fail).getByRole('button', { name: 'Copy fix for Can read pg_replication_slots' }))
    expect(writeText).toHaveBeenCalledWith('GRANT pg_monitor TO piperoom_ro;')
    await waitFor(() => expect(within(fail).getByText('Copied')).toBeInTheDocument())
  })

  it('summarises warnings and marks a stale test', () => {
    const { rerender } = render(<CheckList checks={[check('a', 'pass'), check('b', 'warn')]} />)
    expect(screen.getByRole('status')).toHaveTextContent('1 warning, saved as follow-ups')
    rerender(<CheckList checks={[check('a', 'pass')]} stale />)
    expect(screen.getByRole('status')).toHaveTextContent('Values changed since this test')
  })
})

describe('SetupChecklist', () => {
  const catalog = [postgresKind, connectKind, snowflakeKind, grafanaKind]

  it('shows progress, what a first room still needs, required vs optional rows and status pills', () => {
    const list = [
      view({ last_test: [check('read_only', 'pass')] }),
      view({ id: 'int_sf', category: 'warehouse', kind: 'snowflake', title: 'Snowflake', status: 'failing' }),
    ]
    const onOpenRow = vi.fn()
    render(
      <SetupChecklist rows={checklistRows(catalog, list)} managers={[vaultRow]} admin onOpenRow={onOpenRow} onOpenIntegration={vi.fn()} />,
    )
    expect(screen.getByText('1 of 4 connected')).toBeInTheDocument()
    expect(screen.getByText('You can open your first room once Kafka Connect is connected.')).toBeInTheDocument()
    expect(screen.getByRole('progressbar', { name: 'Connections set up' })).toHaveAttribute('aria-valuetext', '1 of 4 connected')

    const required = screen.getByRole('list', { name: 'Required for a first room' })
    const optional = screen.getByRole('list', { name: 'Optional for now' })
    expect(within(required).getByText('Connected · verified read-only')).toBeInTheDocument()
    expect(within(required).getByText('Not started')).toBeInTheDocument()
    expect(within(optional).getByText('Needs attention')).toBeInTheDocument()
    expect(within(screen.getByRole('list', { name: 'Secret managers' })).getByText(/vault-corp/)).toBeInTheDocument()

    fireEvent.click(within(required).getByRole('button', { name: 'Connect Kafka Connect' }))
    expect(onOpenRow).toHaveBeenCalledWith(expect.objectContaining({ category: 'connect' }))
  })

  it('a row set by environment variables is read-only: "Managed by environment" and View', () => {
    const list = [view({ id: 'env_source_db', source: 'env', version: 0, updated_by: 'env' })]
    render(<SetupChecklist rows={checklistRows(catalog, list)} managers={[]} admin onOpenRow={vi.fn()} onOpenIntegration={vi.fn()} />)
    expect(screen.getByText('Managed by environment')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'View Source database' })).toBeInTheDocument()
  })
})
