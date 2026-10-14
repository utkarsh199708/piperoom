import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { CatalogGrid } from './CatalogGrid'
import { SetupChecklist } from './SetupChecklist'
import { checklistRows } from '../../lib/integrations'
import { check, connectKind, postgresKind, view } from '../../test/integrationsData'

describe('CatalogGrid status', () => {
  it.each([
    ['connected', 'Connected'],
    ['failing', 'Needs attention'],
    ['disabled', 'Disabled'],
    ['untested', 'Not tested yet'],
  ] as const)('a %s row shows "%s" on its card', (status, text) => {
    render(<CatalogGrid catalog={[postgresKind, connectKind]} list={[view({ status })]} admin onOpen={vi.fn()} />)
    const card = document.querySelector('[data-kind="postgres-source"]') as HTMLElement
    expect(card).toHaveTextContent(text)
    if (status !== 'connected') expect(card).not.toHaveTextContent('Connected')
  })
})

describe('SetupChecklist row detail', () => {
  it('a row that needs attention names its first failing check', () => {
    const failing = view({
      status: 'failing',
      last_test: [check('reach', 'pass'), check('read_only', 'fail', { title: 'Role piperoom_ro can INSERT into public.orders' })],
    })
    render(<SetupChecklist rows={checklistRows([postgresKind], [failing])} managers={[]} admin onOpenRow={vi.fn()} onOpenIntegration={vi.fn()} />)
    expect(screen.getByText('Role piperoom_ro can INSERT into public.orders')).toBeInTheDocument()
  })
})
