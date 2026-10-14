import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen, within } from '@testing-library/react'
import { IntegrationsPage } from './IntegrationsPage'
import type { DrawerApi } from './ConnectDrawer'
import { UserContext } from '../../context/user'
import type { CurrentUser } from '../../state/permissions'
import { connectKind, postgresKind, vaultKind, vaultRow, view } from '../../test/integrationsData'

function fakeApi(): DrawerApi {
  return {
    catalog: vi.fn().mockResolvedValue([postgresKind, connectKind, vaultKind]),
    list: vi.fn().mockResolvedValue([view(), vaultRow]),
    setupScript: vi.fn().mockResolvedValue({ script: '' }),
    audit: vi.fn().mockResolvedValue([]),
  } as unknown as DrawerApi
}

function renderPage(user: CurrentUser, api: DrawerApi) {
  return render(
    <UserContext.Provider value={{ user, setUserId: () => undefined, logout: () => undefined, sessionEpoch: 0 }}>
      <IntegrationsPage api={api} />
    </UserContext.Provider>,
  )
}

describe('IntegrationsPage', () => {
  it('an admin gets the checklist and catalog, and a card opens the connect drawer', async () => {
    const api = fakeApi()
    renderPage({ id: 'priya', name: 'Priya', layers: [], admin: true }, api)
    expect(await screen.findByText('1 of 2 connected')).toBeInTheDocument()
    expect(screen.getByRole('list', { name: 'Secret managers' })).toBeInTheDocument()
    const catalog = screen.getByRole('region', { name: 'Catalog' })
    expect(within(catalog).getByRole('button', { name: 'Manage PostgreSQL' })).toBeInTheDocument()

    fireEvent.click(within(catalog).getByRole('button', { name: 'Connect Kafka Connect REST' }))
    const drawer = await screen.findByRole('dialog')
    expect(within(drawer).getByText(/Kafka Connect REST · kafka connect/)).toBeInTheDocument()
    // A secret manager is connected, so secrets start on "From a secret manager".
    expect(within(drawer).getByRole('button', { name: 'From a secret manager' })).toHaveAttribute('aria-pressed', 'true')
  })

  it('a non-admin sees the catalog read-only and the admin-only list is never called', async () => {
    const api = fakeApi()
    renderPage({ id: 'arjun', name: 'Arjun', layers: [] }, api)
    const catalog = await screen.findByRole('region', { name: 'Catalog' })
    expect(api.list).not.toHaveBeenCalled()
    expect(screen.queryByRole('region', { name: 'Setup progress' })).not.toBeInTheDocument()
    fireEvent.click(within(catalog).getByRole('button', { name: 'Details PostgreSQL' }))
    const drawer = await screen.findByRole('dialog')
    expect(within(drawer).getByText(/Only an admin can connect or change integrations/)).toBeInTheDocument()
    expect(within(drawer).queryByRole('button', { name: /Save/ })).not.toBeInTheDocument()
  })
})
