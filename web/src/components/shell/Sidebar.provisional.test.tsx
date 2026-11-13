import { beforeEach, describe, expect, it, vi } from 'vitest'
import { render } from '@testing-library/react'
import { MemoryRouter } from 'react-router-dom'

vi.mock('@/hooks/useTopology', () => ({
  useTopology: vi.fn(() => ({ topology: null, error: null, updatedAt: null, notFound: false, retry: () => undefined })),
}))

vi.mock('./AccountMenu', () => ({ AccountMenu: () => null }))

import { useTopology } from '@/hooks/useTopology'
import { SidebarContent } from './Sidebar'

function show(pipelines: { id: string; open: number; provisional?: boolean }[]) {
  render(
    <MemoryRouter>
      <SidebarContent openRooms={1} pipelines={pipelines} navKey={0} onSearch={() => undefined} />
    </MemoryRouter>,
  )
}

beforeEach(() => vi.clearAllMocks())

// KAN-601: a setup room's pipeline is not registered, so its topology would 404.
describe('Sidebar pipeline rows', () => {
  it('asks for no topology of a provisional pipeline', () => {
    show([{ id: 'regsetup', open: 1, provisional: true }])
    expect(vi.mocked(useTopology).mock.calls.map((c) => c[0])).toEqual([''])
  })

  it('still asks for a registered pipeline with an open room', () => {
    show([{ id: 'orders', open: 1 }])
    expect(vi.mocked(useTopology).mock.calls.map((c) => c[0])).toEqual(['orders'])
  })
})
