// Q1-web-b (G2): the no-driver strip says whose owners were paged.
import { describe, expect, it, vi } from 'vitest'
import { render, screen } from '@testing-library/react'
import { NoDriverBanner } from './NoDriverBanner'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'

const actions = { grantControl: vi.fn() } as unknown as RoomActions
const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka'] }

const paged = (layer: string, seq: number) => ({
  seq,
  at: '2026-10-15T04:10:00Z',
  by: { kind: 'agent' as const, id: 'agent', name: 'Agent' },
  layer,
  owners: ['kabir'],
  reason: 'lag',
  auto: false,
})

describe('NoDriverBanner and owner pages', () => {
  it('says which layers were paged, once each, so the room does not look unnoticed', () => {
    const state = { ...emptyRoomState('r_webhook'), pages: [paged('consumer', 1), paged('warehouse', 2), paged('consumer', 3)] }
    render(<NoDriverBanner state={state} user={arjun} actions={actions} />)
    expect(screen.getByText('Owners paged: consumer, warehouse')).toBeInTheDocument()
  })

  it('says nothing about pages when there were none', () => {
    render(<NoDriverBanner state={emptyRoomState('r_webhook')} user={arjun} actions={actions} />)
    expect(screen.getByText(/No one is driving/)).toBeInTheDocument()
    expect(screen.queryByText(/Owners paged/)).not.toBeInTheDocument()
  })
})
