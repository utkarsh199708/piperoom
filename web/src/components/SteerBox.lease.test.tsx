import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import { SteerBox } from './SteerBox'
import type { RoomActions } from '../hooks/useRoom'
import type { CurrentUser } from '../state/permissions'
import { emptyRoomState } from '../state/types'

const grantControl = vi.fn()
const actions = { grantControl } as unknown as RoomActions
const arjun: CurrentUser = { id: 'arjun', name: 'Arjun', layers: ['kafka'] }
const kabir: CurrentUser = { id: 'kabir', name: 'Kabir', layers: ['consumer'] }

// A lease with `min` minutes left, plus 20 s so the floored minute count is `min`.
const lease = (min: number) => new Date(Date.now() + min * 60_000 + 20_000).toISOString()
const room = (min: number, extra = {}) => ({
  ...emptyRoomState('r1'),
  driver: { id: 'arjun', name: 'Arjun', lease_until: lease(min), ...extra },
})

const status = () => document.querySelector('[data-slot="steer-status"]')

describe('SteerBox lease awareness (F9)', () => {
  it('draws a bar as wide as the lease left of 30 minutes', () => {
    render(<SteerBox state={room(15)} user={arjun} actions={actions} />)
    expect(screen.getByTestId('lease-bar')).toHaveStyle({ width: '50%' })
    expect(screen.getByText(/You are driving · lease 15m/)).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Renew' })).not.toBeInTheDocument()
  })

  it('turns amber at 5 minutes and Renew takes the driver role for yourself', () => {
    render(<SteerBox state={room(4)} user={arjun} actions={actions} />)
    const line = screen.getByText('Lease ends in 4m — any action renews').closest('[data-slot="steer-status"]')!
    expect(line.className).toMatch(/text-warning-foreground/)
    expect(line).toHaveAttribute('aria-live', 'polite')
    fireEvent.click(screen.getByRole('button', { name: 'Renew' }))
    expect(grantControl).toHaveBeenCalledWith('arjun')
  })

  it('says the lease expired, for the driver and for everyone else', () => {
    const { unmount } = render(<SteerBox state={room(20, { lease_expired: true })} user={arjun} actions={actions} />)
    expect(screen.getByText('Your lease expired — you still drive until someone takes over')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Renew' })).toBeInTheDocument()
    unmount()
    render(<SteerBox state={room(20, { lease_expired: true })} user={kabir} actions={actions} />)
    expect(status()).toHaveTextContent(/^Arjun.*'s lease expired · anyone can take control$/)
    expect(screen.queryByRole('button', { name: 'Renew' })).not.toBeInTheDocument()
    expect(screen.queryByTestId('lease-bar')).not.toBeInTheDocument()
  })

  it('in the last minute of a live lease says so, instead of the plain lease text', () => {
    const state = { ...emptyRoomState('r1'), driver: { id: 'arjun', name: 'Arjun', lease_until: new Date(Date.now() + 30_000).toISOString() } }
    render(<SteerBox state={state} user={arjun} actions={actions} />)
    expect(screen.getByText('Lease ends in under a minute — any action renews')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Renew' })).toBeInTheDocument()
  })

  it('non-drivers keep "lease 24m left"', () => {
    render(<SteerBox state={room(24)} user={kabir} actions={actions} />)
    expect(status()).toHaveTextContent(/^Arjun.* is driving · lease 24m left/)
  })
})
