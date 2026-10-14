import { describe, expect, it, vi } from 'vitest'
import { act, render, screen } from '@testing-library/react'
import { MemoryRouter, Route, Routes, useNavigate } from 'react-router-dom'
import { useEffect } from 'react'
import { RoomScreenRoute } from './App'

let mounts = 0
vi.mock('./components/RoomScreen', () => ({
  RoomScreen: () => {
    useEffect(() => {
      mounts += 1
    }, [])
    return <p>room screen</p>
  },
}))

let go: (to: string) => void = () => undefined
function Nav() {
  go = useNavigate()
  return null
}

describe('RoomScreenRoute', () => {
  it('remounts the room screen when the room id changes, so dialogs do not leak', () => {
    mounts = 0
    render(
      <MemoryRouter initialEntries={['/rooms/a']}>
        <Nav />
        <Routes>
          <Route path="/rooms/:roomId" element={<RoomScreenRoute />} />
        </Routes>
      </MemoryRouter>,
    )
    expect(screen.getByText('room screen')).toBeInTheDocument()
    expect(mounts).toBe(1)
    act(() => go('/rooms/b'))
    expect(mounts).toBe(2)
  })
})
