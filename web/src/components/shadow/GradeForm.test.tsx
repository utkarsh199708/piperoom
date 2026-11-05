import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { api } from '../../api/client'
import type { Event } from '../../api/types'
import { GradeForm } from './GradeForm'

const graded = (): Event => ({
  room_id: 'r1',
  seq: 1,
  type: 'shadow.graded',
  actor: { kind: 'human', id: 'u', name: 'U' },
  payload: {},
  at: '2026-10-14T11:01:00Z',
})

beforeEach(() => {
  vi.restoreAllMocks()
})
afterEach(() => vi.restoreAllMocks())

describe('GradeForm', () => {
  it('sends one grade for a double click on Save grade', async () => {
    let release: (e: Event) => void = () => {}
    const grade = vi.spyOn(api, 'shadowGrade').mockReturnValue(new Promise<Event>((r) => (release = r)))
    const onDone = vi.fn()
    render(<GradeForm roomId="r1" revealed canWrite onDone={onDone} />)

    fireEvent.click(screen.getByRole('radio', { name: 'Yes' }))
    fireEvent.click(screen.getByRole('radio', { name: 'Partly' }))
    const save = screen.getByRole('button', { name: 'Save grade' })
    // Both clicks land before React re-renders with busy set.
    act(() => {
      fireEvent.click(save)
      fireEvent.click(save)
    })
    expect(grade).toHaveBeenCalledTimes(1)

    release(graded())
    await waitFor(() => expect(onDone).toHaveBeenCalledTimes(1))
  })

  it('sends one more grade with the new answers on a single Update grade click', async () => {
    const grade = vi.spyOn(api, 'shadowGrade').mockResolvedValue(graded())
    const onDone = vi.fn()
    render(
      <GradeForm
        roomId="r1"
        current={{ layer_correct: true, root_cause: 'partial' }}
        revealed
        canWrite
        onDone={onDone}
      />,
    )

    fireEvent.click(screen.getAllByRole('radio', { name: 'No' })[0])
    fireEvent.click(screen.getByRole('radio', { name: 'Yes, same cause' }))
    fireEvent.click(screen.getByRole('button', { name: 'Update grade' }))
    await waitFor(() => expect(onDone).toHaveBeenCalledTimes(1))
    expect(grade).toHaveBeenCalledTimes(1)
    expect(grade).toHaveBeenCalledWith('r1', { layer_correct: false, root_cause: 'yes' })

    // The guard is released after the save, so a later deliberate click goes through.
    fireEvent.click(screen.getByRole('button', { name: 'Update grade' }))
    await waitFor(() => expect(grade).toHaveBeenCalledTimes(2))
  })
})
