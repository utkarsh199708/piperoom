import { describe, expect, it, vi } from 'vitest'
import { act, renderHook, waitFor } from '@testing-library/react'
import { ApiClientError } from '../api/client'
import { useLoad } from './useLoad'

// A promise the test settles by hand.
function deferred<T>() {
  let resolve!: (v: T) => void
  let reject!: (e: unknown) => void
  const promise = new Promise<T>((res, rej) => {
    resolve = res
    reject = rej
  })
  return { promise, resolve, reject }
}

describe('useLoad', () => {
  it('goes loading -> ready with the data', async () => {
    const load = vi.fn().mockResolvedValue(['a'])
    const { result } = renderHook(() => useLoad(load))
    expect(result.current.status).toBe('loading')
    await waitFor(() => expect(result.current.status).toBe('ready'))
    expect(result.current).toMatchObject({ status: 'ready', data: ['a'] })
  })

  it('a rejection becomes { status: error, message } with the friendly text of an API error', async () => {
    const load = vi.fn().mockRejectedValue(new ApiClientError(404, 'not_found', 'not found'))
    const { result } = renderHook(() => useLoad(load))
    await waitFor(() => expect(result.current.status).toBe('error'))
    expect(result.current).toMatchObject({ status: 'error', message: 'not found' })
  })

  it('a plain Error keeps its message', async () => {
    // `load` must be stable (see useLoad): an inline arrow would re-run the effect on every render.
    const load = () => Promise.reject(new Error('network down'))
    const { result } = renderHook(() => useLoad(load))
    await waitFor(() => expect(result.current).toMatchObject({ status: 'error', message: 'network down' }))
  })

  it('reload() loads again: back to loading, then the new answer', async () => {
    const load = vi.fn().mockRejectedValueOnce(new Error('boom')).mockResolvedValueOnce(['ok'])
    const { result } = renderHook(() => useLoad(load))
    await waitFor(() => expect(result.current.status).toBe('error'))
    act(() => result.current.reload())
    expect(result.current.status).toBe('loading')
    await waitFor(() => expect(result.current).toMatchObject({ status: 'ready', data: ['ok'] }))
    expect(load).toHaveBeenCalledTimes(2)
  })

  it('an answer that arrives after unmount, or after a newer load started, is dropped', async () => {
    const first = deferred<string[]>()
    const second = deferred<string[]>()
    const load = vi.fn().mockReturnValueOnce(first.promise).mockReturnValueOnce(second.promise)
    const { result, unmount } = renderHook(() => useLoad(load))
    act(() => result.current.reload()) // a second load starts; the first is now stale
    await act(async () => {
      first.resolve(['stale'])
      await first.promise
    })
    expect(result.current.status).toBe('loading')
    await act(async () => {
      second.resolve(['fresh'])
      await second.promise
    })
    expect(result.current).toMatchObject({ status: 'ready', data: ['fresh'] })

    // After unmount nothing is set (no state update on an unmounted hook, no error).
    const late = deferred<string[]>()
    const errors = vi.spyOn(console, 'error').mockImplementation(() => undefined)
    const loadLate = () => late.promise
    const h = renderHook(() => useLoad(loadLate))
    h.unmount()
    unmount()
    await act(async () => {
      late.reject(new Error('too late'))
      await late.promise.catch(() => undefined)
    })
    expect(errors).not.toHaveBeenCalled()
    errors.mockRestore()
  })
})
