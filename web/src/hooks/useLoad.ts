import { useCallback, useEffect, useState } from 'react'
import { errorText } from '../lib/errors'

export type LoadState<T> =
  | { status: 'loading' }
  | { status: 'ready'; data: T }
  | { status: 'error'; message: string }

/**
 * Load a list once (and again on `reload`). The error state is real: callers show it with a retry
 * instead of falling back to made-up data. `load` must be stable (module-level or memoised).
 */
export function useLoad<T>(load: () => Promise<T>): LoadState<T> & { reload: () => void } {
  const [state, setState] = useState<LoadState<T>>({ status: 'loading' })
  const [tick, setTick] = useState(0)

  useEffect(() => {
    let cancelled = false
    setState({ status: 'loading' })
    load().then(
      (data) => {
        if (!cancelled) setState({ status: 'ready', data })
      },
      (e: unknown) => {
        if (!cancelled) setState({ status: 'error', message: errorText(e) })
      },
    )
    return () => {
      cancelled = true
    }
  }, [load, tick])

  const reload = useCallback(() => setTick((n) => n + 1), [])
  return { ...state, reload }
}
