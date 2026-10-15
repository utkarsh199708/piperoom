import { useCallback, useEffect, useState } from 'react'
import { ApiClientError, api } from '../api/client'
import type { ShadowResponse } from '../api/types'
import { errorText } from '../lib/errors'

/**
 * What the shadow panel can show. `blind`: no label yet, the server (409 not_labelled)
 * refuses to say anything about the run, so the UI has nothing to leak either.
 */
export type ShadowView =
  | { phase: 'loading' }
  | { phase: 'blind' }
  | { phase: 'ready'; data: ShadowResponse }
  | { phase: 'error'; message: string }

/**
 * Loads GET /api/rooms/{id}/shadow for a closed shadow room. The call doubles as the
 * "is there a label?" probe. `withData` also asks for tool_result.data (a toggle in the UI).
 */
export function useShadowRun(
  roomId: string,
  enabled: boolean,
  withData: boolean,
  /** Live shadow.labelled / shadow.revealed flags: a change means the server's answer changed, so ask again. */
  live: { revealed?: boolean; labelled?: boolean } = {},
) {
  const [view, setView] = useState<ShadowView>({ phase: 'loading' })
  const [tick, setTick] = useState(0)

  useEffect(() => {
    if (!enabled) return
    let cancelled = false
    api
      .shadow(roomId, withData ? ['room', 'data'] : ['room'])
      .then((data) => !cancelled && setView({ phase: 'ready', data }))
      .catch((e: unknown) => {
        if (cancelled) return
        if (e instanceof ApiClientError && e.status === 409 && e.code === 'not_labelled') {
          setView({ phase: 'blind' })
        } else {
          setView({ phase: 'error', message: errorText(e) })
        }
      })
    return () => {
      cancelled = true
    }
  }, [roomId, enabled, withData, tick, live.revealed, live.labelled])

  const reload = useCallback(() => setTick((n) => n + 1), [])
  return { view, reload }
}
