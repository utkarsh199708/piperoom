import { useCallback, useEffect, useState } from 'react'
import { api } from '../api/client'
import type { AdminConfig } from '../api/types'
import { errorText } from '../lib/errors'
import { isForbidden } from '../lib/settings'

export type AdminConfigView =
  | { phase: 'loading' }
  | { phase: 'ready'; config: AdminConfig }
  | { phase: 'forbidden' }
  | { phase: 'error'; message: string }

/** GET /api/admin/config once (when `enabled`), with a retry. A 403 is its own phase: the person is not an admin. */
export function useAdminConfig(enabled: boolean): { view: AdminConfigView; reload: () => void } {
  const [view, setView] = useState<AdminConfigView>({ phase: 'loading' })
  const [attempt, setAttempt] = useState(0)
  useEffect(() => {
    if (!enabled) return
    let cancelled = false
    setView({ phase: 'loading' })
    api
      .adminConfig()
      .then((config) => !cancelled && setView({ phase: 'ready', config }))
      .catch((e: unknown) => {
        if (cancelled) return
        setView(isForbidden(e) ? { phase: 'forbidden' } : { phase: 'error', message: errorText(e) })
      })
    return () => {
      cancelled = true
    }
  }, [enabled, attempt])
  const reload = useCallback(() => setAttempt((a) => a + 1), [])
  return { view, reload }
}
