import { useEffect, useState } from 'react'
import { api } from '../api/client'
import type { ShadowMetrics } from '../api/types'
import { errorText } from '../lib/errors'

export type MetricsView =
  | { phase: 'loading' }
  | { phase: 'ready'; metrics: ShadowMetrics }
  | { phase: 'error'; message: string }

/** GET /api/shadow/metrics for a pipeline and a `since` date; refetches when either changes. */
export function useShadowMetrics(pipeline: string, since: string): MetricsView {
  const [view, setView] = useState<MetricsView>({ phase: 'loading' })
  useEffect(() => {
    let cancelled = false
    api
      .shadowMetrics({ pipeline: pipeline || undefined, since: since || undefined })
      .then((metrics) => !cancelled && setView({ phase: 'ready', metrics }))
      .catch((e: unknown) => !cancelled && setView({ phase: 'error', message: errorText(e) }))
    return () => {
      cancelled = true
    }
  }, [pipeline, since])
  return view
}
