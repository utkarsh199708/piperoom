import { useEffect, useState } from 'react'
import { ApiClientError, api, isFixtures } from '../api/client'
import type { TrendPoint, WatchMetric } from '../api/types'
import { WATCH_METRICS } from '../api/types'
import { useCurrentUser } from '../context/user'
import { cleanSeries } from '../lib/trends'

/** The watcher writes at most one sample per 15 min, so polling faster only repeats the same answer. */
export const TRENDS_REFRESH_MS = 15 * 60_000

export type TrendSeries = Partial<Record<WatchMetric, TrendPoint[]>>

export interface TrendsState {
  /** `loading` until the first answer for this pipeline; `idle` when nothing is fetched (disabled, no id). */
  status: 'idle' | 'loading' | 'ready'
  /** Metric -> its last 24 h. A metric whose request failed is absent; one with no samples is []. */
  series: TrendSeries
}

const NONE: TrendsState = { status: 'idle', series: {} }

/**
 * The last 24 h of every watcher metric of a pipeline (GET /api/pipelines/{pid}/trends, X3), one request per
 * metric, refreshed every 15 min while `enabled`. Trends are decoration: any failure only leaves the metric out,
 * never an error or a toast. A 404 (unknown pipeline, or one outside the caller's tenants) drops every metric and
 * stops the refresh for this pipeline, like the topology's 404.
 */
export function useTrends(pipelineId: string, enabled = true): TrendsState {
  const { user, sessionEpoch } = useCurrentUser()
  const [state, setState] = useState<TrendsState>(NONE)
  const [stateFor, setStateFor] = useState('')

  const active = enabled && pipelineId !== '' && !isFixtures()
  const key = `${pipelineId}|${user.id}|${sessionEpoch}|${active}`
  if (stateFor !== key) {
    // A new pipeline or person never shows the previous one's lines.
    setStateFor(key)
    setState(active ? { status: 'loading', series: {} } : NONE)
  }

  useEffect(() => {
    if (!active) return
    let cancelled = false
    let inFlight = false
    let timer = 0
    const load = async () => {
      if (inFlight || cancelled) return
      inFlight = true
      try {
        let gone = false
        const answers = await Promise.all(
          WATCH_METRICS.map(async (metric): Promise<[WatchMetric, TrendPoint[]] | null> => {
            try {
              return [metric, cleanSeries(await api.trends(pipelineId, metric))]
            } catch (e) {
              if (e instanceof ApiClientError && e.status === 404) gone = true
              return null
            }
          }),
        )
        if (cancelled) return
        if (gone) {
          window.clearInterval(timer)
          setState({ status: 'ready', series: {} })
          return
        }
        const series: TrendSeries = {}
        for (const a of answers) if (a) series[a[0]] = a[1]
        setState({ status: 'ready', series })
      } finally {
        inFlight = false
      }
    }
    void load()
    timer = window.setInterval(() => void load(), TRENDS_REFRESH_MS)
    return () => {
      cancelled = true
      window.clearInterval(timer)
    }
  }, [active, pipelineId, key])

  return active ? state : NONE
}
