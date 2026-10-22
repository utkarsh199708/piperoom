// X3 trend marks drawn with inline SVG (no chart dependency): the lane's sparkline, the forecast ETA badge and
// the forecast room's chart. Numbers only reach the SVG through lib/trends (finite values, parsed times), and
// every text is a React text node, never markup.
import { useState } from 'react'
import type { PointerEvent } from 'react'
import type { ForecastSummary, TrendPoint } from '../../api/types'
import { cn } from '@/lib/utils'
import { clockTime, utcLabel } from '../../lib/time'
import {
  etaBadge,
  formatMetricValue,
  isWatchMetric,
  metricLabel,
  sparklinePath,
  sparklineXY,
  sparklineY,
} from '../../lib/trends'
import type { TrendsState } from '../../hooks/useTrends'

/** A small line of the series; nothing for an empty series. `label` is its accessible name. */
export function Sparkline({
  points,
  label,
  width = 64,
  height = 16,
  className,
}: {
  points: TrendPoint[]
  label: string
  width?: number
  height?: number
  className?: string
}) {
  if (points.length === 0) return null
  const xy = sparklineXY(points, width, height)
  const [lx, ly] = xy[xy.length - 1]
  return (
    <svg
      data-sparkline=""
      role="img"
      aria-label={label}
      width={width}
      height={height}
      viewBox={`0 0 ${width} ${height}`}
      className={cn('shrink-0 overflow-visible text-muted-foreground', className)}
    >
      <path
        d={sparklinePath(points, width, height)}
        fill="none"
        stroke="currentColor"
        strokeWidth={1.5}
        strokeLinejoin="round"
        strokeLinecap="round"
      />
      {/* The newest sample, so a one-point series still shows and the eye finds "now". */}
      <circle cx={lx} cy={ly} r={1.75} fill="currentColor" />
    </svg>
  )
}

/** "fills in 3 d" / "data loss in 18 h"; nothing beyond a week or for an unknown metric. */
export function EtaBadge({ forecast, now, className }: { forecast: ForecastSummary; now: number; className?: string }) {
  const badge = etaBadge(forecast, now)
  if (!badge) return null
  return (
    <span
      data-eta=""
      title={badge.long}
      className={cn(
        'whitespace-nowrap rounded-sm px-1 text-[11px] font-medium leading-4',
        badge.due ? 'bg-destructive-subtle text-destructive-fg' : 'bg-warning-subtle text-warning-foreground',
        className,
      )}
    >
      <span aria-hidden="true">{badge.short}</span>
      <span className="sr-only">{badge.long}</span>
    </span>
  )
}

const CHART_W = 600
const CHART_H = 96

/**
 * The top of a forecast room (room.created.origin "forecast"): the forecast metric's last 24 h against its
 * limit, with the ETA badge and the fit. The forecast's own numbers show even when the trend cannot be read.
 */
export function ForecastChart({ forecast, trends, now }: { forecast: ForecastSummary; trends: TrendsState; now: number }) {
  const [hover, setHover] = useState<number | null>(null)
  if (!isWatchMetric(forecast.metric)) return null
  const metric = forecast.metric
  const points = trends.series[metric] ?? []
  const xy = sparklineXY(points, CHART_W, CHART_H, forecast.limit)
  const limitY = sparklineY(points, CHART_H, forecast.limit, forecast.limit)
  const fmt = (v: number) => formatMetricValue(metric, v)
  const shown = hover !== null && hover < points.length ? hover : null

  const onMove = (e: PointerEvent<SVGSVGElement>) => {
    const box = e.currentTarget.getBoundingClientRect()
    if (box.width <= 0 || xy.length === 0) return
    const x = ((e.clientX - box.left) / box.width) * CHART_W
    let best = 0
    for (let i = 1; i < xy.length; i++) if (Math.abs(xy[i][0] - x) < Math.abs(xy[best][0] - x)) best = i
    setHover(best)
  }

  return (
    <section
      data-forecast-chart=""
      aria-labelledby="forecast-heading"
      className="flex min-w-0 flex-col gap-2 rounded-lg border border-border bg-card px-4 py-3"
    >
      <div className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1">
        <h2 id="forecast-heading" className="m-0 text-sm font-medium">
          Forecast · {metricLabel(metric)}
        </h2>
        {forecast.object && (
          <span className="min-w-0 truncate font-mono text-xs text-muted-foreground" title={forecast.object}>
            {forecast.object}
          </span>
        )}
        <EtaBadge forecast={forecast} now={now} className="ml-auto" />
      </div>
      {points.length > 0 ? (
        <svg
          role="img"
          aria-label={`${metricLabel(metric)}, last 24 h: now ${fmt(points[points.length - 1].value)}, limit ${fmt(forecast.limit)}`}
          viewBox={`0 0 ${CHART_W} ${CHART_H}`}
          preserveAspectRatio="none"
          className="h-24 w-full overflow-visible text-primary"
          onPointerMove={onMove}
          onPointerLeave={() => setHover(null)}
        >
          <line
            x1={0}
            x2={CHART_W}
            y1={limitY}
            y2={limitY}
            className="text-destructive"
            stroke="currentColor"
            strokeWidth={1}
            strokeDasharray="4 4"
            vectorEffect="non-scaling-stroke"
          />
          <path
            d={sparklinePath(points, CHART_W, CHART_H, forecast.limit)}
            fill="none"
            stroke="currentColor"
            strokeWidth={2}
            strokeLinejoin="round"
            strokeLinecap="round"
            vectorEffect="non-scaling-stroke"
          />
          {shown !== null && (
            <line
              x1={xy[shown][0]}
              x2={xy[shown][0]}
              y1={0}
              y2={CHART_H}
              className="text-border"
              stroke="currentColor"
              strokeWidth={1}
              vectorEffect="non-scaling-stroke"
            />
          )}
        </svg>
      ) : (
        <p className="m-0 text-xs text-muted-foreground">
          {trends.status === 'loading'
            ? 'Loading the last 24 h…'
            : trends.status === 'ready' && trends.series[metric]
              ? 'No trend samples to draw for the last 24 h.'
              : 'The trend is not available.'}
        </p>
      )}
      <p className="m-0 flex flex-wrap gap-x-3 text-xs tabular-nums text-muted-foreground">
        {shown !== null ? (
          <span title={utcLabel(points[shown].at)}>
            {clockTime(points[shown].at)}: {fmt(points[shown].value)}
          </span>
        ) : (
          <span>now {fmt(forecast.current)}</span>
        )}
        <span>
          limit <span className="text-destructive-fg">{fmt(forecast.limit)}</span>
        </span>
        <span>+{fmt(forecast.slope_per_hour)} per hour</span>
        <span title="How well a straight line fits the last 24 h (1 is a perfect fit)">
          fit r² {Number.isFinite(forecast.r2) ? forecast.r2.toFixed(2) : '–'}
        </span>
      </p>
    </section>
  )
}
