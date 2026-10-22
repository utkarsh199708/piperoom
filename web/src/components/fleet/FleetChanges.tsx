import { Link } from 'react-router-dom'
import { AlertTriangle, ExternalLink } from 'lucide-react'
import type { PipelineChange } from '../../api/types'
import { useFleetChanges } from '../../hooks/useFleet'
import { changeLayer } from '../../lib/f8'
import { changeLink, laneHref } from '../../lib/fleet'
import { timeAgo, utcLabel } from '../../lib/time'
import { useNow } from '../room/useNow'
import { Badge, layerVariant } from '../ui/badge'
import { Button } from '../ui/button'
import { Card } from '../ui/card'
import { Skeleton } from '../ui/skeleton'

// "What changed this week" (X5b): GET /api/fleet/changes across every pipeline the caller can see, newest first,
// one page at a time. Summaries, objects and actors may come from outside systems (X4 deploys, migrations, vendor
// incidents): text only, and only an http(s) `after.url` becomes a link.
export function FleetChanges({ enabled }: { enabled: boolean }) {
  const page = useFleetChanges(enabled)
  const now = useNow(60_000)

  if (page.status === 'idle' || page.status === 'loading') {
    return (
      <Card role="status" aria-label="Loading changes" className="flex flex-col gap-3 p-4">
        <Skeleton className="h-4 w-2/3" />
        <Skeleton className="h-4 w-1/2" />
        <Skeleton className="h-4 w-3/5" />
      </Card>
    )
  }
  if (page.status === 'error') {
    return (
      <Card role="alert" className="flex flex-col items-center gap-3 px-6 py-10 text-center">
        <AlertTriangle className="size-5 text-destructive-fg" aria-hidden="true" />
        <p className="text-base font-medium">Could not load what changed</p>
        <p className="max-w-[52ch] text-muted-foreground">{page.error}</p>
        <Button variant="outline" onClick={page.reload}>
          Retry
        </Button>
      </Card>
    )
  }
  if (page.changes.length === 0) {
    return (
      <Card className="px-6 py-10 text-center text-sm text-muted-foreground">
        Nothing changed in the last 7 days on the pipelines you can see.
      </Card>
    )
  }
  return (
    <Card className="flex flex-col">
      <ul className="m-0 flex list-none flex-col p-0" aria-label="Changes this week">
        {page.changes.map((c) => (
          <ChangeItem key={`${c.id}|${c.pipeline_id}|${c.fingerprint}`} change={c} now={now} />
        ))}
      </ul>
      <div className="flex flex-wrap items-center gap-3 border-t border-border px-3 py-2 text-xs text-subtle-foreground">
        <span aria-live="polite">
          {page.changes.length} {page.changes.length === 1 ? 'change' : 'changes'}
          {page.next ? ' so far' : ''}
        </span>
        {page.error && (
          <span role="alert" className="text-warning-foreground">
            Could not load more: {page.error}
          </span>
        )}
        {page.next && (
          <Button variant="outline" size="sm" className="ml-auto h-7" disabled={page.loadingMore} onClick={page.loadMore}>
            {page.loadingMore ? 'Loading…' : 'Load more'}
          </Button>
        )}
      </div>
    </Card>
  )
}

function ChangeItem({ change, now }: { change: PipelineChange; now: number }) {
  const href = changeLink(change)
  return (
    <li className="flex min-w-0 flex-col gap-1 border-b border-border px-3 py-2.5 last:border-b-0">
      <div className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1">
        <Badge variant={layerVariant(changeLayer(change.kind))} mono>
          {change.kind}
        </Badge>
        {change.pipeline_id && (
          <Link
            to={laneHref(change.pipeline_id)}
            className="rounded-sm font-mono text-xs text-foreground hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
          >
            {change.pipeline_id}
          </Link>
        )}
        <span className="min-w-0 break-all font-mono text-xs text-muted-foreground">{change.object}</span>
        <time dateTime={change.at} title={utcLabel(change.at)} className="ml-auto text-xs text-subtle-foreground">
          {timeAgo(change.at, now)}
        </time>
      </div>
      {(change.summary || change.actor) && (
        <p className="m-0 break-words text-xs text-muted-foreground">
          {change.actor && <span className="font-medium">{change.actor}: </span>}
          {change.summary}
        </p>
      )}
      {href && (
        <a
          href={href}
          target="_blank"
          rel="noopener noreferrer nofollow"
          className="inline-flex w-fit items-center gap-1 rounded-sm text-xs text-muted-foreground hover:text-foreground hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
        >
          <ExternalLink className="size-3" aria-hidden="true" />
          Open the source
        </a>
      )}
    </li>
  )
}
