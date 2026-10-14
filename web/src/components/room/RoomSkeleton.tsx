import { Card } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { PipelineMap } from '../PipelineMap'
import { StatusDot } from './StatusDot'

// Room-Loading.dc.html: the room's own layout drawn in grey while the first snapshot
// loads. The shimmer only runs when motion is allowed (Skeleton). aria-busy plus one
// polite "Loading room…" line tell a screen reader what is going on.
export function RoomSkeleton({ pipelineId }: { pipelineId?: string }) {
  return (
    <div aria-busy="true" className="flex min-h-full flex-col xl:flex-row">
      <div className="flex min-w-0 flex-1 flex-col gap-4 px-4 pb-6 pt-4 sm:px-6">
        <p role="status" className="sr-only">
          Loading room…
        </p>

        <div className="flex flex-col gap-2.5">
          <div className="flex flex-wrap items-center gap-2.5">
            <Skeleton className="h-[22px] w-80 max-w-full" />
            <Skeleton className="h-5 w-12" />
            <Skeleton className="h-5 w-24" />
            <span
              aria-live="polite"
              className="ml-auto inline-flex h-6 items-center gap-1.5 rounded-full border border-border bg-card px-2 text-xs font-medium text-muted-foreground"
            >
              <StatusDot />
              connecting…
            </span>
          </div>
          <Skeleton className="h-3 w-[420px] max-w-full" />
          <div className="flex items-center gap-2.5">
            <Skeleton className="h-6 w-[84px] rounded-full" />
            <Skeleton className="h-3 w-[120px]" />
          </div>
        </div>

        <PipelineMap topology={null} pipelineId={pipelineId} />

        <Card className="flex flex-col gap-2.5 px-3.5 py-3">
          <div className="flex items-center gap-2">
            <Skeleton className="size-5 rounded-full" />
            <Skeleton className="h-3 w-20" />
          </div>
          <Skeleton className="h-3.5 w-[70%]" />
          <Skeleton className="h-3.5 w-[40%]" />
        </Card>

        <div className="flex items-center gap-2">
          <span className="text-sm font-medium">Investigation</span>
          <span className="text-xs text-subtle-foreground">Loading room…</span>
        </div>
        <div className="flex flex-col gap-2.5">
          {[
            ['w-64', 'w-32'],
            ['w-72', 'w-40'],
          ].map(([title, tool], i) => (
            <div key={i} className="flex items-center gap-2.5 px-2">
              <Skeleton className="size-[22px] rounded-full" />
              <Skeleton className={`h-3.5 max-w-[40%] ${title}`} />
              <Skeleton className={`hidden h-4 rounded-md sm:block ${tool}`} />
              <Skeleton className="ml-auto h-3 w-12" />
            </div>
          ))}
        </div>
      </div>

      <aside className="w-full shrink-0 border-t border-border xl:w-[300px] xl:border-l xl:border-t-0">
        <div className="flex flex-col gap-2.5 px-4 py-3.5">
          <h2 className="m-0 text-sm font-medium">Proposals</h2>
          <Skeleton className="h-3 w-4/5" />
        </div>
        <div className="h-px bg-border" />
        <div className="flex flex-col gap-2 px-4 py-3.5">
          <h2 className="m-0 text-sm font-medium">Who&rsquo;s here</h2>
          {[['w-16'], ['w-20'], ['w-14']].map(([w], i) => (
            <div key={i} className="flex min-h-8 items-center gap-2">
              <Skeleton className="size-6 rounded-full" />
              <Skeleton className={`h-3 ${w}`} />
            </div>
          ))}
        </div>
        <div className="h-px bg-border" />
        <div className="flex flex-col gap-2.5 px-4 py-3.5">
          <h2 className="m-0 text-sm font-medium">Activity</h2>
          <Skeleton className="h-3 w-[90%]" />
          <Skeleton className="h-3 w-[70%]" />
          <Skeleton className="h-3 w-4/5" />
        </div>
      </aside>
    </div>
  )
}
