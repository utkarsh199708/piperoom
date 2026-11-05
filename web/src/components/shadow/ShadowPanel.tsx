import { useState } from 'react'
import type { ShadowGradedPayload } from '@/api/types'
import { useShadowRun } from '@/hooks/useShadowRun'
import { isViewer } from '@/state/permissions'
import type { CurrentUser } from '@/state/permissions'
import type { RoomState } from '@/state/types'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { BlindLabelForm } from './BlindLabelForm'
import { GradeForm } from './GradeForm'
import { RunReveal } from './RunReveal'

// The closed-room panel of a shadow room: blind label -> the agent's answer -> grade.
// The order is the server's (docs/design/shadow.md 3.5); this component only follows it.
// A viewer sees the same screens without the forms, since every write is 403 viewer.
export function ShadowPanel({ state, user }: { state: RoomState; user: CurrentUser }) {
  const [withData, setWithData] = useState(false)
  const { view, reload } = useShadowRun(state.id, true, withData, {
    revealed: state.shadow_revealed,
    labelled: state.shadow_labelled,
  })
  const canWrite = !isViewer(user)

  if (view.phase === 'loading') {
    return (
      <Card role="status" aria-label="Loading the shadow run" className="m-4 flex flex-col gap-2.5 p-4 sm:mx-6">
        <span className="sr-only">Loading the shadow run…</span>
        <Skeleton className="h-4 w-40" />
        <Skeleton className="h-3 w-full" />
        <Skeleton className="h-3 w-2/3" />
      </Card>
    )
  }
  if (view.phase === 'error') {
    return (
      <Card className="m-4 flex flex-col gap-2 p-4 sm:mx-6" role="alert">
        <p className="m-0 text-sm font-medium text-destructive-fg">{view.message}</p>
        <div>
          <Button variant="secondary" size="sm" onClick={reload}>
            Try again
          </Button>
        </div>
      </Card>
    )
  }
  if (view.phase === 'blind') {
    return (
      <div className="px-4 py-4 sm:px-6">
        <BlindLabelForm roomId={state.id} resolution={state.resolution} canWrite={canWrite} onDone={reload} />
      </div>
    )
  }

  const { data } = view
  const current = data.room?.graded?.payload as ShadowGradedPayload | undefined
  const humanMttrS =
    state.opened_at && state.closed_at ? Math.round((Date.parse(state.closed_at) - Date.parse(state.opened_at)) / 1000) : -1
  return (
    <div className="flex flex-col gap-4 px-4 py-4 sm:px-6">
      <RunReveal
        data={data}
        openedAt={state.opened_at}
        humanMttrS={humanMttrS}
        showData={withData}
        onToggleData={() => setWithData((v) => !v)}
      />
      <GradeForm
        roomId={state.id}
        current={current}
        // Only the fetched run decides: the live event can arrive before the refetch, and grading against
        // an answer the grader has not seen would corrupt the metric.
        revealed={data.run.revealed_at !== null}
        canWrite={canWrite}
        onDone={reload}
      />
    </div>
  )
}
