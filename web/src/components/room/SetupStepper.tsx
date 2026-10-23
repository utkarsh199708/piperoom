// The setup stepper (U1-12a): every plan step and where the run is, above the feed. On a phone
// it is a bottom sheet opened from one button, so the feed keeps its height; the sheet is a
// Radix dialog (focus trapped, Escape closes, focus returns to the trigger).
import { useRef, useState } from 'react'
import { ArrowUp, Check, Circle, Loader2, TriangleAlert } from 'lucide-react'
import type { RoomState, SetupState } from '@/state/types'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '@/components/ui/dialog'
import { Progress } from '@/components/ui/progress'
import { useMediaQuery } from '@/hooks/useMediaQuery'
import { SETUP_STEPPER_ID, runnerLabel, setupStepViews, stepLabel } from '@/lib/setup'
import type { SetupStepStatus, SetupStepView } from '@/lib/setup'
import { cn } from '@/lib/utils'

const STATUS_TEXT: Record<SetupStepStatus, string> = {
  done: 'done',
  failed: 'failed',
  running: 'running',
  awaiting: 'needs approval',
  blocked: 'blocked',
  pending: 'pending',
}

function StepIcon({ status }: { status: SetupStepStatus }) {
  switch (status) {
    case 'done':
      return <Check className="size-4 text-success" aria-hidden="true" />
    case 'failed':
      return <TriangleAlert className="size-4 text-destructive-fg" aria-hidden="true" />
    case 'blocked':
      return <TriangleAlert className="size-4 text-warning-foreground" aria-hidden="true" />
    case 'running':
      return <Loader2 className="size-4 animate-spin text-agent" aria-hidden="true" />
    case 'awaiting':
      return <Circle className="size-4 fill-warning text-warning-foreground" aria-hidden="true" />
    default:
      return <Circle className="size-4 text-subtle-foreground" aria-hidden="true" />
  }
}

function StepRow({ step, current }: { step: SetupStepView; current: boolean }) {
  return (
    <li
      aria-current={current ? 'step' : undefined}
      className={cn(
        'flex min-w-0 items-start gap-2 rounded-md px-2 py-1.5',
        current && 'bg-muted',
        step.status === 'awaiting' && 'bg-warning-subtle',
      )}
    >
      <span className="mt-0.5 shrink-0">
        <StepIcon status={step.status} />
      </span>
      <span className="flex min-w-0 flex-1 flex-col gap-0.5">
        <span className="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-0.5">
          <span className="font-mono text-xs tabular-nums text-subtle-foreground">Step {step.number}</span>
          <code className="min-w-0 truncate font-mono text-xs text-foreground">{step.op}</code>
          <Badge variant={step.status === 'awaiting' ? 'warning' : 'outline'} mono>
            {step.tier}
          </Badge>
          <span className="text-xs text-muted-foreground">{STATUS_TEXT[step.status]}</span>
        </span>
        <span className="min-w-0 truncate text-xs text-subtle-foreground">
          {step.phase} · {step.target}
          {step.layer ? ` · ${step.layer}` : ''}
        </span>
      </span>
    </li>
  )
}

function StepList({ setup, steps, currentIndex }: { setup: SetupState; steps: SetupStepView[]; currentIndex: number }) {
  if (steps.length === 0) {
    return (
      <p className="m-0 text-sm text-muted-foreground">
        {setup.plan_id ? 'The plan has no steps yet.' : 'The agent has not written the plan yet.'}
      </p>
    )
  }
  return (
    <ol aria-label="Setup steps" className="m-0 flex list-none flex-col gap-0.5 p-0">
      {steps.map((step) => (
        <StepRow key={step.index} step={step} current={step.index === currentIndex} />
      ))}
    </ol>
  )
}

function RunnerStatus({ setup }: { setup: SetupState }) {
  const connected = setup.runner?.connected === true
  return (
    <span className="flex min-w-0 items-center gap-2">
      <Badge variant={setup.runner ? (connected ? 'success' : 'warning') : 'neutral'}>{runnerLabel(setup)}</Badge>
      {setup.runner && !connected && <span className="text-xs text-warning-foreground">reconnect it to continue</span>}
    </span>
  )
}

/** One-line summary for the phone trigger. */
function Summary({ setup, steps }: { setup: SetupState; steps: SetupStepView[] }) {
  const index = setup.blocked?.step_index ?? setup.step_index
  const current = steps.find((s) => s.index === index) ?? null
  const done = steps.filter((s) => s.status === 'done').length
  return (
    <span className="flex min-w-0 flex-1 flex-col items-start gap-0.5">
      <span className="text-sm font-medium">{stepLabel(current, current?.number ?? 0, current?.total ?? steps.length)}</span>
      <span className="text-xs text-muted-foreground">
        {steps.length > 0 ? `${done} of ${steps.length} steps done` : 'No plan yet'}
      </span>
    </span>
  )
}

/**
 * The stepper above the feed. On phones it renders one trigger and the same list in a bottom
 * sheet; the trigger carries SETUP_STEPPER_ID so the decision bar's "View steps" opens it.
 */
export function SetupStepper({ state }: { state: RoomState }) {
  const phone = !useMediaQuery('(min-width: 768px)')
  const [open, setOpen] = useState(false)
  const triggerRef = useRef<HTMLButtonElement>(null)
  const setup = state.setup
  if (!setup) return null
  const steps = setupStepViews(state)
  const currentIndex = setup.blocked?.step_index ?? setup.step_index
  const current = steps.find((s) => s.index === currentIndex) ?? null
  const progress = steps.length > 0 ? Math.round(((current?.number ?? steps.length) / steps.length) * 100) : 0

  if (phone) {
    return (
      <>
        <Button
          id={SETUP_STEPPER_ID}
          ref={triggerRef}
          variant="outline"
          className="mx-4 my-2 h-auto min-h-11 justify-start gap-3 px-3 py-2 text-left sm:mx-6"
          aria-haspopup="dialog"
          aria-expanded={open}
          onClick={() => setOpen(true)}
        >
          <Summary setup={setup} steps={steps} />
          <ArrowUp className="size-4 shrink-0 text-muted-foreground" aria-hidden="true" />
          <span className="sr-only">Open setup steps</span>
        </Button>
        <Dialog open={open} onOpenChange={setOpen}>
          <DialogContent
            className="fixed inset-x-0 bottom-0 left-0 top-auto w-full max-w-none translate-x-0 translate-y-0 rounded-b-none rounded-t-xl p-4 sm:p-6"
            aria-describedby="setup-sheet-desc"
            onCloseAutoFocus={(e) => {
              // A controlled dialog has no trigger to return to; put focus back on the button.
              e.preventDefault()
              triggerRef.current?.focus()
            }}
          >
            <DialogHeader>
              <DialogTitle>Setup steps</DialogTitle>
              <DialogDescription id="setup-sheet-desc" className="flex flex-wrap items-center gap-2">
                <RunnerStatus setup={setup} />
                {steps.length > 0 && (
                  <span>
                    {current?.number ?? 0} of {steps.length}
                  </span>
                )}
              </DialogDescription>
            </DialogHeader>
            <div className="max-h-[60vh] overflow-y-auto">
              <StepList setup={setup} steps={steps} currentIndex={currentIndex} />
            </div>
          </DialogContent>
        </Dialog>
      </>
    )
  }

  return (
    <Card
      id={SETUP_STEPPER_ID}
      tabIndex={-1}
      role="region"
      aria-labelledby="setup-stepper-title"
      className="mx-4 mt-2 flex min-w-0 flex-col gap-2 p-3 sm:mx-6"
    >
      <div className="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1">
        <h2 id="setup-stepper-title" className="m-0 text-sm font-semibold">
          Setup
        </h2>
        <RunnerStatus setup={setup} />
        <span className="ml-auto text-xs text-muted-foreground">
          {steps.length > 0 ? `${current?.number ?? 0} of ${steps.length} steps` : 'No plan yet'}
        </span>
      </div>
      <Progress value={progress} aria-label="Setup progress" className="h-1" />
      <StepList setup={setup} steps={steps} currentIndex={currentIndex} />
    </Card>
  )
}
