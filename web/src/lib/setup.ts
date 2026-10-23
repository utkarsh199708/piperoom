// Setup rooms (U wave, docs/design/setup-agent.md): what the stepper draws and what the
// decision bar asks of this person. Pure, so every state is a test with a fake room and no
// server. The server's "setup" snapshot carries runner, plan id, phase and step index; the
// plan's phases and the executed steps come from the log (state/reducer.ts).
import type { ProposalState, RoomState, SetupState } from '../state/types'
import type { CurrentUser } from '../state/permissions'
import { canDecideLayer, isViewer } from '../state/permissions'
import { approveAllFor } from './approveAll'
import { waitingForWriteMode } from './writeMode'

/** The stepper's id: the decision bar's "View steps" scrolls to it (or opens its sheet). */
export const SETUP_STEPPER_ID = 'setup-stepper'
export const SETUP_PAIRING_ID = 'setup-pairing'

export type SetupStepStatus = 'done' | 'failed' | 'running' | 'awaiting' | 'blocked' | 'pending'

/** One step of the plan as the stepper shows it. */
export interface SetupStepView {
  index: number
  /** 1-based position in the whole plan, for "Step 2 of 7". */
  number: number
  total: number
  phase: string
  op: string
  target: string
  tier: string
  layer: string
  verify?: string
  status: SetupStepStatus
}

export type SetupStatus =
  | { kind: 'no_runner' }
  | { kind: 'runner_lost'; label: string }
  | { kind: 'paired'; label: string }
  | { kind: 'planning'; step: SetupStepView | null; number: number; total: number }
  | { kind: 'running'; step: SetupStepView | null; number: number; total: number; action: string }
  | {
      kind: 'awaiting_approval'
      step: SetupStepView | null
      number: number
      total: number
      proposal: ProposalState
      layer: string
      pending: string[]
      /** True when this person can decide a pending layer (or approve them all). */
      forYou: boolean
      completing: boolean
      all?: string[]
    }
  | {
      kind: 'blocked'
      step: SetupStepView | null
      number: number
      total: number
      reason: string
      /** agent text, never a badge or markup input */
      question: string
    }
  | { kind: 'registered'; watchedBy: string }

/** The step the plan is on: the blocked step when the server named one, else setup.step_index. */
export function currentSetupStep(state: RoomState): SetupStepView | null {
  const steps = setupSteps(state.setup)
  const index = state.setup?.blocked?.step_index ?? state.setup?.step_index ?? 0
  return steps.find((s) => s.index === index) ?? steps[0] ?? null
}

/** The open proposal that is still missing a layer, if any (the "awaiting approval" step). */
function openProposal(state: RoomState): ProposalState | undefined {
  return state.proposals.find(
    (p) => p.status === 'open' && p.required_layers.some((l) => (p.approvals[l] ?? 'pending') === 'pending'),
  )
}

/** The approved proposal still running its steps, if any. */
function runningProposal(state: RoomState): ProposalState | undefined {
  return state.proposals.find(
    (p) => p.status === 'approved' && !waitingForWriteMode(p) && p.executed.length < p.steps.length,
  )
}

/** Flatten the plan's phases into numbered steps with their status. */
export function setupSteps(setup: SetupState | undefined): SetupStepView[] {
  if (!setup) return []
  const flat = setup.phases.flatMap((phase) => phase.steps.map((step) => ({ ...step, phase: phase.name })))
  const total = flat.length
  return flat.map((step, i) => {
    const run = setup.executed[step.index]
    let status: SetupStepStatus = 'pending'
    if (run) status = run.ok ? 'done' : 'failed'
    return { ...step, number: i + 1, total, status }
  })
}

/**
 * The steps with the current one marked running, awaiting or blocked. Kept separate from
 * setupSteps because it needs the proposals; the stepper calls this.
 */
export function setupStepViews(state: RoomState): SetupStepView[] {
  const steps = setupSteps(state.setup)
  if (!state.setup) return steps
  const current = state.setup.step_index
  const proposal = openProposal(state)
  const running = runningProposal(state)
  return steps.map((step) => {
    if (step.status === 'done' || step.status === 'failed') return step
    if (state.setup?.blocked && (state.setup.blocked.step_index ?? current) === step.index) {
      return { ...step, status: 'blocked' }
    }
    if (step.index !== current) return step
    if (proposal) return { ...step, status: 'awaiting' }
    if (running) return { ...step, status: 'running' }
    return step
  })
}

/** What this room needs from people right now. First match wins. */
export function setupStatus(state: RoomState, user: CurrentUser): SetupStatus | null {
  const setup = state.setup
  if (!setup) return null
  const step = currentSetupStep(state)
  const number = step?.number ?? 0
  const total = step?.total ?? setupSteps(setup).length

  if (!setup.runner) return { kind: 'no_runner' }
  if (!setup.runner.connected) return { kind: 'runner_lost', label: setup.runner.label }
  if (setup.watched_by) return { kind: 'registered', watchedBy: setup.watched_by }

  if (setup.blocked) {
    return {
      kind: 'blocked',
      step,
      number,
      total,
      reason: setup.blocked.reason,
      question: setup.blocked.question,
    }
  }

  const proposal = openProposal(state)
  if (proposal) {
    const pending = proposal.required_layers.filter((l) => (proposal.approvals[l] ?? 'pending') === 'pending')
    const all = isViewer(user) ? undefined : approveAllFor(state, proposal, user)
    const layer = all?.[0] ?? pending.find((l) => canDecideLayer(proposal, user, l, state.driver?.id)) ?? pending[0] ?? ''
    return {
      kind: 'awaiting_approval',
      step,
      number,
      total,
      proposal,
      layer,
      pending,
      forYou: !isViewer(user) && (all != null || pending.some((l) => canDecideLayer(proposal, user, l, state.driver?.id))),
      completing: proposal.required_layers.every((l) => l === layer || proposal.approvals[l] === 'approved'),
      ...(all ? { all } : {}),
    }
  }

  const running = runningProposal(state)
  if (running) {
    const i = Math.min(running.executed.length, running.steps.length - 1)
    return { kind: 'running', step, number, total, action: running.steps[i]?.action ?? '' }
  }

  if (!setup.plan_id) return { kind: 'paired', label: setup.runner.label }
  return { kind: 'planning', step, number, total }
}

/** "Step 2 of 7 · compose_up" — one line for the bar and the stepper trigger. */
export function stepLabel(step: SetupStepView | null, number: number, total: number): string {
  const head = number > 0 && total > 0 ? `Step ${number} of ${total}` : 'Setup'
  return step ? `${head} · ${step.op}` : head
}

/** The runner's status in one short line. */
export function runnerLabel(setup: SetupState | undefined): string {
  if (!setup) return ''
  if (!setup.runner) return 'No runner paired'
  return setup.runner.connected ? `Runner ${setup.runner.label} paired` : `Runner ${setup.runner.label} lost`
}

/** The bar's "View steps" target: opens the mobile sheet, else scrolls to the stepper. */
export function openSetupStepper(): void {
  const el = document.getElementById(SETUP_STEPPER_ID)
  if (!el) return
  if (el instanceof HTMLButtonElement) {
    el.click()
    return
  }
  el.scrollIntoView?.({ block: 'start' })
  const focusable = el.querySelector<HTMLElement>('button, a, [tabindex]') ?? el
  focusable.focus?.()
}

/** The bar's "Pairing command" target: scrolls to and focuses the pairing card. */
export function focusSetupPairing(): void {
  const el = document.getElementById(SETUP_PAIRING_ID)
  if (!el) return
  el.scrollIntoView?.({ block: 'start' })
  const focusable = el.querySelector<HTMLElement>('button, a, [tabindex]') ?? el
  focusable.focus?.()
}

export interface SetupPairing {
  command: string
  expires_at?: string
}

const pairingKey = (roomId: string) => `piperoom.setup.pairing.${roomId}`

/**
 * The pairing command POST /api/setup answered with. The server never returns it again and
 * keeps no copy (the code is a credential), so the browser holds it for this tab only.
 */
export function rememberSetupPairing(roomId: string, pairing: SetupPairing): void {
  try {
    sessionStorage.setItem(pairingKey(roomId), JSON.stringify(pairing))
  } catch {
    // storage blocked: the pairing card says the command was shown once
  }
}

export function setupPairing(roomId: string): SetupPairing | null {
  try {
    const raw = sessionStorage.getItem(pairingKey(roomId))
    if (!raw) return null
    const parsed = JSON.parse(raw) as Partial<SetupPairing>
    return typeof parsed.command === 'string' && parsed.command ? { command: parsed.command, expires_at: parsed.expires_at } : null
  } catch {
    return null
  }
}

export function clearSetupPairing(roomId: string): void {
  try {
    sessionStorage.removeItem(pairingKey(roomId))
  } catch {
    // nothing to clear
  }
}
