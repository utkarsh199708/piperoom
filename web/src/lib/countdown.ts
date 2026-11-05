// The policy countdown (P2b proposal.scheduled, P2c-3 card). Pure: the card passes the clock in.
import type { CurrentUser } from '../state/permissions'
import { isViewer } from '../state/permissions'
import type { DriverInfo, ProposalState, ScheduledState } from '../state/types'

/**
 * How long past run_at a countdown the policy has not fired is still shown. The same grace as the
 * server's room view: internal/room StaleCountdownGrace (internal/room/autoapprove.go, 45 s =
 * lease TTL + takeover scan + two loop ticks + slack; #652). Past it the countdown will most likely
 * never fire, so "Runs in 0:00" is not shown forever; the proposal waits for a person, and the
 * countdown stays cancellable because the server still takes a cancel while it is pending.
 */
export const STALE_COUNTDOWN_GRACE_MS = 45_000

/** The last seconds in which the countdown is announced (aria-live polite); quiet before them. */
export const ANNOUNCE_LAST_S = 10

/**
 * counting: run_at is ahead. due: run_at passed, within the grace (the policy may start any
 * moment). stale: more than the grace past run_at, so the countdown is hidden. A run_at that does
 * not parse is stale: there is nothing honest to count down to.
 */
export type CountdownPhase = 'counting' | 'due' | 'stale'

export interface Countdown {
  phase: CountdownPhase
  /** Whole seconds until run_at, rounded up (0:42 until 42 s are left), 0 once due. */
  remaining_s: number
}

export function countdownOf(s: ScheduledState, now: number): Countdown {
  const runAt = Date.parse(s.run_at)
  if (Number.isNaN(runAt)) return { phase: 'stale', remaining_s: 0 }
  if (now < runAt) return { phase: 'counting', remaining_s: Math.ceil((runAt - now) / 1000) }
  return { phase: now - runAt > STALE_COUNTDOWN_GRACE_MS ? 'stale' : 'due', remaining_s: 0 }
}

/** "0:42", "1:05", "12:00". */
export function formatRemaining(seconds: number): string {
  const s = Math.max(0, Math.floor(seconds))
  return `${Math.floor(s / 60)}:${String(s % 60).padStart(2, '0')}`
}

/** "policy auto_approve": the policy name is server-set, but only a plain word is shown as is. */
export function policyLabel(policy: string): string {
  return /^[a-z_]{1,40}$/.test(policy) ? `policy ${policy}` : 'policy'
}

/**
 * The cancel route's rule (docs/api.md, POST /api/proposals/{pid}/cancel): the driver or an owner
 * of any required layer, never a viewer, on an open room and an open proposal with a countdown in
 * the fold. Hidden or not, the countdown stays cancellable (#652): the phase does not matter here.
 * `driver` is the room's driver while the room is open, else null; `solo` is the room's derived
 * `mode.solo` (the caller may approve every layer, so owns each).
 */
export function canCancelSchedule(
  proposal: Pick<ProposalState, 'status' | 'required_layers' | 'scheduled'>,
  user: CurrentUser,
  driver: Pick<DriverInfo, 'id'> | null,
  solo = false,
): boolean {
  if (isViewer(user) || proposal.status !== 'open' || !proposal.scheduled) return false
  if (driver != null && driver.id === user.id) return true
  return solo || proposal.required_layers.some((l) => user.layers.includes(l))
}
