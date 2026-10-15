// What the composer does with the text in it. Pure: SteerBox never branches on text itself.
import type { RoomState } from '../state/types'

export type ComposerRole = 'driver' | 'suggester'
export type ComposerMode = 'steer' | 'suggest' | 'ask' | 'mention'
export interface ComposerPlan {
  mode: ComposerMode
  hint: string
  /** 'steer' goes to POST /steer (driver steers, anyone else suggests); 'comment' is a room comment. */
  send: 'steer' | 'comment'
  text: string
  stepId?: string
}

/** What this text does for this role: `?` asks the agent, `@name` is a comment, `/` is an explicit steer/suggestion. */
export function planComposer(raw: string, role: ComposerRole, driverName: string | null, mod = 'Ctrl'): ComposerPlan {
  const text = raw.trim()
  const first = text.charAt(0)
  if (first === '?') {
    const rest = text.slice(1).trim()
    if (role === 'driver') {
      return { mode: 'ask', send: 'steer', text: `Question: ${rest}`, hint: 'Asking the agent · it answers in the stream' }
    }
    return {
      mode: 'ask',
      send: 'comment',
      text: `@agent ${rest}`,
      hint: 'Asking the agent · it answers under Room comments',
    }
  }
  if (first === '@') {
    return { mode: 'mention', send: 'comment', text, hint: 'Commenting · @agent also wakes the agent' }
  }
  const body = first === '/' ? text.slice(1).trim() : text
  if (role === 'driver') {
    return { mode: 'steer', send: 'steer', text: body, hint: `Steering the agent · ${mod}⏎ to send` }
  }
  return {
    mode: 'suggest',
    send: 'steer',
    text: body,
    hint: driverName ? `Suggesting to ${driverName} · they can forward it` : 'Suggesting · queued until someone drives',
  }
}

/** Button label for a plan. */
export function sendLabel(plan: ComposerPlan): string {
  switch (plan.mode) {
    case 'steer':
      return 'Send to agent'
    case 'suggest':
      return 'Suggest'
    case 'ask':
      return 'Ask'
    case 'mention':
      return 'Comment'
  }
}

/** Up to three one-click first steps for the current state of the room. They fill the field, never send. */
export function quickSteers(state: RoomState): string[] {
  const out: string[] = []
  if (state.steps.length === 0) {
    out.push('Check the connector status', 'Check consumer lag on the sink group', 'Check the newest warehouse row')
    return out.slice(0, 3)
  }
  const conf = state.hypothesis?.confidence
  const latest = state.proposals[state.proposals.length - 1]
  const hasOpen = state.proposals.some((p) => p.status === 'open')
  if ((conf === 'medium' || conf === 'high') && !hasOpen) {
    out.push('Confirm the hypothesis with one more read', 'Propose a fix')
  }
  if (latest?.status === 'rejected') out.push('Re-plan after the rejection')
  if (latest?.status === 'approved' && latest.steps.length > 0 && latest.executed.length >= latest.steps.length) {
    out.push('Verify the fix end to end')
  }
  return out.slice(0, 3)
}
