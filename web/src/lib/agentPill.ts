import type { RoomState } from '../state/types'

export type PillTone = 'ok' | 'warn' | 'fail' | 'agent' | 'muted'

export interface AgentPillInfo {
  tone: PillTone
  text: string
  /** The agent's own detail line, kept as a tooltip. */
  title?: string
}

// "Agent working · step 5", "Agent waiting · 1 of 2 approvals", "resolved — verified" ...
// The pill in the room header. It is a live region, so a change is read out politely.
export function agentPill(state: RoomState): AgentPillInfo {
  const { state: s, detail } = state.agent_status
  switch (s) {
    case 'done':
      return { tone: 'ok', text: 'resolved — verified', title: detail }
    case 'error':
      return { tone: 'fail', text: 'Agent error', title: detail }
    case 'waiting_approval': {
      const open = state.proposals.find((p) => p.status === 'open' && p.required_layers.length > 0)
      if (!open) return { tone: 'warn', text: 'Agent waiting for approval', title: detail }
      const done = open.required_layers.filter((l) => open.approvals[l] === 'approved').length
      return { tone: 'warn', text: `Agent waiting · ${done} of ${open.required_layers.length} approvals`, title: detail }
    }
    case 'working':
      return {
        tone: 'agent',
        text: state.steps.length > 0 ? `Agent working · step ${state.steps.length}` : 'Agent working',
        title: detail,
      }
    case 'idle':
      return {
        tone: 'muted',
        text: state.status === 'open' && state.driver === null ? 'Agent idle · waiting for a driver' : 'Agent idle',
        title: detail,
      }
    default:
      return { tone: 'muted', text: detail ? `Agent ${s} · ${detail}` : `Agent ${s}` }
  }
}
