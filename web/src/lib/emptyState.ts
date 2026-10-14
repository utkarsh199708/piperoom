export interface EmptyState {
  title: string
  hint?: string
  busy: boolean
  /** Only an idle agent is waiting for a steer. */
  offerSteer: boolean
}

// A shadow room's log holds no agent events, so its status says nothing true; and a steer never
// reaches a shadow agent, so there is nothing to offer.
const SHADOW_EMPTY: EmptyState = {
  title: 'The agent investigates silently in a shadow room.',
  hint: 'Its steps and conclusion are shown after the room is closed and you record your own answer.',
  busy: false,
  offerSteer: false,
}

// What the empty investigation says, by agent status. `starting` is not in the API's list yet but the
// runner may send it; any state we do not know falls back to the idle wording.
export function emptyStateFor(agent?: { state: string; detail?: string }, shadow = false): EmptyState {
  if (shadow) return SHADOW_EMPTY
  const detail = agent?.detail
  switch (agent?.state) {
    case 'starting':
      return {
        title: 'The agent is starting…',
        hint: detail,
        busy: true,
        offerSteer: false,
      }
    case 'working':
      return {
        title: 'The agent is investigating…',
        hint: detail ?? 'The first step appears here as soon as it starts.',
        busy: true,
        offerSteer: false,
      }
    case 'queued':
      // PIPEROOM_MAX_AGENTS runs are busy; the server says how many are ahead in `detail`.
      return {
        title: 'The agent is waiting for a free slot.',
        hint: detail,
        busy: true,
        offerSteer: false,
      }
    case 'log_full':
      // The room log hit its cap and the agent stopped for good; the server refuses a steer.
      return {
        title: 'The agent has stopped: this room\'s log is full.',
        hint: detail ?? 'Open a new room to keep investigating.',
        busy: false,
        offerSteer: false,
      }
    case 'waiting_approval':
      return {
        title: 'The agent is waiting for an approval.',
        hint: detail,
        busy: false,
        offerSteer: false,
      }
    case 'error':
      return {
        title: 'The agent failed before its first step.',
        hint: detail ?? 'Hand the room to someone or steer the agent to retry.',
        busy: false,
        offerSteer: true,
      }
    case 'done':
      return {
        title: 'The agent finished without any steps.',
        hint: detail,
        busy: false,
        offerSteer: false,
      }
    default:
      return {
        title: 'The agent is idle and has not started yet.',
        hint: detail,
        busy: false,
        offerSteer: true,
      }
  }
}
