export interface EmptyState {
  title: string;
  hint?: string;
  busy: boolean;
  /** Only an idle agent is waiting for a steer. */
  offerSteer: boolean;
}

// What the empty investigation says, by agent status. `starting` is not in the API's list yet but the
// runner may send it; any state we do not know falls back to the idle wording.
export function emptyStateFor(agent?: {
  state: string;
  detail?: string;
}): EmptyState {
  const detail = agent?.detail;
  switch (agent?.state) {
    case "starting":
      return {
        title: "The agent is starting…",
        hint: detail,
        busy: true,
        offerSteer: false,
      };
    case "working":
      return {
        title: "The agent is investigating…",
        hint: detail ?? "The first step appears here as soon as it starts.",
        busy: true,
        offerSteer: false,
      };
    case "waiting_approval":
      return {
        title: "The agent is waiting for an approval.",
        hint: detail,
        busy: false,
        offerSteer: false,
      };
    case "error":
      return {
        title: "The agent failed before its first step.",
        hint: detail ?? "Hand the room to someone or steer the agent to retry.",
        busy: false,
        offerSteer: true,
      };
    case "done":
      return {
        title: "The agent finished without any steps.",
        hint: detail,
        busy: false,
        offerSteer: false,
      };
    default:
      return {
        title: "The agent is idle and has not started yet.",
        hint: detail,
        busy: false,
        offerSteer: true,
      };
  }
}
