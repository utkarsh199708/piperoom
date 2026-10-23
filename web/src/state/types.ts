// UI state derived from the room snapshot + the event log. This is the shape
// the reducer (state/reducer.ts) folds events into.
import type {
  ActionExecutedPayload,
  Actor,
  AgentStatusPayload,
  AgentRecoveryCheckPayload,
  AgentVerificationPayload,
  ProposalClass,
  ForecastSummary,
  HandoffSummary,
  HypothesisSummary,
  HypothesisVerdict,
  Identity,
  PostmortemSource,
  ProposalPreviewedPayload,
  ProposalStagedPayload,
  ProposalStep,
  RoomLabelledPayload,
  RoomMode,
  RoomSnapshot,
  SetupAwaitingChangePayload,
  SetupBounds,
  SetupPlanPhase,
  SetupRequest,
  SetupRung,
  SetupRunnerSummary,
  SetupSnapshot,
  SetupTargetSummary,
  ToolCall,
  TraceStage,
} from "../api/types";

export type Severity = "sev1" | "sev2" | "sev3" | "sev4";
export type RoomStatus = "open" | "closed";
export type ApprovalValue = "approved" | "rejected" | "pending";

export interface DriverInfo {
  id: string;
  name: string;
  verified?: boolean;
  lease_until?: string;
  /**
   * Folded from control.lease_expired whose lease_until equals this lease_until (docs/api.md,
   * Lease expiry). The driver stays the driver; the next control.granted clears it.
   */
  lease_expired?: boolean;
  /** control.granted.reason (P2b): "sole_member" when the server made the only person here the driver. */
  reason?: string;
}

export interface AgentStatusState {
  state: AgentStatusPayload["state"];
  detail?: string;
}

export interface VerdictState {
  verdict: HypothesisVerdict;
  by: Actor;
  note?: string;
  at: string;
}

export interface HypothesisState {
  text: string;
  based_on_steps: string[];
  confidence: HypothesisSummary["confidence"];
  /** Derived (F9): at least one confirmed and no disputed verdict on this hypothesis. */
  confirmed: boolean;
  /** F3: terms in `text` that the cited steps do not show. */
  unbacked_terms?: string[];
  /** seq of the agent.hypothesis event; hypothesis.verdict answers it. */
  seq?: number;
  /** Latest verdict per person, in the order they were given. */
  verdicts?: VerdictState[];
}

/** One entry of the confidence path under the hypothesis heading. */
export interface HypothesisHistoryEntry {
  seq: number;
  at: string;
  confidence: string;
}

/** approval.nudged: when and by whom a pending layer was last nudged. */
export interface NudgeState {
  at: string;
  by: Actor;
}

/** The newest agent.triage (F6): the first look, shown as "triage, unverified". */
export interface TriageState {
  status: "ok" | "failed";
  reason?: string;
  headline?: string;
  suspected_layer?: string;
  confidence?: string;
  evidence_refs: string[];
  next_check?: string;
  at: string;
}

export interface ToolCallState {
  tool_use_id: string;
  tool: string;
  input: unknown;
  summary?: string;
  data?: unknown;
  is_error?: boolean;
  truncated?: boolean;
  took_ms?: number;
  /** F3: the reads behind the summary. */
  provenance?: ToolCall[];
}

export interface AgentMessageState {
  text: string;
  at: string;
}

export interface StepState {
  id: string;
  title: string;
  tool_calls: ToolCallState[];
  messages: AgentMessageState[];
}

export interface CommentState {
  id: string;
  step_id?: string;
  text: string;
  mentions_agent: boolean;
  author: Actor;
  at: string;
  replies: AgentMessageState[];
}

/**
 * pending: waiting in the driver's queue. accepted: the driver sent it (steer.sent with its id).
 * declined: the driver declined it (steer.declined); `declined_by` and `reason` say who and why.
 */
export type SuggestionStatus = "pending" | "accepted" | "declined";

export interface SuggestionState {
  suggestion_id: string;
  from: Actor;
  text: string;
  status: SuggestionStatus;
  /** When the suggestion was made. */
  at?: string;
  /** Who forwarded it to the agent, and when (steer.sent with its id). */
  sent_by?: Actor;
  sent_at?: string;
  /** Who declined it, when, and the optional note (steer.declined). */
  declined_by?: Actor;
  declined_at?: string;
  reason?: string;
}

/** Who decided a layer, how they logged in, and the hash they decided on. */
export interface DecisionState {
  outcome: "approved" | "rejected";
  by: Actor;
  identity?: Identity;
  params_hash?: string;
  reason?: string;
  /** When the decision was made (the event's time); absent on a decision built without an event. */
  at?: string;
}

export interface ProposalState {
  proposal_id: string;
  title: string;
  steps: ProposalStep[];
  risk: string;
  rollback: string;
  required_layers: string[];
  approvals: Record<string, ApprovalValue>;
  /** Per layer, for the layers that have been decided. */
  decisions: Record<string, DecisionState>;
  /**
   * The params_hash the UI shows and sends back with approve and reject. Taken from
   * proposal.created (or the snapshot for an older event); "" when there is none.
   */
  params_hash: string;
  status: "open" | "approved" | "rejected";
  executed: ActionExecutedPayload[];
  /** When proposal.created was logged: the preview skeleton counts its 20 s from here. */
  created_at?: string;
  /** What each step would do; absent until proposal.previewed arrives (it may never). */
  preview?: ProposalPreviewedPayload;
  /** The agent's verdict after the last step ran. */
  verification?: AgentVerificationPayload;
  /**
   * X8: step indexes whose action can be staged. From the snapshot (open_proposals[].stageable, the
   * server's registry knows it, the log does not) or, for a stage that starts, from its `steps`.
   * Absent until one of them says so.
   */
  stageable?: number[];
  /** X8: a proposal.stage_started with no proposal.staged yet. The card also checks expires_at against its clock. */
  stage_running?: {
    by: Actor;
    started_at: string;
    expires_at: string;
    steps: number[];
    params_hash: string;
  };
  /** X8: the newest proposal.staged payload; the card calls it stale when its params_hash is not the proposal's. */
  staged?: ProposalStagedPayload;
  /**
   * X8: a step's action is in require_staging: and no healthy stage of this params_hash exists. From
   * the snapshot, lifted by a healthy proposal.staged, and set by a 409 staging_required on approve.
   */
  staging_required?: boolean;
  /** X8: the server's own words from a 409 staging_required (names the step and action); untrusted text. */
  staging_message?: string;
  /**
   * P1a: every required layer approved while write mode is off, so no step runs. From the snapshot
   * (`open_proposals[].blocked`) or, for an approval that arrives over the socket, from `mode.write`
   * (see the reducer). Read it through `waitingForWriteMode`, which also checks that nothing ran.
   */
  blocked?: "write_mode_off";
  /**
   * P2b: proposal.created.fallback_layers, the required layers nobody owns. The room's driver
   * approves them. Absent when every layer has an owner.
   */
  fallback_layers?: string[];
  /**
   * P2b: the pending policy countdown (proposal.scheduled), until a proposal.cancelled, any
   * approval.given (the policy's or a person's) or a rejection of the proposal. Kept in the fold
   * however late it is: lib/countdown.ts decides whether the card still shows it (#652).
   */
  scheduled?: ScheduledState;
  /**
   * A proposal.scheduled was folded once. The server counts only the first per proposal, even after
   * a cancel, so a later one is ignored (internal/room foldScheduled).
   */
  schedule_seen?: boolean;
  /** X2: proposal.created.class. Left off for a fix, so such a proposal folds as before. */
  class?: ProposalClass;
  /** P3c: "driver" for a fix the driver wrote (POST /api/rooms/{id}/proposals). Left off for the agent's. */
  origin?: "driver";
  /** P3c: the driver who wrote it (proposal.created's actor); set with origin "driver" only. */
  author?: Actor;
  /** X2: recovery proposals only; see RecoveryProgress. */
  recovery?: RecoveryProgress;
}

/** X2: what the log says about a recovery proposal after its step ran. */
export interface RecoveryProgress {
  /** The newest agent.recovery_check of this proposal. */
  check?: AgentRecoveryCheckPayload;
  /** When the last step finished ok (action.executed): the start of "recovered in 3 min". */
  ran_at?: string;
  /** When the counted agent.verification arrived. */
  verified_at?: string;
}

/** A pending proposal.scheduled (P2b). */
export interface ScheduledState {
  run_at: string;
  /** Advertised only: the card says "the policy may run from <time>", never a deadline (#629). */
  cancel_until: string;
  policy: string;
}

export interface MemberState {
  id: string;
  name: string;
  /** Server-set, from the latest event or snapshot that named this person. */
  verified?: boolean;
  role: string;
}

export interface ControlRequestState {
  from: Actor;
}

export interface TraceState {
  table: string;
  key: string;
  stages: TraceStage[];
}

export interface HandoffState {
  handoff_id: string;
  to: Actor;
  summary: HandoffSummary;
  stay_as_watcher: boolean;
  /** voided: the server no longer accepts it (a newer hand-off, the driver changed, the room closed). */
  status: "pending" | "accepted" | "voided";
  /** The driver when it was created; the server voids it once someone else drives. */
  from_id?: string;
  /** handoff.created's actor: the driver who handed off. */
  from: Actor;
  /** handoff.created's time. */
  created_at: string;
  /** handoff.accepted's time and actor, once accepted. */
  accepted_at?: string;
  accepted_by?: Actor;
}

/**
 * The post-mortem draft (F2): `drafting` from room.closed until postmortem.drafted or
 * postmortem.failed. The Markdown itself is always available from the server (events-only render).
 */
export type PostmortemState =
  | { status: "drafting" }
  | {
      status: "drafted";
      source: PostmortemSource;
      summary_ref?: string;
      at: string;
    }
  | { status: "failed"; reason: string };

/** A hand-off the agent is still writing the summary for (handoff.requested, R0). */
export interface HandoffRequestState {
  request_id: string;
  to: Actor;
  stay_as_watcher: boolean;
  expires_at: string;
}

/** The last hand-off request that ended without a hand-off (handoff.failed). */
export interface HandoffFailureState {
  request_id: string;
  reason: string;
}

/** The alert that opened the room, from room.created's `alert` webhook body. */
export interface AlertInfo {
  /** Best-effort source inferred from the raw body shape; null when unknown. Untrusted: never a badge input. */
  source: string | null;
  /** Server-set room.created.alert_source: the only input to the alert's verified badge. */
  alert_source?: string;
  /** X1: the drift entries of a watcher-opened room's evidence (cleaned, untrusted text); absent when none. */
  drift?: DriftFact[];
}

/** What the agent has cost in this room, folded from agent.usage (F11; same rules as the server's insight). */
export interface InsightUsageState {
  calls: number;
  unpriced_calls: number;
  tokens: number;
  /** null while no call is priced (never 0 for unknown); a lower bound while unpriced_calls > 0. */
  cost_usd: number | null;
  latency_ms_total: number;
  cache_read_tokens: number;
  /** Model id -> number of calls, for the chip tooltip. */
  models: Record<string, number>;
}

export interface InsightState {
  first_hypothesis_at: string | null;
  /** Whole seconds from opened_at to the first hypothesis, -1 before one exists. */
  time_to_first_hypothesis_s: number;
  steps: number;
  tool_calls: number;
  tool_errors: number;
  usage: InsightUsageState;
}

export function emptyInsight(): InsightState {
  return {
    first_hypothesis_at: null,
    time_to_first_hypothesis_s: -1,
    steps: 0,
    tool_calls: 0,
    tool_errors: 0,
    usage: {
      calls: 0,
      unpriced_calls: 0,
      tokens: 0,
      cost_usd: null,
      latency_ms_total: 0,
      cache_read_tokens: 0,
      models: {},
    },
  };
}

export type { ActivityKind } from "./describe";
import type { ActivityKind } from "./describe";
import type { DriftFact } from "../lib/drift";
import { stripBidi } from "../lib/title";
import { cleanForecast } from "../lib/trends";

/** Who ticked one runbook item and when (checklist.step_done). */
export interface ChecklistTick {
  by: Actor;
  at: string;
  note?: string;
}

/** Key of RoomState.checklist_done: "<tool_use_id>:<hit>:<item>". */
export function checklistKey(
  toolUseId: string,
  hit: number,
  item: number,
): string {
  return `${toolUseId}:${hit}:${item}`;
}

export interface ActivityEntry {
  seq: number;
  at: string;
  actor: Actor;
  text: string;
  /** Full sentence behind a clamped row (tooltip), when there is more to say. */
  detail?: string;
  kind: ActivityKind;
  /** The tool name the `{tool}` placeholder in `text` stands for. */
  tool?: string;
}

/** One action.executed of a setup step, keyed by the plan step index. */
export interface SetupExecutedState {
  ok: boolean;
  at: string;
  /** The runner may or may not have run the step; it is never retried. */
  outcome_unknown?: boolean;
}

/**
 * A setup room (U wave). Folded from the snapshot and the log: the snapshot carries runner,
 * plan id, phase and step index; `phases` and `executed` come from setup.plan and
 * action.executed, so the stepper can name every step before it runs.
 */
export interface SetupState {
  runner: SetupRunnerSummary | null;
  plan_id: string;
  phase: string;
  /** The first step of the plan no action.executed has run yet. */
  step_index: number;
  bounds: SetupBounds;
  targets: SetupTargetSummary[];
  provisional: { topic_prefix: string; tables: string[]; connectors: string[] };
  maintenance_window?: { start: string; end: string };
  verified: SetupRung[];
  blocked?: { reason: string; question: string; step_index?: number };
  /** pipeline.registered.watched_by: "server", "local", or "" before registration. */
  watched_by: string;
  /** setup.secret_set names, in the order they were set, each once. Names only: never a value. */
  secret_names: string[];
  /** setup.credentials_registered: the names the runner registered, once it has. */
  credentials?: { names: string[]; runner_label: string };
  /** The request room.created carries (the form's values); absent until the replay reaches it. */
  request?: SetupRequest;
  /** The current setup.plan's phases; [] before the first plan. */
  phases: SetupPlanPhase[];
  /** Plan step index -> its action.executed. */
  executed: Record<number, SetupExecutedState>;
  /** setup.awaiting_change: rung 8 waits for a change the person makes to `table`. */
  awaiting_change?: SetupAwaitingChangePayload;
}

export const DEFAULT_SETUP_BOUNDS: SetupBounds = {
  attempts_per_step: 3,
  steps_per_plan: 12,
  wall_clock_s: 2700,
  token_budget: 2_000_000,
};

/** The UI's view of a setup snapshot: absent fields read as their empty value. */
export function setupFromSnapshot(snapshot: SetupSnapshot): SetupState {
  return {
    runner: snapshot.runner ?? null,
    plan_id: snapshot.plan_id ?? "",
    phase: snapshot.phase ?? "",
    step_index: snapshot.step_index ?? 0,
    bounds: snapshot.bounds ?? DEFAULT_SETUP_BOUNDS,
    targets: snapshot.targets ?? [],
    provisional: {
      topic_prefix: snapshot.provisional?.topic_prefix ?? "",
      tables: snapshot.provisional?.tables ?? [],
      connectors: snapshot.provisional?.connectors ?? [],
    },
    ...(snapshot.maintenance_window
      ? { maintenance_window: snapshot.maintenance_window }
      : {}),
    verified: snapshot.verified ?? [],
    ...(snapshot.blocked ? { blocked: snapshot.blocked } : {}),
    watched_by: snapshot.watched_by ?? "",
    // The snapshot lists no secret and no credentials: both are names the log carries, folded
    // from setup.secret_set / setup.credentials_registered as the replay reaches them.
    secret_names: [],
    phases: [],
    executed: {},
  };
}

/**
 * The setup member room.created opens: no runner, no plan, the request as the person typed it.
 * The snapshot of a just-created room may arrive before the replay; this is the same shape.
 */
export function newSetupState(request?: SetupRequest): SetupState {
  return {
    runner: null,
    plan_id: "",
    phase: "",
    step_index: 0,
    bounds: DEFAULT_SETUP_BOUNDS,
    targets: (request?.targets ?? []).map((t) => ({
      kind: t.kind,
      address: t.address,
      class: t.class && t.class !== "" ? t.class : "shared",
    })),
    provisional: {
      topic_prefix: request?.topic_prefix ?? "",
      tables: request?.tables ?? [],
      connectors: request?.connectors ?? [],
    },
    verified: [],
    watched_by: "",
    secret_names: [],
    ...(request ? { request } : {}),
    phases: [],
    executed: {},
  };
}

/**
 * Move `step_index` to the first step of the plan no action.executed has run yet.
 *
 * Contract note: action.executed carries the PROPOSAL's step index, and this fold (like the
 * server's setup_snapshot fold) keys the plan's `executed` by it. The two indices agree only
 * while a setup proposal's steps are the plan's steps in plan order; the agent-wiring row
 * (ErrNotInPlan, design §3.3) owns pinning that invariant or carrying a plan index on the
 * event. Until then this mirrors the server exactly.
 */
export function advanceSetupStep(setup: SetupState): SetupState {
  for (const phase of setup.phases) {
    for (const step of phase.steps) {
      if (!setup.executed[step.index]) {
        return { ...setup, phase: phase.name, step_index: step.index };
      }
    }
  }
  const last = setup.phases[setup.phases.length - 1];
  const lastStep = last?.steps[last.steps.length - 1];
  return {
    ...setup,
    phase: last?.name ?? setup.phase,
    step_index: lastStep?.index ?? setup.step_index,
  };
}

export interface RoomState {
  id: string;
  title: string;
  severity: Severity;
  pipeline_id: string;
  status: RoomStatus;
  opened_at: string;
  closed_at?: string;
  resolution?: string;
  last_seq: number;
  /**
   * The snapshot's last_seq: how many events the first replay (after=0) has to fold. 0 when
   * unknown (fixtures, empty room). Drives the replay progress bar.
   */
  replay_target: number;
  driver: DriverInfo | null;
  /** Set when room.created carried an alert webhook body (alert-opened room). */
  alert: AlertInfo | null;
  agent_status: AgentStatusState;
  hypothesis: HypothesisState | null;
  triage: TriageState | null;
  /** The confidence path, capped at 20; a repeat of the same confidence is not added. */
  hypothesis_history: HypothesisHistoryEntry[];
  /** `${proposal_id}/${layer}` -> the last nudge. */
  nudges: Record<string, NudgeState>;
  steps: StepState[];
  messages: AgentMessageState[];
  comments: CommentState[];
  suggestions: SuggestionState[];
  proposals: ProposalState[];
  /**
   * proposal_id -> params_hash from the snapshot's open_proposals. The server computes
   * it for proposals whose proposal.created event predates the hash, so the reducer
   * falls back to it when the event has none.
   */
  snapshot_hashes: Record<string, string>;
  /**
   * proposal_id -> "write_mode_off" from the snapshot's open_proposals[].blocked (refreshed with
   * `mode` on a reconnect). The reducer copies it onto the proposal when proposal.created replays
   * (same reason as snapshot_hashes).
   */
  snapshot_blocked?: Record<string, "write_mode_off">;
  /**
   * X8: proposal_id -> the snapshot's `stageable` and `staging_required` for the open proposals that
   * carry either. The reducer copies it onto the proposal when proposal.created replays (same
   * reason as snapshot_hashes), and applyModeRefresh redoes it.
   */
  snapshot_staging?: Record<
    string,
    { stageable: number[]; staging_required: boolean }
  >;
  /**
   * The room GET's `mode` (P1a, P2b). Write mode changes only when the server restarts, which drops
   * the socket, so it is read again on every reconnect (`mode_refreshed`, applyModeRefresh). The
   * socket never carries it. Absent on a server from before P1a, which has write mode on.
   */
  mode?: RoomMode;
  /**
   * P2c: proposal_id -> the text of the 403 not_layer_owner the server gave this person's
   * approve-all. Never from the log: useRoom sets it, a room reload clears it. While set, the card
   * and the decision bar offer that proposal's per-layer rows instead of "Approve and run".
   */
  approve_all_refused?: Record<string, string>;
  members: MemberState[];
  control_requests: ControlRequestState[];
  trace: TraceState | null;
  handoffs: HandoffState[];
  /** In flight until handoff.created / handoff.failed with its id, or `expires_at`. */
  handoff_request: HandoffRequestState | null;
  handoff_failure: HandoffFailureState | null;
  /** The last room.labelled (re-labelling appends another, the last wins); null before any. */
  labelled: RoomLabelledPayload | null;
  /** Ticked runbook items; the last event per key wins and an untick removes the key. */
  checklist_done: Record<string, ChecklistTick>;
  activity: ActivityEntry[];
  /** Set by shadow.revealed: the run has ended and its result exists (grading may start). */
  shadow_revealed?: boolean;
  /** Set by shadow.labelled: the blind label exists, so the F8 label may follow. */
  shadow_labelled?: boolean;
  /** Shadow room: no cost chip until its numbers are revealed; the shadow panel replaces steering once closed. */
  shadow: boolean;
  insight: InsightState;
  /** room.closed's actor (snapshot closed_by); closed rooms only. */
  closed_by?: Actor;
  /** F2: null on open rooms, shadow rooms and closed rooms from before F2. */
  postmortem: PostmortemState | null;
  /** X3: room.created.origin; "forecast" for a quiet forecast room. Absent for every other room. */
  origin?: string;
  /** X3: the snapshot's newest forecast signal (lane ETA badge, forecast room chart); absent when none. */
  forecast?: ForecastSummary;
  /** U wave: present on a setup room only (room.created.mode "setup" or the snapshot's setup). */
  setup?: SetupState;
}

export function emptyRoomState(id: string): RoomState {
  return {
    id,
    title: "",
    severity: "sev3",
    pipeline_id: "",
    status: "open",
    opened_at: "",
    last_seq: 0,
    replay_target: 0,
    driver: null,
    alert: null,
    agent_status: { state: "idle" },
    hypothesis: null,
    triage: null,
    hypothesis_history: [],
    nudges: {},
    steps: [],
    messages: [],
    comments: [],
    suggestions: [],
    proposals: [],
    snapshot_hashes: {},
    members: [],
    control_requests: [],
    trace: null,
    handoffs: [],
    handoff_request: null,
    handoff_failure: null,
    labelled: null,
    checklist_done: {},
    activity: [],
    shadow: false,
    insight: emptyInsight(),
    postmortem: null,
  };
}

export function fromSnapshot(snapshot: RoomSnapshot): RoomState {
  // The snapshot only carries header/derived state for a fast first paint; the
  // timeline, comments, proposals, suggestions, members, trace and hand-offs are
  // rebuilt from the full event log (WebSocket after=0). So we seed last_seq at 0
  // (letting the replay fold every event) and leave the event-derived collections
  // empty — seeding them here would duplicate on replay.
  return {
    ...emptyRoomState(snapshot.id),
    id: snapshot.id,
    title: stripBidi(snapshot.title),
    severity: snapshot.severity as Severity,
    pipeline_id: snapshot.pipeline_id,
    status: snapshot.status as RoomStatus,
    opened_at: snapshot.opened_at,
    closed_at: snapshot.closed_at,
    resolution: snapshot.resolution,
    closed_by: snapshot.closed_by,
    postmortem: postmortemFromSnapshot(snapshot),
    ...(snapshot.origin ? { origin: snapshot.origin } : {}),
    ...(snapshot.setup ? { setup: setupFromSnapshot(snapshot.setup) } : {}),
    ...forecastField(snapshot),
    last_seq: 0,
    shadow: snapshot.shadow === true,
    replay_target: snapshot.last_seq ?? 0,
    snapshot_hashes: Object.fromEntries(
      (snapshot.open_proposals ?? [])
        .filter((p) => p.params_hash)
        .map((p) => [p.proposal_id, p.params_hash as string]),
    ),
    ...snapshotWriteMode(snapshot),
    ...snapshotStaging(snapshot),
    driver: snapshot.driver
      ? {
          id: snapshot.driver.id,
          name: snapshot.driver.name,
          verified: snapshot.driver.verified === true,
          lease_until: snapshot.driver.lease_until,
        }
      : null,
    agent_status: {
      state: snapshot.agent_status.state,
      detail: snapshot.agent_status.detail,
    },
    // A shadow room shows no agent output, not even on the first render.
    hypothesis:
      snapshot.hypothesis && snapshot.shadow !== true
        ? {
            text: snapshot.hypothesis.text,
            based_on_steps: snapshot.hypothesis.based_on_steps ?? [],
            confidence: snapshot.hypothesis.confidence,
            confirmed: snapshot.hypothesis.confirmed,
            unbacked_terms: snapshot.hypothesis.unbacked_terms ?? [],
            seq: snapshot.hypothesis.seq ?? 0,
            verdicts: [],
          }
        : null,
    triage:
      snapshot.triage && snapshot.shadow !== true
        ? {
            ...snapshot.triage,
            evidence_refs: snapshot.triage.evidence_refs ?? [],
          }
        : null,
  };
}

// X3: left off when the snapshot has no usable forecast, so an older server's snapshot folds as before.
function forecastField(snapshot: RoomSnapshot): Pick<RoomState, "forecast"> {
  const forecast = cleanForecast(snapshot.forecast);
  return forecast ? { forecast } : {};
}

// The P1a fields, left off entirely when the server sent none so a snapshot from an older server
// folds to the same state as before.
export function snapshotWriteMode(
  snapshot: RoomSnapshot,
): Pick<RoomState, "mode" | "snapshot_blocked"> {
  const blocked = Object.fromEntries(
    (snapshot.open_proposals ?? [])
      .filter((p) => p.blocked === "write_mode_off")
      .map((p) => [p.proposal_id, "write_mode_off" as const]),
  );
  return {
    ...(snapshot.mode ? { mode: snapshot.mode } : {}),
    ...(Object.keys(blocked).length > 0 ? { snapshot_blocked: blocked } : {}),
  };
}

// The X8 fields, left off entirely when no open proposal carries one (a server from before X8c).
export function snapshotStaging(
  snapshot: RoomSnapshot,
): Pick<RoomState, "snapshot_staging"> {
  const staging = Object.fromEntries(
    (snapshot.open_proposals ?? [])
      .filter(
        (p) => p.stageable !== undefined || p.staging_required !== undefined,
      )
      .map((p) => [
        p.proposal_id,
        {
          stageable: p.stageable ?? [],
          staging_required: p.staging_required === true,
        },
      ]),
  );
  return Object.keys(staging).length > 0 ? { snapshot_staging: staging } : {};
}

// The snapshot carries status and source only; the replay of postmortem.drafted fills in the rest.
function postmortemFromSnapshot(
  snapshot: RoomSnapshot,
): PostmortemState | null {
  const pm = snapshot.postmortem;
  if (!pm || snapshot.shadow === true) return null;
  switch (pm.status) {
    case "drafting":
      return { status: "drafting" };
    case "drafted":
      return {
        status: "drafted",
        source: pm.source ?? "events",
        at: snapshot.closed_at ?? "",
      };
    case "failed":
      return { status: "failed", reason: pm.reason ?? "internal_error" };
    default:
      return null;
  }
}
