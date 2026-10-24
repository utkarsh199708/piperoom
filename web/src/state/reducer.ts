// The single reducer that folds one room event into UI state, mirroring A5's
// Apply(snapshot, event) semantics. Dedupe (seq <= last_seq) happens here; gap
// filling (seq > last_seq + 1) is the transport's job (see hooks/useRoom.ts).
import type {
  ActionExecutedPayload,
  AgentRecoveryCheckPayload,
  AgentVerificationPayload,
  ProposalPreviewedPayload,
  ProposalStageStartedPayload,
  ProposalStagedPayload,
  PullRequestUpdatedPayload,
  RoomSnapshot,
  Actor,
  AgentHypothesisPayload,
  AgentTriagePayload,
  AgentMessagePayload,
  AgentStatusPayload,
  AgentStepStartPayload,
  AgentToolCallPayload,
  AgentToolResultPayload,
  AgentUsagePayload,
  ApprovalGivenPayload,
  ApprovalRejectedPayload,
  ChecklistStepDonePayload,
  CommentAddedPayload,
  ControlGrantedPayload,
  ControlLeaseExpiredPayload,
  Event,
  HandoffAcceptedPayload,
  HandoffCreatedPayload,
  HandoffFailedPayload,
  HandoffRequestedPayload,
  HypothesisVerdictPayload,
  PostmortemDraftedPayload,
  PostmortemFailedPayload,
  ApprovalNudgedPayload,
  ProposalCancelledPayload,
  ProposalCreatedPayload,
  ProposalScheduledPayload,
  RoomClosedPayload,
  RoomCreatedPayload,
  RoomEscalatedPayload,
  RoomLabelledPayload,
  SetupAwaitingChangePayload,
  SetupBlockedPayload,
  SetupCredentialsRegisteredPayload,
  SetupMaintenanceWindowPayload,
  SetupPipelineExtendedPayload,
  SetupPlanPayload,
  SetupRequest,
  SetupRunnerAttachedPayload,
  SetupRunnerLostPayload,
  SetupSecretSetPayload,
  SetupTargetClassedPayload,
  SetupTargetRegisteredPayload,
  SetupVerifiedPayload,
  PipelineRegisteredPayload,
  SteerDeclinedPayload,
  SteerSentPayload,
  SteerSuggestedPayload,
  TracePinnedPayload,
} from "../api/types";
import { describeEvent } from "./describe";
import type { DescribeContext } from "./describe";
import { alertSource } from "../lib/alerts";
import { driftFacts } from "../lib/drift";
import { runbookHits } from "../lib/f8";
import { stripBidi } from "../lib/title";
import {
  checklistKey,
  advanceSetupStep,
  newSetupState,
  snapshotStaging,
  snapshotWriteMode,
} from "./types";
import { stageLiftsRequirement } from "../lib/staging";
import type {
  AlertInfo,
  HypothesisHistoryEntry,
  InsightState,
  MemberState,
  ProposalState,
  RoomState,
  StepState,
  VerdictState,
} from "./types";

function as<T>(e: Event): T {
  return e.payload as T;
}

// room.created.alert holds the SetupRequest on a setup room. A malformed body (an older
// server, a hand-written fixture) leaves the setup member without the form's values: the
// runner, plan and thread still fold.
function parseSetupRequest(raw: unknown): SetupRequest | undefined {
  if (!raw || typeof raw !== "object") return undefined;
  const req = raw as Partial<SetupRequest>;
  if (typeof req.pipeline_id !== "string" || typeof req.title !== "string")
    return undefined;
  return {
    pipeline_id: req.pipeline_id,
    title: req.title,
    source_kind: typeof req.source_kind === "string" ? req.source_kind : "",
    target: typeof req.target === "string" ? req.target : "",
    topic_prefix: typeof req.topic_prefix === "string" ? req.topic_prefix : "",
    tables: Array.isArray(req.tables)
      ? req.tables.filter((t): t is string => typeof t === "string")
      : [],
    targets: Array.isArray(req.targets)
      ? req.targets
          .filter((t) => t && typeof t.address === "string")
          .map((t) => ({
            kind: typeof t.kind === "string" ? t.kind : "",
            address: t.address,
            ...(t.class ? { class: t.class } : {}),
          }))
      : [],
    ...(typeof req.tenant === "string" && req.tenant
      ? { tenant: req.tenant }
      : {}),
    ...(Array.isArray(req.connectors)
      ? {
          connectors: req.connectors.filter(
            (c): c is string => typeof c === "string",
          ),
        }
      : {}),
  };
}

/**
 * Write mode as the server says it now. It changes only when the server restarts, so the hook asks
 * again on every reconnect and calls this with the fresh room GET. Everything derived from the old
 * answer is redone: a `blocked` mark the new mode no longer backs is dropped (off to on), and an
 * approved proposal that has run nothing is marked (on to off). It does not matter whether the
 * answer lands before or after the replayed events, since each is judged against the mode in hand
 * and this pass re-judges all of them. Everything else in the state is left alone.
 */
export function applyModeRefresh(
  state: RoomState,
  snapshot: RoomSnapshot,
): RoomState {
  const fresh = snapshotWriteMode(snapshot);
  const freshStaging = snapshotStaging(snapshot);
  const next: RoomState = {
    ...state,
    ...fresh,
    proposals: state.proposals.map((p) => {
      const blocked =
        fresh.snapshot_blocked?.[p.proposal_id] ??
        (p.status === "approved" && fresh.mode?.write === false
          ? "write_mode_off"
          : undefined);
      const proposal = { ...p };
      delete proposal.blocked;
      if (blocked) proposal.blocked = blocked;
      // X8: the snapshot is the authority on what can be staged and whether a stage is still owed.
      const staging = freshStaging.snapshot_staging?.[p.proposal_id];
      if (staging) {
        proposal.stageable = staging.stageable;
        if (staging.staging_required) proposal.staging_required = true;
        else {
          delete proposal.staging_required;
          delete proposal.staging_message;
        }
      }
      return proposal;
    }),
    ...freshStaging,
  };
  // A snapshot without them (a server from before P1a, or one with nothing blocked) clears the old ones.
  if (!fresh.mode) delete next.mode;
  if (!fresh.snapshot_blocked) delete next.snapshot_blocked;
  if (!freshStaging.snapshot_staging) delete next.snapshot_staging;
  return next;
}

// True when the event's seq skips ahead — the transport must fetch the missing
// events (/events?after=lastSeq) before applying.
export function detectGap(lastSeq: number, event: Event): boolean {
  return event.seq > lastSeq + 1;
}

function updateStep(
  steps: StepState[],
  stepId: string,
  fn: (s: StepState) => StepState,
): StepState[] {
  return steps.map((s) => (s.id === stepId ? fn(s) : s));
}

// upsertMember mirrors the server's snapshot.Apply: every human actor is a room
// member. New actors join as "watching"; an actor already in the list is never
// downgraded (a driver who acts stays the driver).
// `verified` follows the latest event, like the name: the server decides it when
// it writes each event.
function upsertMember(members: MemberState[], actor: Actor): MemberState[] {
  const verified = actor.verified === true;
  const existing = members.find((m) => m.id === actor.id);
  if (!existing)
    return [
      ...members,
      { id: actor.id, name: actor.name, verified, role: "watching" },
    ];
  if (existing.name === actor.name && existing.verified === verified)
    return members;
  return members.map((m) =>
    m.id === actor.id ? { ...m, name: actor.name, verified } : m,
  );
}

// foldInsight mirrors the server's insight fold (internal/room/insight.go): same
// rules, so the live chip and GET /api/rooms agree. A shadow room stays at zero.
function foldInsight(state: RoomState, event: Event): InsightState {
  const cur = state.insight;
  if (state.shadow) return cur;
  switch (event.type) {
    case "agent.step.started":
      return { ...cur, steps: cur.steps + 1 };
    case "agent.tool_call":
      return { ...cur, tool_calls: cur.tool_calls + 1 };
    case "agent.tool_result":
      return as<AgentToolResultPayload>(event).is_error
        ? { ...cur, tool_errors: cur.tool_errors + 1 }
        : cur;
    case "agent.hypothesis": {
      if (cur.first_hypothesis_at) return cur;
      const secs = Math.floor(
        (Date.parse(event.at) - Date.parse(state.opened_at)) / 1000,
      );
      return {
        ...cur,
        first_hypothesis_at: event.at,
        time_to_first_hypothesis_s: Number.isFinite(secs)
          ? Math.max(0, secs)
          : 0,
      };
    }
    case "agent.usage": {
      const p = as<AgentUsagePayload>(event);
      const u = cur.usage;
      const priced = typeof p.cost_usd === "number";
      return {
        ...cur,
        usage: {
          calls: u.calls + 1,
          unpriced_calls: u.unpriced_calls + (priced ? 0 : 1),
          tokens:
            u.tokens +
            p.input_tokens +
            p.cache_creation_input_tokens +
            p.cache_read_input_tokens +
            p.output_tokens,
          cost_usd: priced
            ? (u.cost_usd ?? 0) + (p.cost_usd as number)
            : u.cost_usd,
          latency_ms_total: u.latency_ms_total + p.latency_ms,
          cache_read_tokens: u.cache_read_tokens + p.cache_read_input_tokens,
          models: { ...u.models, [p.model]: (u.models[p.model] ?? 0) + 1 },
        },
      };
    }
    default:
      return cur;
  }
}

const ACTIVITY_WINDOW = 50;

function describeContext(before: RoomState, after: RoomState): DescribeContext {
  const stepIndex: Record<string, number> = {};
  const stepTitles: Record<string, string> = {};
  after.steps.forEach((s, i) => {
    stepIndex[s.id] = i + 1;
    stepTitles[s.id] = s.title;
  });
  const proposalTitles: Record<string, string> = {};
  for (const p of after.proposals) proposalTitles[p.proposal_id] = p.title;
  const suggesters: Record<string, string> = {};
  for (const s of after.suggestions) suggesters[s.suggestion_id] = s.from.name;
  return {
    driverBefore: before.driver?.id ?? null,
    stepTitles,
    proposalTitles,
    stepIndex,
    suggesters,
    confidenceBefore: before.hypothesis?.confidence ?? null,
    runbookItem: (id, hit, item) => runbookItemOf(before, id, hit, item),
  };
}

function derivedConfirmed(verdicts: VerdictState[]): boolean {
  return (
    verdicts.some((v) => v.verdict === "confirmed") &&
    !verdicts.some((v) => v.verdict === "disputed")
  );
}

const HISTORY_CAP = 20;

// The confidence path: a repeat of the same confidence is not a step on it.
function pushConfidence(
  history: HypothesisHistoryEntry[],
  event: Event,
  confidence: string,
): HypothesisHistoryEntry[] {
  if (
    history.length > 0 &&
    history[history.length - 1].confidence === confidence
  )
    return history;
  return [...history, { seq: event.seq, at: event.at, confidence }].slice(
    -HISTORY_CAP,
  );
}

// The text and section of a runbook item, looked up in the runbook_lookup result already in the
// state (used for the activity line of a tick).
function runbookItemOf(
  state: RoomState,
  toolUseId: string,
  hit: number,
  item: number,
) {
  for (const s of state.steps) {
    const t = s.tool_calls.find(
      (c) => c.tool_use_id === toolUseId && c.tool === "runbook_lookup",
    );
    const h = t ? runbookHits(t.data)[hit] : undefined;
    if (h && h.items[item] !== undefined)
      return { section: h.section, text: h.items[item] };
  }
  return null;
}

// The proposal without its policy countdown.
function unschedule(prop: ProposalState): ProposalState {
  if (!prop.scheduled) return prop;
  const rest = { ...prop };
  delete rest.scheduled;
  return rest;
}

function voidPending(
  handoffs: RoomState["handoffs"],
  when: (h: RoomState["handoffs"][number]) => boolean,
) {
  return handoffs.some((h) => h.status === "pending" && when(h))
    ? handoffs.map((h) =>
        h.status === "pending" && when(h)
          ? { ...h, status: "voided" as const }
          : h,
      )
    : handoffs;
}

/** Target class order (docs/api.md): a class only rises, the same floor the server applies. */
const classRank = (c: string) =>
  c === "production" ? 3 : c === "shared" ? 2 : c === "local" ? 1 : 0;

export function applyEvent(state: RoomState, event: Event): RoomState {
  if (event.seq <= state.last_seq) return state;
  const next = applyCore(state, event);
  const insight = foldInsight(next, event);
  return insight === next.insight ? next : { ...next, insight };
}

function applyCore(state: RoomState, event: Event): RoomState {
  // agent.usage is cost telemetry, one or two per model request. It moves
  // last_seq on (so the next event is not a gap) and nothing else: it must not
  // take a slot of the 50-entry activity window or show up as a raw line.
  if (event.type === "agent.usage") return { ...state, last_seq: event.seq };

  // A shadow room's log holds no agent events (the server keeps them in a separate log until
  // the reveal). If one arrives anyway, a bug or an older server, showing it would un-blind
  // the labeller, so only the seq moves on.
  if (state.shadow && event.type.startsWith("agent."))
    return { ...state, last_seq: event.seq };

  let next: RoomState = state;

  switch (event.type) {
    case "room.created": {
      const p = as<RoomCreatedPayload>(event);
      const driver =
        event.actor.kind === "human"
          ? {
              id: event.actor.id,
              name: event.actor.name,
              verified: event.actor.verified === true,
            }
          : state.driver;
      // Webhook-opened rooms carry the raw alert body here; a room created via
      // POST /api/rooms has no alert at all. alert_source is server-set and is
      // what the alert badge reads; the body-inferred source is only a label.
      const alertSourceField =
        typeof p.alert_source === "string" && p.alert_source !== ""
          ? p.alert_source
          : undefined;
      // Drift facts are read only when the server says the watcher opened the room (X1).
      const drift = driftFacts(p.alert, alertSourceField);
      const alert: AlertInfo | null =
        (p.alert === undefined || p.alert === null) &&
        alertSourceField === undefined
          ? null
          : {
              source: alertSource(p.alert),
              alert_source: alertSourceField,
              ...(drift.length > 0 ? { drift } : {}),
            };
      next = {
        ...state,
        title: stripBidi(p.title),
        severity: p.severity as RoomState["severity"],
        pipeline_id: p.pipeline_id,
        shadow: p.shadow ?? state.shadow,
        ...(p.origin ? { origin: p.origin } : {}),
        status: "open",
        opened_at: event.at,
        driver,
        alert,
        // U: a setup room's alert is its SetupRequest; the setup member starts with no runner.
        ...(p.mode === "setup"
          ? { setup: newSetupState(parseSetupRequest(p.alert)) }
          : {}),
        members:
          event.actor.kind === "human"
            ? [
                {
                  id: event.actor.id,
                  name: event.actor.name,
                  verified: event.actor.verified === true,
                  role: "driver",
                },
              ]
            : state.members,
      };
      break;
    }

    case "room.escalated": {
      // The replay folds room.created's first severity; without this the header would drop back to it (X3).
      const p = as<RoomEscalatedPayload>(event);
      const rank = (s: string) =>
        /^sev[1-4]$/.test(s) ? Number(s.slice(3)) : 99;
      if (rank(p.severity) < rank(state.severity))
        next = { ...state, severity: p.severity as RoomState["severity"] };
      break;
    }

    case "room.closed": {
      const p = as<RoomClosedPayload>(event);
      next = {
        ...state,
        status: "closed",
        closed_at: event.at,
        resolution: p.resolution,
        closed_by: event.actor,
        handoffs: voidPending(state.handoffs, () => true),
        // The server drafts after room.closed (at most 3 min). A shadow room gets no draft.
        postmortem:
          state.postmortem ?? (state.shadow ? null : { status: "drafting" }),
      };
      break;
    }

    case "postmortem.drafted": {
      const p = as<PostmortemDraftedPayload>(event);
      next = {
        ...state,
        postmortem: {
          status: "drafted",
          source: p.source,
          summary_ref: p.summary_ref,
          at: event.at,
        },
      };
      break;
    }

    case "postmortem.failed": {
      const p = as<PostmortemFailedPayload>(event);
      next = { ...state, postmortem: { status: "failed", reason: p.reason } };
      break;
    }

    case "agent.status": {
      const p = as<AgentStatusPayload>(event);
      next = {
        ...state,
        agent_status: { state: p.state, detail: p.detail },
        // U: the agent went back to work, so the setup question no longer asks (server Apply).
        ...(state.setup?.blocked && p.state !== "idle"
          ? { setup: { ...state.setup, blocked: undefined } }
          : {}),
      };
      break;
    }

    case "agent.step.started": {
      const p = as<AgentStepStartPayload>(event);
      if (state.steps.some((s) => s.id === p.step_id)) break;
      next = {
        ...state,
        steps: [
          ...state.steps,
          { id: p.step_id, title: p.title, tool_calls: [], messages: [] },
        ],
      };
      break;
    }

    case "agent.tool_call": {
      const p = as<AgentToolCallPayload>(event);
      next = {
        ...state,
        steps: updateStep(state.steps, p.step_id, (s) => {
          if (s.tool_calls.some((t) => t.tool_use_id === p.tool_use_id))
            return s;
          return {
            ...s,
            tool_calls: [
              ...s.tool_calls,
              { tool_use_id: p.tool_use_id, tool: p.tool, input: p.input },
            ],
          };
        }),
      };
      break;
    }

    case "agent.tool_result": {
      const p = as<AgentToolResultPayload>(event);
      next = {
        ...state,
        steps: updateStep(state.steps, p.step_id, (s) => ({
          ...s,
          tool_calls: s.tool_calls.map((t) =>
            t.tool_use_id === p.tool_use_id
              ? {
                  ...t,
                  tool: p.tool,
                  summary: p.summary,
                  data: p.data,
                  is_error: p.is_error,
                  truncated: p.truncated,
                  took_ms: p.took_ms,
                  provenance: p.provenance,
                }
              : t,
          ),
        })),
      };
      break;
    }

    case "agent.message": {
      const p = as<AgentMessagePayload>(event);
      if (p.reply_to) {
        next = {
          ...state,
          comments: state.comments.map((c) =>
            c.id === p.reply_to
              ? {
                  ...c,
                  replies: [...c.replies, { text: p.text, at: event.at }],
                }
              : c,
          ),
        };
      } else if (p.step_id) {
        next = {
          ...state,
          steps: updateStep(state.steps, p.step_id, (s) => ({
            ...s,
            messages: [...s.messages, { text: p.text, at: event.at }],
          })),
        };
      } else {
        next = {
          ...state,
          messages: [...state.messages, { text: p.text, at: event.at }],
        };
      }
      break;
    }

    case "agent.triage": {
      // Last wins: a re-fire runs the pass again with new numbers.
      const p = as<AgentTriagePayload>(event);
      next = {
        ...state,
        triage: {
          status: p.status,
          reason: p.reason,
          headline: p.headline,
          suspected_layer: p.suspected_layer,
          confidence: p.confidence,
          evidence_refs: p.evidence_refs ?? [],
          next_check: p.next_check,
          at: event.at,
        },
      };
      break;
    }

    case "agent.hypothesis": {
      const p = as<AgentHypothesisPayload>(event);
      next = {
        ...state,
        // confirmed is derived from the verdicts, never from the agent's flag; a new
        // hypothesis starts over with no verdicts.
        hypothesis: {
          text: p.text,
          based_on_steps: p.based_on_steps ?? [],
          confidence: p.confidence,
          confirmed: false,
          unbacked_terms: p.unbacked_terms ?? [],
          seq: event.seq,
          verdicts: [],
        },
        hypothesis_history: pushConfidence(
          state.hypothesis_history,
          event,
          p.confidence,
        ),
      };
      break;
    }

    case "hypothesis.verdict": {
      const p = as<HypothesisVerdictPayload>(event);
      const h = state.hypothesis;
      // A verdict on a replaced hypothesis is stale.
      if (!h || p.hypothesis_seq !== h.seq) break;
      const verdicts = [
        ...(h.verdicts ?? []).filter((v) => v.by.id !== event.actor.id),
        {
          verdict: p.verdict,
          by: event.actor,
          note: p.note || undefined,
          at: event.at,
        },
      ];
      next = {
        ...state,
        hypothesis: { ...h, verdicts, confirmed: derivedConfirmed(verdicts) },
      };
      break;
    }

    case "approval.nudged": {
      const p = as<ApprovalNudgedPayload>(event);
      next = {
        ...state,
        nudges: {
          ...state.nudges,
          [`${p.proposal_id}/${p.layer}`]: { at: event.at, by: event.actor },
        },
      };
      break;
    }

    case "comment.added": {
      const p = as<CommentAddedPayload>(event);
      next = {
        ...state,
        comments: [
          ...state.comments,
          {
            id: p.comment_id,
            step_id: p.step_id,
            text: stripBidi(p.text),
            mentions_agent: p.mentions_agent,
            author: event.actor,
            at: event.at,
            replies: [],
          },
        ],
      };
      break;
    }

    case "steer.suggested": {
      const p = as<SteerSuggestedPayload>(event);
      next = {
        ...state,
        suggestions: [
          ...state.suggestions,
          {
            suggestion_id: p.suggestion_id,
            from: event.actor,
            text: stripBidi(p.text),
            status: "pending" as const,
            at: event.at,
          },
        ],
      };
      break;
    }

    case "steer.sent": {
      const p = as<SteerSentPayload>(event);
      if (p.suggestion_id) {
        next = {
          ...state,
          // Kept, not dropped: the room shows what became of each suggestion.
          suggestions: state.suggestions.map((s) =>
            s.suggestion_id === p.suggestion_id
              ? {
                  ...s,
                  status: "accepted" as const,
                  sent_by: event.actor,
                  sent_at: event.at,
                }
              : s,
          ),
        };
      }
      break;
    }

    case "steer.declined": {
      const p = as<SteerDeclinedPayload>(event);
      next = {
        ...state,
        suggestions: state.suggestions.map((s) =>
          s.suggestion_id === p.suggestion_id
            ? {
                ...s,
                status: "declined" as const,
                declined_by: event.actor,
                declined_at: event.at,
                reason: p.reason,
              }
            : s,
        ),
      };
      break;
    }

    case "control.requested": {
      if (state.control_requests.some((r) => r.from.id === event.actor.id))
        break;
      next = {
        ...state,
        control_requests: [...state.control_requests, { from: event.actor }],
      };
      break;
    }

    case "control.granted": {
      const p = as<ControlGrantedPayload>(event);
      const members = state.members.some((m) => m.id === p.to.id)
        ? state.members.map((m) =>
            m.id === p.to.id
              ? { ...m, verified: p.to.verified === true, role: "driver" }
              : m.role === "driver"
                ? { ...m, role: "watching" }
                : m,
          )
        : [
            ...state.members.map((m) =>
              m.role === "driver" ? { ...m, role: "watching" } : m,
            ),
            {
              id: p.to.id,
              name: p.to.name,
              verified: p.to.verified === true,
              role: "driver",
            },
          ];
      next = {
        ...state,
        driver: {
          id: p.to.id,
          name: p.to.name,
          verified: p.to.verified === true,
          lease_until: p.lease_until,
          ...(p.reason ? { reason: p.reason } : {}),
        },
        members,
        control_requests: state.control_requests.filter(
          (r) => r.from.id !== p.to.id,
        ),
        // A hand-off is accepted only while its creator drives (409 stale_handoff). The receiver's
        // own grant comes just before handoff.accepted, which then marks theirs accepted.
        handoffs: voidPending(state.handoffs, (h) => h.from_id !== p.to.id),
      };
      break;
    }

    case "control.lease_expired": {
      const p = as<ControlLeaseExpiredPayload>(event);
      const d = state.driver;
      // Only the signal for the lease the driver holds now counts; a stale one is history.
      if (
        !d ||
        !d.lease_until ||
        Date.parse(p.lease_until) !== Date.parse(d.lease_until)
      )
        break;
      next = { ...state, driver: { ...d, lease_expired: true } };
      break;
    }

    case "proposal.created": {
      const p = as<ProposalCreatedPayload>(event);
      // A second proposal.created with the same id (a replay over a snapshot, a retry) is
      // ignored: the first one stays, with the decisions it already collected.
      if (state.proposals.some((x) => x.proposal_id === p.proposal_id)) break;
      const approvals: Record<string, "pending"> = {};
      for (const layer of p.required_layers ?? []) approvals[layer] = "pending";
      next = {
        ...state,
        proposals: [
          ...state.proposals,
          {
            proposal_id: p.proposal_id,
            title: p.title,
            steps: p.steps ?? [],
            risk: p.risk,
            rollback: p.rollback,
            required_layers: p.required_layers ?? [],
            approvals,
            decisions: {},
            // The event's hash, else the snapshot's (an event written before the
            // hash existed has none; the server computed one for the snapshot).
            params_hash:
              p.params_hash || state.snapshot_hashes[p.proposal_id] || "",
            status: "open",
            executed: [],
            created_at: event.at,
            // Only the snapshot says so for a proposal that was already blocked when it was taken.
            ...(state.snapshot_blocked?.[p.proposal_id]
              ? { blocked: state.snapshot_blocked[p.proposal_id] }
              : {}),
            // Left off when every layer has an owner, so such a proposal folds as before P2b.
            ...(p.fallback_layers && p.fallback_layers.length > 0
              ? { fallback_layers: [...p.fallback_layers] }
              : {}),
            // X2/X6: only a non-fix class is kept, so a fix folds exactly as before.
            ...(p.class === "recovery" || p.class === "performance"
              ? { class: p.class }
              : {}),
            // P3c: only a driver-authored fix is marked, so the agent's fold exactly as before.
            ...(p.origin === "driver"
              ? { origin: "driver" as const, author: { ...event.actor } }
              : {}),
            // X8: only the snapshot knows what can be staged and whether a stage is owed.
            ...(state.snapshot_staging?.[p.proposal_id]
              ? {
                  stageable: state.snapshot_staging[p.proposal_id].stageable,
                  ...(state.snapshot_staging[p.proposal_id].staging_required
                    ? { staging_required: true }
                    : {}),
                }
              : {}),
          },
        ],
      };
      break;
    }

    // The preview is informational. The latest one wins (a refresh replaces the first).
    case "proposal.previewed": {
      const p = as<ProposalPreviewedPayload>(event);
      next = {
        ...state,
        proposals: state.proposals.map((prop) =>
          prop.proposal_id === p.proposal_id ? { ...prop, preview: p } : prop,
        ),
      };
      break;
    }

    // X8: a stage the driver started. It runs until a proposal.staged (or its expires_at, which the card
    // checks against its clock). A second one replaces the first, as the server only lets one run.
    case "proposal.stage_started": {
      const p = as<ProposalStageStartedPayload>(event);
      next = {
        ...state,
        proposals: state.proposals.map((prop) =>
          prop.proposal_id === p.proposal_id
            ? {
                ...prop,
                stage_running: {
                  by: event.actor,
                  started_at: event.at,
                  expires_at: p.expires_at,
                  steps: p.steps ?? [],
                  params_hash: p.params_hash,
                },
                // The steps of a stage are the stageable ones, so a snapshot that did not say still gets an answer.
                ...(prop.stageable === undefined
                  ? { stageable: p.steps ?? [] }
                  : {}),
              }
            : prop,
        ),
      };
      break;
    }

    // X8: the newest stage wins and ends the running one. A healthy stage of this very fix lifts
    // "staging required" at once; any other leaves it to the snapshot the hook asks for.
    case "proposal.staged": {
      const p = as<ProposalStagedPayload>(event);
      next = {
        ...state,
        proposals: state.proposals.map((prop) => {
          if (prop.proposal_id !== p.proposal_id) return prop;
          const rest = { ...prop };
          delete rest.stage_running;
          const lifted =
            prop.staging_required === true && stageLiftsRequirement(prop, p);
          if (lifted) {
            delete rest.staging_required;
            delete rest.staging_message;
          }
          return { ...rest, staged: p };
        }),
      };
      break;
    }

    // The policy countdown (P2b). Only the first schedule counts. The fold keeps it however late it gets, like the server's fold;
    // whether the card still shows it is a question of the clock (lib/countdown.ts, #652).
    case "proposal.scheduled": {
      const p = as<ProposalScheduledPayload>(event);
      next = {
        ...state,
        proposals: state.proposals.map((prop) =>
          prop.proposal_id === p.proposal_id &&
          prop.status === "open" &&
          !prop.schedule_seen
            ? {
                ...prop,
                schedule_seen: true,
                // A proposal someone already decided on gets no countdown (the server's foldScheduled).
                ...(Object.values(prop.approvals).every((v) => v === "pending")
                  ? {
                      scheduled: {
                        run_at: p.run_at,
                        cancel_until: p.cancel_until || p.run_at,
                        policy: p.policy,
                      },
                    }
                  : {}),
              }
            : prop,
        ),
      };
      break;
    }

    case "proposal.cancelled": {
      const p = as<ProposalCancelledPayload>(event);
      next = {
        ...state,
        proposals: state.proposals.map((prop) =>
          prop.proposal_id === p.proposal_id ? unschedule(prop) : prop,
        ),
      };
      break;
    }

    // A "repeat" (already verified, no checks) never overwrites a full verdict.
    case "agent.verification": {
      const p = as<AgentVerificationPayload>(event);
      next = {
        ...state,
        proposals: state.proposals.map((prop) => {
          if (prop.proposal_id !== p.proposal_id) return prop;
          if (p.repeat && prop.verification && !prop.verification.repeat)
            return prop;
          // X2: the counted verdict ends the polling; when it arrived is the end of "recovered in 3 min".
          const counted =
            prop.class === "recovery" && !p.repeat && p.outcome !== undefined;
          return {
            ...prop,
            verification: p,
            ...(counted
              ? { recovery: { ...prop.recovery, verified_at: event.at } }
              : {}),
          };
        }),
      };
      break;
    }

    // X2: progress of a recovery step's Verify. Only the newest poll is kept; a late or replayed
    // older poll of the same step never moves the count back.
    case "agent.recovery_check": {
      const p = as<AgentRecoveryCheckPayload>(event);
      next = {
        ...state,
        proposals: state.proposals.map((prop) => {
          if (prop.proposal_id !== p.proposal_id) return prop;
          const cur = prop.recovery?.check;
          if (cur && cur.step_index === p.step_index && cur.poll > p.poll)
            return prop;
          return { ...prop, recovery: { ...prop.recovery, check: p } };
        }),
      };
      break;
    }

    case "approval.given": {
      const p = as<ApprovalGivenPayload>(event);
      next = {
        ...state,
        proposals: state.proposals.map((prop) => {
          if (prop.proposal_id !== p.proposal_id) return prop;
          const approvals = {
            ...prop.approvals,
            [p.layer]: "approved" as const,
          };
          const allApproved = (prop.required_layers ?? []).every(
            (l) => approvals[l] === "approved",
          );
          // Any approval ends the countdown: the policy's own (it has started) or a person's.
          return {
            ...unschedule(prop),
            approvals,
            decisions: {
              ...prop.decisions,
              [p.layer]: {
                outcome: "approved" as const,
                by: event.actor,
                identity: p.identity,
                params_hash: p.params_hash,
                at: event.at,
              },
            },
            status: allApproved ? "approved" : "open",
            // The snapshot's `blocked` is not refreshed over the socket: an approval that completes the
            // set while write mode is off leaves nothing to run (docs/api.md, P1a), so mark it here.
            ...(allApproved && !prop.blocked && state.mode?.write === false
              ? { blocked: "write_mode_off" as const }
              : {}),
          };
        }),
      };
      break;
    }

    case "approval.rejected": {
      const p = as<ApprovalRejectedPayload>(event);
      next = {
        ...state,
        proposals: state.proposals.map((prop) =>
          prop.proposal_id === p.proposal_id
            ? {
                ...unschedule(prop),
                approvals: {
                  ...prop.approvals,
                  [p.layer]: "rejected" as const,
                },
                decisions: {
                  ...prop.decisions,
                  [p.layer]: {
                    outcome: "rejected" as const,
                    by: event.actor,
                    identity: p.identity,
                    params_hash: p.params_hash,
                    reason: p.reason,
                    at: event.at,
                  },
                },
                status: "rejected",
              }
            : prop,
        ),
      };
      break;
    }

    case "action.executed": {
      const p = as<ActionExecutedPayload>(event);
      next = {
        ...state,
        proposals: state.proposals.map((prop) =>
          prop.proposal_id === p.proposal_id
            ? {
                ...prop,
                executed: [...prop.executed, p],
                // Q7: the step opened a pull request; nothing changes until a reviewer merges it.
                ...(p.pull_request
                  ? {
                      pull_requests: {
                        ...prop.pull_requests,
                        [p.step_index]: {
                          repo: p.pull_request.repo,
                          number: p.pull_request.number,
                          url: p.pull_request.url,
                          branch: p.pull_request.branch,
                          head_sha: p.pull_request.head_sha,
                          state: "open",
                          head_changed: false,
                          updated_at: event.at,
                        },
                      },
                    }
                  : {}),
                // X2: the clock of "recovered in 3 min" starts when the last step finished ok.
                ...(prop.class === "recovery" && p.ok
                  ? { recovery: { ...prop.recovery, ran_at: event.at } }
                  : {}),
              }
            : prop,
        ),
        // U: the setup stepper advances past the step the plan is on.
        ...(state.setup
          ? {
              setup: advanceSetupStep({
                ...state.setup,
                executed: {
                  ...state.setup.executed,
                  [p.step_index]: {
                    ok: p.ok,
                    at: event.at,
                    ...(p.outcome_unknown ? { outcome_unknown: true } : {}),
                  },
                },
              }),
            }
          : {}),
      };
      break;
    }

    // Q7: one observed change of a fix PR. The newest event wins whole: the follower writes the full
    // state each time, so nothing of an older one is kept but head_sha when the event has none.
    case "pull_request.updated": {
      const p = as<PullRequestUpdatedPayload>(event);
      next = {
        ...state,
        proposals: state.proposals.map((prop) => {
          if (prop.proposal_id !== p.proposal_id) return prop;
          const cur = prop.pull_requests?.[p.step_index];
          return {
            ...prop,
            pull_requests: {
              ...prop.pull_requests,
              [p.step_index]: {
                repo: p.repo ?? cur?.repo ?? "",
                number: p.number ?? cur?.number ?? 0,
                url: p.url ?? cur?.url ?? "",
                branch: p.branch ?? cur?.branch ?? "",
                head_sha: p.head_sha || cur?.head_sha || "",
                state: p.state,
                head_changed: p.head_changed === true,
                checks: p.checks ?? cur?.checks,
                ...(p.merge_sha ? { merge_sha: p.merge_sha } : {}),
                ...(p.merged_at ? { merged_at: p.merged_at } : {}),
                updated_at: event.at,
              },
            },
          };
        }),
      };
      break;
    }

    case "handoff.requested": {
      const p = as<HandoffRequestedPayload>(event);
      next = {
        ...state,
        handoff_request: {
          request_id: p.request_id,
          to: p.to,
          stay_as_watcher: p.stay_as_watcher,
          expires_at: p.expires_at,
        },
        handoff_failure: null,
      };
      break;
    }

    case "handoff.failed": {
      const p = as<HandoffFailedPayload>(event);
      next = {
        ...state,
        handoff_request:
          state.handoff_request?.request_id === p.request_id
            ? null
            : state.handoff_request,
        handoff_failure: { request_id: p.request_id, reason: p.reason },
      };
      break;
    }

    case "handoff.created": {
      const p = as<HandoffCreatedPayload>(event);
      next = {
        ...state,
        // Ends the request it completes; an event from before R0 carries no request_id and ends any.
        handoff_request:
          !p.request_id || state.handoff_request?.request_id === p.request_id
            ? null
            : state.handoff_request,
        handoff_failure: null,
        // A newer hand-off voids the older ones: the driver can redo a hand-off to someone else.
        handoffs: [
          ...voidPending(state.handoffs, () => true),
          {
            handoff_id: p.handoff_id,
            to: p.to,
            summary: p.summary,
            stay_as_watcher: p.stay_as_watcher,
            status: "pending" as const,
            from_id: state.driver?.id,
            from: event.actor,
            created_at: event.at,
          },
        ],
      };
      break;
    }

    case "handoff.accepted": {
      const p = as<HandoffAcceptedPayload>(event);
      const accepted = state.handoffs.find(
        (h) => h.handoff_id === p.handoff_id,
      );
      next = {
        ...state,
        handoffs: state.handoffs.map((h) =>
          h.handoff_id === p.handoff_id
            ? {
                ...h,
                status: "accepted" as const,
                accepted_at: event.at,
                accepted_by: event.actor,
              }
            : h,
        ),
        // The server emits control.granted (driver + lease) immediately before
        // handoff.accepted; keep that lease rather than replacing it with a
        // lease-less driver object. Only move the driver here as a fallback for
        // a log that never saw the control.granted.
        driver: accepted
          ? state.driver?.id === accepted.to.id
            ? state.driver
            : {
                id: accepted.to.id,
                name: accepted.to.name,
                verified: accepted.to.verified === true,
              }
          : state.driver,
        members: accepted
          ? state.members.some((m) => m.id === accepted.to.id)
            ? state.members.map((m) =>
                m.id === accepted.to.id
                  ? { ...m, role: "driver" }
                  : m.role === "driver"
                    ? { ...m, role: "watching" }
                    : m,
              )
            : [
                ...state.members.map((m) =>
                  m.role === "driver" ? { ...m, role: "watching" } : m,
                ),
                {
                  id: accepted.to.id,
                  name: accepted.to.name,
                  verified: accepted.to.verified === true,
                  role: "driver",
                },
              ]
          : state.members,
      };
      break;
    }

    case "room.labelled": {
      next = { ...state, labelled: as<RoomLabelledPayload>(event) };
      break;
    }

    case "checklist.step_done": {
      const p = as<ChecklistStepDonePayload>(event);
      const key = checklistKey(p.tool_use_id, p.hit, p.item);
      const done = { ...state.checklist_done };
      if (p.done) done[key] = { by: event.actor, at: event.at, note: p.note };
      else delete done[key];
      next = { ...state, checklist_done: done };
      break;
    }

    case "trace.pinned": {
      const p = as<TracePinnedPayload>(event);
      next = {
        ...state,
        trace: { table: p.table, key: p.key, stages: p.stages ?? [] },
      };
      break;
    }

    case "shadow.revealed":
      next = { ...state, shadow_revealed: true };
      break;

    case "shadow.labelled":
      next = { ...state, shadow_labelled: true };
      break;

    // ---- U wave: setup rooms ----

    case "setup.runner_attached": {
      if (!state.setup) break;
      const p = as<SetupRunnerAttachedPayload>(event);
      next = {
        ...state,
        setup: {
          ...state.setup,
          runner: {
            label: p.runner_label,
            connected: true,
            tools: p.tools ?? {},
          },
        },
      };
      break;
    }

    case "setup.runner_lost": {
      // No runner was ever attached: nothing to lose (the server fold ignores
      // this case too, and fabricating a record would hide the pairing card).
      if (!state.setup?.runner) break;
      const p = as<SetupRunnerLostPayload>(event);
      next = {
        ...state,
        setup: {
          ...state.setup,
          runner: {
            label: p.runner_label || state.setup.runner.label,
            connected: false,
            tools: state.setup.runner.tools,
          },
        },
      };
      break;
    }

    case "setup.plan": {
      if (!state.setup) break;
      const p = as<SetupPlanPayload>(event);
      const phases = p.phases ?? [];
      next = {
        ...state,
        setup: advanceSetupStep({
          ...state.setup,
          plan_id: p.plan_id,
          phases,
          executed: {},
          awaiting_change: undefined,
        }),
      };
      break;
    }

    case "setup.blocked": {
      if (!state.setup) break;
      const p = as<SetupBlockedPayload>(event);
      next = {
        ...state,
        setup: {
          ...state.setup,
          blocked: {
            reason: p.reason,
            question: p.question,
            ...(p.step_index !== undefined ? { step_index: p.step_index } : {}),
          },
        },
      };
      break;
    }

    case "setup.target_registered": {
      if (!state.setup) break;
      const p = as<SetupTargetRegisteredPayload>(event);
      // A class only rises, here as in target_classed: the server fold uses
      // classAtLeast when the address is already known.
      const targets = state.setup.targets.some((t) => t.address === p.address)
        ? state.setup.targets.map((t) =>
            t.address === p.address
              ? {
                  ...t,
                  kind: p.kind,
                  class:
                    classRank(p.class) > classRank(t.class) ? p.class : t.class,
                }
              : t,
          )
        : [
            ...state.setup.targets,
            { kind: p.kind, address: p.address, class: p.class },
          ];
      next = { ...state, setup: { ...state.setup, targets } };
      break;
    }

    case "setup.target_classed": {
      if (!state.setup) break;
      const p = as<SetupTargetClassedPayload>(event);
      next = {
        ...state,
        setup: {
          ...state.setup,
          targets: state.setup.targets.map((t) =>
            t.address === p.target && classRank(p.class) > classRank(t.class)
              ? { ...t, class: p.class }
              : t,
          ),
        },
      };
      break;
    }

    case "setup.pipeline_extended": {
      if (!state.setup) break;
      const p = as<SetupPipelineExtendedPayload>(event);
      const add = (list: string[], more?: string[]) =>
        [...new Set([...list, ...(more ?? [])])].filter((name) => name !== "");
      next = {
        ...state,
        setup: {
          ...state.setup,
          provisional: {
            ...state.setup.provisional,
            tables: add(state.setup.provisional.tables, p.add_tables),
            connectors: add(
              state.setup.provisional.connectors,
              p.add_connectors,
            ),
          },
        },
      };
      break;
    }

    case "setup.maintenance_window": {
      if (!state.setup) break;
      const p = as<SetupMaintenanceWindowPayload>(event);
      next = {
        ...state,
        setup: {
          ...state.setup,
          maintenance_window: { start: p.start, end: p.end },
        },
      };
      break;
    }

    case "setup.secret_set": {
      if (!state.setup) break;
      const p = as<SetupSecretSetPayload>(event);
      // The name, never the value: the payload has none and the web keeps none.
      if (!p.name) break;
      const names = state.setup.secret_names.includes(p.name)
        ? state.setup.secret_names
        : [...state.setup.secret_names, p.name];
      next = { ...state, setup: { ...state.setup, secret_names: names } };
      break;
    }

    case "setup.credentials_registered": {
      if (!state.setup) break;
      const p = as<SetupCredentialsRegisteredPayload>(event);
      next = {
        ...state,
        setup: {
          ...state.setup,
          credentials: {
            names: p.names ?? [],
            runner_label: p.runner_label ?? "",
          },
        },
      };
      break;
    }

    case "setup.verified": {
      if (!state.setup) break;
      const p = as<SetupVerifiedPayload>(event);
      // A run of an older plan says nothing about this one.
      if (state.setup.plan_id && p.plan_id !== state.setup.plan_id) break;
      // The ladder ran to a verdict, so it no longer waits on a change.
      next = {
        ...state,
        setup: {
          ...state.setup,
          verified: p.rungs ?? [],
          awaiting_change: undefined,
        },
      };
      break;
    }

    case "setup.awaiting_change": {
      if (!state.setup) break;
      const p = as<SetupAwaitingChangePayload>(event);
      next = { ...state, setup: { ...state.setup, awaiting_change: p } };
      break;
    }

    case "pipeline.registered": {
      if (!state.setup) break;
      const p = as<PipelineRegisteredPayload>(event);
      next = { ...state, setup: { ...state.setup, watched_by: p.watched_by } };
      break;
    }

    default:
      break;
  }

  // Every human actor is a room member, mirroring the server's snapshot.Apply
  // (which upserts the actor as "watching" before handling the event). Applied
  // after the switch so no case's members rebuild can discard it.
  const members =
    event.actor.kind === "human"
      ? upsertMember(next.members, event.actor)
      : next.members;

  return {
    ...next,
    members,
    last_seq: event.seq,
    activity: [
      {
        seq: event.seq,
        at: event.at,
        actor: event.actor,
        // The driver and hypothesis come from the pre-event state (a renewal is not a
        // take-over; raised needs the old confidence). Step and suggestion names come
        // from the post-event state so a step that starts now already has its number.
        ...describeEvent(event, describeContext(state, next)),
      },
      ...next.activity,
    ].slice(0, ACTIVITY_WINDOW),
  };
}
