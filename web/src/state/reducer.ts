// The single reducer that folds one room event into UI state, mirroring A5's
// Apply(snapshot, event) semantics. Dedupe (seq <= last_seq) happens here; gap
// filling (seq > last_seq + 1) is the transport's job (see hooks/useRoom.ts).
import type {
  ActionExecutedPayload,
  AgentVerificationPayload,
  ProposalPreviewedPayload,
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
  ApprovalNudgedPayload,
  ProposalCreatedPayload,
  RoomClosedPayload,
  RoomCreatedPayload,
  RoomLabelledPayload,
  SteerDeclinedPayload,
  SteerSentPayload,
  SteerSuggestedPayload,
  TracePinnedPayload,
} from '../api/types'
import { describeEvent } from './describe'
import type { DescribeContext } from './describe'
import { alertSource } from '../lib/alerts'
import { runbookHits } from '../lib/f8'
import { checklistKey } from './types'
import type { HypothesisHistoryEntry, InsightState, MemberState, RoomState, StepState, VerdictState } from './types'

function as<T>(e: Event): T {
  return e.payload as T
}

// True when the event's seq skips ahead — the transport must fetch the missing
// events (/events?after=lastSeq) before applying.
export function detectGap(lastSeq: number, event: Event): boolean {
  return event.seq > lastSeq + 1
}

function updateStep(
  steps: StepState[],
  stepId: string,
  fn: (s: StepState) => StepState,
): StepState[] {
  return steps.map((s) => (s.id === stepId ? fn(s) : s))
}

// upsertMember mirrors the server's snapshot.Apply: every human actor is a room
// member. New actors join as "watching"; an actor already in the list is never
// downgraded (a driver who acts stays the driver).
function upsertMember(members: MemberState[], actor: Actor): MemberState[] {
  const existing = members.find((m) => m.id === actor.id)
  if (!existing) return [...members, { id: actor.id, name: actor.name, role: 'watching' }]
  if (existing.name === actor.name) return members
  return members.map((m) => (m.id === actor.id ? { ...m, name: actor.name } : m))
}

// foldInsight mirrors the server's insight fold (internal/room/insight.go): same
// rules, so the live chip and GET /api/rooms agree. A shadow room stays at zero.
function foldInsight(state: RoomState, event: Event): InsightState {
  const cur = state.insight
  if (state.shadow) return cur
  switch (event.type) {
    case 'agent.step.started':
      return { ...cur, steps: cur.steps + 1 }
    case 'agent.tool_call':
      return { ...cur, tool_calls: cur.tool_calls + 1 }
    case 'agent.tool_result':
      return as<AgentToolResultPayload>(event).is_error ? { ...cur, tool_errors: cur.tool_errors + 1 } : cur
    case 'agent.hypothesis': {
      if (cur.first_hypothesis_at) return cur
      const secs = Math.floor((Date.parse(event.at) - Date.parse(state.opened_at)) / 1000)
      return {
        ...cur,
        first_hypothesis_at: event.at,
        time_to_first_hypothesis_s: Number.isFinite(secs) ? Math.max(0, secs) : 0,
      }
    }
    case 'agent.usage': {
      const p = as<AgentUsagePayload>(event)
      const u = cur.usage
      const priced = typeof p.cost_usd === 'number'
      return {
        ...cur,
        usage: {
          calls: u.calls + 1,
          unpriced_calls: u.unpriced_calls + (priced ? 0 : 1),
          tokens:
            u.tokens + p.input_tokens + p.cache_creation_input_tokens + p.cache_read_input_tokens + p.output_tokens,
          cost_usd: priced ? (u.cost_usd ?? 0) + (p.cost_usd as number) : u.cost_usd,
          latency_ms_total: u.latency_ms_total + p.latency_ms,
          cache_read_tokens: u.cache_read_tokens + p.cache_read_input_tokens,
          models: { ...u.models, [p.model]: (u.models[p.model] ?? 0) + 1 },
        },
      }
    }
    default:
      return cur
  }
}

const ACTIVITY_WINDOW = 50

function describeContext(before: RoomState, after: RoomState): DescribeContext {
  const stepIndex: Record<string, number> = {}
  const stepTitles: Record<string, string> = {}
  after.steps.forEach((s, i) => {
    stepIndex[s.id] = i + 1
    stepTitles[s.id] = s.title
  })
  const proposalTitles: Record<string, string> = {}
  for (const p of after.proposals) proposalTitles[p.proposal_id] = p.title
  const suggesters: Record<string, string> = {}
  for (const s of after.suggestions) suggesters[s.suggestion_id] = s.from.name
  return {
    driverBefore: before.driver?.id ?? null,
    stepTitles,
    proposalTitles,
    stepIndex,
    suggesters,
    confidenceBefore: before.hypothesis?.confidence ?? null,
    runbookItem: (id, hit, item) => runbookItemOf(before, id, hit, item),
  }
}

function derivedConfirmed(verdicts: VerdictState[]): boolean {
  return verdicts.some((v) => v.verdict === 'confirmed') && !verdicts.some((v) => v.verdict === 'disputed')
}

const HISTORY_CAP = 20

// The confidence path: a repeat of the same confidence is not a step on it.
function pushConfidence(history: HypothesisHistoryEntry[], event: Event, confidence: string): HypothesisHistoryEntry[] {
  if (history.length > 0 && history[history.length - 1].confidence === confidence) return history
  return [...history, { seq: event.seq, at: event.at, confidence }].slice(-HISTORY_CAP)
}

// The text and section of a runbook item, looked up in the runbook_lookup result already in the
// state (used for the activity line of a tick).
function runbookItemOf(state: RoomState, toolUseId: string, hit: number, item: number) {
  for (const s of state.steps) {
    const t = s.tool_calls.find((c) => c.tool_use_id === toolUseId && c.tool === 'runbook_lookup')
    const h = t ? runbookHits(t.data)[hit] : undefined
    if (h && h.items[item] !== undefined) return { section: h.section, text: h.items[item] }
  }
  return null
}

export function applyEvent(state: RoomState, event: Event): RoomState {
  if (event.seq <= state.last_seq) return state
  const next = applyCore(state, event)
  const insight = foldInsight(next, event)
  return insight === next.insight ? next : { ...next, insight }
}

function applyCore(state: RoomState, event: Event): RoomState {

  // agent.usage is cost telemetry, one or two per model request. It moves
  // last_seq on (so the next event is not a gap) and nothing else: it must not
  // take a slot of the 50-entry activity window or show up as a raw line.
  if (event.type === 'agent.usage') return { ...state, last_seq: event.seq }

  // A shadow room's log holds no agent events (the server keeps them in a separate log until
  // the reveal). If one arrives anyway, a bug or an older server, showing it would un-blind
  // the labeller, so only the seq moves on.
  if (state.shadow && event.type.startsWith('agent.')) return { ...state, last_seq: event.seq }

  let next: RoomState = state

  switch (event.type) {
    case 'room.created': {
      const p = as<RoomCreatedPayload>(event)
      const driver =
        event.actor.kind === 'human'
          ? { id: event.actor.id, name: event.actor.name }
          : state.driver
      // Webhook-opened rooms carry the raw alert body here; a room created via
      // POST /api/rooms has no alert at all.
      const alert =
        p.alert === undefined || p.alert === null
          ? null
          : { source: alertSource(p.alert) }
      next = {
        ...state,
        title: p.title,
        severity: p.severity as RoomState['severity'],
        pipeline_id: p.pipeline_id,
        shadow: p.shadow ?? state.shadow,
        status: 'open',
        opened_at: event.at,
        driver,
        alert,
        members:
          event.actor.kind === 'human'
            ? [{ id: event.actor.id, name: event.actor.name, role: 'driver' }]
            : state.members,
      }
      break
    }

    case 'room.closed': {
      const p = as<RoomClosedPayload>(event)
      next = {
        ...state,
        status: 'closed',
        closed_at: event.at,
        resolution: p.resolution,
      }
      break
    }

    case 'agent.status': {
      const p = as<AgentStatusPayload>(event)
      next = { ...state, agent_status: { state: p.state, detail: p.detail } }
      break
    }

    case 'agent.step.started': {
      const p = as<AgentStepStartPayload>(event)
      if (state.steps.some((s) => s.id === p.step_id)) break
      next = {
        ...state,
        steps: [
          ...state.steps,
          { id: p.step_id, title: p.title, tool_calls: [], messages: [] },
        ],
      }
      break
    }

    case 'agent.tool_call': {
      const p = as<AgentToolCallPayload>(event)
      next = {
        ...state,
        steps: updateStep(state.steps, p.step_id, (s) => {
          if (s.tool_calls.some((t) => t.tool_use_id === p.tool_use_id)) return s
          return {
            ...s,
            tool_calls: [
              ...s.tool_calls,
              { tool_use_id: p.tool_use_id, tool: p.tool, input: p.input },
            ],
          }
        }),
      }
      break
    }

    case 'agent.tool_result': {
      const p = as<AgentToolResultPayload>(event)
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
                }
              : t,
          ),
        })),
      }
      break
    }

    case 'agent.message': {
      const p = as<AgentMessagePayload>(event)
      if (p.reply_to) {
        next = {
          ...state,
          comments: state.comments.map((c) =>
            c.id === p.reply_to
              ? { ...c, replies: [...c.replies, { text: p.text, at: event.at }] }
              : c,
          ),
        }
      } else if (p.step_id) {
        next = {
          ...state,
          steps: updateStep(state.steps, p.step_id, (s) => ({
            ...s,
            messages: [...s.messages, { text: p.text, at: event.at }],
          })),
        }
      } else {
        next = {
          ...state,
          messages: [...state.messages, { text: p.text, at: event.at }],
        }
      }
      break
    }

    case 'agent.triage': {
      // Last wins: a re-fire runs the pass again with new numbers.
      const p = as<AgentTriagePayload>(event)
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
      }
      break
    }

    case 'agent.hypothesis': {
      const p = as<AgentHypothesisPayload>(event)
      next = {
        ...state,
        // confirmed is derived from the verdicts, never from the agent's flag; a new
        // hypothesis starts over with no verdicts.
        hypothesis: {
          text: p.text,
          based_on_steps: p.based_on_steps ?? [],
          confidence: p.confidence,
          confirmed: false,
          seq: event.seq,
          verdicts: [],
        },
        hypothesis_history: pushConfidence(state.hypothesis_history, event, p.confidence),
      }
      break
    }

    case 'hypothesis.verdict': {
      const p = as<HypothesisVerdictPayload>(event)
      const h = state.hypothesis
      // A verdict on a replaced hypothesis is stale.
      if (!h || p.hypothesis_seq !== h.seq) break
      const verdicts = [
        ...(h.verdicts ?? []).filter((v) => v.by.id !== event.actor.id),
        { verdict: p.verdict, by: event.actor, note: p.note || undefined, at: event.at },
      ]
      next = { ...state, hypothesis: { ...h, verdicts, confirmed: derivedConfirmed(verdicts) } }
      break
    }

    case 'approval.nudged': {
      const p = as<ApprovalNudgedPayload>(event)
      next = {
        ...state,
        nudges: { ...state.nudges, [`${p.proposal_id}/${p.layer}`]: { at: event.at, by: event.actor } },
      }
      break
    }

    case 'comment.added': {
      const p = as<CommentAddedPayload>(event)
      next = {
        ...state,
        comments: [
          ...state.comments,
          {
            id: p.comment_id,
            step_id: p.step_id,
            text: p.text,
            mentions_agent: p.mentions_agent,
            author: event.actor,
            at: event.at,
            replies: [],
          },
        ],
      }
      break
    }

    case 'steer.suggested': {
      const p = as<SteerSuggestedPayload>(event)
      next = {
        ...state,
        suggestions: [
          ...state.suggestions,
          {
            suggestion_id: p.suggestion_id,
            from: event.actor,
            text: p.text,
            status: 'pending' as const,
            at: event.at,
          },
        ],
      }
      break
    }

    case 'steer.sent': {
      const p = as<SteerSentPayload>(event)
      if (p.suggestion_id) {
        next = {
          ...state,
          // Kept, not dropped: the room shows what became of each suggestion.
          suggestions: state.suggestions.map((s) =>
            s.suggestion_id === p.suggestion_id
              ? { ...s, status: 'accepted' as const, sent_by: event.actor, sent_at: event.at }
              : s,
          ),
        }
      }
      break
    }

    case 'steer.declined': {
      const p = as<SteerDeclinedPayload>(event)
      next = {
        ...state,
        suggestions: state.suggestions.map((s) =>
          s.suggestion_id === p.suggestion_id
            ? { ...s, status: 'declined' as const, declined_by: event.actor, declined_at: event.at, reason: p.reason }
            : s,
        ),
      }
      break
    }

    case 'control.requested': {
      if (state.control_requests.some((r) => r.from.id === event.actor.id)) break
      next = {
        ...state,
        control_requests: [...state.control_requests, { from: event.actor }],
      }
      break
    }

    case 'control.granted': {
      const p = as<ControlGrantedPayload>(event)
      const members = state.members.some((m) => m.id === p.to.id)
        ? state.members.map((m) =>
            m.id === p.to.id
              ? { ...m, role: 'driver' }
              : m.role === 'driver'
                ? { ...m, role: 'watching' }
                : m,
          )
        : [
            ...state.members.map((m) =>
              m.role === 'driver' ? { ...m, role: 'watching' } : m,
            ),
            { id: p.to.id, name: p.to.name, role: 'driver' },
          ]
      next = {
        ...state,
        driver: { id: p.to.id, name: p.to.name, lease_until: p.lease_until },
        members,
        control_requests: state.control_requests.filter((r) => r.from.id !== p.to.id),
      }
      break
    }

    case 'control.lease_expired': {
      const p = as<ControlLeaseExpiredPayload>(event)
      const d = state.driver
      // Only the signal for the lease the driver holds now counts; a stale one is history.
      if (!d || !d.lease_until || Date.parse(p.lease_until) !== Date.parse(d.lease_until)) break
      next = { ...state, driver: { ...d, lease_expired: true } }
      break
    }

    case 'proposal.created': {
      const p = as<ProposalCreatedPayload>(event)
      // A second proposal.created with the same id (a replay over a snapshot, a retry) is
      // ignored: the first one stays, with the decisions it already collected.
      if (state.proposals.some((x) => x.proposal_id === p.proposal_id)) break
      const approvals: Record<string, 'pending'> = {}
      for (const layer of p.required_layers ?? []) approvals[layer] = 'pending'
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
            params_hash: p.params_hash || state.snapshot_hashes[p.proposal_id] || '',
            status: 'open',
            executed: [],
            created_at: event.at,
          },
        ],
      }
      break
    }

    // The preview is informational. The latest one wins (a refresh replaces the first).
    case 'proposal.previewed': {
      const p = as<ProposalPreviewedPayload>(event)
      next = {
        ...state,
        proposals: state.proposals.map((prop) => (prop.proposal_id === p.proposal_id ? { ...prop, preview: p } : prop)),
      }
      break
    }

    // A "repeat" (already verified, no checks) never overwrites a full verdict.
    case 'agent.verification': {
      const p = as<AgentVerificationPayload>(event)
      next = {
        ...state,
        proposals: state.proposals.map((prop) => {
          if (prop.proposal_id !== p.proposal_id) return prop
          if (p.repeat && prop.verification && !prop.verification.repeat) return prop
          return { ...prop, verification: p }
        }),
      }
      break
    }

    case 'approval.given': {
      const p = as<ApprovalGivenPayload>(event)
      next = {
        ...state,
        proposals: state.proposals.map((prop) => {
          if (prop.proposal_id !== p.proposal_id) return prop
          const approvals = { ...prop.approvals, [p.layer]: 'approved' as const }
          const allApproved = (prop.required_layers ?? []).every(
            (l) => approvals[l] === 'approved',
          )
          return {
            ...prop,
            approvals,
            decisions: {
              ...prop.decisions,
              [p.layer]: {
                outcome: 'approved' as const,
                by: event.actor,
                identity: p.identity,
                params_hash: p.params_hash,
                at: event.at,
              },
            },
            status: allApproved ? 'approved' : 'open',
          }
        }),
      }
      break
    }

    case 'approval.rejected': {
      const p = as<ApprovalRejectedPayload>(event)
      next = {
        ...state,
        proposals: state.proposals.map((prop) =>
          prop.proposal_id === p.proposal_id
            ? {
                ...prop,
                approvals: { ...prop.approvals, [p.layer]: 'rejected' as const },
                decisions: {
                  ...prop.decisions,
                  [p.layer]: {
                    outcome: 'rejected' as const,
                    by: event.actor,
                    identity: p.identity,
                    params_hash: p.params_hash,
                    reason: p.reason,
                    at: event.at,
                  },
                },
                status: 'rejected',
              }
            : prop,
        ),
      }
      break
    }

    case 'action.executed': {
      const p = as<ActionExecutedPayload>(event)
      next = {
        ...state,
        proposals: state.proposals.map((prop) =>
          prop.proposal_id === p.proposal_id
            ? { ...prop, executed: [...prop.executed, p] }
            : prop,
        ),
      }
      break
    }

    case 'handoff.requested': {
      const p = as<HandoffRequestedPayload>(event)
      next = {
        ...state,
        handoff_request: {
          request_id: p.request_id,
          to: p.to,
          stay_as_watcher: p.stay_as_watcher,
          expires_at: p.expires_at,
        },
        handoff_failure: null,
      }
      break
    }

    case 'handoff.failed': {
      const p = as<HandoffFailedPayload>(event)
      next = {
        ...state,
        handoff_request: state.handoff_request?.request_id === p.request_id ? null : state.handoff_request,
        handoff_failure: { request_id: p.request_id, reason: p.reason },
      }
      break
    }

    case 'handoff.created': {
      const p = as<HandoffCreatedPayload>(event)
      next = {
        ...state,
        // Ends the request it completes; an event from before R0 carries no request_id and ends any.
        handoff_request:
          !p.request_id || state.handoff_request?.request_id === p.request_id ? null : state.handoff_request,
        handoff_failure: null,
        handoffs: [
          ...state.handoffs,
          {
            handoff_id: p.handoff_id,
            to: p.to,
            summary: p.summary,
            stay_as_watcher: p.stay_as_watcher,
            status: 'pending' as const,
          },
        ],
      }
      break
    }

    case 'handoff.accepted': {
      const p = as<HandoffAcceptedPayload>(event)
      const accepted = state.handoffs.find((h) => h.handoff_id === p.handoff_id)
      next = {
        ...state,
        handoffs: state.handoffs.map((h) =>
          h.handoff_id === p.handoff_id ? { ...h, status: 'accepted' as const } : h,
        ),
        // The server emits control.granted (driver + lease) immediately before
        // handoff.accepted; keep that lease rather than replacing it with a
        // lease-less driver object. Only move the driver here as a fallback for
        // a log that never saw the control.granted.
        driver: accepted
          ? state.driver?.id === accepted.to.id
            ? state.driver
            : { id: accepted.to.id, name: accepted.to.name }
          : state.driver,
        members: accepted
          ? state.members.some((m) => m.id === accepted.to.id)
            ? state.members.map((m) =>
                m.id === accepted.to.id
                  ? { ...m, role: 'driver' }
                  : m.role === 'driver'
                    ? { ...m, role: 'watching' }
                    : m,
              )
            : [
                ...state.members.map((m) =>
                  m.role === 'driver' ? { ...m, role: 'watching' } : m,
                ),
                { id: accepted.to.id, name: accepted.to.name, role: 'driver' },
              ]
          : state.members,
      }
      break
    }

    case 'room.labelled': {
      next = { ...state, labelled: as<RoomLabelledPayload>(event) }
      break
    }

    case 'checklist.step_done': {
      const p = as<ChecklistStepDonePayload>(event)
      const key = checklistKey(p.tool_use_id, p.hit, p.item)
      const done = { ...state.checklist_done }
      if (p.done) done[key] = { by: event.actor, at: event.at, note: p.note }
      else delete done[key]
      next = { ...state, checklist_done: done }
      break
    }

    case 'trace.pinned': {
      const p = as<TracePinnedPayload>(event)
      next = {
        ...state,
        trace: { table: p.table, key: p.key, stages: p.stages ?? [] },
      }
      break
    }

    case 'shadow.revealed':
      next = { ...state, shadow_revealed: true }
      break

    case 'shadow.labelled':
      next = { ...state, shadow_labelled: true }
      break

    default:
      break
  }

  // Every human actor is a room member, mirroring the server's snapshot.Apply
  // (which upserts the actor as "watching" before handling the event). Applied
  // after the switch so no case's members rebuild can discard it.
  const members =
    event.actor.kind === 'human' ? upsertMember(next.members, event.actor) : next.members

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
  }
}
