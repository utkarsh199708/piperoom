// REST + WebSocket client for the Piperoom API (docs/api.md).
//
// Same-origin only. Every URL is relative (`/api/...`, `/auth/...`). Under
// `npm run dev` the Vite proxy forwards them to the Go server (vite.config.ts);
// in production the server serves the UI itself (PIPEROOM_WEB_DIR). Identity is
// the HttpOnly `piperoom_session` cookie, which the browser sends by itself.
// This client never sends the old user-id header: with a cookie for another user
// the server answers 403 proxy_identity (docs/design/auth.md section 5.2).
import type {
  ApiError,
  AuthUser,
  ChecklistStepDonePayload,
  Event,
  EventsPage,
  HypothesisVerdict,
  LabelInput,
  Me,
  Person,
  ProbeResult,
  AdminConfig,
  AgentStats,
  FleetChangesPage,
  FleetPipeline,
  PipelineInfo,
  RoomSnapshot,
  ShadowBlindBody,
  ShadowGradeBody,
  ShadowMetrics,
  ShadowResponse,
  Topology,
  TrendPoint,
  WatchMetric,
} from './types'
import { classifyAuthProblem, getAuthEpoch, reportAuthProblem } from '../lib/auth'
import type { AuthMode } from '../lib/auth'

export const isFixtures = () => import.meta.env.VITE_FIXTURES === '1'

export class ApiClientError extends Error {
  code: string
  status: number

  constructor(status: number, code: string, message: string) {
    super(message)
    this.name = 'ApiClientError'
    this.code = code
    this.status = status
  }
}

async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers)
  // CrossOriginProtection + the contract: every non-GET request is application/json.
  const method = (init.method ?? 'GET').toUpperCase()
  if (method !== 'GET' && method !== 'HEAD') headers.set('Content-Type', 'application/json')
  const epoch = getAuthEpoch() // a 401 that arrives after a login/logout is stale
  const res = await fetch(path, { ...init, headers, credentials: 'same-origin' })
  if (!res.ok) {
    let code = res.status === 421 ? 'bad_host' : 'unknown'
    let message = res.statusText
    let authMode: AuthMode | undefined
    try {
      const body = (await res.json()) as ApiError & { error?: { auth_mode?: AuthMode } }
      code = body.error?.code ?? code
      message = body.error?.message ?? message
      authMode = body.error?.auth_mode
    } catch {
      // non-JSON error body; keep statusText
    }
    const problem = classifyAuthProblem(res.status, code, message, authMode)
    if (problem) reportAuthProblem({ ...problem, epoch })
    throw new ApiClientError(res.status, code, message)
  }
  if (res.status === 204) return undefined as T
  const text = await res.text()
  return (text ? JSON.parse(text) : undefined) as T
}

/**
 * Steer and comment stop waiting for an answer after this long. A POST that never answers would
 * otherwise keep the reconnect's copy of the words "in flight" forever, and every Retry of them
 * would wait on it (#390). The server may still store the words (it gives these routes 60 s), so
 * giving up is not a failure: see SendUnconfirmedError.
 */
export const SEND_DEADLINE_MS = 12_000

/**
 * The request was sent but no answer came within SEND_DEADLINE_MS. The server may or may not have
 * stored it: only a retry with the same client_id, to a server that dedupes on it, is safe
 * (docs/api.md, Idempotent posts). See hooks/sendLedger.ts.
 */
export class SendUnconfirmedError extends Error {
  constructor(ms: number) {
    super(`no answer from Piperoom within ${Math.round(ms / 1000)} s`)
    this.name = 'SendUnconfirmedError'
  }
}

/** `request` with a deadline; past it, the request is aborted and the call rejects with SendUnconfirmedError. */
async function requestWithin<T>(path: string, init: RequestInit, ms: number): Promise<T> {
  const ctl = new AbortController()
  let timer: ReturnType<typeof setTimeout> | undefined
  const late = new Promise<never>((_, reject) => {
    timer = setTimeout(() => {
      ctl.abort()
      reject(new SendUnconfirmedError(ms))
    }, ms)
  })
  try {
    return await Promise.race([request<T>(path, { ...init, signal: ctl.signal }), late])
  } finally {
    clearTimeout(timer)
  }
}

export interface SessionProbe {
  /** GET /api/me answered 200: the session is valid after all. */
  valid: boolean
  /** `error.auth_mode` of the 401, when it said. */
  mode?: AuthMode
}

/**
 * Ask GET /api/me whether there is a session and which login the server uses,
 * without raising an auth problem (so it cannot loop).
 */
export async function probeSession(): Promise<SessionProbe> {
  try {
    const res = await fetch('/api/me', { credentials: 'same-origin' })
    if (res.ok) return { valid: true }
    const body = (await res.json()) as { error?: { auth_mode?: AuthMode } }
    const m = body.error?.auth_mode
    return { valid: false, mode: m === 'dev' || m === 'oidc' ? m : undefined }
  } catch {
    return { valid: false }
  }
}

/**
 * Extra fields of POST /api/proposals/{pid}/approve beyond `layer`.
 * `params_hash` is the hash the approver SAW on screen (auth PR 4): the server
 * answers 400 without it and 409 params_mismatch when it is not the proposal's.
 * `confirm` is the solo-mode string "<pipeline_id>/<layer>" typed by hand
 * (docs/design/auth.md 5.3).
 */
export interface ApproveExtras {
  params_hash: string
  confirm?: string
}

/**
 * Body of POST /api/proposals/{pid}/approve-all (P2b): the same `params_hash` rule as approve, and
 * for the solo user `confirm: "<pipeline_id>/*"`, typed once for every layer of the call.
 */
export type ApproveAllExtras = ApproveExtras

/**
 * Download the room's export document (GET /api/rooms/{id}/export). Returned as text, not parsed, so the
 * saved file is byte-for-byte what the server hashed; `sha256` is the X-Piperoom-Export-SHA256 header.
 */
export async function exportRoom(roomId: string): Promise<{ text: string; sha256?: string }> {
  const res = await fetchText(`/api/rooms/${encodeURIComponent(roomId)}/export`)
  return { text: await res.text(), sha256: res.headers.get('X-Piperoom-Export-SHA256') ?? undefined }
}

/**
 * The room as a post-mortem draft (GET /api/rooms/{id}/postmortem.md, F2). Markdown as plain text:
 * the caller shows it as text, never as HTML.
 */
export async function postmortemMarkdown(roomId: string): Promise<string> {
  const res = await fetchText(`/api/rooms/${encodeURIComponent(roomId)}/postmortem.md`, 'text/markdown')
  return res.text()
}

// A GET whose body is not JSON. An error answer is still the JSON error envelope.
async function fetchText(url: string, accept?: string): Promise<Response> {
  const epoch = getAuthEpoch()
  const res = await fetch(url, { credentials: 'same-origin', headers: accept ? { Accept: accept } : undefined })
  if (!res.ok) {
    let code = 'unknown'
    let message = res.statusText
    try {
      const body = (await res.json()) as ApiError
      code = body.error?.code ?? code
      message = body.error?.message ?? message
    } catch {
      // non-JSON error body; keep statusText
    }
    const problem = classifyAuthProblem(res.status, code, message, undefined)
    if (problem) reportAuthProblem({ ...problem, epoch })
    throw new ApiClientError(res.status, code, message)
  }
  return res
}

/**
 * The config export (GET /api/admin/config/export, admin only): the owners file, `---`, the users file, as a
 * `piperoom-config.yaml` attachment. The page links to it instead of fetching it, so the browser saves the
 * bytes the server sent and the session cookie authorises it.
 */
export const ADMIN_CONFIG_EXPORT_URL = '/api/admin/config/export'

const room = (roomId: string) => `/api/rooms/${encodeURIComponent(roomId)}`

export const api = {
  me: () => request<Me>('/api/me'),
  postmortem: (roomId: string) => postmortemMarkdown(roomId),
  // Members only (a viewer gets 403 viewer): who a driver may grant control or hand off to.
  people: () => request<Person[]>('/api/people'),
  // Dev mode only: issues the piperoom_session cookie for a users.yaml user.
  devLogin: (user: string) =>
    request<void>('/auth/dev-login', { method: 'POST', body: JSON.stringify({ user }) }),
  // Dev mode only (404 in oidc mode); needs no session: it is called before login.
  authUsers: () => request<AuthUser[]>('/auth/users'),
  pipelines: () => request<PipelineInfo[]>('/api/pipelines'),
  logout: () => request<void>('/auth/logout', { method: 'POST' }),
  rooms: () => request<RoomSnapshot[]>('/api/rooms'),
  createRoom: (body: { title: string; severity: string; pipeline_id: string }) =>
    request<RoomSnapshot>('/api/rooms', { method: 'POST', body: JSON.stringify(body) }),
  room: (roomId: string) => request<RoomSnapshot>(room(roomId)),
  events: (roomId: string, after: number, limit = 500) =>
    request<EventsPage>(`${room(roomId)}/events?after=${after}&limit=${limit}`),
  topology: (pipelineId: string) =>
    request<Topology>(`/api/pipelines/${encodeURIComponent(pipelineId)}/topology`),
  // X3: the watcher's samples of one metric, oldest first; [] before the first sample. 404 for an unknown
  // pipeline and for one outside the caller's tenants.
  trends: (pipelineId: string, metric: WatchMetric, days = 1) =>
    request<TrendPoint[]>(
      `/api/pipelines/${encodeURIComponent(pipelineId)}/trends?metric=${encodeURIComponent(metric)}&days=${days}`,
    ),
  // X5: one row per pipeline the caller can see, [] when there are none; filtered and sorted in the browser.
  fleet: () => request<FleetPipeline[]>('/api/fleet'),
  // X5: what changed across the caller's pipelines (default the last 7 days), newest first, one page per call.
  // `cursor` is the previous page's `next`, opaque.
  fleetChanges: (page: { cursor?: string; limit?: number } = {}) => {
    const q = new URLSearchParams()
    if (page.limit) q.set('limit', String(page.limit))
    if (page.cursor) q.set('cursor', page.cursor)
    const qs = q.toString()
    return request<FleetChangesPage>(`/api/fleet/changes${qs ? `?${qs}` : ''}`)
  },
  agentStats: (pipelineId: string) =>
    request<AgentStats>(`/api/pipelines/${encodeURIComponent(pipelineId)}/agent-stats?days=90`),
  comment: (roomId: string, body: { step_id?: string; text: string; client_id?: string }) =>
    requestWithin<Event>(`${room(roomId)}/comments`, { method: 'POST', body: JSON.stringify(body) }, SEND_DEADLINE_MS),
  steer: (roomId: string, text: string, clientId?: string) =>
    requestWithin<Event>(
      `${room(roomId)}/steer`,
      { method: 'POST', body: JSON.stringify({ text, client_id: clientId }) },
      SEND_DEADLINE_MS,
    ),
  sendSuggestion: (roomId: string, suggestionId: string) =>
    request<Event>(`${room(roomId)}/suggestions/${encodeURIComponent(suggestionId)}/send`, {
      method: 'POST',
    }),
  // F9: any member may decide; the server answers 409 already_decided for a repeat.
  hypothesis: (roomId: string, verdict: HypothesisVerdict, note?: string) =>
    request<Event>(`${room(roomId)}/hypothesis`, {
      method: 'POST',
      body: JSON.stringify(note ? { verdict, note } : { verdict }),
    }),
  nudge: (roomId: string, proposalId: string, layer: string) =>
    request<Event>(`${room(roomId)}/nudge`, {
      method: 'POST',
      body: JSON.stringify({ proposal_id: proposalId, layer }),
    }),
  declineSuggestion: (roomId: string, suggestionId: string, reason?: string) =>
    request<Event>(`${room(roomId)}/suggestions/${encodeURIComponent(suggestionId)}/decline`, {
      method: 'POST',
      body: JSON.stringify(reason ? { reason } : {}),
    }),
  requestControl: (roomId: string) =>
    request<Event>(`${room(roomId)}/control/request`, { method: 'POST' }),
  grantControl: (roomId: string, to: string) =>
    request<Event>(`${room(roomId)}/control/grant`, {
      method: 'POST',
      body: JSON.stringify({ to }),
    }),
  closeRoom: (roomId: string, resolution: string) =>
    request<Event>(`${room(roomId)}/close`, {
      method: 'POST',
      body: JSON.stringify({ resolution }),
    }),
  approve: (proposalId: string, layer: string, extra: ApproveExtras) =>
    request<Event>(`/api/proposals/${encodeURIComponent(proposalId)}/approve`, {
      method: 'POST',
      body: JSON.stringify({ layer, ...extra }),
    }),
  // One approval.given per pending required layer, in required_layers order, then the steps run
  // as after the last approve. Answers those events.
  approveAll: (proposalId: string, extra: ApproveAllExtras) =>
    request<Event[]>(`/api/proposals/${encodeURIComponent(proposalId)}/approve-all`, {
      method: 'POST',
      body: JSON.stringify(extra),
    }),
  // P2b: stops a pending policy countdown (proposal.cancelled). 409 not_scheduled when none is
  // pending (the policy ran, or someone cancelled first); the proposal stays open either way.
  cancelProposal: (proposalId: string, reason?: string) =>
    request<Event>(`/api/proposals/${encodeURIComponent(proposalId)}/cancel`, {
      method: 'POST',
      body: JSON.stringify(reason ? { reason } : {}),
    }),
  // paramsHash: the hash shown with the proposal, same rule as approve.
  reject: (proposalId: string, layer: string, reason: string, paramsHash: string) =>
    request<Event>(`/api/proposals/${encodeURIComponent(proposalId)}/reject`, {
      method: 'POST',
      body: JSON.stringify({ layer, reason, params_hash: paramsHash }),
    }),
  handoff: (roomId: string, to: string, stayAsWatcher: boolean) =>
    request<Event>(`${room(roomId)}/handoffs`, {
      method: 'POST',
      body: JSON.stringify({ to, stay_as_watcher: stayAsWatcher }),
    }),
  acceptHandoff: (handoffId: string) =>
    request<Event>(`/api/handoffs/${encodeURIComponent(handoffId)}/accept`, { method: 'POST' }),
  label: (roomId: string, body: LabelInput) =>
    request<Event>(`${room(roomId)}/label`, { method: 'POST', body: JSON.stringify(body) }),
  checklist: (roomId: string, body: ChecklistStepDonePayload) =>
    request<Event>(`${room(roomId)}/checklist`, { method: 'POST', body: JSON.stringify(body) }),
  trace: (roomId: string, table: string, key: string) =>
    request<Event>(`${room(roomId)}/trace`, {
      method: 'POST',
      body: JSON.stringify({ table, key }),
    }),
  // Shadow mode. shadow() answers 409 not_labelled until the blind label exists, so the
  // agent's answer cannot be read early; a grade before the reveal answers 409 not_revealed.
  shadowBlind: (roomId: string, body: ShadowBlindBody) =>
    request<Event>(`${room(roomId)}/shadow/blind`, { method: 'POST', body: JSON.stringify(body) }),
  // include: 'data' adds tool_result.data; 'room' adds {created, labelled, graded} (prefills the grade form).
  shadow: (roomId: string, include: ('data' | 'room')[] = []) =>
    request<ShadowResponse>(`${room(roomId)}/shadow${include.length ? `?include=${include.join(',')}` : ''}`),
  shadowGrade: (roomId: string, body: ShadowGradeBody) =>
    request<Event>(`${room(roomId)}/shadow/grade`, { method: 'POST', body: JSON.stringify(body) }),
  // Admin only: any other member gets 403 not_allowed, a viewer 403 viewer. Nothing here changes configuration.
  adminConfig: () => request<AdminConfig>('/api/admin/config'),
  // A failed check is a 200 with ok: false. 404 not_found: unknown id. 429 rate_limited: 10 tests a minute.
  testConnection: (id: string) =>
    request<ProbeResult>('/api/admin/connections/test', { method: 'POST', body: JSON.stringify({ id }) }),
  shadowMetrics: (filter: { pipeline?: string; since?: string } = {}) => {
    const q = new URLSearchParams()
    if (filter.pipeline) q.set('pipeline', filter.pipeline)
    if (filter.since) q.set('since', filter.since)
    const qs = q.toString()
    return request<ShadowMetrics>(`/api/shadow/metrics${qs ? `?${qs}` : ''}`)
  },
}

/**
 * WebSocket URL for a room: same origin as the page (ws or wss by protocol),
 * no user query parameter (the session cookie is checked at upgrade).
 */
export function roomWsUrl(
  roomId: string,
  after: number,
  loc: Pick<Location, 'protocol' | 'host'> = window.location,
): string {
  const scheme = loc.protocol === 'https:' ? 'wss:' : 'ws:'
  return `${scheme}//${loc.host}${room(roomId)}/ws?after=${after}`
}

export function openRoomWs(roomId: string, after: number): WebSocket {
  return new WebSocket(roomWsUrl(roomId, after))
}

/** WebSocket URL of the room list push (GET /api/rooms/ws, F6): same origin, no cursor. */
export function roomsWsUrl(loc: Pick<Location, 'protocol' | 'host'> = window.location): string {
  const scheme = loc.protocol === 'https:' ? 'wss:' : 'ws:'
  return `${scheme}//${loc.host}/api/rooms/ws`
}

export function openListWs(): WebSocket {
  return new WebSocket(roomsWsUrl())
}
