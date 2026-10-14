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
  LabelInput,
  Me,
  Person,
  PipelineInfo,
  RoomSnapshot,
  Topology,
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
 * Download the room's export document (GET /api/rooms/{id}/export). Returned as text, not parsed, so the
 * saved file is byte-for-byte what the server hashed; `sha256` is the X-Piperoom-Export-SHA256 header.
 */
export async function exportRoom(roomId: string): Promise<{ text: string; sha256?: string }> {
  const epoch = getAuthEpoch()
  const res = await fetch(`/api/rooms/${encodeURIComponent(roomId)}/export`, { credentials: 'same-origin' })
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
  return { text: await res.text(), sha256: res.headers.get('X-Piperoom-Export-SHA256') ?? undefined }
}

const room = (roomId: string) => `/api/rooms/${encodeURIComponent(roomId)}`

export const api = {
  me: () => request<Me>('/api/me'),
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
  comment: (roomId: string, body: { step_id?: string; text: string }) =>
    request<Event>(`${room(roomId)}/comments`, { method: 'POST', body: JSON.stringify(body) }),
  steer: (roomId: string, text: string) =>
    request<Event>(`${room(roomId)}/steer`, { method: 'POST', body: JSON.stringify({ text }) }),
  sendSuggestion: (roomId: string, suggestionId: string) =>
    request<Event>(`${room(roomId)}/suggestions/${encodeURIComponent(suggestionId)}/send`, {
      method: 'POST',
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
