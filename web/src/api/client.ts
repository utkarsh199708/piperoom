// REST + WebSocket client for the Piperoom API (docs/api.md).
// Base URL defaults to the local room service; override with VITE_API_BASE.
import type {
  ApiError,
  Event,
  EventsPage,
  Me,
  RoomSnapshot,
  Topology,
} from './types'

const BASE = (import.meta.env.VITE_API_BASE as string | undefined) ?? 'http://localhost:8090'

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

async function request<T>(
  path: string,
  userId: string,
  init: RequestInit = {},
): Promise<T> {
  const headers = new Headers(init.headers)
  headers.set('X-User', userId)
  if (init.body) headers.set('Content-Type', 'application/json')
  const res = await fetch(`${BASE}${path}`, { ...init, headers })
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
    throw new ApiClientError(res.status, code, message)
  }
  if (res.status === 204) return undefined as T
  return (await res.json()) as T
}

export const api = {
  me: (userId: string) => request<Me>('/api/me', userId),
  rooms: (userId: string) => request<RoomSnapshot[]>('/api/rooms', userId),
  createRoom: (
    userId: string,
    body: { title: string; severity: string; pipeline_id: string },
  ) =>
    request<RoomSnapshot>('/api/rooms', userId, {
      method: 'POST',
      body: JSON.stringify(body),
    }),
  room: (userId: string, roomId: string) =>
    request<RoomSnapshot>(`/api/rooms/${encodeURIComponent(roomId)}`, userId),
  events: (userId: string, roomId: string, after: number, limit = 500) =>
    request<EventsPage>(
      `/api/rooms/${encodeURIComponent(roomId)}/events?after=${after}&limit=${limit}`,
      userId,
    ),
  topology: (userId: string, pipelineId: string) =>
    request<Topology>(`/api/pipelines/${encodeURIComponent(pipelineId)}/topology`, userId),
  comment: (userId: string, roomId: string, body: { step_id?: string; text: string }) =>
    request<Event>(`/api/rooms/${encodeURIComponent(roomId)}/comments`, userId, {
      method: 'POST',
      body: JSON.stringify(body),
    }),
  steer: (userId: string, roomId: string, text: string) =>
    request<Event>(`/api/rooms/${encodeURIComponent(roomId)}/steer`, userId, {
      method: 'POST',
      body: JSON.stringify({ text }),
    }),
  sendSuggestion: (userId: string, roomId: string, suggestionId: string) =>
    request<Event>(
      `/api/rooms/${encodeURIComponent(roomId)}/suggestions/${encodeURIComponent(suggestionId)}/send`,
      userId,
      { method: 'POST' },
    ),
  requestControl: (userId: string, roomId: string) =>
    request<Event>(`/api/rooms/${encodeURIComponent(roomId)}/control/request`, userId, {
      method: 'POST',
    }),
  grantControl: (userId: string, roomId: string, to: string) =>
    request<Event>(`/api/rooms/${encodeURIComponent(roomId)}/control/grant`, userId, {
      method: 'POST',
      body: JSON.stringify({ to }),
    }),
  closeRoom: (userId: string, roomId: string, resolution: string) =>
    request<Event>(`/api/rooms/${encodeURIComponent(roomId)}/close`, userId, {
      method: 'POST',
      body: JSON.stringify({ resolution }),
    }),
  approve: (userId: string, proposalId: string, layer: string) =>
    request<Event>(`/api/proposals/${encodeURIComponent(proposalId)}/approve`, userId, {
      method: 'POST',
      body: JSON.stringify({ layer }),
    }),
  reject: (userId: string, proposalId: string, layer: string, reason: string) =>
    request<Event>(`/api/proposals/${encodeURIComponent(proposalId)}/reject`, userId, {
      method: 'POST',
      body: JSON.stringify({ layer, reason }),
    }),
  handoff: (userId: string, roomId: string, to: string, stayAsWatcher: boolean) =>
    request<Event>(`/api/rooms/${encodeURIComponent(roomId)}/handoffs`, userId, {
      method: 'POST',
      body: JSON.stringify({ to, stay_as_watcher: stayAsWatcher }),
    }),
  acceptHandoff: (userId: string, handoffId: string) =>
    request<Event>(`/api/handoffs/${encodeURIComponent(handoffId)}/accept`, userId, {
      method: 'POST',
    }),
  trace: (userId: string, roomId: string, table: string, key: string) =>
    request<Event>(`/api/rooms/${encodeURIComponent(roomId)}/trace`, userId, {
      method: 'POST',
      body: JSON.stringify({ table, key }),
    }),
}

export function openRoomWs(
  roomId: string,
  userId: string,
  after: number,
): WebSocket {
  const url = new URL(`${BASE}/api/rooms/${encodeURIComponent(roomId)}/ws`)
  if (url.protocol === 'https:') url.protocol = 'wss:'
  else url.protocol = 'ws:'
  url.searchParams.set('user', userId)
  url.searchParams.set('after', String(after))
  return new WebSocket(url.toString())
}
