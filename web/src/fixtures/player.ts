// Fixture mode (VITE_FIXTURES=1): serves the mockup story locally so the UI can
// be developed before A5's --demo server exists. Replays events through the same
// reducer as live mode.
import type { Event, RoomSnapshot, Topology } from '../api/types'
import schemaChangeRaw from './schema-change.json'
import roomsRaw from './rooms.json'
import topologyRaw from './topology.json'

const schemaChange = schemaChangeRaw as unknown as {
  snapshot: RoomSnapshot
  events: Event[]
}

export const fixtureRooms: RoomSnapshot[] = roomsRaw as unknown as RoomSnapshot[]
export const fixtureTopology: Topology = topologyRaw as unknown as Topology

export const FIXTURE_ROOM_ID = schemaChange.snapshot.id

export function fixtureRoom(id: string): RoomSnapshot | undefined {
  if (id === schemaChange.snapshot.id) return schemaChange.snapshot
  return fixtureRooms.find((r) => r.id === id)
}

export function fixtureEvents(id: string): Event[] {
  if (id === schemaChange.snapshot.id) return schemaChange.events
  return []
}

export function addFixtureRoom(snapshot: RoomSnapshot): void {
  fixtureRooms.unshift(snapshot)
}

export function emptySnapshot(
  id: string,
  title: string,
  severity: string,
  pipelineId: string,
): RoomSnapshot {
  return {
    id,
    title,
    severity,
    pipeline_id: pipelineId,
    status: 'open',
    opened_at: new Date().toISOString(),
    last_seq: 0,
    driver: null,
    agent_status: { state: 'idle' },
    hypothesis: null,
    open_proposals: [],
    pending_suggestions: [],
    members: [],
  }
}

// Delay between replayed events (ms). The mockup story reads naturally at ~1.2s.
export const FIXTURE_EVENT_DELAY_MS = 1200
