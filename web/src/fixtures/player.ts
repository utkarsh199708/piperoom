// Fixture mode (VITE_FIXTURES=1): serves the mockup story locally so the UI can
// be developed before A5's --demo server exists. Replays events through the same
// reducer as live mode.
import type { Event, RoomSnapshot, Topology } from '../api/types'
import schemaChangeRaw from './schema-change.json'
import roomsRaw from './rooms.json'
import topologyRaw from './topology.json'
import { SETUP_ROOM_ID, planCreated, runnerAttached, setupCreated, setupRoomSnapshot } from './setup'

const schemaChange = schemaChangeRaw as unknown as {
  snapshot: RoomSnapshot
  events: Event[]
}

// U1-12a: a setup room in the story, so the stepper and the setup bar are reachable without a runner.
const setupSnapshot: RoomSnapshot = setupRoomSnapshot({
  runner: { label: 'arjun-laptop', connected: true, tools: { docker: '27.1.1' } },
  plan_id: 'pl_1',
  phase: 'prerequisites',
  step_index: 0,
})

export const fixtureRooms: RoomSnapshot[] = [setupSnapshot, ...(roomsRaw as unknown as RoomSnapshot[])]
export const fixtureTopology: Topology = topologyRaw as unknown as Topology

export const FIXTURE_ROOM_ID = schemaChange.snapshot.id

// A webhook-opened room (no driver) whose room.created event carries the raw
// alert body, so fixture mode exercises the no-driver banner and the alert
// chip in the room header.
const webhookAlertEvents: Record<string, Event[]> = {
  r_webhook_alert: [
    {
      room_id: 'r_webhook_alert',
      seq: 1,
      type: 'room.created',
      actor: { kind: 'system', id: 'alertin', name: 'Alert webhook' },
      payload: {
        title: 'ConsumerLag firing on orders-pipeline',
        severity: 'sev1',
        pipeline_id: 'orders',
        alert: {
          version: '4',
          groupKey: '{}:{alertname="ConsumerLag"}',
          status: 'firing',
          commonLabels: { alertname: 'ConsumerLag', severity: 'critical', pipeline: 'orders' },
          commonAnnotations: { summary: 'Consumer lag above threshold for 10m' },
          alerts: [
            {
              status: 'firing',
              labels: { alertname: 'ConsumerLag', severity: 'critical', pipeline: 'orders' },
              annotations: { summary: 'Consumer lag above threshold for 10m' },
            },
          ],
        },
      },
      at: '2026-10-11T10:05:00Z',
    },
  ],
}

export function fixtureRoom(id: string): RoomSnapshot | undefined {
  if (id === schemaChange.snapshot.id) return schemaChange.snapshot
  return fixtureRooms.find((r) => r.id === id)
}

export function fixtureEvents(id: string): Event[] {
  if (id === schemaChange.snapshot.id) return schemaChange.events
  if (id === SETUP_ROOM_ID) return [setupCreated(), runnerAttached(), planCreated()]
  return webhookAlertEvents[id] ?? []
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
