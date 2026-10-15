import type { ComponentType } from 'react'
import type { ToolCallState } from '../state/types'
import { ConnectStatus } from '../components/room/evidence/ConnectStatus'
import { ConsumerLag } from '../components/room/evidence/ConsumerLag'
import { Freshness } from '../components/room/evidence/Freshness'
import { RecentDdl } from '../components/room/evidence/RecentDdl'
import { ReplicationSlots } from '../components/room/evidence/ReplicationSlots'
import { SchemaDiff } from '../components/room/evidence/SchemaDiff'

// Structured views of a read tool's `data`, keyed by exact tool name. The shape guards live in
// evidenceShapes.ts (the views import them, so they cannot live next to the views).

export type EvidenceView = ComponentType<{ t: ToolCallState }>

const VIEWS: Record<string, EvidenceView> = {
  connect_status: ConnectStatus,
  schema_registry_diff: SchemaDiff,
  kafka_consumer_lag: ConsumerLag,
  pg_recent_ddl: RecentDdl,
  pg_replication_slots: ReplicationSlots,
  warehouse_freshness: Freshness,
}

/** The structured view for a tool, or null (the raw summary is shown). */
export function evidenceFor(tool: string): EvidenceView | null {
  return Object.prototype.hasOwnProperty.call(VIEWS, tool) ? VIEWS[tool] : null
}
