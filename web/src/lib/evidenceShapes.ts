// Structured views of a read tool's `data`, keyed by exact tool name. The shapes are the Go
// ones (internal/tools/kafkatools, internal/tools/dbtools, internal/clients). `data` comes
// from a customer system and may be cut ({truncated: true, ...}), old or malformed: every
// renderer checks its shape with the guard below and shows the raw summary otherwise.

const isObj = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null && !Array.isArray(v)
const isStr = (v: unknown): v is string => typeof v === 'string'
const isNum = (v: unknown): v is number => typeof v === 'number' && Number.isFinite(v)
const optStr = (v: unknown) => v === undefined || isStr(v)

export interface ConnectStatusData {
  name: string
  state: string
  worker: string
  tasks: { id: number; state: string; error?: string }[]
}
export function isConnectStatus(d: unknown): d is ConnectStatusData {
  return (
    isObj(d) &&
    isStr(d.name) &&
    isStr(d.state) &&
    isStr(d.worker) &&
    Array.isArray(d.tasks) &&
    d.tasks.every((k) => isObj(k) && isNum(k.id) && isStr(k.state) && optStr(k.error))
  )
}

export interface SchemaDiffData {
  subject: string
  from_version: number
  to_version: number
  changes: { field: string; kind: string; from?: string; to?: string }[]
}
export function isSchemaDiff(d: unknown): d is SchemaDiffData {
  return (
    isObj(d) &&
    isStr(d.subject) &&
    isNum(d.from_version) &&
    isNum(d.to_version) &&
    Array.isArray(d.changes) &&
    d.changes.every((c) => isObj(c) && isStr(c.field) && isStr(c.kind) && optStr(c.from) && optStr(c.to))
  )
}

export interface ConsumerLagData {
  group: string
  state: string
  members: number
  total_lag: number
  worst_partitions: { topic: string; partition: number; committed: number; end: number; lag: number }[]
}
export function isConsumerLag(d: unknown): d is ConsumerLagData {
  return (
    isObj(d) &&
    isStr(d.group) &&
    isStr(d.state) &&
    isNum(d.members) &&
    isNum(d.total_lag) &&
    Array.isArray(d.worst_partitions) &&
    d.worst_partitions.every(
      (p) => isObj(p) && isStr(p.topic) && isNum(p.partition) && isNum(p.committed) && isNum(p.end) && isNum(p.lag),
    )
  )
}

export interface DdlChange {
  at: string
  command_tag: string
  object_identity: string
  query: string
  username: string
}
export function isDdlList(d: unknown): d is DdlChange[] {
  return (
    Array.isArray(d) &&
    d.every(
      (c) => isObj(c) && isStr(c.at) && isStr(c.command_tag) && isStr(c.object_identity) && isStr(c.query) && isStr(c.username),
    )
  )
}

export interface SlotRow {
  name: string
  plugin: string
  active: boolean
  restart_lsn: string
  confirmed_flush_lsn: string
  retained_wal_bytes: number
  wal_status: string
  safe_wal_bytes?: number
}
export function isSlotList(d: unknown): d is SlotRow[] {
  return (
    Array.isArray(d) &&
    d.every(
      (s) =>
        isObj(s) &&
        isStr(s.name) &&
        typeof s.active === 'boolean' &&
        isNum(s.retained_wal_bytes) &&
        isStr(s.wal_status) &&
        (s.safe_wal_bytes === undefined || isNum(s.safe_wal_bytes)),
    )
  )
}

export interface FreshnessData {
  table: string
  ts_column: string
  newest: string | null
  stale_for?: string
}
export function isFreshness(d: unknown): d is FreshnessData {
  return (
    isObj(d) &&
    isStr(d.table) &&
    isStr(d.ts_column) &&
    (d.newest === null || isStr(d.newest)) &&
    optStr(d.stale_for)
  )
}


/** 1536 -> "1.5 KiB". Bytes of WAL retained by a slot. */
export function humanBytes(n: number): string {
  if (!Number.isFinite(n)) return String(n)
  const sign = n < 0 ? '-' : ''
  let v = Math.abs(n)
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB']
  let i = 0
  while (v >= 1024 && i < units.length - 1) {
    v /= 1024
    i++
  }
  return `${sign}${i === 0 ? v : v.toFixed(v >= 10 ? 0 : 1)} ${units[i]}`
}
