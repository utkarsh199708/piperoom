// Node and trace-stage facts arrive as server keys ("tasks_failed", "newest_record") with string
// values. People read them as short phrases, so the known keys get a word and the rest fall back
// to the key with spaces. The raw key stays available as a tooltip where the fact is drawn.
import { clockTime } from './time'

const FACT_LABEL: Record<string, string> = {
  tasks_failed: 'failed tasks',
  slot_retained: 'WAL held',
  slot_active: 'slot active',
  slot: 'slot',
  newest_record: 'newest',
  newest: 'newest',
  stale_for: 'stale for',
  lag: 'lag',
  committed: 'committed offset',
  offset: 'offset',
  partition: 'partition',
  updated_at: 'updated',
  ts_column: 'time column',
  config_risk: 'config risk',
  errors_tolerance: 'error tolerance',
  future_timestamp: 'future timestamp',
  latest_version: 'latest version',
  new_version_seen: 'new version seen',
  version_deleted_seen: 'version deleted',
  registered_recently: 'registered recently',
  diff_columns: 'changed columns',
  records: 'records',
  recent: 'recent',
  writer: 'writer',
  lsn: 'LSN',
  dlq: 'DLQ',
  // Q8d: a MySQL source node's binlog state (internal/topology/mysql.go).
  binlog_oldest: 'oldest binlog',
  binlog_files: 'binlog files',
  binlog_expire: 'binlogs kept',
  binlog: 'binlog',
  gtid_mode: 'GTID mode',
}

/** "tasks_failed" → "failed tasks"; an unknown key reads with spaces ("rows_behind" → "rows behind"). */
export function factLabel(key: string): string {
  return FACT_LABEL[key] ?? key.replace(/_/g, ' ')
}

// An RFC 3339 time with a clock part, as the server writes it.
const ISO_TIME = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})$/

/** Values pass through, except a full timestamp, which reads as a local clock time. */
export function factValue(value: string): string {
  if (ISO_TIME.test(value)) {
    const t = clockTime(value)
    if (t !== value) return t
  }
  return value
}

/** "failed tasks 1", for one-line summaries such as the trace verdict. */
export function factPhrase(key: string, value: string): string {
  return `${factLabel(key)} ${factValue(value)}`
}
