import { describe, expect, it } from 'vitest'
import { fireEvent, render, screen, within } from '@testing-library/react'
import { evidenceFor } from '@/lib/evidence'
import { humanBytes } from '@/lib/evidenceShapes'
import { Timeline } from '@/components/Timeline'
import type { StepState, ToolCallState } from '@/state/types'

const call = (tool: string, data: unknown, over: Partial<ToolCallState> = {}): ToolCallState => ({
  tool_use_id: 't1',
  tool,
  input: {},
  summary: `SUMMARY of ${tool}`,
  data,
  ...over,
})

function show(t: ToolCallState) {
  const View = evidenceFor(t.tool)
  if (!View) throw new Error(`no view for ${t.tool}`)
  return render(<View t={t} />)
}

const happy: Record<string, unknown> = {
  connect_status: {
    name: 'pg-orders-connector',
    state: 'RUNNING',
    worker: '172.19.0.5:8083',
    tasks: [{ id: 0, state: 'FAILED', error: 'DataException: Failed to serialize\nCaused by: incompatible' }],
  },
  schema_registry_diff: {
    subject: 'pg.public.orders-value',
    from_version: 2,
    to_version: 3,
    changes: [
      { field: 'amount', kind: 'changed', from: 'int', to: 'bytes' },
      { field: 'note', kind: 'added', to: 'string' },
      { field: 'legacy', kind: 'removed', from: 'string' },
    ],
  },
  kafka_consumer_lag: {
    group: 'order-sync',
    state: 'Stable',
    members: 1,
    total_lag: 4821,
    worst_partitions: [{ topic: 'pg.public.orders', partition: 2, committed: 100, end: 4921, lag: 4821 }],
  },
  pg_recent_ddl: [
    {
      at: '2026-10-11T09:47:00Z',
      command_tag: 'ALTER TABLE',
      object_identity: 'public.orders',
      query: 'ALTER TABLE orders ALTER COLUMN amount TYPE numeric(12,2)',
      username: 'etl',
    },
  ],
  pg_replication_slots: [
    { name: 'debezium_orders', plugin: 'pgoutput', active: false, restart_lsn: '0/1', confirmed_flush_lsn: '0/2', retained_wal_bytes: 3 * 1024 * 1024 * 1024, wal_status: 'extended' },
  ],
  warehouse_freshness: { table: 'analytics.orders', ts_column: 'updated_at', newest: '2026-10-14T09:58:14Z', stale_for: '48m' },
}

describe('evidence renderers', () => {
  it('connect_status: connector line, task table with the error trace', () => {
    show(call('connect_status', happy.connect_status))
    expect(screen.getByText('pg-orders-connector')).toBeInTheDocument()
    expect(screen.getByRole('columnheader', { name: 'task' })).toHaveAttribute('scope', 'col')
    expect(screen.getByRole('columnheader', { name: 'error' })).toBeInTheDocument()
    expect(screen.getByText('FAILED')).toBeInTheDocument()
    expect(screen.getByText('DataException: Failed to serialize').tagName).toBe('STRONG')
    expect(screen.queryByText(/SUMMARY of/)).not.toBeInTheDocument()
  })

  it('schema_registry_diff: v2 → v3 and a badge per change kind', () => {
    show(call('schema_registry_diff', happy.schema_registry_diff))
    expect(screen.getByText('v2 → v3')).toBeInTheDocument()
    expect(screen.getByText('changed')).toBeInTheDocument()
    expect(screen.getByText('added')).toBeInTheDocument()
    expect(screen.getByText('removed')).toBeInTheDocument()
    expect(screen.getByText('int → bytes')).toBeInTheDocument()
  })

  it('kafka_consumer_lag: header and partition table', () => {
    show(call('kafka_consumer_lag', happy.kafka_consumer_lag))
    expect(screen.getByText(/Stable · 1 member · total lag/)).toBeInTheDocument()
    const row = screen.getByText('pg.public.orders/2').closest('tr') as HTMLElement
    expect(within(row).getByText('4821')).toHaveClass('text-warning-foreground')
    expect(within(row).getByText('100')).toBeInTheDocument()
  })

  it('pg_recent_ddl: tag, object, user and query', () => {
    show(call('pg_recent_ddl', happy.pg_recent_ddl))
    expect(screen.getByText('ALTER TABLE')).toBeInTheDocument()
    expect(screen.getByText('public.orders')).toBeInTheDocument()
    expect(screen.getByText('etl')).toBeInTheDocument()
    expect(screen.getByText('ALTER TABLE orders ALTER COLUMN amount TYPE numeric(12,2)')).toBeInTheDocument()
    expect(document.querySelector('time')).toHaveAttribute('title', '2026-10-11 09:47:00 UTC')
  })

  it('pg_replication_slots: inactive and non-reserved slots are in the failure tone', () => {
    show(call('pg_replication_slots', happy.pg_replication_slots))
    const row = screen.getByText('debezium_orders').closest('tr') as HTMLElement
    expect(within(row).getByText('false')).toHaveClass('text-destructive-fg')
    expect(within(row).getByText('extended')).toHaveClass('text-destructive-fg')
    expect(within(row).getByText('3.0 GiB')).toBeInTheDocument()
  })

  it('warehouse_freshness: one line, or the empty-table line', () => {
    const { unmount } = show(call('warehouse_freshness', happy.warehouse_freshness))
    expect(screen.getByText('newest row 09:58:14 UTC · 48m ago')).toBeInTheDocument()
    unmount()
    show(call('warehouse_freshness', { table: 'analytics.orders', ts_column: 'updated_at', newest: null }))
    expect(screen.getByText('no rows (empty table)')).toBeInTheDocument()
  })

  it.each(Object.keys(happy))('%s falls back to the summary for a truncated or malformed data', (tool) => {
    for (const data of [{ truncated: true, bytes: 70000 }, 'oops', null, undefined, 42, [{ nope: 1 }]]) {
      const { unmount } = show(call(tool, data))
      expect(screen.getByText(`SUMMARY of ${tool}`)).toBeInTheDocument()
      unmount()
    }
  })

  it('an unknown tool has no view', () => {
    expect(evidenceFor('unknown_tool')).toBeNull()
    expect(evidenceFor('constructor')).toBeNull()
    expect(evidenceFor('toString')).toBeNull()
  })

  it('renders only text nodes: no links, no markup from values', () => {
    const evil = '<a href="http://evil.example">x</a>'
    const { container } = show(
      call('connect_status', { name: evil, state: 'RUNNING', worker: evil, tasks: [{ id: 0, state: 'FAILED', error: evil }] }),
    )
    expect(container.querySelector('a')).toBeNull()
    expect(container.textContent).toContain(evil)
  })

  it('humanBytes', () => {
    expect(humanBytes(0)).toBe('0 B')
    expect(humanBytes(1536)).toBe('1.5 KiB')
    expect(humanBytes(48 * 1024 * 1024)).toBe('48 MiB')
    expect(humanBytes(-2048)).toBe('-2.0 KiB')
  })
})

describe('Timeline evidence', () => {
  const step = (c: ToolCallState): StepState => ({ id: 's1', title: 'Check', messages: [], tool_calls: [c] })
  const open = (c: ToolCallState) => {
    render(<Timeline steps={[step(c)]} messages={[]} comments={[]} onComment={() => undefined} />)
    fireEvent.click(screen.getByRole('button', { name: /Check/ }))
  }

  it('uses the view and keeps the summary under "Raw input and result"', () => {
    open(call('warehouse_freshness', happy.warehouse_freshness))
    expect(screen.getByText('newest row 09:58:14 UTC · 48m ago')).toBeInTheDocument()
    const raw = document.querySelector('details pre') as HTMLElement
    expect(raw.textContent).toContain('SUMMARY of warehouse_freshness')
  })

  it('an errored call shows its summary, not the view', () => {
    open(call('warehouse_freshness', happy.warehouse_freshness, { is_error: true, summary: 'error: timeout' }))
    expect(screen.getByText('error: timeout')).toBeInTheDocument()
    expect(screen.queryByText(/newest row/)).not.toBeInTheDocument()
  })

  it('a tool with no view shows its summary as before', () => {
    open(call('runbook_lookup_x', {}, { summary: 'plain summary' }))
    expect(screen.getByText('plain summary')).toBeInTheDocument()
  })
})
