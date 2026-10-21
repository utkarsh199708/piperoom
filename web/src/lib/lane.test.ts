import { describe, expect, it } from 'vitest'
import type { TopologyNode } from '../api/types'
import { dlqVisible, visibleNodes, worstNode } from './lane'

const n = (id: string, health: TopologyNode['health'], extra: Partial<TopologyNode> = {}): TopologyNode => ({
  id,
  kind: 'topic',
  name: id,
  layer: 'kafka',
  owner: 'o',
  health,
  ...extra,
})

describe('worstNode', () => {
  it('ranks fail > warn > unknown > ok', () => {
    expect(worstNode([n('a', 'ok'), n('b', 'unknown'), n('c', 'warn'), n('d', 'fail')])?.id).toBe('d')
    expect(worstNode([n('a', 'ok'), n('b', 'unknown'), n('c', 'warn')])?.id).toBe('c')
    expect(worstNode([n('a', 'ok'), n('b', 'unknown')])?.id).toBe('b')
  })
  it('takes the first in lane order on a tie', () => {
    expect(worstNode([n('a', 'warn'), n('b', 'warn')])?.id).toBe('a')
  })
  it('is null for an empty lane', () => {
    expect(worstNode([])).toBeNull()
  })
  it('ranks a schema_subject node by health like any other kind', () => {
    const subject = n('sr', 'fail', { kind: 'schema_subject' })
    expect(worstNode([n('a', 'ok'), subject, n('b', 'warn')])?.id).toBe('sr')
  })
})

describe('DLQ visibility', () => {
  const dlq = (health: TopologyNode['health'], records?: string) =>
    n('dlq', health, { kind: 'dlq_topic', facts: records === undefined ? undefined : { records } })
  it('hides an empty healthy DLQ', () => {
    expect(dlqVisible(dlq('ok', '0'))).toBe(false)
    expect(dlqVisible(dlq('ok'))).toBe(false)
  })
  it('shows it with records or when unhealthy', () => {
    expect(dlqVisible(dlq('ok', '3'))).toBe(true)
    expect(dlqVisible(dlq('warn', '0'))).toBe(true)
  })
  it('never hides other kinds', () => {
    expect(visibleNodes([n('a', 'ok'), dlq('ok', '0')]).map((x) => x.id)).toEqual(['a'])
  })
  it('never hides a schema_subject node', () => {
    const subject = n('sr', 'ok', { kind: 'schema_subject' })
    expect(dlqVisible(subject)).toBe(true)
    expect(visibleNodes([n('a', 'ok'), subject]).map((x) => x.id)).toEqual(['a', 'sr'])
  })
})
