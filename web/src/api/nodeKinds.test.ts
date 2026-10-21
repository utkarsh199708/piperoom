import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { describe, expect, it } from 'vitest'
import { NODE_KINDS } from './nodeKinds'

// The web list must track internal/topology/graph.go exactly, in order. A kind
// added to the Go contract but not to nodeKinds.ts fails here; a kind dropped
// from nodeKinds.ts while the lane still renders it fails `tsc -b` (the lane's
// KIND_LABEL is a Record<NodeKind, string>). Tests run from web/ (CI working
// directory and `make web-test`), so `..` is the repository root.
const GRAPH_GO = resolve(process.cwd(), '..', 'internal', 'topology', 'graph.go')

function nodeKindsFrom(src: string): string[] {
  const kinds: string[] = []
  for (const m of src.matchAll(/NodeKind\s*=\s*"([^"]+)"/g)) kinds.push(m[1])
  return kinds
}

describe('NODE_KINDS', () => {
  it('equals every NodeKind constant in internal/topology/graph.go, in order', () => {
    const go = nodeKindsFrom(readFileSync(GRAPH_GO, 'utf8'))
    expect(go).not.toHaveLength(0)
    expect([...NODE_KINDS]).toEqual(go)
  })
})
