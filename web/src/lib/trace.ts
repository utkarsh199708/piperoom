import type { TraceState } from '../state/types'
import { factPhrase } from './facts'

export interface TraceVerdict {
  tone: 'fail' | 'ok' | 'warn' | 'muted'
  text: string
}

const MAX_FACT = 64

function clip(s: string): string {
  return s.length > MAX_FACT ? `${s.slice(0, MAX_FACT - 1)}…` : s
}

/** One line that says where the record stopped, so the reader needs no stage-by-stage scan. Plain text only. */
export function traceVerdict(trace: TraceState): TraceVerdict {
  const stages = trace.stages ?? []
  const stopped = stages.find((s) => s.status === 'stopped_here')
  if (stopped) {
    const fact = stopped.facts ? Object.entries(stopped.facts)[0] : undefined
    const where = `Stopped at ${stopped.layer} ${stopped.name}`
    return { tone: 'fail', text: fact ? `${where} — ${clip(factPhrase(fact[0], fact[1]))}` : where }
  }
  const last = stages[stages.length - 1]
  if (last?.status === 'seen') {
    return { tone: 'ok', text: `Reached ${last.name} — every stage saw key ${clip(trace.key)}` }
  }
  if (!stages.some((s) => s.status === 'seen')) {
    return { tone: 'warn', text: `Not seen anywhere — no stage holds key ${clip(trace.key)}` }
  }
  const unknown = stages.filter((s) => s.status === 'unknown').length
  return { tone: 'muted', text: `Partly traced — ${unknown} ${unknown === 1 ? 'stage' : 'stages'} could not be read` }
}
