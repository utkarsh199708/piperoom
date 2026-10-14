// Maps domain values to chip tone names (see .chip--* in index.css).
import { humanStatus } from './status'
export function severityTone(severity: string): string {
  switch (severity) {
    case 'sev1':
      return 'fail'
    case 'sev2':
      return 'warn'
    case 'sev3':
      return 'info'
    default:
      return 'ok'
  }
}

// Agent status → chip tone + label. `done` gets a green "resolved" chip so the
// end of an investigation is obvious, rather than a plain info chip.
export function agentStatusChip(state: string, detail?: string): { tone: string; label: string } {
  switch (state) {
    case 'done':
      return { tone: 'ok', label: 'resolved — verified' }
    case 'error':
      return { tone: 'fail', label: 'error' }
    case 'waiting_approval':
      return { tone: 'warn', label: 'waiting approval' }
    case 'working':
      return { tone: 'info', label: detail ? `working · ${detail}` : 'working' }
    case 'idle':
      return { tone: 'info', label: 'idle' }
    default:
      return { tone: 'muted', label: detail ? `${humanStatus(state)} · ${detail}` : humanStatus(state) }
  }
}
