// Maps domain values to chip tone names (see .chip--* in index.css).
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
