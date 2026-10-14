import { cn } from '@/lib/utils'

export type DotTone = 'ok' | 'warn' | 'fail' | 'agent' | 'info' | 'accent' | 'muted'

const TONE: Record<DotTone, string> = {
  ok: 'bg-success',
  warn: 'bg-warning',
  fail: 'bg-destructive',
  agent: 'bg-agent',
  info: 'bg-info',
  accent: 'bg-primary',
  muted: 'bg-subtle-foreground',
}

/** The 6px status dot. Always sits next to a word: colour is never the only signal. */
export function StatusDot({ tone = 'muted', className }: { tone?: DotTone; className?: string }) {
  return <span aria-hidden="true" className={cn('size-1.5 shrink-0 rounded-full', TONE[tone], className)} />
}
