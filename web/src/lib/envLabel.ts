import { isFixtures } from '@/api/client'

/**
 * The small badge next to the brand ("staging", "dev"). `VITE_ENV_LABEL` sets it at
 * build time; without it a dev server says "dev", fixture mode says "fixtures", and a
 * production build shows none.
 */
export function envLabel(): string | null {
  const built = (import.meta.env.VITE_ENV_LABEL as string | undefined)?.trim()
  if (built) return built
  if (isFixtures()) return 'fixtures'
  if (import.meta.env.DEV) return 'dev'
  return null
}

/** True on Apple platforms, where the shortcut modifier is ⌘ instead of Ctrl. */
export function isMac(): boolean {
  if (typeof navigator === 'undefined') return false
  return /Mac|iPhone|iPad/i.test(navigator.platform || navigator.userAgent)
}

/** The shortcut modifier as the key cap shows it. */
export function modKeyLabel(): string {
  return isMac() ? '⌘' : 'Ctrl'
}
