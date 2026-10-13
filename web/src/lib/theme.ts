// Theme: dark by default, light when the OS asks for it, or pinned by the person.
// A pinned theme lives in localStorage['piperoom.theme']; "system" stores nothing.
// index.html applies a pinned theme before first paint, so there is no flash.

export type ThemePref = 'dark' | 'light' | 'system'

export const THEME_KEY = 'piperoom.theme'

/** The saved choice. Anything unknown, or a blocked localStorage, means "system". */
export function getTheme(): ThemePref {
  try {
    const v = localStorage.getItem(THEME_KEY)
    return v === 'dark' || v === 'light' ? v : 'system'
  } catch {
    return 'system'
  }
}

// Put a choice on <html data-theme> and keep the color-scheme meta in step.
function paint(t: ThemePref): void {
  const root = document.documentElement
  if (t === 'system') delete root.dataset.theme
  else root.dataset.theme = t
  const meta = document.querySelector('meta[name="color-scheme"]')
  if (meta) meta.setAttribute('content', t === 'system' ? 'dark light' : t)
}

/** Apply the saved choice. */
export function applyTheme(): void {
  paint(getTheme())
}

/** Save a choice and apply it. */
export function setTheme(t: ThemePref): void {
  try {
    if (t === 'system') localStorage.removeItem(THEME_KEY)
    else localStorage.setItem(THEME_KEY, t)
  } catch {
    // Private window or blocked storage: the choice lasts until reload.
  }
  paint(t)
}

/** The theme on screen right now: the pinned one, else what the OS prefers (dark if unknown). */
export function effectiveTheme(): 'dark' | 'light' {
  const t = getTheme()
  if (t !== 'system') return t
  if (typeof window !== 'undefined' && typeof window.matchMedia === 'function') {
    return window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark'
  }
  return 'dark'
}
