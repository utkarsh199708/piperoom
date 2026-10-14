// Theme helpers. Dark is the default; light is chosen with the toggle or comes
// from the OS (prefers-color-scheme) when the person never chose. The choice is
// remembered in localStorage, which can throw (private window, blocked site
// data), so every access is wrapped. public/theme-init.js does the same before
// first paint: keep the key and values in sync with it.
export type Theme = 'dark' | 'light'

export const THEME_KEY = 'piperoom-theme'

export function isTheme(v: unknown): v is Theme {
  return v === 'dark' || v === 'light'
}

/** The saved choice, or null when there is none or storage is unavailable. */
export function readStoredTheme(): Theme | null {
  try {
    const v = window.localStorage.getItem(THEME_KEY)
    return isTheme(v) ? v : null
  } catch {
    return null
  }
}

export function storeTheme(theme: Theme): void {
  try {
    window.localStorage.setItem(THEME_KEY, theme)
  } catch {
    // not remembered; the theme still applies for this visit
  }
}

/** The OS setting. Dark when the browser cannot say. */
export function systemTheme(): Theme {
  try {
    return window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark'
  } catch {
    return 'dark'
  }
}

/** What is on screen now: the attribute set before first paint, else saved, else the OS. */
export function currentTheme(): Theme {
  const attr = document.documentElement.getAttribute('data-theme')
  if (isTheme(attr)) return attr
  return readStoredTheme() ?? systemTheme()
}

export function applyTheme(theme: Theme): void {
  const root = document.documentElement
  root.setAttribute('data-theme', theme)
  root.style.colorScheme = theme
}
