import { useCallback, useEffect, useMemo, useState } from 'react'
import type { ReactNode } from 'react'
import { applyTheme, currentTheme, readStoredTheme, storeTheme } from '../lib/theme'
import type { Theme } from '../lib/theme'
import { ThemeContext } from './theme'

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [theme, setThemeState] = useState<Theme>(currentTheme)

  const setTheme = useCallback((next: Theme) => {
    applyTheme(next)
    storeTheme(next)
    setThemeState(next)
  }, [])

  const toggleTheme = useCallback(() => {
    setTheme(theme === 'dark' ? 'light' : 'dark')
  }, [theme, setTheme])

  // Follow the OS while the person has not chosen.
  useEffect(() => {
    let mq: MediaQueryList
    try {
      mq = window.matchMedia('(prefers-color-scheme: light)')
    } catch {
      return
    }
    const onChange = () => {
      if (readStoredTheme() !== null) return
      const next: Theme = mq.matches ? 'light' : 'dark'
      applyTheme(next)
      setThemeState(next)
    }
    mq.addEventListener('change', onChange)
    return () => mq.removeEventListener('change', onChange)
  }, [])

  const value = useMemo(() => ({ theme, setTheme, toggleTheme }), [theme, setTheme, toggleTheme])
  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>
}
