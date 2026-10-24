// One control's request state: busy while it runs, the server's error text when it fails, and a
// short line when it succeeds. The room's own events update the lists; this only reports the ask.
import { useState } from 'react'
import { errorText } from '@/lib/errors'

export interface SetupAction {
  busy: boolean
  error: string | null
  done: string | null
  /** Run one request; answers true when it succeeded (the caller may then clear its fields). */
  run: (call: () => Promise<unknown>, success: string) => Promise<boolean>
}

export function useSetupAction(): SetupAction {
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [done, setDone] = useState<string | null>(null)
  const run = async (call: () => Promise<unknown>, success: string): Promise<boolean> => {
    setBusy(true)
    setError(null)
    setDone(null)
    try {
      await call()
      setDone(success)
      return true
    } catch (err) {
      setError(errorText(err))
      return false
    } finally {
      setBusy(false)
    }
  }
  return { busy, error, done, run }
}
