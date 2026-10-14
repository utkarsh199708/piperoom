import { useEffect, useRef, useState } from 'react'
import { Check, Copy } from 'lucide-react'
import { Button } from '../ui/button'

/**
 * Copies `text` to the clipboard. `label` names what is copied for screen readers
 * ("Copy fix"); the visible text is "Copy", then "Copied" for two seconds.
 */
export function CopyButton({ text, label = 'Copy', disabled }: { text: string; label?: string; disabled?: boolean }) {
  const [done, setDone] = useState(false)
  const timer = useRef<ReturnType<typeof setTimeout> | undefined>(undefined)
  useEffect(() => () => clearTimeout(timer.current), [])
  const copy = () => {
    if (!navigator.clipboard) return
    void navigator.clipboard.writeText(text).then(
      () => {
        setDone(true)
        clearTimeout(timer.current)
        timer.current = setTimeout(() => setDone(false), 2000)
      },
      () => setDone(false),
    )
  }
  return (
    <Button variant="secondary" size="sm" onClick={copy} disabled={disabled} aria-label={label}>
      {done ? <Check aria-hidden="true" /> : <Copy aria-hidden="true" />}
      <span aria-live="polite">{done ? 'Copied' : 'Copy'}</span>
    </Button>
  )
}
