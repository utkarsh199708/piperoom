import { useEffect, useId, useRef, useState } from 'react'
import { Cpu } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { ModelSection } from './ModelSection'

/**
 * The room header's Model popover: the same read-only panel as in settings, opened from the top bar.
 * A disclosure, not a menu: Escape or a click outside closes it and focus returns to the button. Below
 * 640px it spans the screen width so nothing scrolls sideways.
 */
export function ModelPopover({ canEdit }: { canEdit: boolean }) {
  const [open, setOpen] = useState(false)
  const id = useId()
  const wrap = useRef<HTMLDivElement>(null)
  const button = useRef<HTMLButtonElement>(null)

  useEffect(() => {
    if (!open) return
    const onKey = (e: KeyboardEvent) => {
      if (e.key !== 'Escape') return
      e.stopPropagation()
      setOpen(false)
      button.current?.focus()
    }
    const onDown = (e: MouseEvent) => {
      // A picker dialog is portalled out of the wrapper: a click in it is not "outside".
      const t = e.target as Element | null
      if (t?.closest('[role="dialog"]')) return
      if (wrap.current && t && !wrap.current.contains(t)) setOpen(false)
    }
    document.addEventListener('keydown', onKey)
    document.addEventListener('mousedown', onDown)
    return () => {
      document.removeEventListener('keydown', onKey)
      document.removeEventListener('mousedown', onDown)
    }
  }, [open])

  return (
    <div ref={wrap} className="relative">
      <Button
        ref={button}
        type="button"
        variant="ghost"
        size="sm"
        aria-expanded={open}
        aria-controls={`${id}-panel`}
        onClick={() => setOpen((o) => !o)}
      >
        <Cpu aria-hidden="true" />
        <span className="hidden sm:inline">Model</span>
        <span className="sr-only sm:hidden">Model</span>
      </Button>
      {open && (
        <div
          id={`${id}-panel`}
          role="region"
          aria-label="Model"
          className="fixed inset-x-4 top-14 z-40 max-h-[calc(100dvh-4.5rem)] overflow-y-auto rounded-lg border border-border bg-popover p-3 text-sm text-popover-foreground shadow-lg sm:absolute sm:inset-x-auto sm:right-0 sm:top-full sm:mt-2 sm:w-96"
        >
          <h2 className="m-0 mb-2 text-sm font-semibold">Model</h2>
          <ModelSection canEdit={canEdit} compact />
        </div>
      )}
    </div>
  )
}
