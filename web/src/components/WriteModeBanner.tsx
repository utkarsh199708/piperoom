import { useState } from 'react'
import { Info, X } from 'lucide-react'
import type { RoomMode } from '../api/types'
import { quickstartHref, writeModeOff } from '../lib/writeMode'
import { Button } from '@/components/ui/button'

export const WRITE_MODE_BANNER_TEXT = 'Fixes are suggestions until write mode is on'

// Same key shape as the decision bar's dismissed rejections; a proposal id can never be this string.
export const WRITE_MODE_DISMISS_KEY = 'piperoom.dismissed.write-mode-banner'

function readDismissed(): boolean {
  try {
    return window.sessionStorage.getItem(WRITE_MODE_DISMISS_KEY) === '1'
  } catch {
    return false
  }
}

// Read-only start (P1e): the server runs with write mode off, so an approved fix is recorded and
// never runs. One persistent line under the room header says so; dismissing it lasts for the
// browser session. Storage may be missing or throw (private window, blocked site data): the
// component state still hides it for this screen, and it simply comes back on the next visit.
export function WriteModeBanner({ mode }: { mode?: RoomMode }) {
  const [dismissed, setDismissed] = useState(readDismissed)
  if (!writeModeOff(mode) || dismissed) return null
  const dismiss = () => {
    setDismissed(true)
    try {
      window.sessionStorage.setItem(WRITE_MODE_DISMISS_KEY, '1')
    } catch {
      // the state above already hides it for this visit
    }
  }
  return (
    // One line on a phone: the text gives way (ellipsis), the link and the button never do.
    <div
      role="region"
      aria-label="Write mode is off"
      className="flex min-h-10 flex-nowrap items-center gap-x-2.5 border-b border-border bg-info-subtle px-4 py-1 text-xs text-info-foreground sm:px-6 sm:text-sm"
    >
      <Info className="size-3.5 shrink-0" aria-hidden="true" />
      <span className="min-w-0 truncate font-medium" title={WRITE_MODE_BANNER_TEXT}>
        {WRITE_MODE_BANNER_TEXT}
      </span>
      <span className="shrink-0" aria-hidden="true">
        ·
      </span>
      <a
        href={quickstartHref()}
        target="_blank"
        rel="noopener noreferrer"
        className="shrink-0 whitespace-nowrap rounded-sm font-medium underline underline-offset-2 hover:no-underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      >
        How to turn it on
      </a>
      <Button
        variant="ghost"
        size="icon"
        className="ml-auto size-7 shrink-0 text-info-foreground hover:text-info-foreground coarse:size-11"
        aria-label="Dismiss write-mode notice"
        onClick={dismiss}
      >
        <X aria-hidden="true" />
      </Button>
    </div>
  )
}
