import { toast } from 'sonner'

// The toast for a 409 params_mismatch (docs/design/ui-v2 Stale-409: an inline alert bar, plus a
// toast that offers Reload). useRoom raises it where the 409 happens and takes it down when the
// person reloads or dismisses. It is never tied to a component's mount: RoomScreen renders the
// banner in two branches, so every reload remounts it.
const TOAST_ID = 'stale-params-mismatch'

export function showStaleToast(onReload: () => void) {
  toast.warning('Proposal changed since you looked', {
    id: TOAST_ID,
    description: 'Your decision was not sent (409). Reload to review the current version.',
    // It waits for the person; reload or dismiss takes it down.
    duration: Infinity,
    action: { label: 'Reload', onClick: onReload },
  })
}

export function hideStaleToast() {
  toast.dismiss(TOAST_ID)
}
