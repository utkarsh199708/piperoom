import { useEffect, useState } from 'react'
import { Copy, Download, Loader2, RotateCw } from 'lucide-react'
import { toast } from 'sonner'
import { api } from '@/api/client'
import type { RoomState } from '@/state/types'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Skeleton } from '@/components/ui/skeleton'
import { downloadText, postmortemDrafting, postmortemErrorText, postmortemFilename } from '@/lib/postmortem'
import { RoomDialogClose } from './RoomDialogClose'
import { useNow } from './useNow'
import { useRestoreFocus } from './useRestoreFocus'

type Load = { status: 'loading' } | { status: 'ready'; text: string } | { status: 'error'; message: string }

// postmortem.failed reasons (docs/api.md) in words; the draft is still the events-only render.
const FAILED_TEXT: Record<string, string> = {
  summary_failed: 'The agent could not write the closing summary; this draft is built from the room log only.',
  internal_error: 'The closing summary failed on the server; this draft is built from the room log only.',
}

/**
 * The room as a post-mortem draft (F2a). The server renders Markdown with every log string escaped;
 * it is shown as text in a read-only textarea, never rendered as HTML (no Markdown renderer).
 */
export function PostmortemDialog({
  state,
  onClose,
}: {
  state: Pick<RoomState, 'id' | 'postmortem' | 'closed_at' | 'status'>
  onClose: () => void
}) {
  const restoreFocus = useRestoreFocus()
  const now = useNow(5_000)
  const [load, setLoad] = useState<Load>({ status: 'loading' })
  const [attempt, setAttempt] = useState(0)
  const drafting = postmortemDrafting(state, now)
  // A draft that lands while the dialog is open replaces the events-only text with the agent's prose.
  const draftKey = state.postmortem?.status ?? 'none'

  useEffect(() => {
    let live = true
    setLoad({ status: 'loading' })
    api.postmortem(state.id).then(
      (text) => live && setLoad({ status: 'ready', text }),
      (e: unknown) => live && setLoad({ status: 'error', message: postmortemErrorText(e) }),
    )
    return () => {
      live = false
    }
  }, [state.id, attempt, draftKey])

  const text = load.status === 'ready' ? load.text : ''
  const copy = () => {
    if (!navigator.clipboard) {
      toast.error('Copy is not available in this browser. Use Download .md instead.')
      return
    }
    navigator.clipboard.writeText(text).then(
      () => toast.success('Post-mortem draft copied'),
      () => toast.error('Could not copy. Use Download .md instead.'),
    )
  }
  const failed = state.postmortem?.status === 'failed' ? state.postmortem.reason : null

  return (
    <Dialog open onOpenChange={(open) => !open && onClose()}>
      <DialogContent hideClose className="max-h-[90vh] overflow-y-auto sm:max-w-2xl" onCloseAutoFocus={restoreFocus}>
        <RoomDialogClose label="Close post-mortem draft" />
        <DialogHeader>
          <DialogTitle>Post-mortem draft</DialogTitle>
          <DialogDescription>
            {state.status === 'open'
              ? 'The room is still open: this is the draft so far.'
              : 'Built from the room log. Copy it into your post-mortem doc and edit it there.'}
          </DialogDescription>
        </DialogHeader>

        {drafting && (
          <p role="status" className="m-0 flex items-center gap-2 text-sm text-muted-foreground">
            <Loader2 className="size-4 animate-spin motion-reduce:animate-none" aria-hidden="true" />
            The agent is writing the closing summary…
          </p>
        )}
        {failed && (
          <p role="status" className="m-0 text-sm text-warning-foreground">
            {FAILED_TEXT[failed] ?? FAILED_TEXT.internal_error}
          </p>
        )}

        {load.status === 'loading' && (
          <div role="status" aria-label="Loading the post-mortem draft" className="flex flex-col gap-2 py-2">
            <Skeleton className="h-4 w-3/4" />
            <Skeleton className="h-4 w-1/2" />
          </div>
        )}
        {load.status === 'error' && (
          <div role="alert" className="flex flex-wrap items-center gap-2 text-sm text-destructive-fg">
            <span>Could not load the draft: {load.message}</span>
            <Button type="button" size="sm" variant="outline" onClick={() => setAttempt((n) => n + 1)}>
              <RotateCw aria-hidden="true" />
              Retry
            </Button>
          </div>
        )}
        {load.status === 'ready' && (
          <textarea
            readOnly
            value={text}
            aria-label="Post-mortem draft (Markdown)"
            spellCheck={false}
            rows={18}
            className="max-h-[60vh] min-h-48 w-full resize-y rounded-md border border-border bg-muted px-3 py-2.5 font-mono text-xs leading-4 text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
          />
        )}

        <DialogFooter>
          <Button type="button" variant="secondary" disabled={load.status !== 'ready'} onClick={copy}>
            <Copy aria-hidden="true" />
            Copy Markdown
          </Button>
          <Button
            type="button"
            variant="secondary"
            disabled={load.status !== 'ready'}
            onClick={() => downloadText(text, postmortemFilename(state.id), 'text/markdown')}
          >
            <Download aria-hidden="true" />
            Download .md
          </Button>
          <Button type="button" variant="ghost" onClick={onClose}>
            Close
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
