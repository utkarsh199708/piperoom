// Rung 8 of the verification ladder (U1-12b; docs/design/setup-agent.md §3.4): the person makes
// one real change to a table and the ladder watches for it. `example_sql` comes from the server's
// source driver, never from the model; it is shown as data — selectable, copyable, never run by
// this page.
import { useId, useState } from 'react'
import { Check, Copy, UserRoundCheck } from 'lucide-react'
import { toast } from 'sonner'
import type { RoomState } from '@/state/types'
import { stripBidi } from '@/lib/title'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'

export function CanaryPrompt({ state }: { state: RoomState }) {
  const id = useId()
  const [copied, setCopied] = useState(false)
  const awaiting = state.setup?.awaiting_change
  if (!awaiting) return null
  const table = stripBidi(awaiting.table)
  const exampleSQL = stripBidi(awaiting.example_sql ?? '')

  const copy = () => {
    if (!exampleSQL) return
    if (!navigator.clipboard) {
      toast.error('Copy is not available in this browser. Select the statement instead.')
      return
    }
    navigator.clipboard.writeText(exampleSQL).then(
      () => {
        setCopied(true)
        toast.success('Example change copied')
      },
      () => toast.error('Could not copy. Select the statement instead.'),
    )
  }

  return (
    <Card
      role="region"
      aria-labelledby={`${id}-title`}
      className="mx-4 mt-2 flex min-w-0 flex-col gap-2 border-warning-foreground/40 bg-warning-subtle p-3 sm:mx-6"
    >
      <h2 id={`${id}-title`} className="m-0 flex items-center gap-2 text-sm font-semibold">
        <UserRoundCheck className="size-4 shrink-0" aria-hidden="true" />
        Make one change so the agent can verify
      </h2>
      <p className="m-0 text-xs text-muted-foreground">
        Rung 8 is yours: the ladder waits for a real change to <code className="font-mono">{table}</code>{' '}
        and checks it reaches Kafka. Run this in your own client; the agent verifies when the change
        lands, and stops waiting after ten minutes.
      </p>
      {exampleSQL && (
        <div className="flex min-w-0 items-start gap-2">
          <pre
            aria-label="Example change"
            className="m-0 min-w-0 flex-1 overflow-x-auto whitespace-pre-wrap break-words rounded-md border border-border bg-background px-3 py-2 font-mono text-xs"
          >
            {exampleSQL}
          </pre>
          <Button
            variant="outline"
            size="icon"
            className="shrink-0 coarse:size-11"
            aria-label={copied ? 'Copied example change' : 'Copy example change'}
            onClick={copy}
          >
            {copied ? <Check aria-hidden="true" /> : <Copy aria-hidden="true" />}
          </Button>
        </div>
      )}
      <p role="status" aria-live="polite" className="m-0 min-h-4 text-xs text-success-foreground">
        {copied ? 'Example change copied' : ''}
      </p>
    </Card>
  )
}
