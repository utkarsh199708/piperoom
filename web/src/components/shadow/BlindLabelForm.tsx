import { useState } from 'react'
import type { FormEvent } from 'react'
import { ApiClientError, api } from '@/api/client'
import { LAYERS } from '@/api/types'
import { errorText } from '@/lib/errors'
import { MAX_TERM_LEN, MAX_TERMS, parseTerms, termsProblem } from '@/lib/shadow'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Textarea } from '@/components/ui/textarea'

// Step 1 of grading. The person says what the real cause was BEFORE the server will tell
// them anything about the agent's answer (GET /shadow is 409 not_labelled until this is
// saved), so the label cannot be bent toward the agent. One label per room.
export function BlindLabelForm({
  roomId,
  resolution,
  canWrite,
  onDone,
}: {
  roomId: string
  resolution?: string
  /** False for a viewer: the server answers 403 viewer to the POST. */
  canWrite: boolean
  onDone: () => void
}) {
  const [layer, setLayer] = useState('')
  const [text, setText] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const terms = parseTerms(text)
  const problem = text.trim() === '' ? null : termsProblem(terms)
  const ready = layer !== '' && terms.length > 0 && !termsProblem(terms)

  const submit = async (e: FormEvent) => {
    e.preventDefault()
    if (!ready || busy || !canWrite) return
    setBusy(true)
    setError(null)
    try {
      await api.shadowBlind(roomId, { actual_layer: layer, root_cause_terms: terms })
      onDone()
    } catch (err) {
      // Someone else labelled first (another tab or person): reload and show the run.
      if (err instanceof ApiClientError && err.status === 409 && err.code === 'already_decided') {
        onDone()
        return
      }
      setError(errorText(err))
    } finally {
      setBusy(false)
    }
  }

  return (
    <Card className="flex flex-col gap-3 p-4" role="region" aria-labelledby="shadow-blind-heading">
      <div className="flex flex-col gap-1">
        <h2 id="shadow-blind-heading" className="m-0 text-sm font-medium">
          Record your answer first
        </h2>
        <p className="m-0 max-w-[72ch] text-sm text-muted-foreground">
          The agent investigated this incident silently. Its conclusion stays hidden until you say what you think
          the cause was. You can record this once.
        </p>
        {resolution && (
          <p className="m-0 text-sm">
            <span className="text-subtle-foreground">Your resolution: </span>
            {resolution}
          </p>
        )}
      </div>
      {!canWrite ? (
        <p role="status" className="m-0 text-sm text-muted-foreground">
          Only a member of this pipeline can label the room. You can read the result once someone has.
        </p>
      ) : (
        <form className="flex flex-col gap-3" onSubmit={(e) => void submit(e)}>
          <fieldset className="m-0 flex flex-col gap-1.5 border-0 p-0">
            <legend className="mb-1 text-sm font-medium">Layer where it actually broke</legend>
            <div className="flex flex-wrap gap-x-4 gap-y-1">
              {LAYERS.map((l) => (
                <label key={l} className="flex min-h-8 items-center gap-1.5 text-sm">
                  <input type="radio" name="actual_layer" value={l} checked={layer === l} onChange={() => setLayer(l)} />
                  {l}
                </label>
              ))}
            </div>
          </fieldset>
          <div className="flex flex-col gap-1.5">
            <label htmlFor="shadow-terms" className="text-sm font-medium">
              Root cause terms
            </label>
            <Textarea
              id="shadow-terms"
              rows={3}
              value={text}
              onChange={(e) => setText(e.target.value)}
              aria-describedby="shadow-terms-help"
              placeholder={'one per line, e.g.\nschema compatibility\nconnector failed'}
            />
            <p id="shadow-terms-help" className="m-0 text-xs text-subtle-foreground">
              One short phrase per line, up to {MAX_TERMS} lines of {MAX_TERM_LEN} characters. They are used to
              build an eval scenario from this run.
            </p>
            {problem && (
              <p role="alert" className="m-0 text-sm font-medium text-destructive-fg">
                {problem}
              </p>
            )}
          </div>
          {error && (
            <p role="alert" className="m-0 text-sm font-medium text-destructive-fg">
              {error}
            </p>
          )}
          <div>
            <Button type="submit" disabled={!ready || busy}>
              {busy ? 'Saving…' : 'Save label and show the agent’s answer'}
            </Button>
          </div>
        </form>
      )}
    </Card>
  )
}
