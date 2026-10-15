import { useState } from 'react'
import type { FormEvent } from 'react'
import { api } from '@/api/client'
import type { ShadowGradedPayload, ShadowRootCause } from '@/api/types'
import { errorText } from '@/lib/errors'
import { MAX_NOTES } from '@/lib/shadow'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Textarea } from '@/components/ui/textarea'

const ROOT_CAUSE: { value: ShadowRootCause; label: string }[] = [
  { value: 'yes', label: 'Yes, same cause' },
  { value: 'partial', label: 'Partly' },
  { value: 'no', label: 'No' },
]

// Step 2: the person grades the agent's answer against the resolution and their label.
// Re-grading is allowed (the server keeps every grade, the last wins), so the form
// starts from the current grade when there is one.
export function GradeForm({
  roomId,
  current,
  revealed,
  canWrite,
  onDone,
}: {
  roomId: string
  current?: ShadowGradedPayload
  /** shadow.revealed exists. Before it (grace period) the server answers 409 not_revealed. */
  revealed: boolean
  canWrite: boolean
  onDone: () => void
}) {
  const [layerCorrect, setLayerCorrect] = useState<boolean | null>(current ? current.layer_correct : null)
  const [rootCause, setRootCause] = useState<ShadowRootCause | ''>(current?.root_cause ?? '')
  const [notes, setNotes] = useState(current?.notes ?? '')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [saved, setSaved] = useState(false)
  const ready = layerCorrect !== null && rootCause !== ''

  const submit = async (e: FormEvent) => {
    e.preventDefault()
    if (!ready || busy || !canWrite || !revealed) return
    setBusy(true)
    setError(null)
    setSaved(false)
    try {
      const n = notes.trim()
      await api.shadowGrade(roomId, { layer_correct: layerCorrect, root_cause: rootCause, ...(n ? { notes: n } : {}) })
      setSaved(true)
      onDone()
    } catch (err) {
      setError(errorText(err))
    } finally {
      setBusy(false)
    }
  }

  return (
    <Card className="flex flex-col gap-3 p-4" role="region" aria-labelledby="shadow-grade-heading">
      <h2 id="shadow-grade-heading" className="m-0 text-sm font-medium">
        {current ? 'Your grade' : 'Grade the agent'}
      </h2>
      {!canWrite ? (
        <p role="status" className="m-0 text-sm text-muted-foreground">
          Only a member of this pipeline can grade. {current ? 'The current grade is shown below.' : ''}
        </p>
      ) : !revealed ? (
        <p role="status" className="m-0 text-sm text-muted-foreground">
          The agent is still finishing its run. Grading opens when it ends.
        </p>
      ) : (
        <form className="flex flex-col gap-3" onSubmit={(e) => void submit(e)}>
          <fieldset className="m-0 flex flex-col gap-1 border-0 p-0">
            <legend className="mb-1 text-sm font-medium">Did the agent name the right layer?</legend>
            <div className="flex gap-4">
              {[true, false].map((v) => (
                <label key={String(v)} className="flex min-h-8 items-center gap-1.5 text-sm">
                  <input
                    type="radio"
                    name="layer_correct"
                    checked={layerCorrect === v}
                    onChange={() => setLayerCorrect(v)}
                  />
                  {v ? 'Yes' : 'No'}
                </label>
              ))}
            </div>
          </fieldset>
          <fieldset className="m-0 flex flex-col gap-1 border-0 p-0">
            <legend className="mb-1 text-sm font-medium">Did it find the root cause?</legend>
            <div className="flex flex-wrap gap-x-4">
              {ROOT_CAUSE.map((o) => (
                <label key={o.value} className="flex min-h-8 items-center gap-1.5 text-sm">
                  <input
                    type="radio"
                    name="root_cause"
                    checked={rootCause === o.value}
                    onChange={() => setRootCause(o.value)}
                  />
                  {o.label}
                </label>
              ))}
            </div>
          </fieldset>
          <div className="flex flex-col gap-1.5">
            <label htmlFor="shadow-notes" className="text-sm font-medium">
              Notes <span className="font-normal text-subtle-foreground">(optional)</span>
            </label>
            <Textarea
              id="shadow-notes"
              rows={3}
              maxLength={MAX_NOTES}
              value={notes}
              onChange={(e) => setNotes(e.target.value)}
            />
          </div>
          {error && (
            <p role="alert" className="m-0 text-sm font-medium text-destructive-fg">
              {error}
            </p>
          )}
          {saved && !error && (
            <p role="status" className="m-0 text-sm text-success-foreground">
              Grade saved.
            </p>
          )}
          <div>
            <Button type="submit" disabled={!ready || busy}>
              {busy ? 'Saving…' : current ? 'Update grade' : 'Save grade'}
            </Button>
          </div>
        </form>
      )}
    </Card>
  )
}
