import { useState } from 'react'
import type { LabelInput } from '@/api/types'
import type { RoomState } from '@/state/types'
import { LABEL_LAYERS, MAX_CAUSE, MAX_FIX, draftToInput, executedFix, initialDraft } from '@/lib/label'
import type { LabelDraft } from '@/lib/label'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'

// The label fields, controlled by the caller so the close dialog can share them. One required
// field at most (the cause); the rest are optional.
export function LabelFields({
  state,
  draft,
  onChange,
  idPrefix = 'label',
}: {
  state: RoomState
  draft: LabelDraft
  onChange: (d: LabelDraft) => void
  idPrefix?: string
}) {
  const hasFix = executedFix(state) !== null
  const set = (patch: Partial<LabelDraft>) => onChange({ ...draft, ...patch })
  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-col gap-1.5">
        <label htmlFor={`${idPrefix}-layer`} className="text-sm font-medium">
          Layer at fault
        </label>
        <select
          id={`${idPrefix}-layer`}
          value={draft.layer}
          onChange={(e) => set({ layer: e.target.value })}
          className="h-10 rounded-md bg-muted px-3 text-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
        >
          <option value="">Not sure</option>
          {LABEL_LAYERS.map((l) => (
            <option key={l} value={l}>
              {l}
            </option>
          ))}
        </select>
      </div>
      <div className="flex flex-col gap-1.5">
        <label htmlFor={`${idPrefix}-cause`} className="text-sm font-medium">
          Cause
        </label>
        <Textarea
          id={`${idPrefix}-cause`}
          rows={3}
          maxLength={MAX_CAUSE}
          value={draft.cause}
          onChange={(e) => set({ cause: e.target.value })}
          placeholder="What was the root cause?"
        />
      </div>
      <div className="flex flex-col gap-1.5">
        <label htmlFor={`${idPrefix}-fix`} className="text-sm font-medium">
          Fix that worked
        </label>
        <Input
          id={`${idPrefix}-fix`}
          maxLength={MAX_FIX}
          value={draft.fix}
          disabled={!hasFix}
          onChange={(e) => set({ fix: e.target.value })}
          placeholder={hasFix ? '' : 'No proposed fix ran in this room'}
        />
      </div>
      <fieldset className="m-0 flex flex-wrap items-center gap-x-4 gap-y-1 border-0 p-0">
        <legend className="mb-1 p-0 text-sm font-medium">Did the agent help?</legend>
        {(['yes', 'no', 'skip'] as const).map((v) => (
          <label key={v} className="flex min-h-8 items-center gap-1.5 text-sm">
            <input type="radio" name={`${idPrefix}-helped`} checked={draft.helped === v} onChange={() => set({ helped: v })} />
            {v === 'skip' ? 'Skip' : v === 'yes' ? 'Yes' : 'No'}
          </label>
        ))}
      </fieldset>
    </div>
  )
}

// The label form on a closed room (the "Add label" banner opens it). Failure keeps the text and
// says why, so a second try costs nothing.
export function LabelForm({
  state,
  onSubmit,
  onSkip,
}: {
  state: RoomState
  onSubmit: (input: LabelInput) => Promise<{ ok: boolean; error?: string }>
  onSkip: () => void
}) {
  const [draft, setDraft] = useState(() => initialDraft(state))
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const input = draftToInput(state, draft)
  const submit = async () => {
    if (!input || busy) return
    setBusy(true)
    setError(null)
    const res = await onSubmit(input)
    setBusy(false)
    if (!res.ok) setError(res.error ?? 'The label could not be saved.')
  }
  return (
    <form
      className="flex flex-col gap-4"
      onSubmit={(e) => {
        e.preventDefault()
        void submit()
      }}
    >
      <LabelFields state={state} draft={draft} onChange={setDraft} />
      {error && (
        <p role="alert" className="m-0 text-sm font-medium text-destructive-fg">
          {error}
        </p>
      )}
      <div className="flex justify-end gap-2">
        <Button type="button" variant="ghost" onClick={onSkip}>
          Skip
        </Button>
        <Button type="submit" disabled={!input || busy}>
          {busy ? 'Saving…' : 'Save label'}
        </Button>
      </div>
    </form>
  )
}
