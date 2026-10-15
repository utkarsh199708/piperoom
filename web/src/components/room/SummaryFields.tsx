import { MAX_SUMMARY_ITEM, MAX_SUMMARY_ITEMS, MAX_SUMMARY_TEXT } from '@/lib/handoff'
import type { SummaryDraft } from '@/lib/handoff'
import { Textarea } from '@/components/ui/textarea'

const LISTS: { key: keyof SummaryDraft; label: string; hint: string }[] = [
  { key: 'done', label: 'Done', hint: 'What has been tried or fixed so far' },
  { key: 'ruled_out', label: 'Ruled out', hint: 'Causes that were checked and are not it' },
  { key: 'open', label: 'Still open', hint: 'What is not settled' },
  { key: 'risks', label: 'Risks', hint: 'What could go wrong next' },
]

// The fields of a hand-off summary written by the driver (QA G4), the same six the agent fills in.
// A list field takes one item per line. The room dialog owns the draft and checks it (lib/handoff.ts).
export function SummaryFields({
  draft,
  onChange,
  disabled = false,
}: {
  draft: SummaryDraft
  onChange: (d: SummaryDraft) => void
  disabled?: boolean
}) {
  const set = (patch: Partial<SummaryDraft>) => onChange({ ...draft, ...patch })
  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-col gap-1.5">
        <label htmlFor="summary-what" className="text-sm font-medium">
          What broke
        </label>
        <Textarea
          id="summary-what"
          rows={2}
          aria-required="true"
          disabled={disabled}
          maxLength={MAX_SUMMARY_TEXT}
          value={draft.what_broke}
          onChange={(e) => set({ what_broke: e.target.value })}
          placeholder="e.g. The orders connector stopped; the warehouse is 40 minutes behind"
        />
      </div>
      <div className="flex flex-col gap-1.5">
        <label htmlFor="summary-why" className="text-sm font-medium">
          Why
        </label>
        <Textarea
          id="summary-why"
          rows={2}
          aria-required="true"
          disabled={disabled}
          maxLength={MAX_SUMMARY_TEXT}
          value={draft.why}
          onChange={(e) => set({ why: e.target.value })}
          placeholder="The cause, or your best guess"
        />
      </div>
      {LISTS.map(({ key, label, hint }) => (
        <div key={key} className="flex flex-col gap-1.5">
          <label htmlFor={`summary-${key}`} className="text-sm font-medium">
            {label} <span className="font-normal text-muted-foreground">· one per line</span>
          </label>
          <Textarea
            id={`summary-${key}`}
            rows={2}
            disabled={disabled}
            value={draft[key]}
            onChange={(e) => set({ [key]: e.target.value })}
            placeholder={hint}
            title={`At most ${MAX_SUMMARY_ITEMS} items of ${MAX_SUMMARY_ITEM} characters`}
          />
        </div>
      ))}
    </div>
  )
}
