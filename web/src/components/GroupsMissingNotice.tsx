import { Info } from 'lucide-react'

// Quiet hint for `/api/me.groups_missing` (docs/design/auth.md 2.2): the IdP sent
// no groups claim, so rights that come from a group cannot apply. A subtle line, not a banner:
// nothing is broken for the person, some approvals are just not theirs right now.
export const GROUPS_MISSING_TEXT =
  'Your sign-in did not include your groups, so approvals you hold through a group are not available. Your identity provider admin can add the groups claim.'

export function GroupsMissingNotice() {
  return (
    <p
      className="m-0 flex items-start gap-1.5 border-b border-border px-4 py-1.5 text-xs text-muted-foreground"
      role="status"
      data-testid="groups-missing"
    >
      <Info className="mt-px size-3.5 shrink-0" aria-hidden="true" />
      <span>{GROUPS_MISSING_TEXT}</span>
    </p>
  )
}
