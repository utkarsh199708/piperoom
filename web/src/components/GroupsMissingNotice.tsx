// Quiet hint for `/api/me.groups_missing` (docs/design/auth.md 2.2): the IdP sent
// no groups claim, so rights that come from a group cannot apply.
export const GROUPS_MISSING_TEXT =
  'Your sign-in had no groups claim; group-based rights are not available (IdP overage?)'

export function GroupsMissingNotice() {
  return (
    <p className="notice muted" role="status" data-testid="groups-missing">
      {GROUPS_MISSING_TEXT}
    </p>
  )
}
