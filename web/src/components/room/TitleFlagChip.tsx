import { Badge } from '@/components/ui/badge'

/**
 * Warns that the title came from alert text that looks like an injected instruction (G15).
 * The title itself is still shown, as text; the tooltip names the kinds the server found.
 */
export function TitleFlagChip({ flags }: { flags?: string[] | null }) {
  if (!flags || flags.length === 0) return null
  return (
    <Badge variant="warning" className="shrink-0" title={`Flagged in the alert text: ${flags.join(', ')}`}>
      suspicious alert text
    </Badge>
  )
}
