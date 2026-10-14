import { BellRing } from 'lucide-react'
import { toast } from 'sonner'
import type { Person, PresenceMember } from '@/api/types'
import type { NudgeState } from '@/state/types'
import type { RoomActions } from '@/hooks/useRoom'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { clockTime } from '@/lib/time'
import { NUDGE_WINDOW_MS, nudgeText, ownersOf } from '@/lib/waiting'
import { useNow } from './useNow'

const MAX_NAMES = 3

/** Who a pending layer waits on (with their presence) and the Nudge button. */
export function WaitingFor({
  proposalId,
  layer,
  roomId,
  roomTitle,
  people,
  presence,
  nudge,
  canNudge,
  actions,
}: {
  proposalId: string
  layer: string
  roomId: string
  roomTitle: string
  people?: Person[]
  presence: PresenceMember[]
  nudge?: NudgeState
  /** A member of an open room. */
  canNudge: boolean
  actions: RoomActions
}) {
  const now = useNow()
  const owners = ownersOf(layer, people, presence, now)
  const shown = owners.slice(0, MAX_NAMES)
  const first = owners[0]?.name
  const nudgedAt = nudge ? Date.parse(nudge.at) : NaN
  const cooling = !Number.isNaN(nudgedAt) && now - nudgedAt < NUDGE_WINDOW_MS

  const copy = async (): Promise<boolean> => {
    if (!navigator.clipboard) return false
    try {
      await navigator.clipboard.writeText(nudgeText(layer, roomTitle, roomId))
      return true
    } catch {
      return false
    }
  }

  const onNudge = async () => {
    const copied = await copy()
    const res = await actions.nudge(proposalId, layer)
    if (res.code === 'already_nudged') {
      const mins = Number.isNaN(nudgedAt) ? null : Math.max(0, Math.floor((Date.now() - nudgedAt) / 60_000))
      const ago = mins === null ? 'a few minutes ago' : `${mins} min ago`
      toast(copied ? `Already nudged ${ago} — link copied anyway` : `Already nudged ${ago}`)
    } else if (res.ok && copied) {
      toast(`Copied — paste it to ${first ?? 'the owner'}`)
    }
  }

  return (
    <>
      <Badge className="ml-auto max-w-full" variant="neutral">
        <span className="size-1.5 shrink-0 rounded-full bg-warning" aria-hidden="true" />
        <span className="truncate">
          {owners.length === 0
            ? `Waiting for a ${layer} owner`
            : `Waiting for ${shown.map((o) => `${o.name} · ${o.presence}`).join(', ')}`}
          {owners.length > MAX_NAMES && (
            <span title={owners.slice(MAX_NAMES).map((o) => o.name).join(', ')}> +{owners.length - MAX_NAMES}</span>
          )}
        </span>
      </Badge>
      {nudge && !Number.isNaN(nudgedAt) && (
        <span className="text-xs text-subtle-foreground">nudged {clockTime(nudge.at)}</span>
      )}
      {canNudge && (
        <Button
          variant="ghost"
          size="sm"
          disabled={cooling}
          aria-label={`Nudge ${layer} owners`}
          onClick={() => void onNudge()}
        >
          <BellRing className="size-3.5" aria-hidden="true" />
          Nudge
        </Button>
      )}
    </>
  )
}
