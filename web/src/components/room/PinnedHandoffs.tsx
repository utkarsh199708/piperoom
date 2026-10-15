import { useState } from 'react'
import type { HandoffState } from '@/state/types'
import { shiftIndex } from '@/lib/handoff'
import { HandoffCard } from './HandoffCard'

const collapsedKey = (roomId: string) => `piperoom.handoff.collapsed.${roomId}`

function readCollapsed(roomId: string): string | null {
  try {
    return localStorage.getItem(collapsedKey(roomId))
  } catch {
    return null
  }
}

function writeCollapsed(roomId: string, handoffId: string | null) {
  try {
    if (handoffId) localStorage.setItem(collapsedKey(roomId), handoffId)
    else localStorage.removeItem(collapsedKey(roomId))
  } catch {
    // storage blocked: the card opens again on the next visit
  }
}

/**
 * Every accepted hand-off, newest first, for the room's whole life (F2a). The newest starts open
 * until this viewer collapses it; the choice is remembered per room by hand-off id, so the next
 * shift's summary opens again. Every card is controlled here: when a newer shift lands (live, or
 * one event at a time during the replay) the older ones close, whatever they mounted as.
 */
export function PinnedHandoffs({ roomId, handoffs }: { roomId: string; handoffs: HandoffState[] }) {
  const [collapsed, setCollapsed] = useState(() => readCollapsed(roomId))
  // The viewer's own toggles, valid only while `newest` is still the newest shift.
  const [toggled, setToggled] = useState<{ newest: string; open: Record<string, boolean> }>({ newest: '', open: {} })
  const accepted = handoffs.filter((h) => h.status === 'accepted').reverse()
  if (accepted.length === 0) return null
  const newest = accepted[0].handoff_id
  const overrides = toggled.newest === newest ? toggled.open : {}
  const isOpen = (id: string) => overrides[id] ?? (id === newest && collapsed !== newest)
  const setOpen = (id: string, open: boolean) => {
    setToggled({ newest, open: { ...overrides, [id]: open } })
    if (id === newest) {
      const next = open ? null : newest
      setCollapsed(next)
      writeCollapsed(roomId, next)
    }
  }
  return (
    <div aria-label="Shift summaries" role="region" className="flex flex-col gap-2">
      {accepted.map((h) => (
        <HandoffCard
          key={h.handoff_id}
          handoff={h}
          index={shiftIndex(handoffs, h)}
          from={h.from}
          createdAt={h.created_at}
          acceptedAt={h.accepted_at}
          defaultOpen={false}
          open={isOpen(h.handoff_id)}
          onOpenChange={(open) => setOpen(h.handoff_id, open)}
        />
      ))}
    </div>
  )
}
