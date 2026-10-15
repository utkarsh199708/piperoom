import { useRef } from 'react'
import type { ReactNode } from 'react'
import { useNavigate } from 'react-router-dom'
import { CircleCheck, Copy, Download, FileText, GitCompareArrows, Hand, Inbox, LogOut, Moon, Route, Sun, UserCheck } from 'lucide-react'
import { Badge } from '@/components/ui/badge'
import {
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandShortcut,
} from '@/components/ui/command'
import { Kbd } from '@/components/ui/kbd'
import { isFixtures } from '@/api/client'
import type { RoomSnapshot } from '@/api/types'
import { useCurrentUser } from '@/context/user'
import { useTheme } from '@/context/theme'
import { modKeyLabel } from '@/lib/envLabel'
import { roomStatusCell, sortRoomsForList } from '@/lib/rooms'
import type { RoomCommandRegistry } from './context'

const SEVERITY_VARIANT = { sev1: 'sev1', sev2: 'sev2', sev3: 'sev3', sev4: 'sev4' } as const

function Keys({ children }: { children: ReactNode }) {
  return <CommandShortcut>{children}</CommandShortcut>
}

// The ⌘K menu (docs/design/ui-v2 Command-Menu). Every item calls something that already
// exists: router navigation, the theme toggle, logout, and the open room's own callbacks,
// which RoomScreen registers (see RoomCommands). Items the person may not use are absent.
export function CommandMenu({
  open,
  onOpenChange,
  rooms,
  registry,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  rooms: RoomSnapshot[]
  registry: RoomCommandRegistry
}) {
  const navigate = useNavigate()
  const { user, logout, busy } = useCurrentUser()
  const { theme, toggleTheme } = useTheme()
  // What closed the menu: null = Esc or a click outside (focus goes back where it was),
  // 'dialog' = an action that opens a dialog (it keeps the focus), 'other' = anything else.
  const closedBy = useRef<'dialog' | 'other' | null>(null)
  const mod = modKeyLabel()
  // What had focus before the palette opened. Read while rendering the opening frame, before
  // Radix moves focus into the palette. Dialogs note document.activeElement when they mount, and
  // the palette is gone by then, so run() puts the focus back here first.
  const opener = useRef<Element | null>(null)
  if (!open) opener.current = null
  else if (opener.current === null && typeof document !== 'undefined') opener.current = document.activeElement

  // Read when the menu renders, i.e. when it opens: the room screen wrote it by then.
  const room = open ? registry.get() : null
  const sorted = sortRoomsForList(rooms).slice(0, 25)

  const run = (fn: () => void, opensDialog = false) => {
    closedBy.current = opensDialog ? 'dialog' : 'other'
    const from = opener.current
    onOpenChange(false)
    // After the menu has closed, so a dialog it opens (hand-off, trace) keeps the focus.
    window.setTimeout(() => {
      // The dialog records the focused element when it mounts: make that the pre-palette one.
      // A gone or hidden element is skipped, so the dialog falls back to #main.
      if (opensDialog && from instanceof HTMLElement && from.isConnected && !from.closest('[hidden]')) {
        from.focus({ preventScroll: true })
      }
      fn()
    }, 0)
  }

  return (
    <CommandDialog
      open={open}
      onOpenChange={(next) => {
        if (next) closedBy.current = null
        onOpenChange(next)
      }}
      // Esc or a click outside returns focus to where it was. After a hand-off or trace the
      // new dialog keeps it; after any other action the page content gets it, because the
      // thing that opened the menu may be gone (a link followed, a theme switched).
      onCloseAutoFocus={(e) => {
        if (closedBy.current === null) return
        e.preventDefault()
        if (closedBy.current === 'other') document.getElementById('main')?.focus()
      }}
    >
      {room && (
        <div className="flex items-center gap-2 px-3.5 pt-1.5">
          <Badge variant="outline" className="h-[18px] max-w-[60%] text-[11px]">
            <span className="truncate">{room.title}</span>
          </Badge>
          {user.layers.length > 0 && (
            <span className="truncate text-xs text-subtle-foreground">you own {user.layers.join(', ')}</span>
          )}
        </div>
      )}
      <CommandInput placeholder="Type a command or search…" aria-label="Type a command or search" />
      <CommandList>
        <CommandEmpty>No results.</CommandEmpty>

        {room && (room.takeControl || room.requestControl || room.handoff || room.trace || room.closeRoom || room.invite || room.exportJson || room.postmortem) && (
          <CommandGroup heading="Room">
            {room.takeControl && (
              <CommandItem
                value="take control of the room"
                keywords={['driver', 'steer']}
                onSelect={() => run(() => registry.get()?.takeControl?.())}
              >
                <UserCheck aria-hidden="true" />
                Take control
              </CommandItem>
            )}
            {room.requestControl && (
              <CommandItem
                value="request control from the driver"
                keywords={['driver', 'steer', 'ask']}
                onSelect={() => run(() => registry.get()?.requestControl?.())}
              >
                <Hand aria-hidden="true" />
                Request control
              </CommandItem>
            )}
            {room.handoff && (
              <CommandItem
                value="hand off the room"
                keywords={['handoff', 'shift', 'transfer']}
                onSelect={() => run(() => registry.get()?.handoff?.(), true)}
              >
                <GitCompareArrows aria-hidden="true" />
                Hand off the room
              </CommandItem>
            )}
            {room.trace && (
              <CommandItem
                value="trace a record through the pipeline"
                keywords={['record', 'key', 'table']}
                onSelect={() => run(() => registry.get()?.trace?.(), true)}
              >
                <Route aria-hidden="true" />
                Trace a record through the pipeline
              </CommandItem>
            )}
            {room.closeRoom && (
              <CommandItem
                value="close the room"
                keywords={['resolve', 'resolution', 'finish', 'done']}
                onSelect={() => run(() => registry.get()?.closeRoom?.(), true)}
              >
                <CircleCheck aria-hidden="true" />
                Close the room
              </CommandItem>
            )}
            {room.invite && (
              <CommandItem
                value="copy invite link"
                keywords={['share', 'url']}
                onSelect={() => run(() => registry.get()?.invite?.())}
              >
                <Copy aria-hidden="true" />
                Copy invite link
              </CommandItem>
            )}
            {room.exportJson && (
              <CommandItem
                value="export room as json"
                keywords={['download', 'audit', 'hash']}
                onSelect={() => run(() => registry.get()?.exportJson?.())}
              >
                <Download aria-hidden="true" />
                Export room as JSON
              </CommandItem>
            )}
            {room.postmortem && (
              <CommandItem
                value="post-mortem draft"
                keywords={['markdown', 'retro', 'summary']}
                onSelect={() => run(() => registry.get()?.postmortem?.(), true)}
              >
                <FileText aria-hidden="true" />
                Post-mortem draft
              </CommandItem>
            )}
          </CommandGroup>
        )}

        {sorted.length > 0 && (
          <CommandGroup heading="Go to a room">
            {sorted.map((r) => (
              <CommandItem
                key={r.id}
                value={`room ${r.title} ${r.pipeline_id} ${r.id}`}
                onSelect={() => run(() => navigate(`/rooms/${encodeURIComponent(r.id)}`))}
              >
                <Inbox aria-hidden="true" />
                <span className="min-w-0 truncate">{r.title}</span>
                <span className="shrink-0 font-mono text-xs text-subtle-foreground">{r.pipeline_id}</span>
                <span className="ml-auto flex shrink-0 items-center gap-1.5">
                  <span className="text-xs text-muted-foreground">{roomStatusCell(r).label}</span>
                  <Badge variant={SEVERITY_VARIANT[r.severity as keyof typeof SEVERITY_VARIANT] ?? 'neutral'}>
                    {r.severity.toUpperCase()}
                  </Badge>
                </span>
              </CommandItem>
            ))}
          </CommandGroup>
        )}

        <CommandGroup heading="Navigate">
          <CommandItem value="go to rooms" onSelect={() => run(() => navigate('/'))}>
            <Inbox aria-hidden="true" />
            Go to Rooms
          </CommandItem>
        </CommandGroup>

        <CommandGroup heading="Account">
          <CommandItem value="toggle theme dark light" onSelect={() => run(toggleTheme)}>
            {theme === 'dark' ? <Sun aria-hidden="true" /> : <Moon aria-hidden="true" />}
            Toggle theme
            <Keys>
              <Kbd>{mod}</Kbd>
              <Kbd>⇧</Kbd>
              <Kbd>L</Kbd>
            </Keys>
          </CommandItem>
          {!isFixtures() && (
            <CommandItem value="sign out log out" disabled={busy} onSelect={() => run(logout)}>
              <LogOut aria-hidden="true" />
              Sign out
            </CommandItem>
          )}
        </CommandGroup>
      </CommandList>
      <div className="flex h-9 items-center gap-3.5 border-t border-border bg-muted px-3.5 text-xs text-subtle-foreground">
        <span className="inline-flex items-center gap-1">
          <Kbd>↑</Kbd>
          <Kbd>↓</Kbd>
          navigate
        </span>
        <span className="inline-flex items-center gap-1">
          <Kbd>⏎</Kbd>
          run
        </span>
        <span className="ml-auto inline-flex items-center gap-1">
          <Kbd>esc</Kbd>
          close
        </span>
      </div>
    </CommandDialog>
  )
}
