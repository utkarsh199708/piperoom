import { ChevronsUpDown, LogOut, Moon, Sun, User } from 'lucide-react'
import { Avatar, AvatarFallback } from '@/components/ui/avatar'
import { Button } from '@/components/ui/button'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuLabel,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { isFixtures } from '@/api/client'
import { DEV_USERS } from '@/dev/users'
import { useCurrentUser } from '@/context/user'
import { useTheme } from '@/context/theme'
import { isDev } from '@/lib/env'
import { initials, personColor } from '@/lib/people'
import { isViewer } from '@/state/permissions'

// The signed-in person at the foot of the sidebar. The menu carries what the old
// header switcher did: in a dev build, "switch user" (POST /auth/dev-login through
// setUserId; disabled while it runs), and Log out (hidden in fixture mode, which has
// no session; disabled while a login or logout is in flight).
export function AccountMenu({ onNavigate }: { onNavigate?: () => void }) {
  const { user, setUserId, logout, busy } = useCurrentUser()
  const { theme, toggleTheme } = useTheme()
  const viewer = isViewer(user)
  const role = viewer ? 'View only' : user.layers.length > 0 ? `${user.layers.join(', ')} owner` : 'member'

  return (
    <div className="flex items-center gap-2.5 border-t border-sidebar-border px-4 py-3">
      <Avatar className="size-7">
        <AvatarFallback className="text-[11px]" style={{ backgroundColor: personColor(user.id) }} aria-hidden="true">
          {initials(user.name)}
        </AvatarFallback>
      </Avatar>
      <div className="flex min-w-0 flex-1 flex-col">
        <span className="truncate text-sm font-medium">{user.name}</span>
        <span
          className="truncate text-xs text-subtle-foreground"
          title={viewer ? 'You can read rooms but not change anything' : undefined}
        >
          {role}
        </span>
      </div>
      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button variant="ghost" size="icon" aria-label="Account menu" className="shrink-0">
            <ChevronsUpDown aria-hidden="true" />
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end" side="top" className="w-56">
          <DropdownMenuLabel>{user.name}</DropdownMenuLabel>
          <DropdownMenuItem
            onSelect={() => {
              toggleTheme()
            }}
          >
            {theme === 'dark' ? <Sun aria-hidden="true" /> : <Moon aria-hidden="true" />}
            Switch to {theme === 'dark' ? 'light' : 'dark'} theme
          </DropdownMenuItem>
          {isDev && (
            <>
              <DropdownMenuSeparator />
              <DropdownMenuLabel>Switch user (dev)</DropdownMenuLabel>
              {DEV_USERS.map((u) => (
                <DropdownMenuItem
                  key={u.id}
                  disabled={busy || u.id === user.id}
                  aria-label={`Switch user to ${u.name}`}
                  onSelect={() => {
                    setUserId(u.id)
                    onNavigate?.()
                  }}
                >
                  <User aria-hidden="true" />
                  {u.name}
                  {u.id === user.id && <span className="ml-auto text-xs text-subtle-foreground">you</span>}
                </DropdownMenuItem>
              ))}
            </>
          )}
          {!isFixtures() && (
            <>
              <DropdownMenuSeparator />
              <DropdownMenuItem disabled={busy} onSelect={logout}>
                <LogOut aria-hidden="true" />
                Log out
              </DropdownMenuItem>
            </>
          )}
        </DropdownMenuContent>
      </DropdownMenu>
    </div>
  )
}
