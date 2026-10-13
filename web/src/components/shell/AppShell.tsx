import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import * as DialogPrimitive from '@radix-ui/react-dialog'
import { ChevronRight, Menu } from 'lucide-react'
import { Link, Outlet, useLocation } from 'react-router-dom'
import { Button } from '@/components/ui/button'
import { DialogDescription, DialogOverlay, DialogTitle } from '@/components/ui/dialog'
import { Toaster } from '@/components/ui/sonner'
import { TooltipProvider } from '@/components/ui/tooltip'
import { useTheme } from '@/context/theme'
import { useRoomList } from '@/hooks/useRoomList'
import { CommandMenu } from './CommandMenu'
import { InShellContext, RoomCommandsContext, TopBarSlotContext } from './context'
import type { RoomCommandRegistry, RoomCommands } from './context'
import { pipelineSummaries } from './pipelines'
import { BrandMark } from './BrandMark'
import { SidebarContent } from './Sidebar'
import { ThemeToggle } from './ThemeToggle'

// The app frame (docs/design/ui-v2 Main.dc.html): sidebar, a top bar with a slot for
// screen controls, and the routed screen. Below 768px the sidebar is a drawer.
// It must sit inside UserProvider and ThemeProvider and a router.
export function AppShell() {
  const { toggleTheme } = useTheme()
  const location = useLocation()
  const { rooms, reload } = useRoomList()
  const [paletteOpen, setPaletteOpen] = useState(false)
  const [navOpen, setNavOpen] = useState(false)
  const [slotEl, setSlotEl] = useState<HTMLElement | null>(null)

  // The sidebar's room count and pipeline list follow navigation (a room opened, a
  // room closed), because the list call has no live feed. Skip the first run: the
  // hook already loaded on mount.
  const firstPath = useRef(true)
  useEffect(() => {
    if (firstPath.current) {
      firstPath.current = false
      return
    }
    void reload()
  }, [location.pathname, reload])

  // The open room's callbacks, written by RoomScreen on every render and read by the menu.
  const roomCommands = useRef<RoomCommands | null>(null)
  const registry = useMemo<RoomCommandRegistry>(
    () => ({
      get: () => roomCommands.current,
      set: (c) => {
        roomCommands.current = c
      },
    }),
    [],
  )

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (!(e.metaKey || e.ctrlKey) || e.altKey) return
      const key = e.key.toLowerCase()
      if (key === 'k' && !e.shiftKey) {
        e.preventDefault()
        setPaletteOpen((o) => !o)
      } else if (key === 'l' && e.shiftKey) {
        e.preventDefault()
        toggleTheme()
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [toggleTheme])

  // The drawer only exists below md: close it if the window grows past that.
  useEffect(() => {
    if (typeof window.matchMedia !== 'function') return
    const mq = window.matchMedia('(min-width: 768px)')
    const onChange = () => {
      if (mq.matches) setNavOpen(false)
    }
    mq.addEventListener('change', onChange)
    return () => mq.removeEventListener('change', onChange)
  }, [])

  const openPalette = useCallback(() => {
    setNavOpen(false)
    setPaletteOpen(true)
  }, [])

  const openRooms = rooms.filter((r) => r.status === 'open').length
  const pipelines = useMemo(() => pipelineSummaries(rooms), [rooms])

  return (
    <InShellContext.Provider value>
      <RoomCommandsContext.Provider value={registry}>
        <TopBarSlotContext.Provider value={slotEl}>
          <TooltipProvider>
            <div className="flex min-h-0 flex-1 bg-background text-sm text-foreground">
              <a
                href="#main"
                className="sr-only focus:not-sr-only focus:fixed focus:left-2 focus:top-2 focus:z-[60] focus:rounded-md focus:bg-primary focus:px-3 focus:py-2 focus:text-primary-foreground"
              >
                Skip to content
              </a>

              <aside className="hidden w-60 shrink-0 border-r border-sidebar-border md:block" aria-label="Sidebar">
                <SidebarContent openRooms={openRooms} pipelines={pipelines} onSearch={openPalette} />
              </aside>

              <DialogPrimitive.Root open={navOpen} onOpenChange={setNavOpen}>
                <DialogPrimitive.Portal>
                  <DialogOverlay className="md:hidden" />
                  <DialogPrimitive.Content
                    className="fixed inset-y-0 left-0 z-50 w-[280px] max-w-[85vw] border-r border-sidebar-border shadow-lg duration-150 data-[state=closed]:animate-out data-[state=closed]:slide-out-to-left data-[state=open]:animate-in data-[state=open]:slide-in-from-left md:hidden"
                  >
                    <DialogTitle className="sr-only">Navigation</DialogTitle>
                    <DialogDescription className="sr-only">Rooms and pipelines</DialogDescription>
                    <SidebarContent
                      openRooms={openRooms}
                      pipelines={pipelines}
                      onSearch={openPalette}
                      onNavigate={() => setNavOpen(false)}
                    />
                  </DialogPrimitive.Content>
                </DialogPrimitive.Portal>
              </DialogPrimitive.Root>

              <div className="flex min-w-0 flex-1 flex-col">
                <header className="flex h-12 shrink-0 items-center gap-2 border-b border-border px-3 md:px-6">
                  <Button
                    variant="ghost"
                    size="icon"
                    className="md:hidden"
                    aria-label="Open navigation"
                    onClick={() => setNavOpen(true)}
                  >
                    <Menu aria-hidden="true" />
                  </Button>
                  <BrandMark className="md:hidden" />
                  <Breadcrumb />
                  <div className="ml-auto flex min-w-0 items-center gap-2">
                    <div ref={setSlotEl} className="flex min-w-0 items-center gap-2" data-testid="topbar-slot" />
                    <ThemeToggle />
                  </div>
                </header>
                <main id="main" tabIndex={-1} className="min-h-0 flex-1 overflow-y-auto overflow-x-hidden focus:outline-none">
                  <div className="legacy-surface min-h-full">
                    <Outlet />
                  </div>
                </main>
              </div>
            </div>
            <CommandMenu open={paletteOpen} onOpenChange={setPaletteOpen} rooms={rooms} registry={registry} />
            <Toaster />
          </TooltipProvider>
        </TopBarSlotContext.Provider>
      </RoomCommandsContext.Provider>
    </InShellContext.Provider>
  )
}

function safeDecode(v: string): string {
  try {
    return decodeURIComponent(v)
  } catch {
    return v
  }
}

// "Rooms" or "Rooms > r_abc". Plain text, not a heading: each screen has its own h1.
function Breadcrumb() {
  const { pathname } = useLocation()
  const inRoom = pathname.startsWith('/rooms/')
  const id = inRoom ? safeDecode(pathname.split('/')[2] ?? '') : ''
  return (
    <nav aria-label="Breadcrumb" className="flex min-w-0 items-center gap-1.5 text-sm">
      {inRoom ? (
        <>
          <Link to="/" className="text-muted-foreground hover:text-foreground">
            Rooms
          </Link>
          <ChevronRight className="size-3 shrink-0 text-subtle-foreground" aria-hidden="true" />
          <span className="truncate font-mono text-foreground" aria-current="page">
            {id}
          </span>
        </>
      ) : (
        <span className="font-medium text-foreground" aria-current="page">
          Rooms
        </span>
      )}
    </nav>
  )
}
