import { Suspense, lazy, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import * as DialogPrimitive from '@radix-ui/react-dialog'
import { ChevronRight, Menu } from 'lucide-react'
import { Link, Outlet, useLocation } from 'react-router-dom'
import { Button } from '@/components/ui/button'
import { DialogDescription, DialogOverlay, DialogTitle } from '@/components/ui/dialog'
import { Toaster } from '@/components/ui/sonner'
import { TooltipProvider } from '@/components/ui/tooltip'
import { useTheme } from '@/context/theme'
import { useRoomList } from '@/hooks/useRoomList'
import { BreadcrumbTailContext, InShellContext, RoomCommandsContext, RoomListContext, TopBarSlotContext } from './context'
import type { RoomCommandRegistry, RoomCommands } from './context'
import { isEditableTarget } from './keys'
import { pipelineSummaries } from './pipelines'
import { BrandMark } from './BrandMark'
import { SidebarContent } from './Sidebar'
import { ThemeToggle } from './ThemeToggle'

// cmdk and the menu are only needed once it is opened: a separate chunk.
const CommandMenu = lazy(() => import('./CommandMenu').then((m) => ({ default: m.CommandMenu })))

// True from 768px (Tailwind md). Without matchMedia (tests) it is true.
function useIsDesktop(): boolean {
  const query = '(min-width: 768px)'
  const [matches, setMatches] = useState(() =>
    typeof window.matchMedia === 'function' ? window.matchMedia(query).matches : true,
  )
  useEffect(() => {
    if (typeof window.matchMedia !== 'function') return
    const mq = window.matchMedia(query)
    const onChange = () => setMatches(mq.matches)
    mq.addEventListener('change', onChange)
    return () => mq.removeEventListener('change', onChange)
  }, [])
  return matches
}

// The app frame (docs/design/ui-v2 Main.dc.html): sidebar, a top bar with a slot for
// screen controls, and the routed screen. Below 768px the sidebar is a drawer.
// It must sit inside UserProvider and ThemeProvider and a router.
export function AppShell() {
  const { toggleTheme } = useTheme()
  const location = useLocation()
  // The one room list: the sidebar reads it here and the room list screen through RoomListContext.
  const { rooms, loading: roomsLoading, error: roomsError, reload } = useRoomList()
  const roomList = useMemo(
    () => ({ rooms, loading: roomsLoading, error: roomsError, reload }),
    [rooms, roomsLoading, roomsError, reload],
  )
  const [paletteOpen, setPaletteOpen] = useState(false)
  const [navOpen, setNavOpen] = useState(false)
  const [slotEl, setSlotEl] = useState<HTMLElement | null>(null)
  // The pipeline the open room screen reports for the breadcrumb (useBreadcrumbTail).
  const [crumbPipeline, setCrumbPipeline] = useState<string | null>(null)
  // Bumps on every navigation; the sidebar row of the open room's pipeline refetches on it.
  const [navKey, setNavKey] = useState(0)
  // The palette's chunk loads on first open and stays.
  const [paletteLoaded, setPaletteLoaded] = useState(false)

  // The sidebar's room count and pipeline list follow navigation (a room opened, a
  // room closed), because the list call has no live feed. Skip the first run: the
  // hook already loaded on mount.
  const firstPath = useRef(true)
  useEffect(() => {
    if (firstPath.current) {
      firstPath.current = false
      return
    }
    setNavKey((k) => k + 1)
  }, [location.pathname])
  useEffect(() => {
    if (navKey > 0) void reload()
  }, [navKey, reload])

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
      // Not while an IME is composing, and not on key repeat (a held key would flicker the menu).
      if (e.isComposing || e.repeat) return
      if (!(e.metaKey || e.ctrlKey) || e.altKey) return
      const key = e.key.toLowerCase()
      if (key === 'k' && !e.shiftKey) {
        // The one shortcut that works everywhere, also inside a text field.
        e.preventDefault()
        setPaletteOpen((o) => !o)
      } else if (key === 'l' && e.shiftKey) {
        // Anything else never fires while the person is typing.
        if (isEditableTarget(e.target)) return
        e.preventDefault()
        toggleTheme()
      }
    }
    window.addEventListener('keydown', onKey)
    return () => window.removeEventListener('keydown', onKey)
  }, [toggleTheme])

  // The sidebar exists once: the fixed column from md up, the drawer below. Only the one on
  // screen is mounted, so only it polls pipeline health.
  const desktop = useIsDesktop()
  useEffect(() => {
    if (desktop) setNavOpen(false)
  }, [desktop])

  const openPalette = useCallback(() => {
    setNavOpen(false)
    setPaletteOpen(true)
  }, [])
  useEffect(() => {
    if (paletteOpen) setPaletteLoaded(true)
  }, [paletteOpen])

  const openRooms = rooms.filter((r) => r.status === 'open').length
  const pipelines = useMemo(() => pipelineSummaries(rooms), [rooms])
  const roomId = location.pathname.startsWith('/rooms/') ? safeDecode(location.pathname.split('/')[2] ?? '') : ''
  const currentPipelineId = rooms.find((r) => r.id === roomId)?.pipeline_id

  return (
    <InShellContext.Provider value>
     <RoomListContext.Provider value={roomList}>
      <RoomCommandsContext.Provider value={registry}>
        <TopBarSlotContext.Provider value={slotEl}>
         <BreadcrumbTailContext.Provider value={setCrumbPipeline}>
          <TooltipProvider>
            <div className="flex min-h-0 flex-1 overflow-hidden bg-background text-sm text-foreground">
              <a
                href="#main"
                className="sr-only focus:not-sr-only focus:fixed focus:left-2 focus:top-2 focus:z-[60] focus:rounded-md focus:bg-primary focus:px-3 focus:py-2 focus:text-primary-foreground"
              >
                Skip to content
              </a>

              {desktop && (
                <aside className="w-60 shrink-0 border-r border-sidebar-border" aria-label="Sidebar">
                  <SidebarContent
                    openRooms={openRooms}
                    pipelines={pipelines}
                    currentPipelineId={currentPipelineId}
                    navKey={navKey}
                    onSearch={openPalette}
                  />
                </aside>
              )}

              <DialogPrimitive.Root open={navOpen && !desktop} onOpenChange={setNavOpen}>
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
                      currentPipelineId={currentPipelineId}
                      navKey={navKey}
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
                  <Breadcrumb pipelineId={crumbPipeline ?? currentPipelineId} />
                  <div className="ml-auto flex min-w-0 items-center gap-2">
                    <div ref={setSlotEl} className="flex min-w-0 items-center gap-2" data-testid="topbar-slot" />
                    <ThemeToggle />
                  </div>
                </header>
                <main id="main" tabIndex={-1} className="relative min-h-0 flex-1 overflow-y-auto overflow-x-hidden [scroll-padding-bottom:calc(var(--composer-h,0px)+var(--sheet-h,0px))] [scroll-padding-top:3rem] focus:outline-none">
                  <div className="min-h-full">
                    <Outlet />
                  </div>
                </main>
              </div>
            </div>
            {paletteLoaded && (
              <Suspense fallback={null}>
                <CommandMenu open={paletteOpen} onOpenChange={setPaletteOpen} rooms={rooms} registry={registry} />
              </Suspense>
            )}
            <Toaster />
          </TooltipProvider>
         </BreadcrumbTailContext.Provider>
        </TopBarSlotContext.Provider>
      </RoomCommandsContext.Provider>
     </RoomListContext.Provider>
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

// The page title in the top bar. On the room list it is the page's h1 ("Rooms"), so the list
// draws no header of its own. In a room it is "Rooms › <pipeline>" (never the room id; the
// room screen has its own h1 with the title). The pipeline comes from the room screen
// (useBreadcrumbTail) and, until it has loaded, from the room list the shell already holds.
function Breadcrumb({ pipelineId }: { pipelineId?: string }) {
  const { pathname } = useLocation()
  const inRoom = pathname.startsWith('/rooms/')
  if (!inRoom) {
    return (
      <h1 className="truncate text-xl font-semibold tracking-tight text-foreground">Rooms</h1>
    )
  }
  return (
    <nav aria-label="Breadcrumb" className="flex min-w-0 items-center gap-1.5 text-sm">
      <Link to="/" className="text-muted-foreground hover:text-foreground">
        Rooms
      </Link>
      <ChevronRight className="size-3 shrink-0 text-subtle-foreground" aria-hidden="true" />
      {pipelineId ? (
        <Link
          to={`/?pipeline=${encodeURIComponent(pipelineId)}`}
          className="truncate font-mono text-foreground hover:underline"
        >
          {pipelineId}
        </Link>
      ) : (
        <span className="truncate text-muted-foreground" aria-current="page">
          Room
        </span>
      )}
    </nav>
  )
}
