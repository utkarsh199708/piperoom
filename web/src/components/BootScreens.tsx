import type { ReactNode } from 'react'
import { LogIn } from 'lucide-react'
import { Card } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { BrandMark } from './shell/BrandMark'

// What shows before the app knows who you are: the shell drawn in grey (the same shapes as the
// room list's loading state), so the page does not jump from a line of text to the full layout.
// One polite status names what is loading for a screen reader.
export function AppSkeleton({ label = 'Loading Piperoom' }: { label?: string }) {
  return (
    <div role="status" aria-label={label} className="flex min-h-0 flex-1 bg-background">
      <span className="sr-only">{label}…</span>
      <div aria-hidden="true" className="hidden w-60 shrink-0 flex-col gap-4 border-r border-sidebar-border bg-sidebar p-3 md:flex">
        <div className="flex items-center gap-2">
          <BrandMark className="size-5 rounded-md" />
          <Skeleton className="h-3.5 w-20" />
        </div>
        <Skeleton className="h-8 w-full rounded-md" />
        <div className="flex flex-col gap-2.5">
          <Skeleton className="h-3.5 w-28" />
          <Skeleton className="h-3.5 w-24" />
        </div>
        <div className="mt-auto flex items-center gap-2">
          <Skeleton className="size-7 rounded-full" />
          <Skeleton className="h-3.5 w-24" />
        </div>
      </div>
      <div aria-hidden="true" className="flex min-w-0 flex-1 flex-col">
        <div className="flex h-12 items-center border-b border-border px-3 md:px-6">
          <Skeleton className="h-5 w-24" />
        </div>
        <div className="flex flex-col gap-4 p-4 md:p-6">
          <Card className="grid grid-cols-2 md:grid-cols-4">
            {[0, 1, 2, 3].map((i) => (
              <div key={i} className="flex flex-col gap-2 p-4">
                <Skeleton className="h-3 w-16" />
                <Skeleton className="h-6 w-8" />
                <Skeleton className="h-3 w-24" />
              </div>
            ))}
          </Card>
          <Card className="flex flex-col">
            {[0, 1, 2, 3].map((i) => (
              <div key={i} className="flex h-11 items-center gap-4 border-b border-border px-3 last:border-b-0">
                <Skeleton className="h-4 w-1/3" />
                <Skeleton className="h-5 w-12" />
                <Skeleton className="ml-auto h-4 w-10" />
              </div>
            ))}
          </Card>
        </div>
      </div>
    </div>
  )
}

// A full-page notice in the sign-in screen's frame: brand, one card, a title, a sentence and
// one way forward. Used where the app cannot show itself (signed out, unknown user, no user).
export function NoticeScreen({
  title,
  children,
  action,
  alert = false,
}: {
  title: string
  children: ReactNode
  action?: ReactNode
  /** The body is announced at once (an error the person did not cause by clicking). */
  alert?: boolean
}) {
  return (
    <main className="flex flex-1 flex-col items-center justify-center gap-6 bg-background px-4 py-12 text-sm text-foreground">
      <div className="flex items-center gap-2">
        <BrandMark className="size-6 rounded-md" />
        <span className="text-base font-semibold">Piperoom</span>
      </div>
      <Card className="flex w-full max-w-[400px] flex-col gap-4 p-6 shadow-md">
        <div className="flex items-start gap-3">
          <span
            className="mt-0.5 inline-flex size-8 shrink-0 items-center justify-center rounded-md bg-muted text-muted-foreground"
            aria-hidden="true"
          >
            <LogIn className="size-4" />
          </span>
          <div className="flex min-w-0 flex-col gap-1">
            <h1 className="text-xl font-semibold tracking-tight">{title}</h1>
            <p role={alert ? 'alert' : undefined} className="m-0 text-muted-foreground">
              {children}
            </p>
          </div>
        </div>
        {action}
      </Card>
    </main>
  )
}
