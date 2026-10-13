import { TriangleAlert } from 'lucide-react'
import { buttonVariants } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { LOGIN_AGAIN_URL } from '../lib/loginError'
import type { LoginErrorInfo } from '../lib/loginError'
import { cn } from '@/lib/utils'
import { BrandMark } from './shell/BrandMark'

// Shown instead of the app when the OIDC callback sent the person back with an
// error. No automatic redirect: the link is clicked by hand, so a failing IdP
// cannot make a loop.
export function LoginErrorScreen({ info }: { info: LoginErrorInfo }) {
  return (
    <div className="shell">
      <main className="flex flex-1 flex-col items-center justify-center gap-6 bg-background px-4 py-12 text-sm text-foreground">
        <div className="flex items-center gap-2">
          <BrandMark className="size-6 rounded-md" />
          <span className="text-base font-semibold">Piperoom</span>
        </div>
        <Card
          className="login-error flex w-full max-w-[400px] flex-col gap-4 p-6 shadow-md"
          data-code={info.code}
        >
          <div className="flex items-start gap-3">
            <span
              className="mt-0.5 inline-flex size-8 shrink-0 items-center justify-center rounded-md bg-warning-subtle text-warning-foreground"
              aria-hidden="true"
            >
              <TriangleAlert className="size-4" />
            </span>
            <div className="flex min-w-0 flex-col gap-1">
              <h1 className="text-xl font-semibold tracking-tight">{info.title}</h1>
              <p role="alert" className="text-muted-foreground">
                {info.body}
              </p>
            </div>
          </div>
          {info.canRetry && (
            <a className={cn(buttonVariants(), 'w-full')} href={LOGIN_AGAIN_URL}>
              Sign in again
            </a>
          )}
        </Card>
      </main>
    </div>
  )
}
