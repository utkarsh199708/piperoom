import { TriangleAlert } from 'lucide-react'
import { Link } from 'react-router-dom'
import { Button } from '@/components/ui/button'

// The room could not be loaded (network, 404, forbidden): say what happened and offer
// the two ways out. The text keeps the wording the old screen used.
export function RoomError({ message, onRetry }: { message: string; onRetry: () => void }) {
  return (
    <div className="mx-auto flex max-w-md flex-col items-center gap-3 px-4 py-20 text-center">
      <span className="flex size-10 items-center justify-center rounded-full bg-destructive-subtle text-destructive-fg">
        <TriangleAlert className="size-5" aria-hidden="true" />
      </span>
      <h1 className="m-0 text-lg font-semibold">Could not load room</h1>
      <p role="alert" className="m-0 break-words text-muted-foreground">
        {message}
      </p>
      <div className="mt-1 flex items-center gap-2">
        <Button variant="secondary" onClick={onRetry}>
          Try again
        </Button>
        <Button variant="ghost" asChild>
          <Link to="/">Back to rooms</Link>
        </Button>
      </div>
    </div>
  )
}
