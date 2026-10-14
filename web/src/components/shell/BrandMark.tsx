import { cn } from '@/lib/utils'

export function BrandMark({ className }: { className?: string }) {
  return (
    <span
      className={cn('inline-flex size-5 items-center justify-center rounded-[5px] bg-primary', className)}
      aria-hidden="true"
    >
      <svg viewBox="0 0 24 24" className="size-3" fill="none" stroke="#fff" strokeWidth="1.75" strokeLinecap="round" strokeLinejoin="round">
        <path d="M4 12h5l2-5 3 10 2-5h4" />
      </svg>
    </span>
  )
}
