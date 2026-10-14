import { Moon, Sun } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Kbd } from '@/components/ui/kbd'
import { Tooltip, TooltipContent, TooltipTrigger } from '@/components/ui/tooltip'
import { useTheme } from '@/context/theme'
import { modKeyLabel } from '@/lib/envLabel'

export function ThemeToggle() {
  const { theme, toggleTheme } = useTheme()
  const next = theme === 'dark' ? 'light' : 'dark'
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <Button variant="ghost" size="icon" aria-label={`Switch to ${next} theme`} onClick={toggleTheme}>
          {theme === 'dark' ? <Sun aria-hidden="true" /> : <Moon aria-hidden="true" />}
        </Button>
      </TooltipTrigger>
      <TooltipContent className="flex items-center gap-2">
        Switch to {next} theme
        <span className="inline-flex gap-0.5">
          <Kbd>{modKeyLabel()}</Kbd>
          <Kbd>⇧</Kbd>
          <Kbd>L</Kbd>
        </span>
      </TooltipContent>
    </Tooltip>
  )
}
