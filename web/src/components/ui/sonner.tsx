import { Toaster as Sonner } from 'sonner'
import { useTheme } from '@/context/theme'

type ToasterProps = React.ComponentProps<typeof Sonner>

// Title one line (truncate), description, actions bottom-right, close top-right with a 24px hit area.
// Colours come from the theme tokens; the theme itself comes from our own ThemeProvider.
function Toaster(props: ToasterProps) {
  const { theme } = useTheme()
  return (
    <Sonner
      theme={theme}
      closeButton
      className="toaster group"
      toastOptions={{
        classNames: {
          toast:
            'group toast !rounded-lg !border !border-border !bg-popover !text-popover-foreground !shadow-lg !font-sans',
          title: '!truncate !text-sm !font-medium',
          description: '!text-xs !text-muted-foreground',
          actionButton: '!bg-primary !text-primary-foreground',
          cancelButton: '!bg-muted !text-muted-foreground',
          closeButton: '!border-border !bg-popover !text-muted-foreground',
        },
      }}
      {...props}
    />
  )
}

export { Toaster }
