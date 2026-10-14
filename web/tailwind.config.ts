import type { Config } from 'tailwindcss'
import animate from 'tailwindcss-animate'
import plugin from 'tailwindcss/plugin'

// Tokens live in src/index.css as full CSS colours (hex / rgb), per the UI v2
// handoff (docs/design/ui-v2/project/Handoff.dc.html). Tailwind 3 cannot add an
// opacity to `var(--x)` by itself, so each colour is a function: with an
// opacity modifier (`bg-primary/90`, `bg-layer-postgres/[.12]`) it becomes a
// color-mix of the token, without one it is the plain token.
// (Tailwind accepts the function at runtime; its types only list strings, hence the cast.)
const token = (name: string): string =>
  (({ opacityValue }: { opacityValue?: string | number }) =>
    // With no modifier Tailwind passes undefined or its own var(--tw-*-opacity): use the plain token.
    // Gradient stops (`from-background`) pass a number, hence the String().
    opacityValue === undefined || String(opacityValue) === '1' || String(opacityValue).startsWith('var(')
      ? `var(${name})`
      : `color-mix(in srgb, var(${name}) ${Number(opacityValue) * 100}%, transparent)`) as unknown as string

const config: Config = {
  darkMode: ['selector', '[data-theme="dark"]'],
  content: ['./index.html', './src/**/*.{ts,tsx}'],
  theme: {
    extend: {
      colors: {
        background: token('--background'),
        foreground: token('--foreground'),
        card: { DEFAULT: token('--card'), foreground: token('--card-foreground') },
        popover: { DEFAULT: token('--popover'), foreground: token('--popover-foreground') },
        secondary: { DEFAULT: token('--secondary'), foreground: token('--secondary-foreground') },
        muted: { DEFAULT: token('--muted'), foreground: token('--muted-foreground') },
        accent: { DEFAULT: token('--accent'), foreground: token('--accent-foreground') },
        border: token('--border'),
        input: token('--input'),
        'input-border': token('--input-border'),
        ring: token('--ring'),
        sidebar: { DEFAULT: token('--sidebar'), border: token('--sidebar-border') },
        'subtle-foreground': token('--subtle-foreground'),
        primary: {
          DEFAULT: token('--primary'),
          foreground: token('--primary-foreground'),
          subtle: token('--primary-subtle'),
          fg: token('--primary-fg'),
        },
        success: {
          DEFAULT: token('--success'),
          solid: token('--success-solid'),
          subtle: token('--success-subtle'),
          foreground: token('--success-foreground'),
        },
        warning: {
          DEFAULT: token('--warning'),
          subtle: token('--warning-subtle'),
          foreground: token('--warning-foreground'),
        },
        destructive: {
          DEFAULT: token('--destructive'),
          subtle: token('--danger-subtle'),
          foreground: token('--destructive-foreground'),
          fg: token('--danger-foreground'),
        },
        info: {
          DEFAULT: token('--info'),
          subtle: token('--info-subtle'),
          foreground: token('--info-foreground'),
        },
        agent: {
          DEFAULT: token('--agent'),
          subtle: token('--agent-subtle'),
          foreground: token('--agent-foreground'),
        },
        layer: {
          postgres: token('--layer-postgres'),
          'kafka-connect': token('--layer-kafka-connect'),
          'schema-registry': token('--layer-schema-registry'),
          kafka: token('--layer-kafka'),
          consumer: token('--layer-consumer'),
          warehouse: token('--layer-warehouse'),
        },
      },
      fontFamily: {
        sans: ['var(--font-sans)'],
        mono: ['var(--font-mono)'],
      },
      borderRadius: {
        lg: 'var(--radius)',
        md: 'calc(var(--radius) - 2px)',
        sm: 'calc(var(--radius) - 4px)',
      },
      fontSize: {
        xs: ['12px', '16px'],
        sm: ['13px', '18px'],
        base: ['14px', '20px'],
        lg: ['16px', '22px'],
        xl: ['20px', '26px'],
      },
      boxShadow: {
        sm: 'var(--shadow-sm)',
        md: 'var(--shadow-md)',
        lg: 'var(--shadow-lg)',
      },
      keyframes: {
        indeterminate: {
          '0%': { transform: 'translateX(-100%)' },
          '100%': { transform: 'translateX(250%)' },
        },
        check: {
          '0%': { transform: 'scale(0.6)', opacity: '0' },
          '100%': { transform: 'scale(1)', opacity: '1' },
        },
        heal: {
          '0%': { backgroundColor: 'hsl(var(--success-subtle))' },
          '100%': { backgroundColor: 'transparent' },
        },
        shimmer: {
          '0%, 100%': { opacity: '1' },
          '50%': { opacity: '.55' },
        },
      },
      animation: {
        indeterminate: 'indeterminate 1.4s ease-in-out infinite',
        shimmer: 'shimmer 1.6s ease-in-out infinite',
        check: 'check 200ms cubic-bezier(.2,.8,.2,1) both',
        heal: 'heal 600ms cubic-bezier(.2,.8,.2,1) both',
      },
    },
  },
  plugins: [
    animate,
    // Touch screens: bigger hit targets (`coarse:h-11`) and no key caps; mouse screens keep both as they are.
    plugin(({ addVariant }) => {
      addVariant('coarse', '@media (pointer: coarse)')
      addVariant('fine', '@media (pointer: fine)')
    }),
  ],
}

export default config
