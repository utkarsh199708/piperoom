import { readFileSync } from 'node:fs'
import { join } from 'node:path'
import { beforeAll, describe, expect, it } from 'vitest'
import { compile } from 'tailwindcss'
import { LAYER_VARIANTS } from './badge'

// The badge variants reference `text-layer-<key>` / `bg-layer-<key>`, which Tailwind
// only emits when the key is bridged in the `@theme` block of src/index.css. This
// guard compiles the real stylesheet and fails when a layer is added to
// LAYER_VARIANTS but forgotten in the theme (no CSS) or in a colour mode (no token).
// Vitest runs from web/, and jsdom has no usable `import.meta.url`, hence cwd.
const webRoot = process.cwd()
const entryCss = join(webRoot, 'src/index.css')

// The stylesheet imports Tailwind's own CSS by package name; `compile` has no
// bundler, so it resolves those imports through this table.
const stylesheets: Record<string, string> = {
  tailwindcss: 'node_modules/tailwindcss/index.css',
  'tw-animate-css': 'node_modules/tw-animate-css/dist/tw-animate.css',
}

const layerKeys = Object.keys(LAYER_VARIANTS)

let css = ''

beforeAll(async () => {
  const compiler = await compile(readFileSync(entryCss, 'utf8'), {
    base: webRoot,
    loadStylesheet: async (id) => {
      const file = stylesheets[id]
      if (!file) throw new Error(`unexpected @import "${id}" in src/index.css`)
      return { path: join(webRoot, file), base: webRoot, content: readFileSync(join(webRoot, file), 'utf8') }
    },
  })
  css = compiler.build(layerKeys.flatMap((key) => [`bg-layer-${key}/[.12]`, `text-layer-${key}`]))
})

describe('layer badge colours', () => {
  it('has a LAYER_VARIANTS entry per layer colour', () => {
    // The keys below are the `--layer-*` tokens in src/index.css.
    expect(layerKeys.sort()).toEqual(
      [
        'consumer',
        'kafka',
        'kafka-connect',
        'orchestrator',
        'postgres',
        'schema-registry',
        'transform',
        'warehouse',
      ].sort(),
    )
  })

  it.each(layerKeys)('%s: the badge utilities are generated', (key) => {
    expect(css).toMatch(new RegExp(`\\.text-layer-${key}\\s*\\{\\s*color:\\s*var\\(--layer-${key}\\)`))
    // The badge tint is `bg-layer-<key>/[.12]`, which only exists as a color-mix of the token.
    expect(css).toMatch(new RegExp(`\\.bg-layer-${key}[^{]*\\{[^}]*color-mix\\([^)]*var\\(--layer-${key}\\)`))
  })

  it.each(layerKeys)('%s: the token is defined in every colour mode', (key) => {
    // Dark (:root), light ([data-theme='light']) and OS-light (prefers-color-scheme).
    const values = [...css.matchAll(new RegExp(`--layer-${key}:\\s*([^;]+);`, 'g'))].map((m) => m[1].trim())
    expect(values).toHaveLength(3)
    // Both light modes must agree, and light must not be a copy of dark.
    expect(values[1]).toBe(values[2])
    expect(values[0]).not.toBe(values[1])
  })
})
