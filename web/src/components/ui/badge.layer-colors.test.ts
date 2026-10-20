import { describe, expect, it } from 'vitest'
import tailwindConfig from '../../../tailwind.config'
import { LAYER_VARIANTS } from './badge'

// The badge variants reference `text-layer-<key>` / `bg-layer-<key>`, which Tailwind
// only emits if the key is listed in `theme.extend.colors.layer`. This guard fails
// when a layer is added to LAYER_VARIANTS but forgotten in tailwind.config.ts.
type ThemeShape = { theme?: { extend?: { colors?: Record<string, unknown> } } }

describe('layer badge colours', () => {
  it('every LAYER_VARIANTS key has a theme.extend.colors.layer entry', () => {
    const layer = (tailwindConfig as ThemeShape).theme?.extend?.colors?.layer as
      | Record<string, unknown>
      | undefined
    expect(layer).toBeDefined()
    for (const key of Object.keys(LAYER_VARIANTS)) {
      expect(layer?.[key], `missing theme.extend.colors.layer["${key}"]`).toBeDefined()
    }
  })
})
