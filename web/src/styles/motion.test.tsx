import { afterEach, describe, expect, it } from 'vitest'
import { render } from '@testing-library/react'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { Timeline } from '../components/Timeline'

const read = (name: string) => readFileSync(fileURLToPath(new URL(name, import.meta.url)), 'utf8')
const timelineCss = read('./timeline.css')
const roomCss = read('./room.css')

// Every `animation` and `@keyframes` in the P4 stylesheets must sit inside
// `@media (prefers-reduced-motion: no-preference)`. Static is the base (spec section 5).

/** The CSS with every no-preference block cut out, brace-matched. */
function withoutOptInMotion(css: string): string {
  const marker = '@media (prefers-reduced-motion: no-preference)'
  let out = css
  for (;;) {
    const at = out.indexOf(marker)
    if (at < 0) return out
    const open = out.indexOf('{', at)
    let depth = 0
    let end = open
    for (; end < out.length; end++) {
      if (out[end] === '{') depth++
      else if (out[end] === '}') {
        depth--
        if (depth === 0) break
      }
    }
    out = out.slice(0, at) + out.slice(end + 1)
  }
}

describe('reduced motion', () => {
  it.each([
    ['timeline.css', timelineCss],
    ['room.css', roomCss],
  ])('%s has no animation outside the no-preference block', (_name, css) => {
    const base = withoutOptInMotion(css)
    expect(base).not.toMatch(/animation\s*:/)
    expect(base).not.toMatch(/animation-name\s*:/)
    expect(base).not.toMatch(/@keyframes/)
    // and the opt-in block really exists, so the check above is not vacuous
    expect(css).toMatch(/prefers-reduced-motion: no-preference/)
    expect(css).toMatch(/animation\s*:/)
  })

  describe('live bar, computed style', () => {
    afterEach(() => {
      document.head.querySelectorAll('style[data-p4]').forEach((n) => n.remove())
    })

    it('the live bar and the steps carry no animation unless the page opts in to motion', () => {
      // The whole stylesheet is injected. jsdom has no media engine and never matches
      // `prefers-reduced-motion: no-preference`, which is what a reduced-motion browser does.
      const style = document.createElement('style')
      style.dataset.p4 = '1'
      style.textContent = timelineCss
      document.head.appendChild(style)

      const { container } = render(
        <div className="room-ui">
          <Timeline
            agentState="working"
            steps={[
              {
                id: 's',
                title: 'Check',
                messages: [],
                tool_calls: [{ tool_use_id: 'u', tool: 'warehouse_freshness', input: null }],
              },
            ]}
            messages={[]}
            comments={[]}
            onComment={() => undefined}
          />
        </div>,
      )
      const none = /^(none|)$/
      const bar = container.querySelector('.live__bar') as HTMLElement
      const step = container.querySelector('.step--live') as HTMLElement
      expect(bar).not.toBeNull()
      expect(step).not.toBeNull()
      // the rules are really loaded: the static bar styles are applied
      expect(getComputedStyle(bar).height).toBe('2px')
      expect(getComputedStyle(bar).animationName ?? '').toMatch(none)
      expect(getComputedStyle(step).animationName ?? '').toMatch(none)
    })
  })
})
