// Usage: node site/src/shoot.mjs site/public/index.html <outdir> [label]
// Needs Playwright with Chromium (preinstalled in Claude Code cloud sessions).
// Renders the page at desktop and phone widths, light and dark, normal and reduced motion,
// full-page and above-the-fold, plus explainer frames if window.__explainerSeek exists.
import { chromium } from '/opt/node-tools/node_modules/playwright/index.mjs'
import { pathToFileURL } from 'node:url'
import fs from 'node:fs'
const [page, out, label = 'shot'] = process.argv.slice(2)
fs.mkdirSync(out, { recursive: true })
// The page is published inside a skeleton that adds charset/viewport; mimic it.
const body = fs.readFileSync(page, 'utf8')
// public/index.html is already a full document; only a bare page body needs the skeleton.
const isDoc = /^\s*<!doctype/i.test(body)
const wrapped = `<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover"><style>:root{color-scheme:light;padding-top:env(safe-area-inset-top);padding-bottom:env(safe-area-inset-bottom)}body{margin:0;font:14px system-ui}img{max-width:100%}[hidden]{display:none!important}</style></head><body>${body}</body></html>`
const tmp = out + '/_wrapped.html'
fs.writeFileSync(tmp, isDoc ? body : wrapped)
const url = pathToFileURL(tmp).href
const b = await chromium.launch()
const log = []
for (const [w, h, name] of [[1440, 900, 'desktop'], [390, 844, 'phone']]) {
  for (const scheme of ['dark', 'light']) {
    for (const motion of ['no-preference', 'reduce']) {
      if (motion === 'reduce' && !(name === 'desktop' && scheme === 'dark')) continue
      const ctx = await b.newContext({ viewport: { width: w, height: h }, colorScheme: scheme, reducedMotion: motion, deviceScaleFactor: 1 })
      const p = await ctx.newPage()
      p.on('pageerror', (e) => log.push(`[${name}/${scheme}/${motion}] pageerror: ${e.message}`))
      p.on('console', (m) => { if (m.type() === 'error') log.push(`[${name}/${scheme}/${motion}] console.error: ${m.text()}`) })
      await p.goto(url, { waitUntil: 'load', timeout: 30000 }).catch((e) => log.push('goto: ' + e.message))
      await p.waitForTimeout(1800)
      const tag = `${label}-${name}-${scheme}${motion === 'reduce' ? '-reduced' : ''}`
      await p.screenshot({ path: `${out}/${tag}-fold.png` })
      await p.screenshot({ path: `${out}/${tag}-full.png`, fullPage: true })
      const overflow = await p.evaluate(() => {
        const W = document.documentElement.clientWidth, bad = []
        for (const el of document.querySelectorAll('body *')) {
          const r = el.getBoundingClientRect()
          if (r.width && (r.right > W + 1 || r.left < -1)) {
            let a = el, ok = false
            while (a && a !== document.body) { const s = getComputedStyle(a); if (/(auto|scroll|hidden|clip)/.test(s.overflowX)) { ok = true; break } a = a.parentElement }
            if (!ok) bad.push((el.tagName + '.' + (el.className?.baseVal ?? el.className)).slice(0, 80) + ' right=' + Math.round(r.right))
          }
        }
        return { scrollW: document.documentElement.scrollWidth, clientW: W, offenders: bad.slice(0, 12) }
      })
      log.push(`[${tag}] scrollWidth=${overflow.scrollW} clientWidth=${overflow.clientW} offenders=${JSON.stringify(overflow.offenders)}`)
      if (name === 'desktop' && motion === 'no-preference') {
        const has = await p.evaluate(() => typeof window.__explainerSeek === 'function')
        if (has) {
          const dur = await p.evaluate(() => (window.__explainerDuration ?? 60))
          const el = await p.$('[data-explainer]')
          for (const t of [0.5, dur * 0.17, dur * 0.34, dur * 0.5, dur * 0.67, dur * 0.84, dur - 0.5]) {
            await p.evaluate((t) => window.__explainerSeek(t), t)
            await p.waitForTimeout(350)
            if (el) await el.screenshot({ path: `${out}/${tag}-explainer-t${Math.round(t)}.png` }).catch(() => {})
          }
        } else log.push(`[${tag}] no window.__explainerSeek`)
      }
      await ctx.close()
    }
  }
}
await b.close()
fs.writeFileSync(out + '/log.txt', log.join('\n') + '\n')
console.log(log.join('\n'))
console.log('screenshots in', out)
