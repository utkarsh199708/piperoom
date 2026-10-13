// Contrast check for the design tokens. Parses src/styles/tokens.css (so it
// always tests the real file), resolves var() and rgba tints, and prints the
// WCAG contrast ratio of every text pair in both themes. Exit code 1 if any
// pair is below 4.5:1.
//
//   node scripts/contrast.mjs

import { readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'

const here = dirname(fileURLToPath(import.meta.url))
const css = readFileSync(join(here, '..', 'src', 'styles', 'tokens.css'), 'utf8')

// ---- parse the three blocks ----
function blockAfter(marker) {
  const start = css.indexOf(marker)
  if (start < 0) throw new Error(`tokens.css: missing ${marker}`)
  const open = css.indexOf('{', start)
  // the "light, OS preference" block nests one level deeper
  let depth = 0
  for (let i = open; i < css.length; i++) {
    if (css[i] === '{') depth++
    if (css[i] === '}' && --depth === 0) return css.slice(open + 1, i)
  }
  throw new Error(`tokens.css: unbalanced ${marker}`)
}

function declarations(text) {
  const out = {}
  for (const m of text.matchAll(/(--[a-z0-9-]+)\s*:\s*([^;]+);/g)) out[m[1]] = m[2].trim()
  return out
}

const dark = declarations(blockAfter(':root {'))
const lightMedia = declarations(blockAfter('@media (prefers-color-scheme: light)'))
const lightPinned = declarations(blockAfter(":root[data-theme='light']"))

// the two light blocks must agree, or one of the two ways to get light is wrong
for (const k of new Set([...Object.keys(lightMedia), ...Object.keys(lightPinned)])) {
  if (lightMedia[k] !== lightPinned[k]) {
    console.error(`light blocks differ on ${k}: media=${lightMedia[k]} pinned=${lightPinned[k]}`)
    process.exit(1)
  }
}

const themes = { dark, light: { ...dark, ...lightPinned } }

// ---- colour maths ----
function resolve(theme, name, seen = new Set()) {
  let v = theme[name]
  if (v === undefined) throw new Error(`unknown token ${name}`)
  const ref = v.match(/^var\((--[a-z0-9-]+)\)$/)
  if (ref) {
    if (seen.has(name)) throw new Error(`cycle at ${name}`)
    return resolve(theme, ref[1], new Set(seen).add(name))
  }
  return v
}

function parseColor(v) {
  let m = v.match(/^#([0-9a-f]{6})$/i)
  if (m) {
    const n = parseInt(m[1], 16)
    return { r: (n >> 16) & 255, g: (n >> 8) & 255, b: n & 255, a: 1 }
  }
  m = v.match(/^rgba?\(\s*([\d.]+)\s*,\s*([\d.]+)\s*,\s*([\d.]+)\s*(?:,\s*([\d.]+)\s*)?\)$/)
  if (m) return { r: +m[1], g: +m[2], b: +m[3], a: m[4] === undefined ? 1 : +m[4] }
  throw new Error(`cannot parse colour ${v}`)
}

const over = (top, bottom) => ({
  r: top.r * top.a + bottom.r * (1 - top.a),
  g: top.g * top.a + bottom.g * (1 - top.a),
  b: top.b * top.a + bottom.b * (1 - top.a),
  a: 1,
})

const lin = (c) => {
  const s = c / 255
  return s <= 0.03928 ? s / 12.92 : ((s + 0.055) / 1.055) ** 2.4
}
const luminance = ({ r, g, b }) => 0.2126 * lin(r) + 0.7152 * lin(g) + 0.0722 * lin(b)
const ratio = (a, b) => {
  const [hi, lo] = [luminance(a), luminance(b)].sort((x, y) => y - x)
  return (hi + 0.05) / (lo + 0.05)
}

// ---- the pairs ----
// A background is `token` or `tint:<tint-token>@<surface-token>` (a translucent
// tint composited on a surface). Status text sits only on --bg-* or its own tint.
const SURFACES = ['--bg-0', '--bg-1', '--bg-2', '--bg-3']
const STATUS = ['--accent', '--fail', '--warn', '--ok', '--agent']
const LAYERS = [
  '--layer-postgres',
  '--layer-kafka-connect',
  '--layer-schema-registry',
  '--layer-kafka',
  '--layer-consumer',
  '--layer-warehouse',
]
const HUES = ['--h1', '--h2', '--h3', '--h4', '--h5', '--h6']

function pairs() {
  const list = []
  const add = (fg, bg, min = 4.5) => list.push({ fg, bg, min })
  for (const s of SURFACES) add('--fg', s)
  for (const s of SURFACES) add('--fg-2', s)
  for (const s of SURFACES) add('--fg-3', s)
  for (const c of STATUS) for (const s of SURFACES) add(c, s)
  // status text on its own tint, over the page and over the panel
  for (const c of STATUS) {
    for (const surface of ['--bg-0', '--bg-1']) add(c, `tint:${c}-tint@${surface}`)
  }
  // filled controls: --on-strong on the fill
  for (const c of STATUS) add('--on-strong', c)
  for (const l of LAYERS) for (const s of ['--bg-2', '--bg-3']) add(l, s)
  for (const h of HUES) add('--on-strong', h)
  // body text on the tints it appears on (banners, notices, strips)
  for (const c of ['--warn', '--accent', '--fail', '--ok', '--agent']) {
    add('--fg', `tint:${c}-tint@--bg-0`)
    add('--fg-2', `tint:${c}-tint@--bg-0`)
  }
  return list
}

function bgColor(theme, bg) {
  const m = bg.match(/^tint:(--[a-z0-9-]+)@(--[a-z0-9-]+)$/)
  if (!m) return parseColor(resolve(theme, bg))
  return over(parseColor(resolve(theme, m[1])), parseColor(resolve(theme, m[2])))
}

const short = (t) => t.replace(/^tint:(--[a-z0-9-]+)@(--[a-z0-9-]+)$/, '$1 on $2')
let failed = 0
let total = 0

for (const [name, theme] of Object.entries(themes)) {
  console.log(`\n== ${name} theme ==`)
  console.log('text'.padEnd(26) + 'on'.padEnd(44) + 'ratio  min   result')
  for (const { fg, bg, min } of pairs()) {
    const f = parseColor(resolve(theme, fg))
    const b = bgColor(theme, bg)
    const r = ratio(over(f, b), b)
    const ok = r >= min
    total++
    if (!ok) failed++
    console.log(
      fg.padEnd(26) +
        short(bg).padEnd(44) +
        r.toFixed(2).padStart(5) +
        String(min).padStart(5) +
        '   ' +
        (ok ? 'ok' : 'FAIL'),
    )
  }
}

console.log(`\n${total - failed}/${total} pairs pass`)
if (failed) {
  console.error(`${failed} pair(s) below the minimum`)
  process.exit(1)
}
