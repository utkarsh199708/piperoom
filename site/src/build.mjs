// Assembles the part files in this folder into ../public/index.html: expands icon placeholders,
// merges the extra CSS, builds the transcript, and wraps it in the document skeleton.
// Usage: node site/src/build.mjs
import fs from 'node:fs'
import { createRequire } from 'node:module'
const require = createRequire(import.meta.url)
const dir = new URL('./', import.meta.url)
const rd = (f) => fs.readFileSync(new URL(f, dir), 'utf8')
let head = rd('a-head.html')
const extra = rd('a2-css.html').replace(/<\/?style>/g, '')
head = head.replace('</style>', extra + '</style>')
const script = rd('e-script.html')
let body = rd('b-sprite.html') + rd('c-hero.html') + rd('d-sections.html')
// transcript from the script's CHAPTERS/CUES so the two never drift apart
const grab = (name) => {
  const m = script.match(new RegExp(`const ${name} = (\\[[\\s\\S]*?\\n\\]);`))
  return Function(`return ${m[1]}`)()
}
const CH = grab('CHAPTERS'), CU = grab('CUES')
const esc = (s) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;')
const fmt = (s) => `${Math.floor(s / 60)}:${String(Math.floor(s % 60)).padStart(2, '0')}`
const tr = CH.map((c) => `<div class="tr-ch"><h3>${c.n}. ${esc(c.title)}</h3>` +
  CU.filter((q) => q.s >= c.start && q.s < c.end).map((q) => `<p><b>${fmt(q.s)}</b>${esc(q.text)}</p>`).join('') + '</div>').join('')
body = body.replace('{TRANSCRIPT}', tr)
body = body.replace(/\{i:([a-z-]+)(?::([a-z0-9 -]+))?\}/g, (_, n, x) =>
  `<svg class="ico${x ? ' ' + x : ''}" aria-hidden="true"><use href="#i-${n}"/></svg>`)
const minCss = (c) => c.replace(/\/\*[\s\S]*?\*\//g, '').replace(/\s+/g, ' ').replace(/\s*([{};])\s*/g, '$1').replace(/;}/g, '}').replace(/([^\s(]):\s+/g, (m, a) => a + ':').replace(/,\s+/g, ',').replace(/\s*>\s*/g, '>')
head = head.replace(/<style>([\s\S]*?)<\/style>/, (_, c) => '<style>' + minCss(c) + '</style>')
const minJsLines = (j) => j.split('\n').map((l) => l.trim()).filter((l) => l && !l.startsWith('//')).join('\n').replace(/^\/\* [^\n]*\*\/$/gm, '')
// a real parse + compact print (acorn + escodegen) when available; the line stripper is the fallback
const minJs = (j) => {
  try {
    const acorn = require('/opt/node-tools/node_modules/acorn'), eg = require('/opt/node-tools/node_modules/escodegen')
    return eg.generate(acorn.parse(j, { ecmaVersion: 'latest' }), { format: { compact: true, quotes: 'double' } })
  } catch (e) { console.warn('minJs fallback:', e.message); return minJsLines(j) }
}
const js = script.replace(/<script>([\s\S]*?)<\/script>/, (_, c) => '<script>\n' + minJs(c) + '\n</script>')
body = body.split('\n').map((l) => l.trim()).filter(Boolean).join('\n').replace(/<!--[\s\S]*?-->\n?/g, '')
body = body.replace(/<svg class="ico([^"]*)" viewBox="0 0 24 24" aria-hidden="true">/g, '<svg class="ico$1" aria-hidden="true">').replace(/<svg viewBox="0 0 64 64"( aria-hidden="true")?><use href="#av-/g, '<svg$1><use href="#av-')
const out = head + '\n' + body + '\n' + js
if (/\{i:|\{TRANSCRIPT\}/.test(out)) throw new Error('unexpanded placeholder')
const skel = '<!doctype html><html lang="en"><head><meta charset="utf-8">' +
  '<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">' +
  '<meta name="description" content="Piperoom: a self-hosted incident room for Postgres, Debezium and Kafka pipelines. Claude investigates with read-only tools; your team decides.">' +
  '<style>:root{color-scheme:light;padding-top:env(safe-area-inset-top);padding-bottom:env(safe-area-inset-bottom)}body{margin:0;font:14px system-ui}img{max-width:100%}[hidden]{display:none!important}</style>' +
  '</head><body>'
const doc = skel + out + '</body></html>\n'
fs.writeFileSync(new URL('../public/index.html', import.meta.url), doc)
console.log('bytes', Buffer.byteLength(doc))
