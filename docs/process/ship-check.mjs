// Usage: node docs/process/ship-check.mjs [--board tasks/P-board.md] <manifest-row.json> <file-list.txt> <current-patch-id>
//
// The deterministic check Lead 1 runs before merging a PR that piperoom-ship.workflow.js marked ready
// (docs/process/ship-workflow.md §5). It re-derives the gates from the PR as it is now, not as the workflow saw it:
//
//   manifest-row.json  one row of the manifest the workflow returned ({row, status, agent, tier, head, patchId,
//                      approvals, files, ...}), saved as JSON
//   file-list.txt      the PR's real changed files, one per line, both sides of every rename (a file moved out of a
//                      folder counts for that folder too), from GitHub:
//                        gh api repos/<owner>/<repo>/pulls/<n>/files --paginate --jq '.[] | .filename, (.previous_filename // empty)'
//                      or in the row's worktree after git fetch origin main:
//                        git -c core.quotePath=false diff --no-renames --name-only $(git merge-base origin/main HEAD) HEAD
//   current-patch-id   the row's patch id now, in its worktree after git fetch origin main:
//                        git diff $(git merge-base origin/main HEAD) HEAD | git patch-id --verbatim | cut -d' ' -f1
//   --board <path>     the wave board that names the row's Agent, relative to this checkout (default tasks/P-board.md;
//                      e.g. tasks/Q-board.md)
//
// Run it from a checkout detached at origin/main (git fetch origin main && git checkout --detach origin/main), never
// from the PR branch: it reads the rules (the RULES-BEGIN..RULES-END block of piperoom-ship.workflow.js, evaluated as
// is, not copied), the owners (ownership.json, a copy of AGENTS.md §2) and the board from its own checkout, so a PR
// that edits them is checked by the versions it would change. It refuses to run anywhere else. The flag
// --i-know-this-is-not-main skips that refusal; it exists for ship-workflow.dryrun.mjs and is never valid for a merge.
//
// Checks, one PASS or FAIL line each; exit 1 if any fails, 0 only when all pass:
//   checkout   HEAD here is origin/main and the files read here are unmodified
//   status     the row is ready
//   agent      the board's Agent cell for the row equals the manifest's agent (the planner cannot relabel a row)
//   files      the list is not empty and every file is one the workflow reviewed (in the manifest's files)
//   tier       the tier of the real files is not above the manifest's
//   ownership  every file is under the board agent's folders in ownership.json and none of its exclusions; contract
//              files only on a leads row (the manifest's folders, which come from the planner, are not used)
//   approvals  the stricter tier's count (0, 1, 2) of approve=true at the manifest head, from different models
//   patch id   given, and equal to the manifest's: if the row's own patch changed, its approvals must be given again
import { readFileSync, realpathSync, existsSync } from 'node:fs'
import { spawnSync } from 'node:child_process'
import { dirname, join, relative, resolve, sep } from 'node:path'
import { fileURLToPath } from 'node:url'

const USAGE = 'Usage: node docs/process/ship-check.mjs [--board tasks/P-board.md] <manifest-row.json> <file-list.txt> <current-patch-id>'
const HERE = dirname(fileURLToPath(import.meta.url))
const ROOT = resolve(HERE, '..', '..')
const WORKFLOW = join(HERE, 'piperoom-ship.workflow.js')
const OWNERSHIP = join(HERE, 'ownership.json')
const PIN_HINT = 'run ship-check from a checkout detached at origin/main'
// Tier order and approvals per tier, as in ship-workflow.md §2 (the workflow's APPROVALS map: 0, 1 and 2 models).
const RANK = { low: 0, medium: 1, high: 2 }
const NEED = { low: 0, medium: 1, high: 2 }

function usage(msg) {
  console.error(`ship-check: ${msg}\n${USAGE}`)
  process.exit(1)
}

// ---------- arguments ----------
let boardArg = 'tasks/P-board.md'
let unpinned = false
const pos = []
const argv = process.argv.slice(2)
for (let i = 0; i < argv.length; i++) {
  const a = argv[i]
  if (a === '--i-know-this-is-not-main') unpinned = true
  else if (a === '--board') { if (i + 1 >= argv.length) usage('--board needs a path'); boardArg = argv[++i] }
  else if (a.startsWith('--board=')) boardArg = a.slice('--board='.length)
  else if (a.startsWith('--')) usage(`unknown flag ${a}`)
  else pos.push(a)
}
const [rowPath, listPath, patchArg] = pos
if (!rowPath || !listPath || pos.length > 3) usage('a manifest row, a file list and the current patch id are required')
const boardPath = resolve(ROOT, boardArg)
const boardRel = relative(ROOT, boardPath).split(sep).join('/')

// ---------- the checkout: rules, owners and board must be origin/main's ----------
function git(...args) {
  const r = spawnSync('git', ['-C', ROOT, ...args], { encoding: 'utf8' })
  // trimEnd only: a porcelain status line starts with a space
  return { ok: r.status === 0, out: (r.stdout || '').trimEnd(), err: (r.stderr || (r.error && r.error.message) || '').trim().split('\n')[0] }
}
function pinProblem() {
  if (!/^tasks\/[A-Za-z0-9]+-board\.md$/.test(boardRel)) return `the board must be a tasks/<wave>-board.md file of this checkout, not ${boardArg}`
  const top = git('rev-parse', '--show-toplevel')
  if (!top.ok) return `not a git checkout (${top.err})`
  if (realpathSync(top.out) !== realpathSync(ROOT)) return `ship-check is not at docs/process/ of the checkout ${top.out}`
  const fetch = git('fetch', '--quiet', 'origin', 'main')
  if (!fetch.ok) return `git fetch origin main failed (${fetch.err})`
  const head = git('rev-parse', 'HEAD')
  const main = git('rev-parse', 'refs/remotes/origin/main')
  if (!head.ok || !main.ok) return `cannot resolve HEAD or origin/main (${head.err || main.err})`
  if (head.out !== main.out) return `HEAD ${head.out.slice(0, 12)} is not origin/main ${main.out.slice(0, 12)}`
  const read = ['docs/process/piperoom-ship.workflow.js', 'docs/process/ship-check.mjs', 'docs/process/ownership.json', boardRel]
  const dirty = git('status', '--porcelain', '--untracked-files=all', '--', ...read)
  if (!dirty.ok) return `git status failed (${dirty.err})`
  if (dirty.out) return `local changes to files ship-check reads: ${dirty.out.split('\n').map(l => l.slice(3)).join(', ')}`
  return { head: main.out }
}

const results = []
const check = (ok, name, detail) => results.push({ mark: ok ? 'PASS' : 'FAIL', name, detail })
const print = r => console.log(`${r.mark}  ${r.name.padEnd(10)} ${r.detail}`)

if (unpinned) {
  results.push({ mark: 'SKIP', name: 'checkout', detail: '--i-know-this-is-not-main: rules, owners and board are read from a checkout not pinned to origin/main; dry run only, never valid for a merge' })
} else {
  const pin = pinProblem()
  if (typeof pin === 'string') {
    print({ mark: 'FAIL', name: 'checkout', detail: `${pin}: ${PIN_HINT}` })
    console.log('FAIL: 1 check(s) failed. Do not merge.')
    process.exit(1)
  }
  check(true, 'checkout', `HEAD is origin/main ${pin.head.slice(0, 12)}; rules, ownership.json and ${boardRel} unmodified`)
}

// ---------- inputs ----------
function loadRules(file) {
  const text = readFileSync(file, 'utf8')
  const begin = text.indexOf('// RULES-BEGIN')
  const end = text.indexOf('// RULES-END')
  if (begin < 0 || end < begin) throw new Error(`no "// RULES-BEGIN" ... "// RULES-END" block in ${file}`)
  const block = text.slice(text.indexOf('\n', begin) + 1, end)
  return new Function(`'use strict'\n${block}\nreturn { norm, tierOf, ownershipViolations, prefixOf, CONTRACT, PIPELINE }`)()
}
const read = (what, fn) => { try { return fn() } catch (e) { usage(`${what}: ${e.message}`) } }
const R = read('loading the rules', () => loadRules(WORKFLOW))
const owners = read(`reading ${OWNERSHIP}`, () => JSON.parse(readFileSync(OWNERSHIP, 'utf8')).agents)
if (!owners || typeof owners !== 'object') usage(`${OWNERSHIP} has no "agents" object`)
const row = read(`reading ${rowPath}`, () => JSON.parse(readFileSync(rowPath, 'utf8')))
if (!row || typeof row !== 'object' || Array.isArray(row)) usage(`${rowPath} is not a manifest row object`)
if (Array.isArray(row.rows)) usage(`${rowPath} is a whole manifest; save the one row to check`)
const listText = read(`reading ${listPath}`, () => readFileSync(listPath, 'utf8'))
const files = [...new Set(listText.split(/\r?\n/).map(l => R.norm(l)).filter(Boolean))]
const head = String(row.head || '')
const short = s => String(s || '?').slice(0, 12)

// The board's Agent cell for the row: the first table row whose ID cell starts with the row id, in a table whose
// header has ID and Agent columns.
function boardAgent(text, id) {
  const cells = l => l.trim().replace(/^\||\|$/g, '').split('|').map(c => c.trim())
  let cols = null
  for (const line of text.split(/\r?\n/)) {
    if (!line.trim().startsWith('|')) { cols = null; continue }
    const c = cells(line)
    if (!cols) { cols = { id: c.indexOf('ID'), agent: c.indexOf('Agent') }; continue }
    if (cols.id < 0 || cols.agent < 0 || /^:?-+:?$/.test(c[0])) continue
    const rid = (c[cols.id] || '').replace(/[`*]/g, '').split(/\s+/)[0]
    if (rid === id) return (c[cols.agent] || '').replace(/[`*]/g, '').trim()
  }
  return null
}
const boardText = existsSync(boardPath) ? readFileSync(boardPath, 'utf8') : null
const owner = boardText ? boardAgent(boardText, String(row.row || '')) : null

// ---------- checks ----------
check(row.status === 'ready', 'status', row.status === 'ready' ? 'ready' : `${row.status || 'missing'}: only a ready row may merge`)

const mAgent = String(row.agent || '')
if (!boardText) check(false, 'agent', `board ${boardArg} not found`)
else if (!owner) check(false, 'agent', `row ${row.row || '(none)'} not found on ${boardRel}; pass --board for another wave`)
else check(owner === mAgent, 'agent', owner === mAgent ? `${owner} on ${boardRel} and in the manifest` : `${boardRel} says ${owner}, the manifest says ${mAgent || '(none)'}: the planner relabelled the row`)

// Every real file must be one the workflow reviewed: a file it never saw had no review, vote or integrity check,
// even when the tier and the approvals would still add up.
const seen = new Set((Array.isArray(row.files) ? row.files : []).map(f => R.norm(f)))
const unseen = files.filter(f => !seen.has(f))
check(files.length > 0 && unseen.length === 0, 'files', !files.length
  ? `${listPath} lists no files: wrong list, or nothing to merge`
  : unseen.length ? `${unseen.length} file(s) the workflow never reviewed: ${unseen.join(', ')}` : `${files.length} changed file(s), all in the manifest`)

// Tier: the real files decide it; a manifest tier above the path tier (raised by the classifier) is kept.
const realTier = R.tierOf(files)
const claimed = row.tier
if (!(claimed in RANK)) {
  check(false, 'tier', `manifest tier is ${JSON.stringify(claimed)}; real tier is ${realTier}`)
} else if (RANK[realTier] > RANK[claimed]) {
  const raisers = files.filter(f => RANK[R.tierOf([f])] > RANK[claimed])
  check(false, 'tier', `real tier ${realTier} is above the manifest's ${claimed}, from: ${raisers.join(', ')}`)
} else {
  check(true, 'tier', RANK[realTier] === RANK[claimed] ? `${realTier}, as in the manifest` : `real ${realTier}, manifest ${claimed} (raised by the classifier; kept)`)
}

// Ownership from ownership.json for the board's agent. A '*' pattern is expanded to the real files it matches, so
// the rules block's own ownershipViolations decides; a file named exactly in folders wins over a pattern in excluded.
const glob = p => new RegExp('^' + p.split('*').map(s => s.replace(/[.+?^${}()|[\]\\]/g, '\\$&')).join('[^/]*') + '$')
const isPattern = p => R.prefixOf(p).includes('*')
const expand = (list, keep) => list.flatMap(p => (isPattern(p) ? files.filter(f => glob(R.prefixOf(p)).test(f) && !keep.has(f)) : [p]))
const entry = owner ? owners[owner] : null
if (!owner) {
  check(false, 'ownership', 'no board agent to check against')
} else if (!entry || !Array.isArray(entry.folders) || !Array.isArray(entry.excluded)) {
  check(false, 'ownership', `${owner} has no entry in ownership.json`)
} else {
  const exact = new Set(entry.folders.filter(p => !isPattern(p)).map(R.prefixOf))
  const own = R.ownershipViolations(files, owner, expand(entry.folders, new Set()), expand(entry.excluded, exact))
  check(own.length === 0, 'ownership', own.length ? own.join('; ') : `every file inside ${owner}'s folders${files.some(f => R.CONTRACT.test(f)) ? ' (contract files on a leads row)' : ''}`)
}

// Approvals: the stricter of the two tiers; only approve=true at the manifest head counts, one per model.
const tier = claimed in RANK && RANK[claimed] > RANK[realTier] ? claimed : realTier
const valid = (Array.isArray(row.approvals) ? row.approvals : []).filter(a => a && a.approve === true && head !== '' && a.sha === head)
const models = [...new Set(valid.map(a => String(a.model)))]
check(models.length >= NEED[tier], 'approvals',
  `${tier} needs ${NEED[tier]} approval(s) of ${short(head)} from different models; the manifest has ${models.length}${models.length ? ` (${models.join(', ')})` : ''}`)

const now = String(patchArg || '').trim().split(/\s+/)[0].toLowerCase()
const then = String(row.patchId || '').trim().toLowerCase()
if (!now) check(false, 'patch id', 'not given: pass the row\'s current patch id (see the usage); without it the approvals cannot be tied to the PR')
else check(now === then, 'patch id', now === then
  ? `${short(now)} unchanged since the approvals`
  : `now ${short(now)}, manifest ${short(then)}: the row's own patch changed, so its approvals must be given again (rerun the workflow on the row)`)

// ---------- report ----------
const pipeline = files.filter(f => R.PIPELINE.test(f))
console.log(`ship-check ${row.row || '?'}: head ${short(head)}, manifest tier ${claimed}, real tier ${realTier}${pipeline.length ? `, pipeline files: ${pipeline.join(', ')}` : ''}`)
if (entry) console.log(`owner      ${owner} (ownership.json): folders ${entry.folders.join(', ') || 'none'}; excluded ${entry.excluded.join(', ') || 'none'}`)
results.forEach(print)
const failed = results.filter(r => r.mark === 'FAIL')
console.log(failed.length ? `FAIL: ${failed.length} check(s) failed. Do not merge.` : unpinned ? 'OK for a dry run only: the checkout was not pinned to origin/main.' : 'OK: every check passed.')
process.exit(failed.length ? 1 : 0)
