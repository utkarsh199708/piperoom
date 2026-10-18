// Usage: node docs/process/ship-check.mjs <manifest-row.json> <file-list.txt> [current-patch-id]
//
// The deterministic check Lead 1 runs before merging a PR that piperoom-ship.workflow.js marked ready
// (docs/process/ship-workflow.md §5). It re-derives the gates from the PR as it is now, not as the workflow saw it:
//
//   manifest-row.json  one row of the manifest the workflow returned ({row, status, agent, folders, excluded, tier,
//                      head, patchId, approvals, ...}), saved as JSON
//   file-list.txt      the PR's real changed files, one per line, both sides of every rename (a file moved out of a
//                      folder counts for that folder too), from GitHub:
//                        gh api repos/<owner>/<repo>/pulls/<n>/files --paginate --jq '.[] | .filename, (.previous_filename // empty)'
//                      or in the row's worktree after git fetch origin main:
//                        git -c core.quotePath=false diff --no-renames --name-only $(git merge-base origin/main HEAD) HEAD
//   current-patch-id   optional, the row's patch id now, in its worktree after git fetch origin main:
//                        git diff $(git merge-base origin/main HEAD) HEAD | git patch-id --verbatim | cut -d' ' -f1
//
// It fails (exit 1) when the status is not ready, the file list is empty or names a file the manifest does not list
// (the workflow never reviewed it), the tier recomputed from the real files is
// above the manifest's, any file breaks ownership or the contract-file rule, the approvals at the manifest head are
// fewer than the tier needs, or the patch id differs from the manifest's (the row's own change moved, so its approvals
// no longer cover it). Exit 0 only when every check passes.
//
// The rules are not copied here: they are the RULES-BEGIN..RULES-END block of piperoom-ship.workflow.js next to this
// file, evaluated as is. Run it from a checkout of origin/main, not from the PR branch, so a PR that edits the rules is
// checked by the rules it would change rather than by its own.
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'

const USAGE = 'Usage: node docs/process/ship-check.mjs <manifest-row.json> <file-list.txt> [current-patch-id]'
const WORKFLOW = fileURLToPath(new URL('./piperoom-ship.workflow.js', import.meta.url))
// Tier order and approvals per tier, as in ship-workflow.md §2 (the workflow's APPROVALS map: 0, 1 and 2 models).
const RANK = { low: 0, medium: 1, high: 2 }
const NEED = { low: 0, medium: 1, high: 2 }

function fail(msg) {
  console.error(`ship-check: ${msg}\n${USAGE}`)
  process.exit(1)
}

function loadRules(file) {
  const text = readFileSync(file, 'utf8')
  const begin = text.indexOf('// RULES-BEGIN')
  const end = text.indexOf('// RULES-END')
  if (begin < 0 || end < begin) throw new Error(`no "// RULES-BEGIN" ... "// RULES-END" block in ${file}`)
  const block = text.slice(text.indexOf('\n', begin) + 1, end)
  return new Function(`'use strict'\n${block}\nreturn { norm, tierOf, ownershipViolations, CONTRACT, PIPELINE }`)()
}

const [rowPath, listPath, patchArg] = process.argv.slice(2)
if (!rowPath || !listPath) fail('a manifest row and a file list are required')

let R
try { R = loadRules(WORKFLOW) } catch (e) { fail(`loading the rules: ${e.message}`) }
let row
try { row = JSON.parse(readFileSync(rowPath, 'utf8')) } catch (e) { fail(`reading ${rowPath}: ${e.message}`) }
if (!row || typeof row !== 'object' || Array.isArray(row)) fail(`${rowPath} is not a manifest row object`)
if (Array.isArray(row.rows)) fail(`${rowPath} is a whole manifest; save the one row to check`)
let listText
try { listText = readFileSync(listPath, 'utf8') } catch (e) { fail(`reading ${listPath}: ${e.message}`) }
const files = [...new Set(listText.split(/\r?\n/).map(l => R.norm(l)).filter(Boolean))]

const head = String(row.head || '')
const short = s => String(s || '?').slice(0, 12)
const results = []
const check = (ok, name, detail) => results.push({ mark: ok ? 'PASS' : 'FAIL', name, detail })

check(row.status === 'ready', 'status', row.status === 'ready' ? 'ready' : `${row.status || 'missing'}: only a ready row may merge`)
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

const agentName = String(row.agent || '')
const own = R.ownershipViolations(files, agentName, Array.isArray(row.folders) ? row.folders : [], Array.isArray(row.excluded) ? row.excluded : [])
check(own.length === 0, 'ownership', own.length ? own.join('; ') : `every file inside ${agentName}'s folders${files.some(f => R.CONTRACT.test(f)) ? ' (contract files on a leads row)' : ''}`)

// Approvals: the stricter of the two tiers; only approve=true at the manifest head counts, one per model.
const tier = claimed in RANK && RANK[claimed] > RANK[realTier] ? claimed : realTier
const valid = (Array.isArray(row.approvals) ? row.approvals : []).filter(a => a && a.approve === true && head !== '' && a.sha === head)
const models = [...new Set(valid.map(a => String(a.model)))]
check(models.length >= NEED[tier], 'approvals',
  `${tier} needs ${NEED[tier]} approval(s) of ${short(head)} from different models; the manifest has ${models.length}${models.length ? ` (${models.join(', ')})` : ''}`)

if (patchArg === undefined) {
  results.push({ mark: 'SKIP', name: 'patch id', detail: 'not checked: pass the current patch id as the third argument' })
} else {
  const now = String(patchArg).trim().split(/\s+/)[0].toLowerCase()
  const then = String(row.patchId || '').trim().toLowerCase()
  check(now !== '' && now === then, 'patch id', now !== '' && now === then
    ? `${short(now)} unchanged since the approvals`
    : `now ${short(now)}, manifest ${short(then)}: the row's own patch changed, so its approvals must be given again (rerun the workflow on the row)`)
}

const pipeline = files.filter(f => R.PIPELINE.test(f))
console.log(`ship-check ${row.row || '?'}: head ${short(head)}, manifest tier ${claimed}, real tier ${realTier}${pipeline.length ? `, pipeline files: ${pipeline.join(', ')}` : ''}`)
for (const r of results) console.log(`${r.mark}  ${r.name.padEnd(10)} ${r.detail}`)
const failed = results.filter(r => r.mark === 'FAIL')
console.log(failed.length ? `FAIL: ${failed.length} check(s) failed. Do not merge.` : 'OK: every check passed.')
process.exit(failed.length ? 1 : 0)
