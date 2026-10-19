export const meta = {
  name: 'piperoom-ship',
  description: 'Build Piperoom board rows end to end with no human approval: implement, re-verify, parallel bug hunt, verifier votes, security review, risk tier from the diff, capped fix rounds, tiered agent approvals; returns a ship manifest',
  whenToUse: 'Run on rows of tasks/P-board.md (or another wave board) whose After rows are merged. Lead 1 then ships each ready row as docs/process/ship-workflow.md §5 says.',
  phases: [
    { title: 'Plan', detail: 'is each row dispatchable; owner, folders, task file', model: 'opus' },
    { title: 'Build', detail: 'one implementer per row in its own worktree', model: 'opus' },
    { title: 'Verify', detail: 're-run format, vet, build and the full test suites independently', model: 'opus' },
    { title: 'Review', detail: 'four bug hunters and a security reviewer in parallel', model: 'fable' },
    { title: 'Vote', detail: 'three verifiers try to refute each finding', model: 'fable' },
    { title: 'Fix', detail: 'fix every verified finding by class; max 3 rounds; diff-only after the full review', model: 'opus' },
    { title: 'Approve', detail: 'risk tier from the diff; 0, 1 or 2 independent agent approvals at head', model: 'fable' },
  ],
}

// ---------- inputs (clamped: arguments can only tighten the gates) ----------
// args: ["P3-0", "P1a-2"] or {rows, maxRounds (1..3), trailers, board, worktreeRoot,
//        implementModel, reviewModel, secondModel, resume: {"P3-0": {worktree, branch}}}
const A = Array.isArray(args) ? { rows: args } : (args || {})
const ROWS = (A.rows || []).map(String)
const MAX_ROUNDS = Math.min(3, Math.max(1, Math.floor(Number(A.maxRounds)) || 3))
const BOARD = A.board || 'tasks/P-board.md'
const TRAILERS = A.trailers || 'Co-Authored-By: Claude <noreply@anthropic.com>'
const WT_ROOT = A.worktreeRoot || '../piperoom-ship'
const RESUME = A.resume || {}
const BUILD_MODEL = A.implementModel || 'opus'
const REVIEW_MODEL = A.reviewModel || 'fable'
let SECOND_MODEL = A.secondModel || 'opus'
if (String(SECOND_MODEL).toLowerCase() === String(REVIEW_MODEL).toLowerCase()) {
  SECOND_MODEL = String(REVIEW_MODEL).toLowerCase() === 'opus' ? 'fable' : 'opus'
  log(`secondModel equal to reviewModel is not allowed; using ${SECOND_MODEL} for the independent second opinion`)
}

if (!ROWS.length) {
  log('No rows given. Pass args like ["P3-0", "P1a-2"].')
  return { rows: [] }
}

// RULES-BEGIN (docs/process/ship-check.mjs evaluates this block, so Lead 1 re-checks a PR with the same rules)
// The change sets the tier, not the author. Paths are repo-relative after norm().
const CONTRACT = /^(internal\/tools\/tool\.go|internal\/events\/events\.go|internal\/clients\/clients\.go|internal\/actions\/action\.go|internal\/agent\/contract\.go|internal\/agent\/llm\/llm\.go|internal\/topology\/graph\.go|internal\/config\/config\.go|internal\/version\/version\.go|docs\/api\.md)$/
const PIPELINE = /^(docs\/process\/|docs\/collab\.md$|\.claude\/|\.github\/|tasks\/|Makefile$|\.gitattributes$|web\/(eslint|vite|vitest)\.config|web\/\.eslintrc|web\/tsconfig)|(^|\/)(AGENTS|CLAUDE|REVIEW)\.md$/
const HIGH = [
  /^internal\/actions\//,                                        // everything that writes to a customer system
  /^internal\/clients\/(connect|schemareg|pgconv|netsec|kafka|postgres)\//, // clients with write paths, the code writers share, the read-only pool
  /^internal\/clients\/[^/]+\/writer/,                            // write clients
  /^internal\/room\//,                                            // approvals, write gate, executor, preview, replicas, control, policies
  /^internal\/owners\//, /^internal\/wiring\//, /^cmd\/server\//, /^internal\/tools\/scope\.go$/,
  /^internal\/auth\//, /^internal\/httpapi\/(auth|server)/, /^internal\/alertin\//, /^internal\/store\//,
  /^internal\/agent\/llm\//,                                      // model adapters: provider keys, the SSRF dialer, wire translation (V wave)
  /^internal\/redact\//, /^internal\/injectscan\//, /^internal\/agent\/(alerttext|alertevidence|injectflag|approvals|prompt|readonly|redact|run\.go$)/,
  /^migrations\//, /^deploy\//, /Dockerfile$/, /^go\.(mod|sum)$/, /^web\/package(-lock)?\.json$/,
  CONTRACT, PIPELINE,
]
const LOW = [/^docs\//, /^reviews\//, /^research\//, /^README\.md$/]
const norm = (f, wt) => {
  let s = String(f || '').trim().replace(/\\/g, '/')
  const w = String(wt || '').trim().replace(/\\/g, '/').replace(/\/+$/, '')
  if (w && s.startsWith(w + '/')) s = s.slice(w.length + 1)
  return s.replace(/^(\.\/)+/, '').replace(/^\/+/, '')
}
function tierOf(files) {
  if (!files.length) return 'high' // never fail open
  if (files.some(f => HIGH.some(rx => rx.test(f)))) return 'high'
  if (files.every(f => LOW.some(rx => rx.test(f)))) return 'low'
  return 'medium'
}
const prefixOf = p => norm(String(p).replace(/`/g, '').replace(/\*+$/, '').replace(/\/+$/, ''))
const under = (f, p) => { const q = prefixOf(p); return q !== '' && (f === q || f.startsWith(q + '/')) }
// Ownership: every file under one of the owner's folders and under none of its exclusions; contract files only
// on a leads' row. Returns the violations, [] when clean.
function ownershipViolations(files, agentName, folders, excluded) {
  const out = []
  for (const f of files) {
    if (CONTRACT.test(f) && agentName !== 'leads') { out.push(`${f}: contract file (needs a leads' contract change request)`); continue }
    if (!folders.some(p => under(f, p)) || excluded.some(p => under(f, p))) out.push(`${f}: outside ${agentName}'s folders`)
  }
  return out
}
// RULES-END
// Approvals stand while the row's own patch is unchanged: the first field of
// `git diff $(git merge-base origin/main HEAD) HEAD | git patch-id --verbatim`, run in the row's worktree after
// `git fetch origin main`, equals the manifest's `patchId`. A conflict resolution or any other change means rerunning
// the workflow on that row with resume (`args.resume = {ROW: {worktree, branch}}`). --verbatim keeps whitespace, so
// an indentation-only change to YAML or a Makefile counts as a change (git 2.39 or later).
const RANK = { low: 0, medium: 1, high: 2 }
const APPROVALS = { low: [], medium: [REVIEW_MODEL], high: [REVIEW_MODEL, SECOND_MODEL] }

// ---------- schemas ----------
const CHECK = { type: 'object', properties: { command: { type: 'string' }, result: { type: 'string', enum: ['pass', 'fail'] }, output: { type: 'string' } }, required: ['command', 'result', 'output'] }
const PLAN = {
  type: 'object',
  properties: { rows: { type: 'array', items: { type: 'object', properties: {
    id: { type: 'string' }, ready: { type: 'boolean' }, missing: { type: 'array', items: { type: 'string' } },
    agent: { type: 'string' }, folders: { type: 'array', items: { type: 'string' } }, excluded: { type: 'array', items: { type: 'string' } },
    taskFile: { type: 'string' }, summary: { type: 'string' }, doneWhen: { type: 'string' }, prTitle: { type: 'string' },
  }, required: ['id', 'ready', 'missing', 'agent', 'folders', 'excluded', 'taskFile', 'summary', 'doneWhen', 'prTitle'] } } },
  required: ['rows'],
}
const BUILD = {
  type: 'object',
  properties: {
    status: { type: 'string', enum: ['built', 'blocked'] }, blockedReason: { type: 'string' },
    worktree: { type: 'string' }, branch: { type: 'string' }, head: { type: 'string' },
    checks: { type: 'array', items: CHECK }, howToRevert: { type: 'string' }, offSwitch: { type: 'string' }, notDone: { type: 'string' },
    newDependencies: { type: 'string' }, contractChangeRequest: { type: 'string' },
  },
  required: ['status', 'worktree', 'branch', 'head', 'checks', 'howToRevert', 'offSwitch', 'notDone', 'newDependencies', 'contractChangeRequest'],
}
const VERIFY = {
  type: 'object',
  properties: { pass: { type: 'boolean' }, base: { type: 'string' }, head: { type: 'string' }, patchId: { type: 'string' },
    files: { type: 'array', items: { type: 'string' } }, checks: { type: 'array', items: CHECK }, problems: { type: 'array', items: { type: 'string' } } },
  required: ['pass', 'base', 'head', 'patchId', 'files', 'checks', 'problems'],
}
const FINDING = { type: 'object', properties: {
  file: { type: 'string' }, line: { type: 'integer' }, severity: { type: 'string', enum: ['important', 'nit', 'preexisting'] },
  bugClass: { type: 'string' }, claim: { type: 'string' }, evidence: { type: 'string' }, fix: { type: 'string' },
}, required: ['file', 'line', 'severity', 'bugClass', 'claim', 'evidence', 'fix'] }
const FINDINGS = { type: 'object', properties: { findings: { type: 'array', items: FINDING } }, required: ['findings'] }
const VERDICT = { type: 'object', properties: { refuted: { type: 'boolean' }, reason: { type: 'string' } }, required: ['refuted', 'reason'] }
const STILL = { type: 'object', properties: { fixed: { type: 'boolean' }, evidence: { type: 'string' } }, required: ['fixed', 'evidence'] }
const FIX = { type: 'object', properties: { head: { type: 'string' },
  fixed: { type: 'array', items: { type: 'string' } }, notFixed: { type: 'array', items: { type: 'string' } } },
  required: ['head', 'fixed', 'notFixed'] }
const TIER = { type: 'object', properties: { raiseTo: { type: 'string', enum: ['none', 'medium', 'high'] }, reason: { type: 'string' },
  files: { type: 'array', items: { type: 'string' } } }, required: ['raiseTo', 'reason', 'files'] }
const APPROVAL = { type: 'object', properties: { approve: { type: 'boolean' }, sha: { type: 'string' },
  reasons: { type: 'array', items: { type: 'string' } }, blocking: { type: 'array', items: FINDING } },
  required: ['approve', 'sha', 'reasons', 'blocking'] }
const INTEGRITY = { type: 'object', properties: { weakens: { type: 'boolean' }, what: { type: 'array', items: { type: 'string' } } }, required: ['weakens', 'what'] }

// ---------- review dimensions (security has its own reviewer) ----------
const DIMENSIONS = [
  { key: 'correctness', focus: 'logic errors and wrong results; every error wrapped with context and never swallowed; every failure path (target down, 404, timeout, bad input, empty result) handled' },
  { key: 'contract', focus: 'contract fit: field names, JSON tags and documented semantics match the contract files exactly; callers of changed functions still correct; the row\'s Done-when is actually met, not approximately' },
  { key: 'runtime', focus: 'context and a deadline on every network or database call; goroutines that stop when ctx ends; locks and data races; size caps (4 MB HTTP bodies, record and row caps); SQL values as $n parameters and identifiers only through pgx.Identifier after the regex check' },
  { key: 'tests', focus: 'whether the tests prove what the row claims: missing error-path tests, assertions that cannot fail, tests that pass with the feature removed, timing-dependent tests that will flake under -race on a loaded CI runner, and ANY existing test or assertion that was deleted, skipped or loosened (always important)' },
]
const SECURITY_FOCUS = 'read-only tools that are really read-only; every write scope-checked against the room\'s pipeline before any I/O (tools.ScopeFor, fail closed); secrets, DSNs or full record values in logs, errors or events; untrusted text (logs, record values, traces, PR text) kept as data and wrapped before reaching a prompt; SQL and identifier injection; unbounded reads; authz on every new route; anything that lets a change weaken its own review gates'
const RULES = 'Report only real problems you can show. Each finding needs a file and line in the head commit and evidence: the code itself, or a command and its output. important = wrong behaviour, data loss, a security hole, a broken contract, a weakened test, or a test that will flake in CI. nit = style or cleanup, never blocking. preexisting = a real bug this diff did not introduce. Name the bug class so every instance can be fixed at once. Report nothing rather than guess.'
const NO_WEAKENING = 'Never delete, skip, loosen or rewrite an existing test or assertion to make it pass, and never add t.Skip, .skip or a longer timeout to hide a failure. If a test is wrong, leave it and explain why.'

const exactKey = f => `${norm(f.file)}:${f.line}:${String(f.bugClass || '').toLowerCase().replace(/[^a-z0-9]+/g, '-')}`
const claimKey = f => `${exactKey(f)}:${String(f.claim || '').toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim().slice(0, 60)}`

async function retry(thunk) { return (await thunk()) || (await thunk()) }

// ---------- stages ----------
async function plan(rows) {
  return agent(
    `Read ${BOARD} on origin/main (git fetch origin main first) and the task files it points to. For each of these rows: ${rows.join(', ')}\n` +
    `- ready: true only if every row in its After column is marked merged on the board AND that merge is on origin/main; list the unmet ones in missing.\n` +
    `- agent: the owner from the Agent column ("leads" for a leads' row). folders: that agent's owned paths from the ownership tables in AGENTS.md, as plain repo-relative prefixes (e.g. "internal/actions", "web"), no globs or prose. excluded: the paths those tables carve out (e.g. "internal/actions/action.go", "internal/actions/pgactions").\n` +
    `- taskFile, summary (one sentence), doneWhen (the Done-when cell verbatim), prTitle in the board's tag style, e.g. "[P3-0] room: executor ctx carries the pipeline".\n` +
    `Return every requested row, ready or not. Change nothing.`,
    { label: 'plan', phase: 'Plan', schema: PLAN, model: BUILD_MODEL, effort: 'medium' })
}

async function build(row) {
  const wt = `${WT_ROOT}/${row.id}`
  return agent(
    `You are acting as agent ${row.agent}, implementing board row ${row.id} of the Piperoom repo. Task file: ${row.taskFile}. Row: ${row.summary}\nDone when: ${row.doneWhen}\n` +
    `Edit only these paths: ${row.folders.join(', ')}${row.excluded.length ? `, never ${row.excluded.join(', ')}` : ''}. Never edit contract files: if the row needs one, stop with status blocked and put the exact proposed code in contractChangeRequest. Follow AGENTS.md §4 and §5. ${NO_WEAKENING}\n` +
    `Steps:\n1. From the main checkout: git fetch origin main; git worktree add ${wt} -b p/${row.id} origin/main (if it exists, stop: status blocked, "worktree exists; rerun with resume"). Work only in ${wt}.\n` +
    `2. Implement the row with tests, including the error paths. Keep the diff under about 600 changed lines.\n` +
    `3. Prove it from ${wt}: the toolchain gofmt -l on changed Go dirs ($(go env GOROOT)/bin/gofmt), go vet ./..., go build ./..., go test ./..., go test -race on changed packages that start goroutines; for web/** npm ci, npm run lint, npm run build, npx vitest run. Put each command and the tail of its real output in checks. Fix failures before you finish.\n` +
    `4. Commit (imperative "area: what" subject) ending with these trailer lines:\n${TRAILERS}\nDo not push.\n` +
    `Return worktree (absolute path of ${wt}), branch, head (full sha), checks, howToRevert (what reverting the merge undoes, and any data or config to undo too), offSwitch (the flag or setting that turns new behaviour off, or "none"), notDone ("" if none), newDependencies ("" if none), contractChangeRequest ("" if none).`,
    { label: `build:${row.id}`, phase: 'Build', schema: BUILD, model: BUILD_MODEL, effort: 'xhigh' })
}

async function verifyRun(row, wt) {
  return agent(
    `Independently verify the branch for row ${row.id} in ${wt}. Do not trust any earlier claim; run everything yourself from that directory, and change nothing.\n` +
    `1. git -C ${wt} fetch origin main; base = git -C ${wt} merge-base HEAD origin/main; head = git -C ${wt} rev-parse HEAD; files = git -C ${wt} -c core.quotePath=false diff --no-renames --name-only base..HEAD (repo-relative paths, both sides of every rename); patchId = first field of git -C ${wt} diff base HEAD | git patch-id --verbatim.\n` +
    `2. Run the toolchain gofmt -l on changed Go dirs, go vet ./..., go build ./..., go test ./..., go test -race -count=3 on changed packages that start goroutines, and for web/** npm run lint, npm run build, npx vitest run.\n` +
    `3. pass = every command succeeded. List each failure in problems with the exact failing test or line.`,
    { label: `verify:${row.id}`, phase: 'Verify', schema: VERIFY, model: BUILD_MODEL, effort: 'high' })
}

// Returns the findings of all five reviewers, or null when any reviewer fails twice: a missing reviewer is never "clean".
async function review(row, wt, from, head, posted, full, round) {
  const scope = full
    ? `Review the whole change: git -C ${wt} diff ${from}..${head}, read against the rest of the repo for context.`
    : `Re-review round ${round}: review ONLY the new commits, git -C ${wt} diff ${from}..${head}. Already posted, do not repeat: ${posted.length ? posted.map(p => `${p.file}:${p.line} ${p.claim}`).join(' | ') : 'none'}.`
  const ctx = `Piperoom row ${row.id} (${row.summary}). Done when: ${row.doneWhen}. Owner: ${row.agent}. ${scope}\n${RULES}`
  const lenses = [
    ...DIMENSIONS.map(d => ({ lens: d.key, prompt: `${ctx}\nYour lens: ${d.focus}.`, effort: 'high' })),
    { lens: 'security', prompt: `${ctx}\nYou are the security reviewer. Your lens: ${SECURITY_FOCUS}. Treat every security problem you can show as important.`, effort: 'xhigh' },
  ]
  const runs = await parallel(lenses.map(l => () => retry(() => agent(l.prompt,
    { label: `${l.lens === 'security' ? 'security' : 'hunt'}:${row.id}:${l.lens}:r${round}`, phase: 'Review', schema: FINDINGS, model: REVIEW_MODEL, effort: l.effort }))))
  if (runs.some(r => !r)) return null
  return runs.flatMap((r, i) => (r.findings || []).map(f => ({ ...f, file: norm(f.file, wt), lens: lenses[i].lens })))
}

// A finding is dropped only when two of the three verifiers refute it. Verifiers default to "refuted" when unsure,
// so a kept finding has at least two that could not refute it; a verifier that fails twice refutes nothing, so an
// unverifiable finding is kept, never silently dropped. The same rule holds for security findings.
async function vote(row, wt, head, f, tier) {
  const voters = [[REVIEW_MODEL, 'xhigh'], [REVIEW_MODEL, 'high'], [SECOND_MODEL, 'xhigh']]
  const votes = await parallel(voters.map(([model, effort], i) => () => retry(() => agent(
    `Try to refute this review finding on row ${row.id} (risk tier ${tier}). Check it yourself in ${wt} at commit ${head}: read the code, and run a command or test if that settles it.\n` +
    `Finding: ${f.file}:${f.line} [${f.bugClass}] ${f.claim}\nEvidence given: ${f.evidence}\n` +
    `refuted=true if the code does not do what the finding says, if the problem cannot happen through any real caller or input, or if you cannot confirm it with evidence. Default to refuted=true when unsure.`,
    { label: `vote:${row.id}:${f.file.split('/').pop()}:${f.line}:${i + 1}`, phase: 'Vote', schema: VERDICT, model, effort }))))
  return votes.filter(v => v && v.refuted).length < 2
}

async function stillOpen(row, wt, head, f) {
  const r = await retry(() => agent(
    `Row ${row.id}, ${wt} at commit ${head}. This verified finding was sent to be fixed: ${f.file}:${f.line} [${f.bugClass}] ${f.claim}\nEvidence: ${f.evidence}\n` +
    `Check the code at ${head}: is this problem, and every other instance of the same class in the change, gone? fixed=true only with evidence. A deleted or loosened test is not a fix.`,
    { label: `check:${row.id}:${f.file.split('/').pop()}:${f.line}`, phase: 'Fix', schema: STILL, model: REVIEW_MODEL, effort: 'high' }))
  return !(r && r.fixed)
}

async function fix(row, wt, head, items, round) {
  return agent(
    `Fix round ${round} for row ${row.id} in ${wt} (acting as ${row.agent}; edit only ${row.folders.join(', ')}${row.excluded.length ? `, never ${row.excluded.join(', ')}` : ''}). HEAD is ${head}. ${NO_WEAKENING}\n` +
    `Each item below was verified by independent reviewers. Fix the CLASS, not just the line: find every instance of the same mistake in this change and fix them all.\n` +
    items.map((f, i) => `${i + 1}. ${f.file}:${f.line} [${f.bugClass}] ${f.claim}\n   evidence: ${f.evidence}\n   suggested: ${f.fix}`).join('\n') +
    `\nThen re-run the checks for the touched packages, and commit with the subject "${row.id}: review round ${round} fixes" and these trailer lines:\n${TRAILERS}\nDo not push. ` +
    `If an item is wrong or cannot be fixed in your folders, leave it and say why in notFixed. Return the new head sha.`,
    { label: `fix:${row.id}:r${round}`, phase: 'Fix', schema: FIX, model: BUILD_MODEL, effort: 'xhigh' })
}

async function classify(row, wt, base, head, pathTier) {
  return retry(() => agent(
    `Risk classifier for row ${row.id}. List the changed files yourself (git -C ${wt} -c core.quotePath=false diff --no-renames --name-only ${base}..${head}, both sides of every rename) into files, and read the diff. The path rules put it at "${pathTier}".\n` +
    `Raise the tier if the change, whatever its paths, can alter what gets written to a customer system, who may approve or run a fix, authentication, what data leaves the server, the review and merge gates, or deletes or weakens a test assertion. You may only raise, never lower. raiseTo "none" keeps "${pathTier}".`,
    { label: `tier:${row.id}`, phase: 'Approve', schema: TIER, model: SECOND_MODEL, effort: 'high' }))
}

async function approve(row, wt, base, head, tier, model, n, note) {
  return agent(
    `You are independent approver ${n} for row ${row.id}, risk tier ${tier}. Review the final change git -C ${wt} diff ${base}..${head} as a whole: does it do what the row asks (Done when: ${row.doneWhen}), fit the system and its contracts, and avoid breaking anything else? ` +
    `It has already passed verification, parallel review and verifier votes, so look for what those miss: wrong approach, missing behaviour, broken callers, weakened tests.\n` +
    (note ? `${note}\n` : '') +
    `approve=true only if you would merge commit ${head} as it stands; set sha to the commit you reviewed. If not, put each blocking problem in blocking with file, line and evidence. ${RULES}`,
    { label: `approve:${row.id}:${n}${note ? ':again' : ''}`, phase: 'Approve', schema: APPROVAL, model, effort: 'xhigh' })
}

async function integrity(row, wt, base, head) {
  return retry(() => agent(
    `This change for row ${row.id} touches the review pipeline itself (docs/process, .claude, .github, tasks, Makefile, lint or test config, AGENTS.md, CLAUDE.md or REVIEW.md). Read git -C ${wt} diff ${base}..${head}. ` +
    `weakens=true if it removes, skips, loosens or bypasses any check, vote, tier rule, approval, round cap, required CI job, ownership rule or Done-when, or makes such a change easier later. List each in what.`,
    { label: `integrity:${row.id}`, phase: 'Approve', schema: INTEGRITY, model: REVIEW_MODEL, effort: 'max' }))
}

// ---------- one row, end to end ----------
async function shipRow(row) {
  const out = { row: row.id, title: row.prTitle, agent: row.agent, folders: row.folders, excluded: row.excluded,
    status: 'blocked', reasons: [], tier: null, rounds: 0, found: 0, verified: 0, dropped: 0,
    nits: [], preexisting: [], approvals: [], files: [] }
  const stop = why => { out.reasons.push(why); return out }

  const b = RESUME[row.id]
    ? { status: 'built', worktree: RESUME[row.id].worktree, branch: RESUME[row.id].branch, head: '', checks: [], howToRevert: RESUME[row.id].howToRevert || 'see the earlier manifest', offSwitch: RESUME[row.id].offSwitch || 'see the earlier manifest', notDone: '', newDependencies: '', contractChangeRequest: '' }
    : await build(row)
  if (!b) return stop('implementer did not return')
  Object.assign(out, { branch: b.branch, worktree: b.worktree })
  if (b.status !== 'built') return stop([b.blockedReason || 'implementer blocked', b.contractChangeRequest ? 'contract change request: ' + b.contractChangeRequest : ''].filter(Boolean).join('; '))
  const wt = b.worktree

  let open = []            // verified findings not yet confirmed fixed
  const posted = []        // everything already raised, for diff-only rounds
  const refuted = new Set() // claimKey of findings the verifiers refuted
  let fullReviewed = false
  let reviewedHead = null
  let v = null
  for (let round = 1; round <= MAX_ROUNDS; round++) {
    out.rounds = round
    v = await retry(() => verifyRun(row, wt))
    if (!v) return stop('verifier did not return')
    const head = v.head
    const files = [...new Set(v.files.map(f => norm(f, wt)).filter(Boolean))]
    Object.assign(out, { head, base: v.base, patchId: v.patchId, files })
    if (!files.length) return stop('empty diff against origin/main')
    const own = ownershipViolations(files, row.agent, row.folders, row.excluded)
    if (own.length) return stop('ownership: ' + own.join('; '))

    // Items sent to the last fix stay open until a checker confirms them gone at the new head.
    const carried = open.filter(f => f.file !== '(checks)')
    const still = await parallel(carried.map(f => () => stillOpen(row, wt, head, f).then(o => o ? f : null)))
    open = still.filter(Boolean)
    if (!v.pass) {
      open.push(...v.problems.map(p => ({ file: '(checks)', line: 0, severity: 'important', bugClass: 'failing-check', claim: p, evidence: 'independent re-run failed', fix: 'make the check pass without weakening it', lens: 'checks' })))
    } else {
      const full = !fullReviewed
      const from = full ? v.base : reviewedHead
      const found = await review(row, wt, from, head, posted, full, round)
      if (!found) return stop(`review incomplete in round ${round}: a reviewer failed twice`)
      fullReviewed = true
      reviewedHead = head
      const byKey = new Map()
      for (const f of found.filter(x => x.severity === 'important')) if (!byKey.has(claimKey(f))) byKey.set(claimKey(f), f)
      // Skip only an identical claim: a different bug on the same line and class is new work, not a repeat.
      const openKeys = new Set(open.map(claimKey))
      const fresh = [...byKey.values()].filter(f => !refuted.has(claimKey(f)) && !openKeys.has(claimKey(f)))
      const skipped = byKey.size - fresh.length
      if (skipped) log(`${row.id} round ${round}: ${skipped} finding(s) repeat an open or refuted claim and were not re-voted`)
      for (const f of found.filter(x => x.severity === 'nit')) if (out.nits.length < 3) out.nits.push(f)
      out.preexisting.push(...found.filter(x => x.severity === 'preexisting'))
      out.found += fresh.length
      const kept = await parallel(fresh.map(f => () => vote(row, wt, head, f, tierOf(files)).then(real => ({ f, real }))))
      for (const k of kept.filter(Boolean)) { if (k.real) open.push(k.f); else refuted.add(claimKey(k.f)) }
      out.verified += kept.filter(k => k && k.real).length
      out.dropped += kept.filter(k => k && !k.real).length
      posted.push(...fresh)

      if (!open.length) {
        // Clean: tier from the union of both file lists, integrity for pipeline files, then the approvals at head.
        const c = await classify(row, wt, v.base, head, tierOf(files))
        if (!c) return stop('risk classifier did not return')
        const all = [...new Set([...files, ...c.files.map(f => norm(f, wt)).filter(Boolean)])]
        const own2 = ownershipViolations(all, row.agent, row.folders, row.excluded)
        if (own2.length) return stop('ownership: ' + own2.join('; '))
        let tier = tierOf(all)
        if (c.raiseTo !== 'none' && RANK[c.raiseTo] > RANK[tier]) { tier = c.raiseTo; out.reasons.push(`tier raised to ${tier}: ${c.reason}`) }
        Object.assign(out, { tier, files: all })
        if (all.some(f => PIPELINE.test(f))) {
          const g = await integrity(row, wt, v.base, head)
          if (!g || g.weakens) return stop('change weakens the review pipeline: ' + (g ? g.what.join('; ') : 'integrity check did not return'))
        }
        const need = APPROVALS[tier]
        let given = await parallel(need.map((model, i) => () => retry(() => approve(row, wt, v.base, head, tier, model, i + 1))))
        const blocking = []
        for (let i = 0; i < need.length; i++) {
          const a = given[i]
          if (!a || a.approve !== true || a.sha !== head) {
            const items = (a && !a.approve ? a.blocking || [] : []).map(f => ({ ...f, file: norm(f.file, wt), lens: 'approval' }))
            const fresh2 = items.filter(f => !refuted.has(claimKey(f)))
            const k2 = await parallel(fresh2.map(f => () => vote(row, wt, head, f, tier).then(real => ({ f, real }))))
            const real = k2.filter(k => k && k.real).map(k => k.f)
            k2.filter(k => k && !k.real).forEach(k => refuted.add(claimKey(k.f)))
            if (real.length) { blocking.push(...real); continue }
            // No verified item behind the "no" (or no answer, or the wrong commit): ask once more, then block.
            const note = a && !a.approve ? `Your earlier blocking items were checked by three verifiers and refuted: ${items.map(f => `${f.file}:${f.line} ${f.claim}`).join(' | ') || '(you listed none)'}. Decide again.` : `Your earlier answer ${a ? `named commit ${a.sha}` : 'did not arrive'}; review ${head}.`
            given[i] = await approve(row, wt, v.base, head, tier, need[i], i + 1, note)
          }
        }
        out.approvals = need.map((model, i) => ({ model, approve: !!(given[i] && given[i].approve === true && given[i].sha === head), sha: given[i] ? given[i].sha : '' }))
        if (!blocking.length) {
          if (out.approvals.every(a => a.approve)) { out.status = 'ready'; break }
          return stop('an approver did not approve the head commit after a second ask: leads decide')
        }
        open.push(...blocking)
        out.verified += blocking.length
        posted.push(...blocking)
      }
    }
    if (round === MAX_ROUNDS) return stop(`not converged after ${MAX_ROUNDS} rounds; step back and reconsider the approach. Open: ${open.map(f => `${f.file}:${f.line} ${f.claim}`).join(' | ')}`)
    const f = await fix(row, wt, head, open, round)
    if (!f) return stop(`fix round ${round} did not return`)
    if (f.notFixed.length) out.reasons.push(...f.notFixed.map(n => `round ${round} not fixed: ${n}`))
    if (!f.head || f.head === head) return stop(`fix round ${round} made no commit; open: ${open.map(x => `${x.file}:${x.line} ${x.claim}`).join(' | ')}`)
  }

  out.prBody = [
    '## What', row.summary, '',
    '## Why', `Board row ${row.id} (${row.taskFile}). Done when: ${row.doneWhen}`, '',
    '## How I tested', ...(v ? v.checks.map(c => `- \`${c.command}\`: ${c.result}`) : ['- not verified']), '',
    '## Risk', `Tier **${out.tier}**, set from the diff. Off switch: ${b.offSwitch}. Rounds: ${out.rounds}. Findings: ${out.found} raised, ${out.verified} verified and fixed, ${out.dropped} refuted by verifiers.` +
      (out.approvals.length ? ` Agent approvals of ${String(out.head).slice(0, 8)}: ${out.approvals.map(a => a.model).join(', ')}.` : ' No approval required at this tier.') +
      ` Patch id ${String(out.patchId).slice(0, 12)}.`, '',
    '## How to revert', b.howToRevert, '',
    '## Not done / known gaps', b.notDone || 'None.',
    ...(out.preexisting.length ? ['', 'Pre-existing bugs seen (not introduced here):', ...out.preexisting.map(p => `- ${p.file}:${p.line} ${p.claim}`)] : []),
    ...(out.nits.length ? ['', 'Nits (not blocking):', ...out.nits.map(p => `- ${p.file}:${p.line} ${p.claim}`)] : []), '',
    '## New dependencies', b.newDependencies || 'None.',
  ].join('\n')
  return out
}

// ---------- run ----------
phase('Plan')
const p = await plan(ROWS)
if (!p) return { rows: ROWS.map(id => ({ row: id, status: 'blocked', reasons: ['plan did not return'] })) }
// Only the rows asked for, once each: a plan that adds or repeats a row must not ship it.
p.rows.filter(r => !ROWS.includes(r.id)).forEach(r => log(`plan returned ${r.id}, which was not asked for: ignored`))
const planned = p.rows.filter((r, i, all) => ROWS.includes(r.id) && all.findIndex(x => x.id === r.id) === i).map(r => ({ ...r, folders: r.folders.map(prefixOf).filter(Boolean), excluded: (r.excluded || []).map(prefixOf).filter(Boolean) }))
const skipped = [
  ...planned.filter(r => !r.ready).map(r => ({ row: r.id, status: 'skipped', reasons: ['waits on ' + (r.missing.join(', ') || 'unknown')] })),
  ...ROWS.filter(id => !planned.some(r => r.id === id)).map(id => ({ row: id, status: 'skipped', reasons: [`not found on ${BOARD}`] })),
]
skipped.forEach(s => log(`skipping ${s.row}: ${s.reasons[0]}`))
const ready = planned.filter(r => r.ready)
if (!ready.length) return { rows: skipped }

// Rows whose folders overlap would conflict at Ship: run them in separate waves.
const waves = []
for (const r of ready) {
  const overlaps = w => w.some(o => o.folders.some(a => r.folders.some(b => under(a, b) || under(b, a))))
  const w = waves.find(x => !overlaps(x))
  if (w) w.push(r); else waves.push([r])
}
log(`shipping ${ready.length} row(s) in ${waves.length} wave(s): ${waves.map(w => w.map(r => r.id).join(' + ')).join(' | ')}`)
const rows = []
for (const w of waves) {
  const res = await pipeline(w, row => shipRow(row))
  rows.push(...res.map((r, i) => r || { row: w[i].id, status: 'blocked', reasons: ['pipeline stage failed'] }))
}
for (const r of rows) log(`${r.row}: ${r.status}${r.tier ? ` (tier ${r.tier}, ${r.rounds} round(s))` : ''}${r.reasons && r.reasons.length ? ' — ' + r.reasons[r.reasons.length - 1] : ''}`)
return { rows: [...rows, ...skipped] }
