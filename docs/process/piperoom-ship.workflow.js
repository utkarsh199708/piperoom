export const meta = {
  name: 'piperoom-ship',
  description: 'Build Piperoom board rows end to end with no human approval: implement, re-verify, parallel bug hunt, verifier votes, security review, risk tier from the diff, capped fix rounds, tiered agent approvals; returns a ship manifest',
  whenToUse: 'Run on rows of tasks/P-board.md (or another wave board) whose After rows are merged. Lead 1 then pushes each ready branch, opens the PR from the manifest, and merges on green CI. docs/process/ship-workflow.md describes the process.',
  phases: [
    { title: 'Plan', detail: 'is each row dispatchable; owner, folders, task file' },
    { title: 'Build', detail: 'one implementer per row in its own worktree', model: 'opus' },
    { title: 'Verify', detail: 're-run format, vet, build and tests independently', model: 'opus' },
    { title: 'Review', detail: 'four bug hunters and a security reviewer in parallel', model: 'fable' },
    { title: 'Vote', detail: 'three verifiers try to refute each finding', model: 'fable' },
    { title: 'Fix', detail: 'fix every verified finding by class; max 3 rounds, diff-only after round 1', model: 'opus' },
    { title: 'Approve', detail: 'risk tier from the diff; 0, 1 or 2 independent agent approvals', model: 'fable' },
  ],
}

// ---------- inputs ----------
// args: ["P3-0", "P1a-2"]  or  {rows: [...], maxRounds: 3, trailers: "Co-Authored-By: ...\nClaude-Session: ...",
//        implementModel: "opus", reviewModel: "fable", secondModel: "opus", board: "tasks/P-board.md"}
const A = Array.isArray(args) ? { rows: args } : (args || {})
const ROWS = (A.rows || []).map(String)
const MAX_ROUNDS = A.maxRounds || 3
const BOARD = A.board || 'tasks/P-board.md'
const TRAILERS = A.trailers || 'Co-Authored-By: Claude <noreply@anthropic.com>'
const BUILD_MODEL = A.implementModel || 'opus'
const REVIEW_MODEL = A.reviewModel || 'fable'
const SECOND_MODEL = A.secondModel || 'opus' // the independent second opinion in votes and high-tier approval

if (!ROWS.length) {
  log('No rows given. Pass args like ["P3-0", "P1a-2"].')
  return { rows: [] }
}

// ---------- risk tiers: the change sets the tier, not the author ----------
const CONTRACT = /^(internal\/tools\/tool\.go|internal\/events\/events\.go|internal\/clients\/clients\.go|internal\/actions\/action\.go|internal\/agent\/contract\.go|internal\/topology\/graph\.go|internal\/config\/config\.go|internal\/version\/version\.go|docs\/api\.md)$/
const PIPELINE = /^(docs\/process\/|\.claude\/|\.github\/|AGENTS\.md$|REVIEW\.md$)/
const HIGH = [
  /^internal\/actions\//,               // everything that writes to a customer system
  /^internal\/clients\/[^/]+\/writer/,  // write clients
  /^internal\/auth\//, /^internal\/httpapi\/auth/, /^internal\/redact\//, /^internal\/injectscan\//,
  /^internal\/room\/service_(executor|approve|proposals)/,
  /^migrations\//, /^deploy\//, /Dockerfile$/, /^go\.(mod|sum)$/,
  CONTRACT, PIPELINE,
]
const LOW = [/^docs\//, /^tasks\//, /^reviews\//, /^research\//, /_test\.go$/, /\.test\.tsx?$/, /^evals\/scenarios\//, /^README\.md$/]

function tierOf(files) {
  if (!files.length) return 'low'
  if (files.some(f => HIGH.some(rx => rx.test(f)))) return 'high'
  if (files.every(f => LOW.some(rx => rx.test(f)))) return 'low'
  return 'medium'
}
const RANK = { low: 0, medium: 1, high: 2 }
const APPROVALS = { low: [], medium: [REVIEW_MODEL], high: [REVIEW_MODEL, SECOND_MODEL] }

// ---------- schemas ----------
const CHECK = { type: 'object', properties: { command: { type: 'string' }, result: { type: 'string', enum: ['pass', 'fail'] }, output: { type: 'string' } }, required: ['command', 'result', 'output'] }
const PLAN = {
  type: 'object',
  properties: { rows: { type: 'array', items: { type: 'object', properties: {
    id: { type: 'string' }, ready: { type: 'boolean' }, missing: { type: 'array', items: { type: 'string' } },
    agent: { type: 'string' }, folders: { type: 'array', items: { type: 'string' } }, taskFile: { type: 'string' },
    summary: { type: 'string' }, doneWhen: { type: 'string' }, prTitle: { type: 'string' },
  }, required: ['id', 'ready', 'missing', 'agent', 'folders', 'taskFile', 'summary', 'doneWhen', 'prTitle'] } } },
  required: ['rows'],
}
const BUILD = {
  type: 'object',
  properties: {
    status: { type: 'string', enum: ['built', 'blocked'] }, blockedReason: { type: 'string' },
    worktree: { type: 'string' }, branch: { type: 'string' }, base: { type: 'string' }, head: { type: 'string' },
    checks: { type: 'array', items: CHECK }, howToRevert: { type: 'string' }, notDone: { type: 'string' },
    newDependencies: { type: 'string' }, contractChangeRequest: { type: 'string' },
  },
  required: ['status', 'worktree', 'branch', 'base', 'head', 'checks', 'howToRevert', 'notDone', 'newDependencies', 'contractChangeRequest'],
}
const VERIFY = {
  type: 'object',
  properties: { pass: { type: 'boolean' }, head: { type: 'string' }, files: { type: 'array', items: { type: 'string' } },
    checks: { type: 'array', items: CHECK }, problems: { type: 'array', items: { type: 'string' } } },
  required: ['pass', 'head', 'files', 'checks', 'problems'],
}
const FINDING = { type: 'object', properties: {
  file: { type: 'string' }, line: { type: 'integer' }, severity: { type: 'string', enum: ['important', 'nit', 'preexisting'] },
  bugClass: { type: 'string' }, claim: { type: 'string' }, evidence: { type: 'string' }, fix: { type: 'string' },
}, required: ['file', 'line', 'severity', 'bugClass', 'claim', 'evidence', 'fix'] }
const FINDINGS = { type: 'object', properties: { findings: { type: 'array', items: FINDING } }, required: ['findings'] }
const VERDICT = { type: 'object', properties: { refuted: { type: 'boolean' }, reason: { type: 'string' } }, required: ['refuted', 'reason'] }
const FIX = { type: 'object', properties: { head: { type: 'string' },
  fixed: { type: 'array', items: { type: 'string' } }, notFixed: { type: 'array', items: { type: 'string' } } },
  required: ['head', 'fixed', 'notFixed'] }
const TIER = { type: 'object', properties: { raiseTo: { type: 'string', enum: ['none', 'medium', 'high'] }, reason: { type: 'string' } }, required: ['raiseTo', 'reason'] }
const APPROVAL = { type: 'object', properties: { approve: { type: 'boolean' }, sha: { type: 'string' },
  reasons: { type: 'array', items: { type: 'string' } }, blocking: { type: 'array', items: FINDING } },
  required: ['approve', 'sha', 'reasons', 'blocking'] }
const INTEGRITY = { type: 'object', properties: { weakens: { type: 'boolean' }, what: { type: 'array', items: { type: 'string' } } }, required: ['weakens', 'what'] }

// ---------- review dimensions (security has its own reviewer) ----------
const DIMENSIONS = [
  { key: 'correctness', focus: 'logic errors and wrong results; every error wrapped with context and never swallowed; every failure path (target down, 404, timeout, bad input, empty result) handled' },
  { key: 'contract', focus: 'contract fit: only the owner agent\'s folders changed (AGENTS.md ownership tables); contract files untouched unless this is a leads\' contract row; field names, JSON tags and documented semantics match the contract files exactly; the row\'s Done-when is actually met, not approximately' },
  { key: 'runtime', focus: 'context and a deadline on every network or database call; goroutines that stop when ctx ends; locks and data races; size caps (4 MB HTTP bodies, record and row caps); SQL values as $n parameters and identifiers only through pgx.Identifier after the regex check' },
  { key: 'tests', focus: 'whether the tests prove what the row claims: missing error-path tests, assertions that cannot fail, tests that pass with the feature removed, and timing-dependent tests (sleeps, wall-clock limits, async events) that will flake under -race on a loaded CI runner' },
]
const SECURITY_FOCUS = 'read-only tools that are really read-only; every write scope-checked against the room\'s pipeline before any I/O (tools.ScopeFor, fail closed); secrets, DSNs or full record values in logs, errors or events; untrusted text (logs, record values, traces, PR text) kept as data and wrapped before reaching a prompt; SQL and identifier injection; unbounded reads; authz on every new route; anything that lets a change weaken its own review gates'

const RULES = 'Report only real problems you can show. Each finding needs a file and line in the head commit and evidence: the code itself, or a command and its output. important = wrong behaviour, data loss, a security hole, a broken contract, or a test that will flake in CI. nit = style or cleanup; at most 3, never blocking. preexisting = a real bug this diff did not introduce. Name the bug class so every instance can be fixed at once. Report nothing rather than guess.'

const keyOf = f => `${f.file}:${Math.floor((f.line || 0) / 8)}:${(f.bugClass || '').toLowerCase().replace(/[^a-z]+/g, '-')}`

// ---------- stages ----------
async function plan(rows) {
  return agent(
    `Read ${BOARD} on origin/main (git fetch origin main first) and the task files it points to. For each of these rows: ${rows.join(', ')}\n` +
    `- ready: true only if every row in its After column is marked merged on the board AND that merge is on origin/main; list the unmet ones in missing.\n` +
    `- agent and folders: the owner from the Agent column and that agent's folders from the ownership tables in AGENTS.md.\n` +
    `- taskFile, summary (one sentence), doneWhen (the Done-when cell verbatim), prTitle in the board's tag style, e.g. "[P3-0] room: executor ctx carries the pipeline".\n` +
    `Return every requested row, ready or not. Change nothing.`,
    { label: 'plan', phase: 'Plan', schema: PLAN, model: BUILD_MODEL, effort: 'medium' })
}

async function build(row) {
  return agent(
    `You are agent ${row.agent} implementing board row ${row.id} of the Piperoom repo. Task file: ${row.taskFile}. Row: ${row.summary}\nDone when: ${row.doneWhen}\n` +
    `Rules that decide review: edit only ${row.agent}'s folders (${row.folders.join(', ')}); never edit contract files (if the row needs one, stop and fill contractChangeRequest with the exact proposed code, status blocked); follow AGENTS.md §4 and §5.\n` +
    `Steps:\n1. git fetch origin main; git checkout -b p/${row.id} origin/main. Record base = origin/main sha.\n` +
    `2. Implement the row with tests, including the error paths. Keep the diff under about 600 changed lines.\n` +
    `3. Prove it: run, from the repo root, the toolchain gofmt -l on changed Go dirs ($(go env GOROOT)/bin/gofmt), go vet ./..., go build ./..., go test on every changed package, and go test -race on changed packages that start goroutines; for web/** run npm ci, npm run lint, npm run build and the Vitest files you touched. Put each command and the tail of its real output in checks. Fix failures before you finish.\n` +
    `4. Commit (imperative "area: what" subject) ending with these trailer lines:\n${TRAILERS}\nDo not push.\n` +
    `Return worktree (absolute path of this checkout), branch, base, head (full shas), checks, howToRevert (one sentence: what reverting the merge undoes, and whether data or config must be undone too), notDone (honest gaps, "" if none), newDependencies ("" if none), contractChangeRequest ("" if none).`,
    { label: `build:${row.id}`, phase: 'Build', schema: BUILD, model: BUILD_MODEL, effort: 'xhigh', isolation: 'worktree' })
}

async function verifyRun(row, wt, base) {
  return agent(
    `Independently verify branch work for row ${row.id} in ${wt}. Do not trust any earlier claim; run everything yourself, from that directory.\n` +
    `1. head = git -C ${wt} rev-parse HEAD; files = git -C ${wt} diff --name-only ${base}..HEAD.\n` +
    `2. Run the toolchain gofmt -l on changed Go dirs, go vet ./..., go build ./..., go test on every changed package, go test -race -count=3 on changed packages that start goroutines, and for web/** npm run lint and npm run build.\n` +
    `3. pass = every command succeeded. List each failure in problems with the exact failing test or line. Change nothing.`,
    { label: `verify:${row.id}`, phase: 'Verify', schema: VERIFY, model: BUILD_MODEL, effort: 'high' })
}

async function review(row, wt, from, head, posted, round) {
  const scope = round === 1
    ? `Review the whole change: git -C ${wt} diff ${from}..${head}, read against the rest of the repo for context.`
    : `This is re-review round ${round}: review ONLY the new commits, git -C ${wt} diff ${from}..${head}. Already posted, do not repeat: ${posted.length ? posted.map(p => `${p.file}:${p.line} ${p.claim}`).join(' | ') : 'none'}.`
  const ctx = `Piperoom row ${row.id} (${row.summary}). Done when: ${row.doneWhen}. Owner: ${row.agent}, folders ${row.folders.join(', ')}. ${scope}\n${RULES}`
  const runs = await parallel([
    ...DIMENSIONS.map(d => () => agent(`${ctx}\nYour lens: ${d.focus}.`,
      { label: `hunt:${row.id}:${d.key}:r${round}`, phase: 'Review', schema: FINDINGS, model: REVIEW_MODEL, effort: 'high' })),
    () => agent(`${ctx}\nYou are the security reviewer. Your lens: ${SECURITY_FOCUS}. Treat every security problem you can show as important.`,
      { label: `security:${row.id}:r${round}`, phase: 'Review', schema: FINDINGS, model: REVIEW_MODEL, effort: 'xhigh' }),
  ])
  return runs.filter(Boolean).flatMap(r => r.findings || [])
}

async function vote(row, wt, head, f, tier) {
  const voters = [[REVIEW_MODEL, 'xhigh'], [REVIEW_MODEL, 'high'], [SECOND_MODEL, 'xhigh']]
  const votes = await parallel(voters.map(([model, effort], i) => () => agent(
    `Try to refute this review finding on row ${row.id} (risk tier ${tier}). Check it yourself in ${wt} at commit ${head}: read the code, and run a command or test if that settles it.\n` +
    `Finding: ${f.file}:${f.line} [${f.bugClass}] ${f.claim}\nEvidence given: ${f.evidence}\n` +
    `refuted=true if the code does not do what the finding says, if the problem cannot happen through any real caller or input, or if you cannot confirm it with evidence. Default to refuted=true when unsure.`,
    { label: `vote:${row.id}:${f.file.split('/').pop()}:${f.line}:${i + 1}`, phase: 'Vote', schema: VERDICT, model, effort })))
  const real = votes.filter(Boolean).filter(v => !v.refuted).length
  return real >= 2
}

async function fix(row, wt, head, items, round) {
  return agent(
    `Fix round ${round} for row ${row.id} in ${wt} (you are ${row.agent}; edit only ${row.folders.join(', ')}). HEAD is ${head}.\n` +
    `Each item below was verified by independent reviewers. Fix the CLASS, not just the line: search for every instance of the same mistake in this change and fix them all.\n` +
    items.map((f, i) => `${i + 1}. ${f.file}:${f.line} [${f.bugClass}] ${f.claim}\n   evidence: ${f.evidence}\n   suggested: ${f.fix}`).join('\n') +
    `\nThen re-run the checks for the touched packages, and commit with the subject "${row.id}: review round ${round} fixes" and these trailer lines:\n${TRAILERS}\nDo not push. ` +
    `If an item is wrong or cannot be fixed in your folders, leave it and say why in notFixed. Return the new head sha.`,
    { label: `fix:${row.id}:r${round}`, phase: 'Fix', schema: FIX, model: BUILD_MODEL, effort: 'xhigh' })
}

async function classify(row, wt, base, head, files, pathTier) {
  return agent(
    `Risk classifier for row ${row.id}. The path rules put this change at "${pathTier}". Files: ${files.join(', ')}. Read git -C ${wt} diff ${base}..${head}.\n` +
    `Raise the tier if the change, whatever its paths, can alter what gets written to a customer system, who may approve or run a fix, authentication, what data leaves the server, or the review and merge gates. You may only raise, never lower. raiseTo "none" keeps "${pathTier}".`,
    { label: `tier:${row.id}`, phase: 'Approve', schema: TIER, model: SECOND_MODEL, effort: 'medium' })
}

async function approve(row, wt, base, head, tier, model, n) {
  return agent(
    `You are independent approver ${n} for row ${row.id}, risk tier ${tier}. Review the final change git -C ${wt} diff ${base}..${head} as a whole: does it do what the row asks (Done when: ${row.doneWhen}), fit the system and its contracts, and avoid breaking anything else? ` +
    `It has already passed verification, parallel review and verifier votes, so look for what those miss: wrong approach, missing behaviour, broken callers, ownership or contract violations.\n` +
    `approve=true only if you would merge commit ${head} as it stands; set sha to the commit you reviewed. If not, put each blocking problem in blocking with file, line and evidence. ${RULES}`,
    { label: `approve:${row.id}:${n}`, phase: 'Approve', schema: APPROVAL, model, effort: 'xhigh' })
}

async function integrity(row, wt, base, head) {
  return agent(
    `This change for row ${row.id} touches the review pipeline itself (docs/process, .claude, .github, AGENTS.md or REVIEW.md). Read git -C ${wt} diff ${base}..${head}. ` +
    `weakens=true if it removes, skips, loosens or bypasses any check, vote, tier rule, approval, round cap, required CI job or ownership rule, or makes such a change easier later. List each in what.`,
    { label: `integrity:${row.id}`, phase: 'Approve', schema: INTEGRITY, model: REVIEW_MODEL, effort: 'max' })
}

// ---------- one row, end to end ----------
async function shipRow(row) {
  const out = { row: row.id, title: row.prTitle, status: 'blocked', reasons: [], tier: null, rounds: 0,
    found: 0, verified: 0, dropped: 0, nits: [], preexisting: [], approvals: [] }
  const b = await build(row)
  if (!b) { out.reasons.push('implementer did not return'); return out }
  Object.assign(out, { branch: b.branch, worktree: b.worktree, base: b.base, head: b.head })
  if (b.status !== 'built') { out.reasons.push(b.blockedReason || 'implementer blocked', ...(b.contractChangeRequest ? ['contract change request: ' + b.contractChangeRequest] : [])); return out }

  const posted = []
  const seen = new Set()
  let head = b.head
  let from = b.base
  let lastVerify = null
  for (let round = 1; round <= MAX_ROUNDS; round++) {
    out.rounds = round
    lastVerify = await verifyRun(row, b.worktree, b.base)
    if (!lastVerify) { out.reasons.push('verifier did not return'); return out }
    head = lastVerify.head
    out.head = head
    let open = []
    if (!lastVerify.pass) {
      open = lastVerify.problems.map(p => ({ file: '(checks)', line: 0, severity: 'important', bugClass: 'failing-check', claim: p, evidence: 'independent re-run failed', fix: 'make the check pass without weakening it' }))
    } else {
      const pathTier = tierOf(lastVerify.files)
      const found = await review(row, b.worktree, from, head, posted, round)
      const important = found.filter(f => f.severity === 'important' && !seen.has(keyOf(f)))
      found.filter(f => f.severity === 'nit').slice(0, 3).forEach(f => out.nits.push(f))
      found.filter(f => f.severity === 'preexisting').forEach(f => out.preexisting.push(f))
      important.forEach(f => seen.add(keyOf(f)))
      out.found += important.length
      const kept = await parallel(important.map(f => () => vote(row, b.worktree, head, f, pathTier).then(real => real ? f : null)))
      open = kept.filter(Boolean)
      out.verified += open.length
      out.dropped += important.length - open.length
      posted.push(...important)

      if (!open.length) {
        // Clean on review: set the tier from the final diff and collect the approvals it needs.
        let tier = pathTier
        const c = await classify(row, b.worktree, b.base, head, lastVerify.files, pathTier)
        if (c && c.raiseTo !== 'none' && RANK[c.raiseTo] > RANK[tier]) { tier = c.raiseTo; out.reasons.push(`tier raised to ${tier}: ${c.reason}`) }
        out.tier = tier
        if (lastVerify.files.some(f => PIPELINE.test(f))) {
          const g = await integrity(row, b.worktree, b.base, head)
          if (!g || g.weakens) { out.reasons.push('change weakens the review pipeline: ' + (g ? g.what.join('; ') : 'integrity check did not return')); return out }
        }
        const votes = await parallel(APPROVALS[tier].map((model, i) => () => approve(row, b.worktree, b.base, head, tier, model, i + 1)))
        const given = votes.filter(Boolean)
        out.approvals = given.map((v, i) => ({ model: APPROVALS[tier][i], approve: v.approve, sha: v.sha }))
        const stale = given.filter(v => v.approve && v.sha !== head)
        const blocking = given.filter(v => !v.approve).flatMap(v => v.blocking || []).filter(f => !seen.has(keyOf(f)))
        blocking.forEach(f => seen.add(keyOf(f)))
        const keptBlocks = (await parallel(blocking.map(f => () => vote(row, b.worktree, head, f, tier).then(real => real ? f : null)))).filter(Boolean)
        // An approver's block stands only if the verifiers confirm at least one of its items.
        const approvedAll = given.length === APPROVALS[tier].length && !stale.length && !keptBlocks.length
        if (approvedAll) {
          out.status = 'ready'
          break
        }
        if (given.length < APPROVALS[tier].length) out.reasons.push('an approver did not return')
        if (stale.length) out.reasons.push('an approval named a different commit than head; approvals cover one commit')
        open = keptBlocks
        out.verified += keptBlocks.length
        posted.push(...blocking)
      }
    }
    if (round === MAX_ROUNDS) {
      out.reasons.push(`not converged after ${MAX_ROUNDS} rounds; step back and reconsider the approach. Open: ` +
        (open.map(f => `${f.file}:${f.line} ${f.claim}`).join(' | ') || 'approvals incomplete'))
      break
    }
    if (!open.length) { out.reasons.push('approval incomplete with nothing left to fix'); break }
    const f = await fix(row, b.worktree, head, open, round)
    if (!f) { out.reasons.push(`fix round ${round} did not return`); break }
    if (f.notFixed.length) out.reasons.push(...f.notFixed.map(n => `round ${round} not fixed: ${n}`))
    from = head          // diff-only from here on
    head = f.head
    out.head = head
  }

  out.prBody = [
    '## What', row.summary, '',
    '## Why', `Board row ${row.id} (${row.taskFile}). Done when: ${row.doneWhen}`, '',
    '## How I tested', ...(lastVerify ? lastVerify.checks.map(c => `- \`${c.command}\`: ${c.result}`) : ['- not verified']), '',
    '## Risk', `Tier **${out.tier || 'not reached'}**, set from the diff. Rounds: ${out.rounds}. Findings: ${out.found} raised, ${out.verified} verified and sent to fix, ${out.dropped} refuted by verifiers.` +
      (out.approvals.length ? ` Agent approvals: ${out.approvals.map(a => `${a.model} ${a.approve ? 'approved' : 'blocked'} ${String(a.sha).slice(0, 8)}`).join(', ')}.` : ' No approval required at this tier.'), '',
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
const planned = (p && p.rows) || []
const ready = planned.filter(r => r.ready)
for (const r of planned.filter(r => !r.ready)) log(`skipping ${r.id}: waits on ${r.missing.join(', ') || 'unknown'}`)
for (const id of ROWS.filter(id => !planned.some(r => r.id === id))) log(`skipping ${id}: not found on ${BOARD}`)
const skipped = planned.filter(r => !r.ready).map(r => ({ row: r.id, status: 'skipped', reasons: ['waits on ' + r.missing.join(', ')] }))
if (!ready.length) return { rows: skipped }
log(`shipping ${ready.length} row(s): ${ready.map(r => r.id).join(', ')}`)

const results = await pipeline(ready, row => shipRow(row))
const rows = results.map((r, i) => r || { row: ready[i].id, status: 'blocked', reasons: ['pipeline stage failed'] })
for (const r of rows) log(`${r.row}: ${r.status}${r.tier ? ` (tier ${r.tier}, ${r.rounds} round(s))` : ''}${r.reasons && r.reasons.length ? ' — ' + r.reasons[0] : ''}`)
return { rows: [...rows, ...skipped] }
