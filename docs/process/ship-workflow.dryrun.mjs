// Usage: node docs/process/ship-workflow.dryrun.mjs
//
// Regression test for piperoom-ship.workflow.js and ship-check.mjs. It compiles the workflow script and runs it
// against stubbed agents (no tokens spent), one scenario per review finding on PR #597 plus a happy path per tier,
// and asserts on the returned manifest. Then it runs ship-check.mjs on manifests and file lists. Prints PASS or FAIL
// per scenario and exits 1 if any fails. To try a patch, pass another copy of the script as the first argument.
//
// Stubs route by the agent label prefix (plan, build, verify, hunt, security, vote, check, fix, tier, integrity,
// approve). By default every stub answers like a clean row: build at h1, checks pass, no findings, approvals at head.
// A scenario overrides a stub by kind; a stub that returns undefined falls back to the default, null is a failed agent.
import { readFileSync, writeFileSync, mkdtempSync, rmSync } from 'node:fs'
import { spawnSync } from 'node:child_process'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'

const SCRIPT = process.argv[2] || fileURLToPath(new URL('./piperoom-ship.workflow.js', import.meta.url))
const SHIP_CHECK = fileURLToPath(new URL('./ship-check.mjs', import.meta.url))
const src = readFileSync(SCRIPT, 'utf8').replace(/^export (const meta =)/m, '$1')
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor
const workflow = new AsyncFunction('agent', 'pipeline', 'parallel', 'phase', 'log', 'args', 'budget', src)
const budget = { total: null, spent: () => 0, remaining: () => Infinity }

// ---------- harness ----------
const planRow = r => ({ id: r.id, ready: r.ready !== false, missing: r.missing || [], agent: r.agent || 'A5',
  folders: r.folders || ['internal/room/**'], excluded: r.excluded || [], taskFile: 'tasks/P-board.md',
  summary: `do ${r.id}`, doneWhen: 'tests pass', prTitle: `[${r.id}] x` })

async function run(rowDefs, { args, stubs = {} } = {}) {
  const calls = [], logs = [], errors = []
  const head = {}, seen = {}
  const byId = Object.fromEntries(rowDefs.map(r => [r.id, r]))
  const D = {
    plan: () => ({ rows: rowDefs.map(planRow) }),
    build: c => {
      head[c.id] = 'h1'
      return { status: 'built', worktree: c.row.worktree || `/wt/${c.id}`, branch: `p/${c.id}`, head: 'h1',
        checks: [{ command: 'go test ./...', result: 'pass', output: 'ok' }], howToRevert: 'revert the merge', offSwitch: 'none',
        notDone: '', newDependencies: '', contractChangeRequest: '' }
    },
    verify: c => ({ pass: true, base: 'b0', head: head[c.id], patchId: `pid-${head[c.id]}`, files: c.row.files,
      checks: [{ command: 'go test ./...', result: 'pass', output: 'ok' }], problems: [] }),
    hunt: () => ({ findings: [] }),
    security: () => ({ findings: [] }),
    vote: () => ({ refuted: false, reason: 'confirmed' }),
    check: () => ({ fixed: true, evidence: 'gone at the new head' }),
    fix: c => { head[c.id] = 'h' + (Number(head[c.id].slice(1)) + 1); return { head: head[c.id], fixed: ['all'], notFixed: [] } },
    tier: () => ({ raiseTo: 'none', reason: '', files: [] }),
    integrity: () => ({ weakens: false, what: [] }),
    approve: c => ({ approve: true, sha: head[c.id], reasons: ['looks right'], blocking: [] }),
  }
  async function agent(prompt, o = {}) {
    const label = String(o.label || '')
    const [kind, id, ...rest] = label.split(':')
    const m = label.match(/:r(\d+)$/)
    const c = { kind, id, rest, label, prompt, model: o.model, row: byId[id] || {}, head,
      round: m ? Number(m[1]) : 0, nth: (seen[label] = (seen[label] || 0) + 1) - 1 }
    calls.push(c)
    let r = stubs[kind] ? await stubs[kind](c) : undefined
    if (r === undefined) {
      if (!D[kind]) throw new Error(`unexpected agent label "${label}"`)
      r = await D[kind](c)
    }
    return r
  }
  const parallel = thunks => Promise.all(thunks.map(async t => { try { return await t() } catch (e) { errors.push(e); return null } }))
  const pipeline = (items, ...stages) => Promise.all(items.map(async (it, i) => {
    let r = it
    try { for (const s of stages) r = await s(r, it, i); return r } catch (e) { errors.push(e); return null }
  }))
  let out = null, thrown = null
  try {
    out = await workflow(agent, pipeline, parallel, () => {}, msg => logs.push(String(msg)),
      args === undefined ? { rows: rowDefs.map(r => r.id) } : args, budget)
  } catch (e) { thrown = e }
  const rows = out && Array.isArray(out.rows) ? out.rows : []
  return { out, rows, row: id => rows.find(r => r.row === id), calls, logs, errors, thrown, head,
    count: (kind, id) => calls.filter(c => c.kind === kind && (id === undefined || c.id === id)).length }
}

const scenarios = []
const scenario = (name, fn) => scenarios.push({ name, fn })
const J = v => JSON.stringify(v)

function tester() {
  const fails = []
  const t = {
    fails,
    ok: (cond, msg) => { if (!cond) fails.push(msg) },
    eq: (got, want, msg) => { if (J(got) !== J(want)) fails.push(`${msg}: got ${J(got)}, want ${J(want)}`) },
    // clean: the workflow returned and no stub threw (unless the scenario makes one throw on purpose).
    clean: (res, allowErrors = false) => {
      if (res.thrown) fails.push(`workflow threw: ${res.thrown.stack || res.thrown}`)
      if (!allowErrors && res.errors.length) fails.push(`stub errors: ${res.errors.map(e => e.message).join('; ')}`)
    },
    // row checks one manifest row: status, reasons substring(s), tier, rounds, approvals.
    // approvals: an array of models that must all have approved the row's head; notApproved: some approval is a no.
    row: (res, id, want) => {
      t.clean(res, want.allowErrors)
      const r = res.row(id)
      if (!r) { fails.push(`${id}: no manifest row (rows: ${J(res.rows.map(x => x.row))})`); return {} }
      if (want.status && r.status !== want.status) fails.push(`${id} status: got ${r.status}, want ${want.status}; reasons ${J(r.reasons)}`)
      for (const s of [].concat(want.reason || [])) {
        if (!(r.reasons || []).some(x => x.includes(s))) fails.push(`${id} reasons should include "${s}", got ${J(r.reasons)}`)
      }
      if ('tier' in want) t.eq(r.tier, want.tier, `${id} tier`)
      if ('rounds' in want) t.eq(r.rounds, want.rounds, `${id} rounds`)
      if ('approvals' in want) {
        t.eq((r.approvals || []).map(a => a.model), want.approvals, `${id} approval models`)
        if ((r.approvals || []).some(a => !a.approve || a.sha !== r.head)) fails.push(`${id}: every approval must be approve=true at head ${r.head}, got ${J(r.approvals)}`)
      }
      if (want.notApproved && !(r.approvals || []).some(a => !a.approve)) fails.push(`${id}: expected an approval that is a no, got ${J(r.approvals)}`)
      return r
    },
  }
  return t
}

// ---------- fixtures ----------
const ROW = {
  low: id => ({ id, agent: 'C9', folders: ['docs/design/**'], files: ['docs/design/watcher.md'] }),
  medium: id => ({ id, agent: 'A2', folders: ['internal/clients/kafka/**'], files: ['internal/clients/kafka/lag.go', 'internal/clients/kafka/lag_test.go'] }),
  high: id => ({ id, agent: 'A5', folders: ['internal/room/**'], files: ['internal/room/service_executor.go', 'internal/room/executor_test.go'] }),
  pipeline: id => ({ id, agent: 'leads', folders: ['docs/**', 'tasks/**'], files: ['docs/process/ship-workflow.md'] }),
}
const F = (file, line, claim, extra = {}) => ({ file, line, severity: 'important', bugClass: 'missing ctx', claim,
  evidence: `${file}:${line} shows it`, fix: 'pass ctx through', ...extra })
const ROOM_BUG = F('internal/room/service_executor.go', 40, 'Validate runs without the pipeline ctx')
// findIn: a hunt stub that reports the given findings from one lens in the given rounds.
const findIn = (lens, rounds, findings) => c => (c.rest[0] === lens && rounds.includes(c.round) ? { findings } : undefined)
const firstReview = (res, id) => res.calls.find(c => (c.kind === 'hunt' || c.kind === 'security') && c.id === id)

// ---------- happy paths ----------
scenario('happy path, low tier: docs only, ready with no approval', async t => {
  const res = await run([ROW.low('L')])
  const r = t.row(res, 'L', { status: 'ready', tier: 'low', rounds: 1, approvals: [] })
  t.eq(res.count('approve'), 0, 'approve calls')
  t.eq(res.count('integrity'), 0, 'integrity calls (docs/design is not a pipeline file)')
  t.eq(res.count('hunt') + res.count('security'), 5, 'reviewers in round 1')
  t.ok(String(r.prBody).includes('No approval required at this tier'), 'PR body says no approval was needed')
})

scenario('happy path, medium tier: one approval (reviewModel) at head', async t => {
  const res = await run([ROW.medium('M')])
  const r = t.row(res, 'M', { status: 'ready', tier: 'medium', rounds: 1, approvals: ['fable'] })
  t.eq(r.head, 'h1', 'head')
  t.ok(String(r.prBody).includes('Agent approvals of h1: fable'), 'PR body names the approval')
})

scenario('happy path, high tier: two approvals from different models at head', async t => {
  const res = await run([ROW.high('H')])
  const r = t.row(res, 'H', { status: 'ready', tier: 'high', rounds: 1, approvals: ['fable', 'opus'] })
  t.eq(r.patchId, 'pid-h1', 'patch id recorded')
  t.eq(r.files, ROW.high('H').files, 'files')
  const reviews = res.calls.filter(c => c.kind === 'hunt' || c.kind === 'security')
  t.eq(reviews.length, 5, 'four hunters and a security reviewer')
  t.ok(reviews.every(c => c.prompt.includes('Review the whole change') && c.prompt.includes('diff b0..h1')), 'round 1 reviews the whole change from base')
  t.eq(res.calls.find(c => c.kind === 'tier').model, 'opus', 'classifier runs on the second model')
})

// ---------- 1 ----------
scenario('1. approve:false with no blocking items, or only refuted ones, never yields ready', async t => {
  // a: medium row; the approver says no and lists nothing, also when asked again.
  let res = await run([ROW.medium('1a')], { stubs: { approve: c => ({ approve: false, sha: c.head[c.id], reasons: ['unsure'], blocking: [] }) } })
  t.row(res, '1a', { status: 'blocked', reason: 'did not approve the head commit', notApproved: true })
  t.ok(res.calls.some(c => c.label === 'approve:1a:1:again' && c.prompt.includes('(you listed none)')), '1a: asked once more')
  t.eq(res.count('fix'), 0, '1a: no fix round without a verified item')

  // b: its blocking items are refuted by the verifiers; asked again, it still says no.
  const B = F('internal/clients/kafka/lag.go', 7, 'lag can go negative')
  res = await run([ROW.medium('1b')], { stubs: {
    approve: c => ({ approve: false, sha: c.head[c.id], reasons: ['bug'], blocking: [B] }),
    vote: () => ({ refuted: true, reason: 'cannot happen' }) } })
  t.row(res, '1b', { status: 'blocked', reason: 'did not approve the head commit', notApproved: true })
  t.ok(res.calls.some(c => c.label === 'approve:1b:1:again' && c.prompt.includes('refuted') && c.prompt.includes('lag can go negative')), '1b: re-ask names the refuted items')
  t.eq(res.count('vote'), 3, '1b: the blocking item got three votes')

  // c: high row; approver 1 says yes, approver 2 says no with nothing behind it.
  res = await run([ROW.high('1c')], { stubs: { approve: c => (c.rest[0] === '2' ? { approve: false, sha: c.head[c.id], reasons: ['no'], blocking: [] } : undefined) } })
  t.row(res, '1c', { status: 'blocked', reason: 'did not approve the head commit', notApproved: true })

  // d: the approver blocks on a finding the review's verifiers already refuted: no new vote, still not ready.
  res = await run([ROW.high('1d')], { stubs: {
    hunt: findIn('correctness', [1], [ROOM_BUG]),
    vote: () => ({ refuted: true, reason: 'not reachable' }),
    approve: c => ({ approve: false, sha: c.head[c.id], reasons: ['bug'], blocking: [ROOM_BUG] }) } })
  t.row(res, '1d', { status: 'blocked', reason: 'did not approve the head commit', notApproved: true })
  t.eq(res.count('vote'), 3, '1d: a refuted finding is not voted on again')
})

// ---------- 2 ----------
scenario('2. a verified finding the fixer does not fix never vanishes', async t => {
  const bug = id => F('internal/room/service_executor.go', 40, `F-${id} executor ignores ctx`)
  // a: the fixer says notFixed and commits something else; the checker still sees the bug.
  let res = await run([ROW.high('2a')], { stubs: {
    hunt: c => (c.rest[0] === 'correctness' && c.round === 1 ? { findings: [bug(c.id)] } : undefined),
    fix: c => { c.head[c.id] = 'h' + (Number(c.head[c.id].slice(1)) + 1); return { head: c.head[c.id], fixed: [], notFixed: ['1: needs a contract change'] } },
    check: () => ({ fixed: false, evidence: 'line 40 unchanged' }) } })
  t.row(res, '2a', { status: 'blocked', rounds: 3, reason: ['not converged after 3 rounds', 'F-2a executor ignores ctx', 'round 1 not fixed: 1: needs a contract change'] })
  t.eq(res.calls.filter(c => c.kind === 'fix').map(c => c.prompt.includes('F-2a')), [true, true], '2a: every fix round is sent the open item')

  // b: no new commit (same head, then an empty head).
  for (const [id, newHead] of [['2b', c => c.head[c.id]], ['2c', () => '']]) {
    res = await run([ROW.high(id)], { stubs: {
      hunt: c => (c.rest[0] === 'correctness' && c.round === 1 ? { findings: [bug(c.id)] } : undefined),
      fix: c => ({ head: newHead(c), fixed: [], notFixed: [] }) } })
    t.row(res, id, { status: 'blocked', rounds: 1, reason: ['fix round 1 made no commit', `F-${id} executor ignores ctx`] })
  }

  // d: the checker fails twice: the item stays open.
  res = await run([ROW.high('2d')], { stubs: {
    hunt: c => (c.rest[0] === 'correctness' && c.round === 1 ? { findings: [bug(c.id)] } : undefined),
    check: () => null } })
  t.row(res, '2d', { status: 'blocked', reason: ['not converged', 'F-2d executor ignores ctx'] })
  t.eq(res.calls.filter(c => c.kind === 'check' && c.label.endsWith(':40')).length, 4, '2d: checker retried in rounds 2 and 3')

  // e: an approver's verified blocking item that is not fixed stays open too.
  const A = F('internal/room/service_executor.go', 55, 'approval: lease never released')
  res = await run([ROW.high('2e')], { stubs: {
    approve: c => (c.round === 0 && c.head[c.id] === 'h1' ? { approve: false, sha: 'h1', reasons: ['bug'], blocking: [A] } : undefined),
    check: () => ({ fixed: false, evidence: 'still leaks' }) } })
  t.row(res, '2e', { status: 'blocked', reason: ['not converged', 'lease never released'] })
})

// ---------- 3 ----------
scenario('3. checks fail in round 1: the first review that runs is a full review from base', async t => {
  const failing = upTo => c => (c.nth < upTo ? { pass: false, base: 'b0', head: c.head[c.id], patchId: 'p', files: c.row.files,
    checks: [{ command: 'go test ./internal/room/', result: 'fail', output: '--- FAIL: TestExecutor' }], problems: ['TestExecutor fails: nil ctx'] } : undefined)
  let res = await run([ROW.high('3a')], { stubs: { verify: failing(1) } })
  t.row(res, '3a', { status: 'ready', rounds: 2, approvals: ['fable', 'opus'] })
  let first = firstReview(res, '3a')
  t.ok(first && first.label.endsWith(':r2'), `3a: no review in the failing round (first review ${first && first.label})`)
  t.ok(first && first.prompt.includes('Review the whole change') && first.prompt.includes('diff b0..h2') && !first.prompt.includes('Re-review'),
    `3a: first review is full from base: ${first && first.prompt.split('\n')[0]}`)
  t.ok(res.calls.some(c => c.label === 'fix:3a:r1' && c.prompt.includes('TestExecutor fails')), '3a: fixer gets the failing check')

  // b: checks fail in rounds 1 and 2; round 3 is the first review and it is full.
  res = await run([ROW.high('3b')], { stubs: { verify: failing(2) } })
  t.row(res, '3b', { status: 'ready', rounds: 3 })
  first = firstReview(res, '3b')
  t.ok(first && first.label.endsWith(':r3') && first.prompt.includes('Review the whole change') && first.prompt.includes('diff b0..h3'), '3b: first review is full from base in round 3')

  // c: reviewed in round 1, checks fail in round 2: round 3 re-reviews from the last reviewed head, not from round 2.
  res = await run([ROW.high('3c')], { stubs: { hunt: findIn('correctness', [1], [ROOM_BUG]), verify: c => (c.nth === 1 ? failing(2)(c) : undefined) } })
  t.row(res, '3c', { status: 'ready', rounds: 3 })
  const r3 = res.calls.find(c => c.label === 'hunt:3c:correctness:r3')
  t.ok(r3 && r3.prompt.includes('Re-review round 3') && r3.prompt.includes('diff h1..h3'), '3c: diff-only review spans the failed round')
})

// ---------- 4 ----------
scenario('4. a reviewer that fails twice blocks the row, never "clean"', async t => {
  let res = await run([ROW.high('4a')], { stubs: { hunt: c => (c.rest[0] === 'tests' ? null : undefined) } })
  t.row(res, '4a', { status: 'blocked', reason: 'review incomplete in round 1: a reviewer failed twice' })
  t.eq(res.calls.filter(c => c.label === 'hunt:4a:tests:r1').length, 2, '4a: retried once')
  t.eq(res.count('tier') + res.count('approve'), 0, '4a: no classifier or approval after a missing reviewer')

  res = await run([ROW.high('4b')], { stubs: { security: () => null } })
  t.row(res, '4b', { status: 'blocked', reason: 'review incomplete in round 1' })

  res = await run([ROW.high('4c')], { stubs: { hunt: c => { if (c.rest[0] === 'runtime') throw new Error('terminal API error') } } })
  t.row(res, '4c', { status: 'blocked', reason: 'review incomplete in round 1', allowErrors: true })

  res = await run([ROW.high('4d')], { stubs: { hunt: c => (c.rest[0] === 'contract' && c.round === 2 ? null : findIn('correctness', [1], [ROOM_BUG])(c)) } })
  t.row(res, '4d', { status: 'blocked', reason: 'review incomplete in round 2' })

  // a reviewer that fails once and answers on the retry is fine
  res = await run([ROW.high('4e')], { stubs: { hunt: c => (c.rest[0] === 'tests' && c.nth === 0 ? null : undefined) } })
  t.row(res, '4e', { status: 'ready', approvals: ['fable', 'opus'] })

  // the gate agents fail closed too: classifier, integrity check
  res = await run([ROW.high('4f')], { stubs: { tier: () => null } })
  t.row(res, '4f', { status: 'blocked', reason: 'risk classifier did not return' })
  res = await run([ROW.pipeline('4g')], { stubs: { integrity: () => null } })
  t.row(res, '4g', { status: 'blocked', reason: 'integrity check did not return' })
  res = await run([ROW.pipeline('4h')], { stubs: { integrity: () => ({ weakens: true, what: ['drops the third vote'] }) } })
  t.row(res, '4h', { status: 'blocked', reason: 'weakens the review pipeline: drops the third vote' })
})

// ---------- 5 ----------
scenario('5. file list normalised; empty diff blocks; the tier never fails open', async t => {
  let res = await run([{ ...ROW.high('5a'), files: ['./internal/room/a.go', '/wt/5a/internal/room/b.go', '/internal/room/c.go',
    'internal\\room\\d.go', '  internal/room/e.go  ', '././internal/room/a.go', '.\\internal\\room\\f.go'] }])
  const r = t.row(res, '5a', { status: 'ready', tier: 'high', approvals: ['fable', 'opus'] })
  t.eq(r.files, ['internal/room/a.go', 'internal/room/b.go', 'internal/room/c.go', 'internal/room/d.go', 'internal/room/e.go', 'internal/room/f.go'], '5a: normalised, deduplicated files')

  for (const [id, files] of [['5b', []], ['5c', ['', '   ', './', '/']]]) {
    res = await run([{ ...ROW.high(id), files }])
    t.row(res, id, { status: 'blocked', reason: 'empty diff' })
    t.eq(res.count('hunt'), 0, `${id}: no review of an empty diff`)
  }

  // d: the verifier lists only a medium file; the classifier's list adds an actions file: the union sets the tier.
  const mixed = { id: '5d', agent: 'A2', folders: ['internal/clients/kafka/**', 'internal/actions/**'], excluded: ['internal/actions/action.go'], files: ['internal/clients/kafka/lag.go'] }
  res = await run([mixed], { stubs: { tier: () => ({ raiseTo: 'none', reason: '', files: ['./internal/clients/kafka/lag.go', 'internal/actions/kafka_actions.go'] }) } })
  t.row(res, '5d', { status: 'ready', tier: 'high', approvals: ['fable', 'opus'] })

  // e: the classifier raises a docs row to medium; f: it cannot lower a high row.
  res = await run([ROW.low('5e')], { stubs: { tier: () => ({ raiseTo: 'medium', reason: 'changes what approvers read', files: [] }) } })
  t.row(res, '5e', { status: 'ready', tier: 'medium', reason: 'tier raised to medium', approvals: ['fable'] })
  res = await run([ROW.high('5f')], { stubs: { tier: () => ({ raiseTo: 'medium', reason: 'small', files: [] }) } })
  t.row(res, '5f', { status: 'ready', tier: 'high', approvals: ['fable', 'opus'] })
  // g: an unknown path is medium, never low
  res = await run([{ id: '5g', agent: 'A1', folders: ['dev'], files: ['dev/seed.sql'] }])
  t.row(res, '5g', { status: 'ready', tier: 'medium', approvals: ['fable'] })
})

// ---------- 6 ----------
const CONTRACT_FILES = ['internal/tools/tool.go', 'internal/events/events.go', 'internal/clients/clients.go', 'internal/actions/action.go',
  'internal/agent/contract.go', 'internal/topology/graph.go', 'internal/config/config.go', 'internal/version/version.go', 'docs/api.md']
const PIPELINE_FILES = ['docs/process/ship-workflow.md', 'docs/process/ship-check.mjs', '.claude/settings.json', '.github/workflows/ci.yml',
  '.github/scripts/fail-on-skips.sh', 'tasks/P-board.md', 'Makefile', 'web/eslint.config.js', 'web/vitest.config.ts', 'web/vite.config.ts',
  'web/.eslintrc.cjs', 'AGENTS.md', 'internal/agent/CLAUDE.md', 'REVIEW.md', 'web/REVIEW.md']
const HIGH_FILES = ['internal/room/service_executor.go', 'internal/room/executor_test.go', 'internal/owners/owners.go', 'internal/wiring/wiring.go',
  'cmd/server/main.go', 'internal/tools/scope.go', 'internal/clients/connect/client.go', 'internal/clients/schemareg/client.go',
  'internal/clients/pgconv/convert.go', 'internal/clients/netsec/tls.go', 'internal/clients/kafka/writer.go', 'internal/clients/postgres/writer.go',
  'internal/alertin/ingest.go', 'internal/store/events.go', 'internal/httpapi/server.go', 'internal/httpapi/auth.go', 'internal/httpapi/auth_oidc.go',
  'internal/auth/session.go', 'internal/actions/kafka_actions.go', 'internal/actions/pgactions/reset.go', 'internal/redact/redact.go',
  'internal/injectscan/scan.go', 'internal/agent/alerttext.go', 'internal/agent/alertevidence.go', 'internal/agent/injectflag.go',
  'internal/agent/approvals.go', 'internal/agent/prompt.go', 'migrations/0042_leases.sql', 'deploy/helm/values.yaml', 'Dockerfile',
  'cmd/devconsumer/Dockerfile', 'go.mod', 'go.sum', 'web/package.json', 'web/package-lock.json', ...CONTRACT_FILES, ...PIPELINE_FILES]
const MEDIUM_FILES = ['internal/clients/kafka/lag_test.go', 'evals/scenarios/schema-change.yaml', 'evals/scenarios/heldout/x.yaml',
  'web/src/App.test.tsx', 'test/e2e/room.spec.ts', 'internal/agent/run_test.go', 'internal/httpapi/hypothesis.go']
const LOW_FILES = ['docs/design/watcher.md', 'reviews/capacity-2026-10/run1.md', 'research/notes.md', 'README.md']

// One row per path, folders = that path, so a row's tier comes from that one file.
async function tiersOf(paths) {
  const defs = paths.map((p, i) => ({ id: `t${i}`, agent: CONTRACT_FILES.includes(p) ? 'leads' : 'A5', folders: [p], files: [p] }))
  const res = await run(defs)
  return { res, of: p => res.row(`t${paths.indexOf(p)}`) || {} }
}

scenario('6. high tier covers the listed paths; tests and evals/scenarios are not low', async t => {
  const all = [...HIGH_FILES, ...MEDIUM_FILES, ...LOW_FILES]
  const { res, of } = await tiersOf(all)
  t.clean(res)
  const want = p => (HIGH_FILES.includes(p) ? 'high' : MEDIUM_FILES.includes(p) ? 'medium' : 'low')
  const need = { low: 0, medium: 1, high: 2 }
  for (const p of all) {
    const r = of(p)
    if (r.status !== 'ready' || r.tier !== want(p)) t.ok(false, `${p}: got ${r.status} tier ${r.tier}, want ready tier ${want(p)} (${J(r.reasons)})`)
    else t.eq((r.approvals || []).filter(a => a.approve && a.sha === r.head).length, need[want(p)], `${p}: approvals`)
  }
  for (const p of PIPELINE_FILES) t.eq(res.count('integrity', `t${all.indexOf(p)}`), 1, `${p}: integrity check`)
  t.eq(res.count('integrity'), PIPELINE_FILES.length, 'integrity runs only for pipeline files')
})

// ---------- 7 ----------
scenario('7. ownership and the contract-file rule block the row', async t => {
  let res = await run([{ ...ROW.high('7a'), files: ['internal/room/a.go', 'internal/store/b.go'] }])
  t.row(res, '7a', { status: 'blocked', reason: "ownership: internal/store/b.go: outside A5's folders" })
  t.eq(res.count('hunt'), 0, '7a: stops before review')

  res = await run([{ id: '7b', agent: 'A2', folders: ['internal/actions/**'], excluded: ['`internal/actions/action.go`', 'internal/actions/pgactions/**'], files: ['internal/actions/kafka_actions.go', 'internal/actions/pgactions/reset.go'] }])
  t.row(res, '7b', { status: 'blocked', reason: "internal/actions/pgactions/reset.go: outside A2's folders" })

  res = await run([{ id: '7c', agent: 'A4', folders: ['internal/agent'], files: ['internal/agent/run.go', 'internal/agent/contract.go'] }])
  t.row(res, '7c', { status: 'blocked', reason: 'internal/agent/contract.go: contract file' })

  // a leads' row may change a contract file inside its folders
  res = await run([{ id: '7d', agent: 'leads', folders: ['internal/agent/contract.go', 'docs/**'], files: ['internal/agent/contract.go', 'docs/api.md'] }])
  t.row(res, '7d', { status: 'ready', tier: 'high', approvals: ['fable', 'opus'] })

  // the classifier's file list is checked too
  res = await run([ROW.high('7e')], { stubs: { tier: () => ({ raiseTo: 'none', reason: '', files: ['internal/room/a.go', 'internal/store/hidden.go'] }) } })
  t.row(res, '7e', { status: 'blocked', reason: 'internal/store/hidden.go: outside' })
  t.eq(res.count('approve'), 0, '7e: no approvals')

  // every round: a fix commit that strays outside the folders blocks round 2
  res = await run([ROW.high('7f')], { stubs: { hunt: findIn('correctness', [1], [ROOM_BUG]),
    verify: c => (c.nth > 0 ? { pass: true, base: 'b0', head: c.head[c.id], patchId: 'p', files: [...c.row.files, 'internal/wiring/wiring.go'], checks: [], problems: [] } : undefined) } })
  t.row(res, '7f', { status: 'blocked', rounds: 2, reason: 'internal/wiring/wiring.go: outside' })

  // a glob-only folder grants nothing
  res = await run([{ id: '7g', agent: 'A5', folders: ['**', '/', './'], files: ['internal/room/a.go'] }])
  t.row(res, '7g', { status: 'blocked', reason: 'ownership' })
})

// ---------- 8 ----------
const neverConverges = { hunt: c => (c.rest[0] === 'correctness' ? { findings: [F('internal/room/sole_member.go', 10 * c.round, `race ${c.round}`, { bugClass: `race-${c.round}` })] } : undefined) }

scenario('8. maxRounds is clamped to 1..3; secondModel never equals reviewModel', async t => {
  for (const [maxRounds, rounds] of [[10, 3], [-5, 1], ['abc', 3], [1, 1], ['2', 2], [undefined, 3]]) {
    const res = await run([ROW.high('8a')], { args: { rows: ['8a'], maxRounds }, stubs: neverConverges })
    t.row(res, '8a', { status: 'blocked', rounds, reason: `not converged after ${rounds} rounds` })
    t.eq(res.count('fix'), rounds - 1, `maxRounds ${J(maxRounds)}: fix rounds`)
  }
  let res = await run([ROW.high('8b')], { args: { rows: ['8b'], maxRounds: 0 }, stubs: neverConverges })
  const r = t.row(res, '8b', { status: 'blocked', reason: 'not converged' })
  t.ok(r.rounds >= 1 && r.rounds <= 3, `maxRounds 0: rounds ${r.rounds} within 1..3`)

  // same model asked for both: the second opinion is switched to the other model everywhere it is used
  res = await run([ROW.high('8c')], { args: { rows: ['8c'], reviewModel: 'opus', secondModel: 'opus' }, stubs: { hunt: findIn('correctness', [1], [ROOM_BUG]) } })
  t.row(res, '8c', { status: 'ready', approvals: ['opus', 'fable'] })
  t.ok(res.logs.some(l => l.includes('not allowed')), '8c: logged the switch')
  t.eq(res.calls.filter(c => c.kind === 'vote').map(c => c.model), ['opus', 'opus', 'fable'], '8c: third voter is the other model')
  t.eq(res.calls.find(c => c.kind === 'tier').model, 'fable', '8c: classifier is the other model')
  res = await run([ROW.high('8d')], { args: { rows: ['8d'], reviewModel: 'fable', secondModel: 'fable' } })
  t.row(res, '8d', { status: 'ready', approvals: ['fable', 'opus'] })
  res = await run([ROW.high('8e')], { args: ['8e'] })
  t.row(res, '8e', { status: 'ready', approvals: ['fable', 'opus'] })
})

// ---------- 9 ----------
scenario('9. a finding is dropped only when 2 of 3 verifiers refute it; failed verifiers do not refute', async t => {
  // voters: (c) => answer for voter n = 1, 2, 3 (last label part)
  const voters = answers => c => { const a = answers[Number(c.rest[c.rest.length - 1]) - 1]; return a === null ? null : { refuted: a, reason: 'x' } }
  const cases = [
    ['9a', [false, false, true], true], // one refutes: kept
    ['9b', [false, true, true], false], // two refute: dropped
    ['9c', [null, null, true], true], // two verifiers fail, one refutes: kept
    ['9d', [null, null, null], true], // all fail: kept
    ['9e', [true, true, true], false],
    ['9f', [true, null, true], false], // a failed verifier does not save a finding two others refuted
  ]
  for (const [id, answers, kept] of cases) {
    const res = await run([ROW.high(id)], { stubs: { hunt: findIn('correctness', [1], [ROOM_BUG]), vote: voters(answers) } })
    const r = t.row(res, id, { status: 'ready', rounds: kept ? 2 : 1 })
    t.eq([r.verified, r.dropped, res.count('fix')], kept ? [1, 0, 1] : [0, 1, 0], `${id} ${J(answers)}: [verified, dropped, fix rounds]`)
  }
  // the same rule for a security finding
  const sec = F('internal/room/service_executor.go', 12, 'DSN logged on connect error', { bugClass: 'secret in log' })
  for (const [id, answers, kept] of [['9g', [true, false, false], true], ['9h', [true, true, false], false]]) {
    const res = await run([ROW.high(id)], { stubs: { security: c => (c.round === 1 ? { findings: [sec] } : undefined), vote: voters(answers) } })
    const r = t.row(res, id, { status: 'ready', rounds: kept ? 2 : 1 })
    t.eq([r.verified, r.dropped], kept ? [1, 0] : [0, 1], `${id} security ${J(answers)}: [verified, dropped]`)
  }
  // a refuted finding raised again in a later round is not voted on again
  const G = F('internal/room/service_executor.go', 80, 'retry loop never stops', { bugClass: 'unbounded retry' })
  const res = await run([ROW.high('9i')], { stubs: {
    hunt: c => (c.rest[0] === 'correctness' ? { findings: c.round === 1 ? [ROOM_BUG, G] : [ROOM_BUG] } : undefined),
    vote: c => ({ refuted: c.label.includes(':40:') && c.rest[c.rest.length - 1] !== '3', reason: 'x' }) } })
  const r = t.row(res, '9i', { status: 'ready', rounds: 2 })
  t.eq(res.calls.filter(c => c.kind === 'vote' && c.label.includes(':40:')).length, 3, '9i: refuted finding voted once')
  t.eq([r.verified, r.dropped], [1, 1], '9i: [verified, dropped]')
})

// ---------- 10 ----------
scenario('10. approvals name the head; nits capped at 3; overlapping rows in separate waves; plan null blocks every row', async t => {
  // a: approval of another commit, also on the second ask
  let res = await run([ROW.medium('10a')], { stubs: { approve: () => ({ approve: true, sha: 'h0-old', reasons: ['ok'], blocking: [] }) } })
  const a = t.row(res, '10a', { status: 'blocked', reason: 'did not approve the head commit', notApproved: true })
  t.eq(a.approvals, [{ model: 'fable', approve: false, sha: 'h0-old' }], '10a: approvals')
  t.ok(res.calls.some(c => c.label === 'approve:10a:1:again' && c.prompt.includes('named commit h0-old')), '10a: second ask names the wrong commit')
  // b: wrong commit first, head on the second ask
  res = await run([ROW.medium('10b')], { stubs: { approve: c => (c.label.endsWith(':again') ? undefined : { approve: true, sha: 'h0-old', reasons: [], blocking: [] }) } })
  t.row(res, '10b', { status: 'ready', approvals: ['fable'] })

  // c: nits from every lens in two rounds: three in total
  const nit = (c, i) => ({ file: 'internal/room/a.go', line: 100 + i, severity: 'nit', bugClass: 'naming', claim: `${c.rest[0]} nit ${c.round}.${i}`, evidence: 'e', fix: 'f' })
  const nits = c => ({ findings: [nit(c, 1), nit(c, 2), ...(c.rest[0] === 'correctness' && c.round === 1 ? [ROOM_BUG] : [])] })
  res = await run([ROW.high('10c')], { stubs: { hunt: nits, security: nits } })
  const nc = t.row(res, '10c', { status: 'ready', rounds: 2 })
  t.eq((nc.nits || []).length, 3, '10c: nits capped at 3')
  t.eq((String(nc.prBody).split('Nits (not blocking):')[1] || '').split('\n').filter(l => l.startsWith('- ')).length, 3, '10c: three nits in the PR body')

  // d: overlapping folders run in separate waves, in order; disjoint rows share a wave
  const rows = [
    { id: 'WA', agent: 'A5', folders: ['internal/room/**'], files: ['internal/room/a.go'] },
    { id: 'WB', agent: 'A5', folders: ['internal/room/policy'], files: ['internal/room/policy/p.go'] },
    { id: 'WC', agent: 'A6', folders: ['web/**'], files: ['web/src/App.tsx'] },
    { id: 'WD', agent: 'A5', folders: ['internal/room'], files: ['internal/room/d.go'] },
  ]
  res = await run(rows)
  for (const r of rows) t.row(res, r.id, { status: 'ready' })
  t.ok(res.logs.some(l => l.includes('in 3 wave(s): WA + WC | WB | WD')), `10d: waves logged: ${J(res.logs.find(l => l.includes('wave')))}`)
  const at = (pred) => res.calls.findIndex(pred)
  const last = id => res.calls.map(c => c.id).lastIndexOf(id)
  t.ok(at(c => c.label === 'build:WB') > last('WA'), '10d: WB starts after WA ends')
  t.ok(at(c => c.label === 'build:WD') > last('WB'), '10d: WD starts after WB ends')
  t.ok(at(c => c.label === 'build:WC') < at(c => c.label === 'build:WB'), '10d: WC runs in the first wave')

  // e: plan returns null: every requested row is blocked and nothing is built
  res = await run([ROW.high('PA'), ROW.medium('PB')], { stubs: { plan: () => null } })
  t.clean(res)
  t.eq(res.rows.map(r => [r.row, r.status, r.reasons[0]]), [['PA', 'blocked', 'plan did not return'], ['PB', 'blocked', 'plan did not return']], '10e: rows')
  t.eq(res.count('build'), 0, '10e: nothing built')
  // f: a row that waits, and a row the plan does not know
  res = await run([ROW.high('PC'), { ...ROW.medium('PD'), ready: false, missing: ['P0-9'] }], { args: { rows: ['PC', 'PD', 'PE'] } })
  t.row(res, 'PC', { status: 'ready' })
  t.row(res, 'PD', { status: 'skipped', reason: 'waits on P0-9' })
  t.row(res, 'PE', { status: 'skipped', reason: 'not found on tasks/P-board.md' })
})

// ---------- extra: gaps found while writing this test (each fails until its patch lands) ----------
scenario('extra 5w. a Windows worktree prefix is stripped from the file list', async t => {
  const res = await run([{ ...ROW.high('X5'), worktree: 'C:\\work\\piperoom-ship\\X5', files: ['C:\\work\\piperoom-ship\\X5\\internal\\room\\a.go', 'internal/room/b.go'] }])
  const r = t.row(res, 'X5', { status: 'ready', tier: 'high' })
  t.eq(r.files, ['internal/room/a.go', 'internal/room/b.go'], 'X5 files')
})

// git diff --name-only shows only the new path of a renamed file, so moving internal/actions/x.go into web/ would
// list web/x.go alone: medium tier, inside a web row's folders. The file-list commands must use --no-renames.
scenario('extra 5r. the verifier and the classifier list both sides of a rename', async t => {
  const res = await run([ROW.high('XR')])
  t.row(res, 'XR', { status: 'ready' })
  for (const kind of ['verify', 'tier']) {
    const c = res.calls.find(x => x.kind === kind)
    t.ok(c && /diff --no-renames --name-only/.test(c.prompt), `${kind} prompt lists files with --no-renames: ${c && (c.prompt.match(/git[^;)]*--name-only[^;)]*/) || [''])[0]}`)
  }
})

scenario('extra 6x. agent read-only guard and agent-side redaction are high tier', async t => {
  const paths = ['internal/agent/readonly.go', 'internal/agent/redact.go']
  const { res, of } = await tiersOf(paths)
  t.clean(res)
  for (const p of paths) t.eq(of(p).tier, 'high', `${p} tier`)
})

scenario('extra 8x. a fractional maxRounds still stops with a reason after its last round', async t => {
  const res = await run([ROW.high('X8')], { args: { rows: ['X8'], maxRounds: 2.5 }, stubs: neverConverges })
  const r = t.row(res, 'X8', { status: 'blocked', reason: 'not converged' })
  t.eq(res.count('fix'), Math.max(0, (r.rounds || 0) - 1), 'X8: no fix commit after the last reviewed round')
})

scenario('extra 10x. only requested rows are shipped, once each', async t => {
  // the plan answers for XA twice and adds XZ, which nobody asked for
  const XA = ROW.high('XA'), XZ = ROW.medium('XZ')
  const res = await run([XA, XZ], { args: { rows: ['XA'] }, stubs: { plan: () => ({ rows: [planRow(XA), planRow(XA), planRow(XZ)] }) } })
  t.clean(res)
  t.eq(res.count('build', 'XZ'), 0, 'unrequested row XZ built')
  t.eq(res.count('build', 'XA'), 1, 'row XA builds')
  t.eq(res.rows.map(r => r.row), ['XA'], 'manifest rows')
})

// ---------- ship-check.mjs ----------
const TMP = mkdtempSync(join(tmpdir(), 'ship-check-'))
let tmpN = 0
function shipCheck(row, files, patchId) {
  const n = ++tmpN
  const rowFile = join(TMP, `row${n}.json`), listFile = join(TMP, `files${n}.txt`)
  writeFileSync(rowFile, JSON.stringify(row, null, 2))
  writeFileSync(listFile, files.join('\r\n') + '\n')
  const r = spawnSync(process.execPath, [SHIP_CHECK, rowFile, listFile, ...(patchId === undefined ? [] : [patchId])], { encoding: 'utf8' })
  return { code: r.status, out: (r.stdout || '') + (r.stderr || '') }
}
const line = (out, mark, name) => out.split('\n').some(l => l.startsWith(`${mark}  ${name}`))

scenario('ship-check 1. a ready manifest row and its real files pass (CRLF and ./ paths normalised)', async t => {
  const res = await run([ROW.high('SC1')])
  const row = t.row(res, 'SC1', { status: 'ready', tier: 'high' })
  const c = shipCheck(row, row.files.map(f => './' + f), row.patchId)
  t.eq(c.code, 0, `exit code\n${c.out}`)
  for (const name of ['status', 'tier', 'ownership', 'approvals', 'patch id']) t.ok(line(c.out, 'PASS', name), `PASS ${name}\n${c.out}`)
})

scenario('ship-check 2. a file the workflow did not see raises the tier and fails', async t => {
  const res = await run([{ id: 'SC2', agent: 'A2', folders: ['internal/clients/kafka/**', 'internal/actions/**'], excluded: ['internal/actions/action.go'], files: ['internal/clients/kafka/lag.go'] }])
  const row = t.row(res, 'SC2', { status: 'ready', tier: 'medium', approvals: ['fable'] })
  const c = shipCheck(row, [...row.files, 'internal/actions/kafka_actions.go'], row.patchId)
  t.eq(c.code, 1, `exit code\n${c.out}`)
  t.ok(line(c.out, 'FAIL', 'tier') && c.out.includes('internal/actions/kafka_actions.go'), `FAIL tier names the file\n${c.out}`)
  t.ok(line(c.out, 'FAIL', 'approvals') && c.out.includes('high needs 2'), `FAIL approvals\n${c.out}`)
  t.ok(line(c.out, 'FAIL', 'files') && c.out.includes('never reviewed: internal/actions/kafka_actions.go'), `FAIL files (unreviewed)\n${c.out}`)
  t.ok(line(c.out, 'PASS', 'ownership'), `PASS ownership\n${c.out}`)
  // on a row that is already high, a hidden pipeline file leaves tier and approvals intact but had no integrity check
  const h = await run([{ id: 'SC2h', agent: 'leads', folders: ['docs', 'tasks'], files: ['docs/process/ship-workflow.md'] }])
  const hrow = t.row(h, 'SC2h', { status: 'ready', tier: 'high' })
  const c2 = shipCheck(hrow, [...hrow.files, 'tasks/P-board.md'])
  t.ok(c2.code === 1 && line(c2.out, 'FAIL', 'files') && line(c2.out, 'PASS', 'tier') && line(c2.out, 'PASS', 'approvals'), `FAIL files for a hidden pipeline file at high tier\n${c2.out}`)
})

scenario('ship-check 3. ownership and contract files fail', async t => {
  const res = await run([ROW.high('SC3')])
  const row = t.row(res, 'SC3', { status: 'ready' })
  let c = shipCheck(row, [...row.files, 'internal/store/events.go'], row.patchId)
  t.eq(c.code, 1, `exit code (outside folders)\n${c.out}`)
  t.ok(line(c.out, 'FAIL', 'ownership') && c.out.includes("internal/store/events.go: outside A5's folders"), `FAIL ownership\n${c.out}`)
  c = shipCheck({ ...row, folders: ['internal/room', 'internal/events'] }, [...row.files, 'internal/events/events.go'], row.patchId)
  t.eq(c.code, 1, `exit code (contract file)\n${c.out}`)
  t.ok(line(c.out, 'FAIL', 'ownership') && c.out.includes('internal/events/events.go: contract file'), `FAIL contract file\n${c.out}`)
})

scenario('ship-check 4. a changed patch id, a stale or same-model approval, or bad input fails', async t => {
  const res = await run([ROW.high('SC4')])
  const row = t.row(res, 'SC4', { status: 'ready' })
  let c = shipCheck(row, row.files, 'pid-somethingelse')
  t.eq(c.code, 1, `exit code (patch id)\n${c.out}`)
  t.ok(line(c.out, 'FAIL', 'patch id') && c.out.includes('approvals must be given again'), `FAIL patch id\n${c.out}`)
  c = shipCheck(row, row.files)
  t.eq(c.code, 0, `exit code (no patch id given)\n${c.out}`)
  t.ok(line(c.out, 'SKIP', 'patch id'), `SKIP patch id\n${c.out}`)
  c = shipCheck({ ...row, approvals: [row.approvals[0], { ...row.approvals[1], sha: 'h0' }] }, row.files, row.patchId)
  t.ok(c.code === 1 && line(c.out, 'FAIL', 'approvals'), `FAIL stale approval\n${c.out}`)
  c = shipCheck({ ...row, approvals: [row.approvals[0], { ...row.approvals[0] }] }, row.files, row.patchId)
  t.ok(c.code === 1 && line(c.out, 'FAIL', 'approvals'), `FAIL two approvals from one model\n${c.out}`)
  c = shipCheck({ ...row, status: 'blocked' }, row.files, row.patchId)
  t.ok(c.code === 1 && line(c.out, 'FAIL', 'status'), `FAIL blocked row\n${c.out}`)
  c = shipCheck(row, [], row.patchId)
  t.ok(c.code === 1 && line(c.out, 'FAIL', 'files'), `FAIL empty file list\n${c.out}`)
  const u = spawnSync(process.execPath, [SHIP_CHECK], { encoding: 'utf8' })
  t.ok(u.status === 1 && u.stderr.includes('Usage: node docs/process/ship-check.mjs'), `usage error exits 1: ${u.status} ${u.stderr}`)
})

// ---------- run ----------
let failed = 0
for (const s of scenarios) {
  const t = tester()
  try { await s.fn(t) } catch (e) { t.fails.push(`threw: ${e.stack || e}`) }
  if (t.fails.length) failed++
  console.log(`${t.fails.length ? 'FAIL' : 'PASS'}  ${s.name}`)
  for (const f of t.fails) console.log('      - ' + f.replace(/\n/g, '\n        '))
}
rmSync(TMP, { recursive: true, force: true })
console.log(`\n${scenarios.length - failed}/${scenarios.length} scenarios pass${SCRIPT.endsWith('piperoom-ship.workflow.js') ? '' : ` (script: ${SCRIPT})`}`)
process.exit(failed ? 1 : 0)
