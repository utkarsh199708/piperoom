// Dry run of piperoom-ship.workflow.js with stubbed agents: no tokens spent. Six scenario rows cover a fix
// round with two approvals, a stale approval, non-convergence, a contract stop, a pipeline-weakening change and
// an unready row. Run from the repo root: node docs/process/ship-workflow.dryrun.mjs
import { readFileSync } from 'node:fs'
const src = readFileSync(process.argv[2] || new URL('./piperoom-ship.workflow.js', import.meta.url), 'utf8').replace(/^export const meta =/m, 'const meta =')
const AsyncFunction = Object.getPrototypeOf(async function () {}).constructor
const calls = []
const ROWS = {
  'P3-0':  { files: ['internal/room/service_executor.go', 'internal/room/executor_test.go'], plan: 'ok' },
  'P1a-2': { files: ['cmd/server/main.go'], plan: 'ok', staleApproval: true },
  'P2b-2': { files: ['internal/room/sole_member.go'], plan: 'ok', neverConverges: true },
  'P4a-4': { files: [], plan: 'notready' },
  'P3a-1': { files: [], plan: 'ok', contract: true },
  'P0-x':  { files: ['docs/process/piperoom-ship.workflow.js'], plan: 'ok', weakens: true },
}
const heads = {}
let fixN = 0
async function agent(prompt, opts = {}) {
  const label = opts.label || ''
  calls.push(label)
  const [kind, row, ...rest] = label.split(':')
  const R = ROWS[row] || {}
  switch (kind) {
    case 'plan': return { rows: Object.entries(ROWS).map(([id, r]) => ({ id, ready: r.plan === 'ok', missing: r.plan === 'ok' ? [] : ['P4a-1x'], agent: 'A5', folders: ['internal/room/**'], taskFile: 'tasks/P3.md', summary: `do ${id}`, doneWhen: 'tests pass', prTitle: `[${id}] x` })) }
    case 'build':
      if (R.contract) return { status: 'blocked', blockedReason: 'needs a contract field', worktree: '/wt/' + row, branch: 'p/' + row, base: 'b0', head: 'b0', checks: [], howToRevert: '', notDone: '', newDependencies: '', contractChangeRequest: 'add Foo to clients.go' }
      heads[row] = 'h1'
      return { status: 'built', worktree: '/wt/' + row, branch: 'p/' + row, base: 'b0', head: 'h1', checks: [{ command: 'go test', result: 'pass', output: 'ok' }], howToRevert: 'revert the merge', notDone: '', newDependencies: '', contractChangeRequest: '' }
    case 'verify': return { pass: true, head: heads[row], files: R.files, checks: [{ command: 'go test ./internal/room/', result: 'pass', output: 'ok' }], problems: [] }
    case 'hunt': case 'security': {
      const round = Number(label.split(':').pop().slice(1))
      const f = []
      if (kind === 'hunt' && rest[0] === 'correctness') {
        if (row === 'P3-0' && round === 1) f.push({ file: 'internal/room/service_executor.go', line: 40, severity: 'important', bugClass: 'missing ctx', claim: 'Validate runs without pipeline ctx', evidence: 'line 40', fix: 'wrap ctx' }, { file: 'a.go', line: 1, severity: 'nit', bugClass: 'naming', claim: 'rename', evidence: 'x', fix: 'y' })
        if (row === 'P3-0' && round === 2) f.push({ file: 'internal/room/service_executor.go', line: 41, severity: 'important', bugClass: 'missing ctx', claim: 'same again', evidence: 'repeat', fix: 'wrap' }) // dup of round 1 -> must be deduped
        if (row === 'P2b-2') f.push({ file: 'internal/room/sole_member.go', line: 10 * round, severity: 'important', bugClass: 'race' + round, claim: 'race ' + round, evidence: 'e', fix: 'f' })
      }
      if (kind === 'security' && row === 'P3-0' && round === 1) f.push({ file: 'internal/room/old.go', line: 5, severity: 'preexisting', bugClass: 'log leak', claim: 'old leak', evidence: 'e', fix: 'f' })
      return { findings: f }
    }
    case 'vote': return { refuted: rest[rest.length - 1] === '3', reason: 'x' } // voters 1,2 confirm, 3 refutes -> 2 of 3 real
    case 'fix': fixN++; heads[row] = 'h' + (Number(heads[row].slice(1)) + 1); return { head: heads[row], fixed: ['ok'], notFixed: [] }
    case 'tier': return { raiseTo: 'none', reason: '' }
    case 'integrity': return { weakens: !!R.weakens, what: R.weakens ? ['removes the vote'] : [] }
    case 'approve': return { approve: true, sha: R.staleApproval ? 'old' : heads[row], reasons: ['fine'], blocking: [] }
  }
  throw new Error('unexpected label ' + label)
}
const parallel = async thunks => Promise.all(thunks.map(t => t().catch(e => { console.error('THUNK', e); return null })))
const pipeline = async (items, ...stages) => Promise.all(items.map(async (it, i) => { let r = it; for (const s of stages) { try { r = await s(r, it, i) } catch (e) { console.error('STAGE', e); return null } } return r }))
const fn = new AsyncFunction('agent', 'pipeline', 'parallel', 'phase', 'log', 'args', 'budget', src)
const out = await fn(agent, pipeline, parallel, () => {}, m => console.log('LOG', m), Object.keys(ROWS), { total: null, spent: () => 0, remaining: () => Infinity })
for (const r of out.rows) console.log(JSON.stringify({ row: r.row, status: r.status, tier: r.tier, rounds: r.rounds, found: r.found, verified: r.verified, dropped: r.dropped, nits: r.nits && r.nits.length, pre: r.preexisting && r.preexisting.length, approvals: r.approvals, reasons: r.reasons, head: r.head }))
console.log('agent calls:', calls.length, '| by kind:', JSON.stringify(calls.reduce((m, c) => (m[c.split(':')[0]] = (m[c.split(':')[0]] || 0) + 1, m), {})))
const p30 = out.rows.find(r => r.row === 'P3-0'); console.log('\nP3-0 PR body:\n' + p30.prBody)
