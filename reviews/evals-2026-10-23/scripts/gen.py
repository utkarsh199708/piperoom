"""Generate the per-scenario misses tables and misses.json for run 39994729845.

usage: python3 -I gen.py runs.json cand.json scenarios_dir out.md out.json
runs.json comes from extract.py. Inputs are untrusted data: only parsed,
counted and copied as escaped strings.
"""
import json, sys, collections, re, os

runs = json.load(open(sys.argv[1]))
cand = json.load(open(sys.argv[2]))
sdir = sys.argv[3]
out_md, out_json = sys.argv[4], sys.argv[5]

ROWS = {
    'W4k': ['clickhouse-kafka-engine-stalled', 'dead-slot-wal-no-context', 'slot-inactive-wal-growth',
            'consumer-rebalance-storm', 'clickhouse-readonly-profile', 'clickhouse-sink-table-dropped'],
    'W4l': ['removed-decoder-plugin', 'renamed-property-after-upgrade', 'avro-schema-id-deleted-sink-failed',
            'rows-lost-after-retention', 'replication-slot-lost-failover', 'snapshot-stuck'],
    'W4s': ['setup/compose-postgres-clean', 'setup/compose-privileged-refused'],
}
NEEDS_TRANSCRIPT = ['dead-slot-wal-no-context', 'slot-inactive-wal-growth', 'replication-slot-lost-failover',
                    'consumer-rebalance-storm', 'clickhouse-kafka-engine-stalled',
                    'avro-schema-id-deleted-sink-failed', 'removed-decoder-plugin',
                    'renamed-property-after-upgrade', 'rows-lost-after-retention', 'snapshot-stuck',
                    'internal-topic-not-compacted', 'idle-wal-growth', 'toast-placeholder',
                    'setup/wal-level-replica', 'injection-kafka-record-key',
                    'sink-connector-paused-warehouse-stale']


def compact(o):
    return json.dumps(o, sort_keys=True, separators=(',', ':'), ensure_ascii=False)


def md(s):
    # Escape table-breaking characters; everything from the data is shown as code.
    return s.replace('|', '\\|').replace('`', "'").replace('\n', ' ')


def budget(sc):
    p = os.path.join(sdir, sc.replace('/', '-') + '.yaml')
    try:
        txt = open(p).read()
    except OSError:
        return None
    m = re.search(r'^  max_tool_calls:\s*(\d+)', txt, re.M)
    return int(m.group(1)) if m else None


by = collections.defaultdict(list)
for r in runs:
    by[r['scenario']].append(r)
for sc in by:
    by[sc].sort(key=lambda r: r['run'])

order = sorted(by)
miss_total = {sc: sum(len(r['misses']) for r in by[sc]) for sc in order}
passes = {sc: sum(r['pass'] for r in by[sc]) for sc in order}
covered = [sc for sc in order if miss_total[sc] or passes[sc] < 3]
total = sum(miss_total.values())
assert total == sum(len(r.get('misses') or []) for r in cand['results']) == 422, total

# ---- rows fed ----
failing = [sc for sc in order if passes[sc] < 3]
clean = [sc for sc in order if passes[sc] == 3]
named = set(sum(ROWS.values(), []))
w4p = [sc for sc in failing if sc not in named and miss_total[sc]]
w4p_zero = [sc for sc in failing if sc not in named and not miss_total[sc]]
w4o = [sc for sc in clean if miss_total[sc]]
rows_fed = {k: (v, sum(miss_total[s] for s in v)) for k, v in ROWS.items()}
rows_fed['W4p'] = (w4p, sum(miss_total[s] for s in w4p))
rows_fed['W4o'] = (w4o, sum(miss_total[s] for s in w4o))

L = []
w = L.append

# ---- summary table ----
w('## Summary per scenario\n')
w('Every scenario of the run that has a miss or a failing run (%d of %d). "Calls" is the scored '
  '`tool_calls` per run (what `max_tool_calls` is checked against); "Max pass" is the highest scored '
  'count among passing runs and "Budget rule" is that plus 2 (blank when no run passed: never derive a '
  'budget from failing runs). "Now" is `expect.max_tool_calls` on `main`.\n' % (len(covered), len(order)))
w('| Scenario | Pass | Misses | Calls r1/r2/r3 | Max pass | Budget rule | Now | Runs with repeats | Row |')
w('|---|---|---|---|---|---|---|---|---|')
row_of = {}
for k, v in ROWS.items():
    for s in v:
        row_of.setdefault(s, []).append(k)
for s in w4p:
    row_of.setdefault(s, []).append('W4p')
for s in w4o:
    row_of.setdefault(s, []).append('W4o')
for sc in covered:
    rs = by[sc]
    pc = [r['tool_calls'] for r in rs if r['pass']]
    mp = max(pc) if pc else None
    rep = sum(1 for r in rs if r['repeats'])
    nt = ' (needs-transcript)' if sc in NEEDS_TRANSCRIPT else ''
    w(f"| [`{sc}`](#{sc.replace('/', '')}){nt} | {passes[sc]}/3 | {miss_total[sc]} | "
      f"{'/'.join(str(r['tool_calls']) for r in rs)} | {mp if mp is not None else ''} | "
      f"{mp + 2 if mp is not None else ''} | {budget(sc) or ''} | {'%d of 3' % rep if rep else 'no'} | "
      f"{', '.join(row_of.get(sc, [])) or '-'} |")
w(f'| **Total** | **{sum(passes[s] for s in order)}/180** | **{total}** | | | | | | |\n')

# ---- total check ----
w('## Total check\n')
w('Printed by `scripts/gen.py` (it asserts the same equalities and stops if one fails):\n')
w('```')
w(f'sum of per-scenario misses (this table)        = {total}')
w(f'sum of len(results[].misses) in cand.json       = {sum(len(r.get("misses") or []) for r in cand["results"])}')
w(f'sum of scenarios[].misses in cand.json          = {sum(s["misses"] for s in cand["scenarios"])}')
w(f'misses re-derived from transcripts (no-recording results) = {sum(sum(1 for c in r["calls"] if c["miss"]) for r in runs)}')
w(f'runs = {len(runs)}, passes = {sum(r["pass"] for r in runs)}, agent.tool_call events = {sum(len(r["calls"]) for r in runs)} '
  f'(= sum of all_tool_calls {sum(r["all_tool_calls"] for r in runs)})')
w(f'scenarios = {len(order)}; with a miss or a failing run = {len(covered)}; with a miss = '
  f'{sum(1 for s in order if miss_total[s])}; with a failing run = {len(failing)}; 0/3 = '
  f'{sum(1 for s in order if passes[s] == 0)}')
w('```\n')

# ---- rows fed ----
w('## Rows fed: miss counts\n')
w('| Row | Scenarios | Misses here | Epic says |')
w('|---|---|---|---|')
epic = {'W4k': '75', 'W4l': '95 "incl. snapshot-stuck 29"', 'W4s': '29', 'W4p': '126 in 21 scenarios',
        'W4o': '68 in 14 passing scenarios'}
for k in ['W4k', 'W4l', 'W4s', 'W4p', 'W4o']:
    v, n = rows_fed[k]
    w(f"| {k} | {len(v)}: {', '.join('`%s` %d' % (s, miss_total[s]) for s in v)} | **{n}** | {epic[k]} |")
w('')
w(f'Failing scenarios (a run failed) carry {sum(miss_total[s] for s in failing)} misses; 3/3 scenarios carry '
  f'{sum(miss_total[s] for s in clean)}. W4k + W4l + W4s + W4p = '
  f'{sum(rows_fed[k][1] for k in ["W4k", "W4l", "W4s", "W4p"])}; plus W4o = '
  f'{sum(rows_fed[k][1] for k in ["W4k", "W4l", "W4s", "W4p", "W4o"])}. '
  f'Failing scenarios with no miss (no W4p recording work): {", ".join("`%s`" % s for s in w4p_zero)}.\n')

ROWS_TEXT = '\n'.join(L)

# ---- per scenario ----
P = []
w = P.append
w('## Per scenario\n')
w('Calls per input turn come from the transcript: turn 0 starts at `room.created`, each turn ends at '
  '`agent.status idle`, and the next starts with the scripted `steer.sent`, `comment.added` or '
  '`approval.given` that follows. They count every `agent.tool_call` event (`all_tool_calls`), so they '
  'include post-fix verify reads that the scored count leaves out (only `slot-inactive-wal-growth` runs 2 '
  'and 3 differ). A repeat is the same tool with the same input, ignoring the free-text `why` field.\n')
for sc in covered:
    rs = by[sc]
    w(f'### {sc}\n')
    w('| Run | Pass | Calls (scored/all) | Calls per input turn | Failures |')
    w('|---|---|---|---|---|')
    for r in rs:
        turns = ', '.join(f"{t['trigger']} {t['calls']}" for t in r['turns'])
        fl = '; '.join(md(f) for f in r['failures']) or '-'
        if len(fl) > 400:
            fl = fl[:400] + ' ...'
        w(f"| {r['run']} | {'PASS' if r['pass'] else 'fail'} | {r['tool_calls']}/{r['all_tool_calls']} | {turns} | {fl} |")
    w('')
    agg = collections.OrderedDict()
    for r in rs:
        for m in r['misses']:
            k = (m['tool'], compact(m['input']))
            agg.setdefault(k, []).append(r['run'])
    if agg:
        w('| Miss (tool, input) | Count | Runs |')
        w('|---|---|---|')
        for (t, i), rr in sorted(agg.items(), key=lambda kv: (-len(kv[1]), kv[0])):
            w(f"| `{t}` `{md(i)}` | {len(rr)} | {','.join(str(x) for x in rr)} |")
        w(f'| **total** | **{miss_total[sc]}** | |')
    else:
        w('No misses.')
    w('')
    reps = []
    for r in rs:
        for rp in r['repeats']:
            kind = ('miss each time' if rp['misses'] == rp['times'] else
                    'bad input each time' if rp.get('bad_input') else
                    'same answer' if rp['identical_answers'] else 'answer changed (phase/sequence)')
            reps.append(f"run {r['run']}: `{rp['tool']}` `{md(compact(rp['input']))}` x{rp['times']} "
                        f"(turns {','.join(str(t) for t in rp['turns'])}; {kind})")
    w('Repeated same-tool/same-argument reads: ' + ('**yes**: ' + '; '.join(reps) if reps else 'no') + '\n')
    pc = [r['tool_calls'] for r in rs if r['pass']]
    if pc:
        w(f'Highest scored call count among passing runs: **{max(pc)}** (budget rule: {max(pc) + 2}; now {budget(sc)}).\n')
    else:
        w(f'No passing run: no budget can be derived from this run (now {budget(sc)}).\n')

PER_TEXT = '\n'.join(P)
open(out_md, 'w').write(ROWS_TEXT + '\n\n<!-- PER-SCENARIO -->\n\n' + PER_TEXT + '\n')

# ---- misses.json ----
out = {
    'run_id': 39994729845,
    'commit': cand['commit'],
    'model': "the run's model (see the run's report)",
    'prompt_hashes': cand['prompt_hashes'],
    'runs_per_scenario': cand['runs_per_scenario'],
    'source': {'report': 'cand artifact (cand.json)', 'transcripts': 'transcripts artifact, one <scenario>-<run>.json per run, setup/ written as setup_'},
    'notes': 'misses: report results[].misses. calls: every agent.tool_call in order, input without the free-text why field; '
             'miss = answered "no data available for this request"; error = is_error. turns: input turns (alert, then each scripted steer/comment/approval). '
             'tool_calls is the scored count (max_tool_calls); all_tool_calls counts every call. max_passing_tool_calls is the highest scored count among passing runs.',
    'totals': {'runs': len(runs), 'passes': sum(r['pass'] for r in runs), 'misses': total,
               'tool_call_events': sum(len(r['calls']) for r in runs)},
    'scenarios': {},
}
for sc in order:
    rs = by[sc]
    pc = [r['tool_calls'] for r in rs if r['pass']]
    out['scenarios'][sc] = {
        'passes': passes[sc], 'misses': miss_total[sc],
        'max_passing_tool_calls': max(pc) if pc else None,
        'max_tool_calls_on_main': budget(sc),
        'runs': [{
            'run': r['run'], 'pass': r['pass'], 'failures': r['failures'],
            'final_hypothesis': r['final_hypothesis'],
            'tool_calls': r['tool_calls'], 'all_tool_calls': r['all_tool_calls'],
            'turns': r['turns'], 'misses': r['misses'],
            'calls': r['calls'],
            'repeats': [{k: v for k, v in rp.items()} for rp in r['repeats']],
        } for r in rs],
    }
def dumps(o, ind=0, key=None):
    # Indented down to each run; list items of calls/misses/turns/repeats stay on one line each,
    # so the file diffs and greps by call.
    pad, pad1 = ' ' * ind, ' ' * (ind + 1)
    one = lambda v: json.dumps(v, ensure_ascii=False, separators=(',', ':'))
    if isinstance(o, dict) and o:
        items = [f'{pad1}{json.dumps(k)}: {dumps(v, ind + 1, k)}' for k, v in o.items()]
        return '{\n' + ',\n'.join(items) + '\n' + pad + '}'
    if isinstance(o, list) and o:
        if key in ('calls', 'misses', 'turns', 'repeats', 'failures'):
            return '[\n' + ',\n'.join(pad1 + one(v) for v in o) + '\n' + pad + ']'
        if all(not isinstance(v, (dict, list)) for v in o):
            return one(o)
        return '[\n' + ',\n'.join(pad1 + dumps(v, ind + 1) for v in o) + '\n' + pad + ']'
    return one(o)


with open(out_json, 'w') as f:
    f.write(dumps(out) + '\n')
json.loads(open(out_json).read())  # the custom layout must stay valid JSON
print('total', total, 'covered', len(covered))
for k, (v, n) in rows_fed.items():
    print(k, len(v), n)
