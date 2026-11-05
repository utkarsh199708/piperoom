"""Extract per-run tool calls, turns and misses from run 39994729845.

Inputs are untrusted data: they are parsed as JSON and only counted, compared
and copied as strings. Nothing from them is executed.
usage: python3 -I extract.py cand.json transcripts_dir out.json
"""
import json, os, sys, collections

NO_REC = 'no data available for this request'

cand = json.load(open(sys.argv[1]))
tdir = sys.argv[2]
out_path = sys.argv[3]


def canon(o):
    return json.dumps(o, sort_keys=True, separators=(',', ':'))


def strip_why(inp):
    return {k: v for k, v in (inp or {}).items() if k != 'why'}


def tfile(scenario, run):
    return os.path.join(tdir, f"{scenario.replace('/', '_')}-{run}.json")


runs = []
problems = []
for res in cand['results']:
    sc, rn = res['scenario'], res['run']
    tx = json.load(open(tfile(sc, rn)))
    if tx['result']['scenario'] != sc or tx['result']['run'] != rn:
        problems.append(f'{sc}#{rn}: transcript header mismatch')
    events = tx['transcript']['events']
    # Input turns: turn 0 starts at room.created; a turn ends at agent.status
    # idle; the next turn starts with the human event that follows the idle.
    turns = [{'trigger': 'alert', 'calls': []}]
    idle = False
    pending = {}
    calls = []
    for e in events:
        t, p = e['type'], e.get('payload') or {}
        actor = (e.get('actor') or {}).get('kind')
        if t == 'agent.status' and p.get('state') == 'idle':
            idle = True
            continue
        if idle and actor == 'human' and t in ('steer.sent', 'comment.added', 'approval.given'):
            trig = {'steer.sent': 'steer', 'comment.added': 'comment', 'approval.given': 'approval'}[t]
            turns.append({'trigger': trig, 'calls': []})
            idle = False
            continue
        if t == 'agent.tool_call':
            c = {'tool': p['tool'], 'input': strip_why(p.get('input')), 'turn': len(turns) - 1}
            pending[p['tool_use_id']] = c
            calls.append(c)
            turns[-1]['calls'].append(c)
        elif t == 'agent.tool_result':
            c = pending.get(p['tool_use_id'])
            if c is None:
                problems.append(f'{sc}#{rn}: result without call')
                continue
            c['is_error'] = bool(p.get('is_error'))
            summ = p.get('summary') or ''
            c['miss'] = c['is_error'] and NO_REC in summ
            c['summary'] = summ
    # Cross-checks against the candidate report.
    tx_misses = sorted(canon({'tool': c['tool'], 'input': c['input']}) for c in calls if c.get('miss'))
    rep_misses = sorted(canon({'tool': m['tool'], 'input': m['input']}) for m in (res.get('misses') or []))
    if tx_misses != rep_misses:
        problems.append(f'{sc}#{rn}: misses differ tx={len(tx_misses)} report={len(rep_misses)}')
    if len(calls) != res['all_tool_calls']:
        problems.append(f'{sc}#{rn}: transcript tool_call events {len(calls)} != all_tool_calls {res["all_tool_calls"]}')
    # Repeats: same tool and same input (without "why") more than once.
    cnt = collections.Counter(canon({'tool': c['tool'], 'input': c['input']}) for c in calls)
    repeats = []
    for k, n in cnt.items():
        if n > 1:
            kk = json.loads(k)
            same = [c for c in calls if c['tool'] == kk['tool'] and canon(c['input']) == canon(kk['input'])]
            repeats.append({
                'tool': kk['tool'], 'input': kk['input'], 'times': n,
                'turns': [c['turn'] for c in same],
                'misses': sum(1 for c in same if c.get('miss')),
                'bad_input': all('bad tool input' in (c.get('summary') or '') for c in same),
                'identical_answers': len({c.get('summary') for c in same}) == 1,
            })
    runs.append({
        'scenario': sc, 'run': rn, 'pass': res['pass'],
        'failures': res.get('failures') or [],
        'final_hypothesis': res.get('final_hypothesis', ''),
        'tool_calls': res['tool_calls'], 'all_tool_calls': res['all_tool_calls'],
        'tool_errors': res['tool_errors'],
        'proposals': res['proposals'], 'rejected_attempts': res['rejected_attempts'],
        'proposed_actions': res.get('proposed_actions') or [],
        'forbidden_proposed': res.get('forbidden_proposed') or [],
        'misses': [{'tool': m['tool'], 'input': m['input']} for m in (res.get('misses') or [])],
        'turns': [{'trigger': t['trigger'], 'calls': len(t['calls'])} for t in turns],
        'calls': [{'turn': c['turn'], 'tool': c['tool'], 'input': c['input'],
                   'error': c.get('is_error', False), 'miss': c.get('miss', False)} for c in calls],
        'repeats': repeats,
        '_summaries': [c.get('summary', '') for c in calls],
    })

print('problems:', len(problems))
for p in problems:
    print(' ', p)
json.dump(runs, open(out_path, 'w'))
print('runs', len(runs), 'misses', sum(len(r['misses']) for r in runs),
      'calls', sum(len(r['calls']) for r in runs))
