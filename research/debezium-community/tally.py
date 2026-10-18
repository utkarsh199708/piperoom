#!/usr/bin/env python3
"""Turn labels.jsonl into the percentages, with 95% Wilson intervals."""
import json, math, os, sys, collections
HERE = os.path.dirname(os.path.abspath(__file__))

def wilson(k, n, z=1.96):
    if n == 0: return (0.0, 0.0)
    p = k / n; d = 1 + z*z/n; c = (p + z*z/(2*n)) / d
    h = z * math.sqrt(p*(1-p)/n + z*z/(4*n*n)) / d
    return (100*(c-h), 100*(c+h))

def row(label, k, n):
    lo, hi = wilson(k, n)
    pct = 100*k/n if n else 0
    print(f"| {label} | {k} / {n} | {pct:.1f}% | {lo:.1f}–{hi:.1f}% |")

L = [json.loads(l) for l in open(sys.argv[1] if len(sys.argv) > 1 else os.path.join(HERE, "labels.jsonl"))]
probs = [x for x in L if x["kind"] in ("incident", "setup")]
inc = [x for x in L if x["kind"] == "incident"]
scope = [x for x in probs if x.get("in_scope")]
scope_inc = [x for x in inc if x.get("in_scope")]
print(f"threads labelled: {len(L)}  kinds: {dict(collections.Counter(x['kind'] for x in L))}")
print("| Measure | Count | Share | 95% interval |\n|---|---|---|---|")
row("problems (incident or setup) among all threads", len(probs), len(L))
row("problems Piperoom covers (Postgres source, Kafka Connect)", len(scope), len(probs))
row("in-scope problems Piperoom would diagnose (yes)", sum(x["diagnose"] == "yes" for x in scope), len(scope))
row("in-scope problems diagnosed yes or partial", sum(x["diagnose"] in ("yes", "partial") for x in scope), len(scope))
row("in-scope problems fixed by an action on main today", sum(x["fix"] == "today" for x in scope), len(scope))
row("in-scope problems fixed after P3", sum(x["fix"] in ("today", "after_p3") for x in scope), len(scope))
row("ALL problems fixed by Piperoom today", sum(x.get("in_scope") and x["fix"] == "today" for x in probs), len(probs))
row("ALL problems fixed by Piperoom after P3", sum(x.get("in_scope") and x["fix"] in ("today", "after_p3") for x in probs), len(probs))
row("ALL problems Piperoom would at least diagnose", sum(x.get("in_scope") and x["diagnose"] in ("yes", "partial") for x in probs), len(probs))
row("in-scope problems the community left unresolved", sum(x["community_resolved"] == "no" for x in scope), len(scope))
print("\nout-of-scope reasons:", dict(collections.Counter(
    ("source:" + x["source"]) if x["source"] not in ("postgres", "unknown") else ("runtime:" + x["runtime"])
    for x in probs if not x.get("in_scope"))))
print("in-scope failure classes:", dict(collections.Counter(x["failure_class"] for x in scope).most_common()))
print("actions that would fix:", dict(collections.Counter(x["action"] for x in scope if x["fix"] in ("today", "after_p3")).most_common()))
