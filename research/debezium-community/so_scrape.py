#!/usr/bin/env python3
"""Pull every Stack Overflow question tagged `debezium`, with its answers. No key, no account.

Uses the public Stack Exchange API (about 300 requests a day per IP without a key; this needs
roughly 2 per 100 questions). Writes threads.jsonl in the same shape as threads.py, so the
rubric and tally.py work unchanged.
Usage: python so_scrape.py [--tag debezium] [--out DIR]
"""
import argparse, gzip, html, json, os, re, sys, time, urllib.parse, urllib.request, urllib.error

API = "https://api.stackexchange.com/2.3"
SOURCES = {
    "postgres": r"\bpostgres|\bpg_|pgoutput|replication slot|\bwal\b|publication|decoderbufs",
    "mysql": r"\bmysql|binlog|mariadb|\bgtid", "oracle": r"\boracle|logminer|\bxstream|\bscn\b",
    "sqlserver": r"sql ?server|\bmssql", "mongodb": r"\bmongo", "db2": r"\bdb2\b",
    "cassandra": r"cassandra", "spanner": r"spanner",
}
INCIDENT = r"exception|error|fail|stuck|lag|not (?:working|receiving|streaming)|missing|lost|duplicate|crash|hang|slow|grow|full|oom|out of memory|timeout|stopped|restart"
RUNTIME = r"debezium server|embedded engine|debezium-server|\bembedded\b"

def get(path, params, api, base_pause):
    url = f"{api}/{path}?" + urllib.parse.urlencode(params)
    for attempt in range(6):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "piperoom-research/0.1", "Accept-Encoding": "gzip"})
            with urllib.request.urlopen(req, timeout=60) as r:
                raw = r.read()
                if r.headers.get("Content-Encoding") == "gzip" or raw[:2] == b"\x1f\x8b":
                    raw = gzip.decompress(raw)
                d = json.loads(raw)
        except urllib.error.HTTPError as e:
            body = e.read()
            if body[:2] == b"\x1f\x8b": body = gzip.decompress(body)
            msg = body[:300].decode("utf-8", "replace")
            if e.code in (429, 502, 503):
                time.sleep(10 * (attempt + 1)); continue
            raise SystemExit(f"{path}: HTTP {e.code}: {msg}")
        except urllib.error.URLError as e:
            if attempt == 5: raise SystemExit(f"{path}: {e}")
            time.sleep(2 ** attempt); continue
        if d.get("backoff"):  # the API asks us to wait this many seconds before the next call
            time.sleep(int(d["backoff"]))
        time.sleep(base_pause)
        if d.get("quota_remaining", 1) <= 0:
            raise SystemExit("daily quota used up; run again tomorrow, it resumes from scratch")
        return d
    raise SystemExit(f"{path}: gave up after retries")

def text(s):
    s = re.sub(r"<pre><code>(.*?)</code></pre>", lambda m: "\n```\n" + m.group(1) + "\n```\n", s or "", flags=re.S)
    return html.unescape(re.sub(r"<[^>]+>", "", s)).strip()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tag", default="debezium")
    ap.add_argument("--out", default=os.path.dirname(os.path.abspath(__file__)))
    ap.add_argument("--api", default=API)
    ap.add_argument("--pause", type=float, default=1.0)
    a = ap.parse_args()

    qs, page = [], 1
    while True:
        d = get("questions", {"site": "stackoverflow", "tagged": a.tag, "pagesize": 100, "page": page,
                              "order": "asc", "sort": "creation", "filter": "withbody"}, a.api, a.pause)
        qs += d.get("items", [])
        print(f"questions: {len(qs)}", file=sys.stderr)
        if not d.get("has_more"): break
        page += 1

    answers = {}
    for i in range(0, len(qs), 100):
        ids = ";".join(str(q["question_id"]) for q in qs[i:i+100])
        page = 1
        while True:
            d = get(f"questions/{ids}/answers", {"site": "stackoverflow", "pagesize": 100, "page": page,
                                                 "filter": "withbody", "sort": "votes", "order": "desc"}, a.api, a.pause)
            for ans in d.get("items", []):
                answers.setdefault(ans["question_id"], []).append(ans)
            if not d.get("has_more"): break
            page += 1
    print(f"answers: {sum(len(v) for v in answers.values())}", file=sys.stderr)

    out = os.path.join(a.out, "threads.jsonl")
    with open(out, "w") as f:
        for q in qs:
            ans = sorted(answers.get(q["question_id"], []), key=lambda x: (not x.get("is_accepted"), -x.get("score", 0)))
            body = text(q.get("body"))
            head = (q["title"] + " " + body).lower()
            alltext = (head + " " + " ".join(text(x.get("body")) for x in ans)).lower()
            src = [k for k, rx in SOURCES.items() if re.search(rx, head)] or \
                  [k for k, rx in SOURCES.items() if re.search(rx, alltext)]
            f.write(json.dumps({
                "stream": "stackoverflow", "topic": html.unescape(q["title"]),
                "messages": 1 + len(ans), "people": None,
                "first_ts": q["creation_date"], "last_ts": q.get("last_activity_date"),
                "marked_resolved": bool(q.get("accepted_answer_id")),
                "sources": src, "incident_like": bool(re.search(INCIDENT, head)),
                "runtime_not_connect": bool(re.search(RUNTIME, alltext)),
                "opening": [body[:4000]], "closing": [text(x.get("body"))[:2000] for x in ans[:3]],
                "tags": q.get("tags", []), "score": q.get("score"), "link": q.get("link"),
            }) + "\n")
    print(f"{len(qs)} threads -> {out}", file=sys.stderr)

if __name__ == "__main__":
    main()
