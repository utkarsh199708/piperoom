#!/usr/bin/env python3
"""Group messages.jsonl into threads (stream + topic) and pre-sort them for scoring.

Writes threads.jsonl: per thread the opening messages (the problem), the closing
messages (the answer, if any), counts, dates, whether the topic was marked resolved
(Zulip prefixes a resolved topic with a check mark), and a keyword pre-class.
"""
import collections, json, os, re, sys

HERE = os.path.dirname(os.path.abspath(__file__))
SOURCES = {
    "postgres": r"\bpostgres|\bpg_|pgoutput|replication slot|\bwal\b|publication|decoderbufs",
    "mysql": r"\bmysql|binlog|mariadb|\bgtid",
    "oracle": r"\boracle|logminer|\bxstream|\bolr\b|\bscn\b",
    "sqlserver": r"sql ?server|\bmssql|\bcdc\.\w+_ct\b",
    "mongodb": r"\bmongo",
    "db2": r"\bdb2\b",
    "cassandra": r"cassandra",
    "spanner": r"spanner",
}
INCIDENT = r"exception|error|fail|stuck|lag|not (?:working|receiving|streaming)|missing|lost|duplicate|crash|hang|slow|grow|full|oom|out of memory|timeout|stopped|restart"
RUNTIME = r"debezium server|embedded engine|debezium-server|\bembedded\b"

def main():
    path = os.path.join(HERE, "messages.jsonl")
    by = collections.defaultdict(list)
    with open(path) as f:
        for line in f:
            m = json.loads(line)
            # Resolving a topic in Zulip renames it with a leading check mark; one
            # thread either way, with the mark kept as a flag.
            key = m["topic"].lstrip("\u2714").strip()
            by[(m["stream"], key)].append(m)
    out = os.path.join(HERE, "threads.jsonl")
    n = 0
    with open(out, "w") as f:
        for (stream, topic), ms in by.items():
            ms.sort(key=lambda m: m["id"])
            text = " ".join(m["content"] for m in ms).lower()
            head = (topic + " " + ms[0]["content"]).lower()
            src = [k for k, rx in SOURCES.items() if re.search(rx, head)] or \
                  [k for k, rx in SOURCES.items() if re.search(rx, text)]
            f.write(json.dumps({
                "stream": stream, "topic": topic, "messages": len(ms),
                "people": len({m["sender"] for m in ms}),
                "first_ts": ms[0]["ts"], "last_ts": ms[-1]["ts"],
                "marked_resolved": any(m["topic"].startswith("\u2714") for m in ms),
                "sources": src,
                "incident_like": bool(re.search(INCIDENT, head)),
                "runtime_not_connect": bool(re.search(RUNTIME, text)),
                "opening": [m["content"][:4000] for m in ms[:3]],
                "closing": [m["content"][:2000] for m in ms[-3:]] if len(ms) > 3 else [],
            }) + "\n")
            n += 1
    print(f"{n} threads -> {out}", file=sys.stderr)

if __name__ == "__main__":
    main()
